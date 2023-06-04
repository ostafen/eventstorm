package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/ostafen/eventstorm/internal/model"
)

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

type Backend struct {
	db DBTX
}

func NewBackend(db DBTX) *Backend {
	return &Backend{
		db: db,
	}
}

func (b *Backend) WithTx(tx DBTX) *Backend {
	return &Backend{
		db: tx,
	}
}

func (r *Backend) Init() error {
	_, err := r.db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS events (
			stream TEXT NOT NULL,
			uuid TEXT NOT NULL,
			data BYTEA NULL,
			metadata JSONB NULL,
			custom_metadata BYTEA NULL,
			revision BIGINT,
			position BIGSERIAL UNIQUE,

			PRIMARY KEY (stream, uuid),
			UNIQUE (stream, revision)
		);

		CREATE INDEX IF NOT EXISTS position_index on events (position);
		CREATE INDEX IF NOT EXISTS stream_index on events (stream);

		CREATE TABLE IF NOT EXISTS projections (
			name TEXT PRIMARY KEY,
			query TEXT NOT NULL
		);
	`)
	return convertError(err)
}

func (r *Backend) Append(ctx context.Context, stream string, e *model.Event) (uint64, error) {
	query := `
		INSERT INTO events (stream, uuid, data, metadata, custom_metadata, revision)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6
		) RETURNING position;`

	res := r.db.QueryRowContext(ctx, query, stream, e.UUID, e.Data, e.Metadata, e.CustomMetadata, e.StreamRevision)

	var pos uint64
	err := res.Scan(&pos)
	return pos, convertError(err)
}

func (r *Backend) StreamRevision(stream string) (uint64, error) {
	row := r.db.QueryRowContext(context.TODO(), `
		SELECT revision FROM events 
		WHERE stream = $1 
		ORDER BY revision DESC LIMIT 1
	`, stream)

	var revision uint64
	err := row.Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrStreamNotExist
	}
	return revision, convertError(err)
}

func builReadQuery(opts model.ReadOptions) *Query {
	if opts.StreamOptions != nil {
		return buildStreamQuery(opts)
	}
	return buildAllQuery(opts)
}

type SortDirection string

const (
	SortDirectionAsc  = "ASC"
	SortDirectionDesc = "DESC"
)

func (d SortDirection) Operator() string {
	if d == SortDirectionAsc {
		return ">="
	}
	return "<="
}

func buildStreamQuery(opts model.ReadOptions) *Query {
	var sort SortDirection = SortDirectionAsc
	var limit int = -1
	if opts.Count > 0 {
		limit = int(opts.Count)
	}

	conditions := []string{
		fmt.Sprintf("stream = '%s'", opts.StreamOptions.Identifier),
	}

	switch opts.StreamOptions.RevisionKind {
	case model.RevisionReadKindRevision:
		if opts.Direction == model.DirectionBackwards {
			sort = SortDirectionDesc
		}
		conditions = append(conditions, fmt.Sprintf("revision %s %d", sort.Operator(), opts.StreamOptions.Revision))
	case model.RevisionReadKindStart:
		if opts.Direction == model.DirectionBackwards {
			limit = 1
		}
	case model.RevisionReadKindEnd:
		sort = SortDirectionDesc
		if opts.Direction == model.DirectionForwards {
			limit = 1
		}
	}

	return &Query{
		Sort:         sort,
		OrderByField: "revision",
		Where:        conditions,
		Limit:        limit,
	}
}

func buildAllQuery(opts model.ReadOptions) *Query {
	var sort SortDirection = SortDirectionAsc
	var limit int = -1
	if opts.Count > 0 {
		limit = int(opts.Count)
	}

	conditions := []string{}

	switch opts.AllOptions.Kind {
	case model.ReadAllKindStart:
		if opts.Direction == model.DirectionBackwards {
			limit = 1
		}
	case model.ReadAllKindEnd:
		sort = SortDirectionDesc
		if opts.Direction == model.DirectionForwards {
			limit = 1
		}
	case model.ReadAllKindPosition:
		if opts.Direction == model.DirectionBackwards {
			sort = SortDirectionDesc
		}
		conditions = append(conditions, fmt.Sprintf("position %s %d", sort.Operator(), opts.AllOptions.CommitPosition))
	}

	filter := opts.AllOptions.Filter
	switch filter.Kind {
	case model.FilterKindEventType:
		conditions = append(conditions, buildFilter("metadata->>'type'", filter.Expr))
	case model.FilterKindStreamIdentifier:
		conditions = append(conditions, buildFilter("metadata->>'content-type'", filter.Expr))
	}

	return &Query{
		Where:        conditions,
		Sort:         sort,
		OrderByField: "position",
		Limit:        limit,
	}
}

type Query struct {
	Where        []string
	Limit        int
	OrderByField string
	Sort         SortDirection
}

func (q *Query) Sql() string {
	where := q.Where
	if len(where) == 0 {
		where = append(where, "1=1")
	}

	limit := ""
	if q.Limit >= 0 {
		limit = fmt.Sprintf("LIMIT %d", q.Limit)
	}

	return fmt.Sprintf(`
		SELECT stream, uuid, data, metadata, custom_metadata, revision, position
		FROM events
		WHERE %s
		ORDER BY %s %s
		%s
	`,
		strings.Join(where, " AND "),
		q.OrderByField,
		q.Sort,
		limit,
	)
}

func buildPrefixRegex(prefixes []string) string {
	regex := "^("
	for i, prefix := range prefixes {
		regex += prefix

		if i < len(prefix)-1 {
			regex += "|"
		}
	}
	return regex + ")"
}

func buildFilter(field string, expr model.FilterExpression) string {
	regex := expr.Regex
	if regex == "" {
		regex = buildPrefixRegex(expr.Prefix)
	}
	return fmt.Sprintf("%s ~ '%s'", field, regex)
}

func (r *Backend) ReadStream(ctx context.Context, opts model.ReadOptions, onEvent func(e *model.Event) error) error {
	query := builReadQuery(opts)
	rows, err := r.db.QueryContext(ctx, query.Sql())
	if err != nil {
		return convertError(err)
	}
	defer rows.Close()

	hasNext := false
	for rows.Next() {
		hasNext = true

		e, err := scanEvent(rows)
		if err != nil {
			return convertError(err)
		}

		if err := onEvent(e); err != nil {
			return convertError(err)
		}
	}

	if !hasNext {
		return ErrStreamNotExist
	}
	return nil
}

func scanEvent[T interface{ Scan(...any) error }](rows T) (*model.Event, error) {
	var e model.Event

	err := rows.Scan(
		&e.StreamIdentifier,
		&e.UUID,
		&e.Data,
		&e.Metadata,
		&e.CustomMetadata,
		&e.StreamRevision,
		&e.GlobalPosition,
	)
	return &e, err
}

func (b *Backend) SaveProjection(ctx context.Context, name string, query string) error {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO projections (name, query) 
			VALUES ($1, $2)
			ON CONFLICT (name)
			DO
				UPDATE SET query = $2;
		`,
		name, query)
	return err
}

func (b *Backend) GetProjectionByName(ctx context.Context, name string) (string, error) {
	row := b.db.QueryRowContext(ctx, `SELECT query WHERE name = $1`, name)

	var query string
	err := row.Scan(&query)
	return query, err
}

var (
	ErrConflict       = errors.New("conflict")
	ErrStreamNotExist = errors.New("stream not found")
)

func convertError(err error) error {
	switch e := err.(type) {
	case *pq.Error:
		switch e.Code {
		case "23505":
			return ErrConflict
		}
	}
	return err
}
