package projections

import (
	"context"
	"database/sql"

	"github.com/ostafen/eventstorm/internal/backend"
)

type projectionsService struct {
	runtime *Runtime
	db      *sql.DB
}

type CreateProjectionOptions struct {
	Name  string
	Query string
}

func (s *projectionsService) CreateProjection(ctx context.Context, opts CreateProjectionOptions) error {
	r := backend.NewBackend(s.db)

	if err := s.runtime.Register(opts.Query, opts.Name); err != nil {
		return err
	}

	return r.SaveProjection(ctx, opts.Name, opts.Query)
}

func (s *projectionsService) UpdateProjection(ctx context.Context, opts CreateProjectionOptions) error {
	return nil
}
