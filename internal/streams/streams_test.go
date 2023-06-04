package streams_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/ostafen/eventstorm/internal/backend"
	"github.com/ostafen/eventstorm/internal/model"
	"github.com/ostafen/eventstorm/internal/streams"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ServiceSuite struct {
	suite.Suite

	db        *sql.DB
	container testcontainers.Container
	svc       streams.StreamService
}

func TestServiceSuite(t *testing.T) {
	suite.Run(t, &ServiceSuite{})
}

const (
	Database = "eventstorm_test"
	User     = "test"
	Password = "test"
	Port     = "5432"
)

func (s *ServiceSuite) openDB(host, port string) *sql.DB {
	db, err := sql.Open("postgres", connectionString(host, User, port, Password, Database))
	s.NoError(err)
	return db
}

func connectionString(address, user, port, password, db string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", address, port, user, password, db)
}

func (s *ServiceSuite) setupContainer() (string, string) {
	port := fmt.Sprintf("%s/tcp", Port)

	req := testcontainers.ContainerRequest{
		Image: "postgres:latest",
		Env: map[string]string{
			"POSTGRES_DB":       Database,
			"POSTGRES_USER":     User,
			"POSTGRES_PASSWORD": Password,
		},
		ExposedPorts: []string{port},
		WaitingFor: wait.ForSQL(nat.Port(port), "postgres", func(host string, port nat.Port) string {
			return connectionString(host, User, port.Port(), Password, Database)
		}),
	}

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.NoError(err)

	s.container = container

	endpoint, err := s.container.Endpoint(ctx, "")
	s.NoError(err)

	parts := strings.Split(endpoint, ":")
	return parts[0], nat.Port(parts[1]).Port()
}

func (s *ServiceSuite) SetupTest() {
	_, err := s.db.Exec("DROP TABLE IF EXISTS events")
	s.NoError(err)

	s.svc = streams.NewSteamsService(s.db)
}

func (s *ServiceSuite) SetupSuite() {
	host, port := s.setupContainer()

	s.db = s.openDB(host, port)
}

func (s *ServiceSuite) TearDownSuite() {
	err := s.container.Terminate(context.Background())
	s.NoError(err)
}

func buildMetadata(metadata ...string) model.Metadata {
	meta := model.Metadata{}
	for i := 0; i < len(metadata); i += 2 {
		meta[metadata[i]] = metadata[i+1]
	}
	return meta
}

func genEvents(n int) []*model.Event {
	events := make([]*model.Event, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, &model.Event{
			UUID: uuid.NewString(),
			Metadata: buildMetadata(
				"type", "event-type",
				"content-type", "content-type",
			),
			CustomMetadata: nil,
			Data:           nil,
		})
	}
	return events
}

func (s *ServiceSuite) TestAppendInvalidEvent() {
	_, err := s.svc.Append(context.TODO(), "test-stream", &bufferEventStream{Events: []*model.Event{{}}}, model.AppendOptions{Kind: model.RevisionAppendKindAny})
	s.Error(err, streams.ErrNoContentTypeMetadata)

	_, err = s.svc.Append(context.TODO(), "test-stream",
		&bufferEventStream{Events: []*model.Event{{Metadata: model.Metadata{"content-type": "type"}}}},
		model.AppendOptions{Kind: model.RevisionAppendKindAny})

	s.Error(err, streams.ErrNoEventTypeMetadata)
}

func (s *ServiceSuite) TestAppendToNonExistentStream() {
	ctx := context.TODO()

	events := genEvents(10)

	res, err := s.svc.Append(ctx, "test-stream", &bufferEventStream{Events: events}, model.AppendOptions{Kind: model.RevisionAppendKindExist})
	s.Equal(res, model.AppendResult{})
	s.Error(err, streams.ErrInvalidStreamRevision)

	res, err = s.svc.Append(ctx, "test-stream1", &bufferEventStream{Events: events}, model.AppendOptions{Kind: model.RevisionAppendKindAny})
	s.Equal(res, model.AppendResult{Revision: 9, PreparePosition: 1, CommitPosition: 1})
	s.NoError(err)

	res, err = s.svc.Append(ctx, "test-stream2", &bufferEventStream{Events: events}, model.AppendOptions{Kind: model.RevisionAppendKindNoStream})
	s.Equal(res, model.AppendResult{Revision: 9, PreparePosition: 11, CommitPosition: 11})
	s.NoError(err)
}

func (s *ServiceSuite) TestAppendToExistentStream() {
	ctx := context.TODO()

	res := s.createStreamWithEvents("test-stream", &bufferEventStream{Events: genEvents(10)})
	s.Equal(res, model.AppendResult{Revision: 9, PreparePosition: 1, CommitPosition: 1})

	res, err := s.svc.Append(ctx, "test-stream", &bufferEventStream{Events: genEvents(1)}, model.AppendOptions{Kind: model.RevisionAppendKindExist})
	s.Equal(res, model.AppendResult{Revision: 10, PreparePosition: 11, CommitPosition: 11})
	s.NoError(err)

	res, err = s.svc.Append(ctx, "test-stream", &bufferEventStream{Events: genEvents(5)}, model.AppendOptions{Kind: model.RevisionAppendKindRevision, Revision: 10})
	s.Equal(res, model.AppendResult{Revision: 15, PreparePosition: 12, CommitPosition: 12})
	s.NoError(err)

	// append with invalid revision
	res, err = s.svc.Append(ctx, "test-stream", &bufferEventStream{Events: genEvents(5)}, model.AppendOptions{Kind: model.RevisionAppendKindRevision, Revision: 20})
	s.Zero(res)
	s.Error(err, streams.ErrInvalidStreamRevision)
}

func (s *ServiceSuite) createStreamWithEvents(name string, stream model.EventStream) model.AppendResult {
	res, err := s.svc.Append(context.TODO(), name, stream, model.AppendOptions{Kind: model.RevisionAppendKindNoStream})
	s.NoError(err)
	return res
}

func readStreamOpts(stream string, direction model.Direction, revision, count uint64) model.ReadOptions {
	return model.ReadOptions{
		Direction: direction,
		StreamOptions: &model.StreamOptions{
			Identifier:   stream,
			RevisionKind: model.RevisionReadKindRevision,
			Revision:     revision,
		},
		Count: int64(count),
	}
}

func readStreamOptsStart(stream string, direction model.Direction, count uint64) model.ReadOptions {
	opts := readStreamOpts(stream, direction, 0, count)
	opts.StreamOptions.RevisionKind = model.RevisionReadKindStart
	return opts
}

func readStreamOptsEnd(stream string, direction model.Direction, count uint64) model.ReadOptions {
	opts := readStreamOpts(stream, direction, 0, count)
	opts.StreamOptions.RevisionKind = model.RevisionReadKindEnd
	return opts
}

func readAllOpts(direction model.Direction, position, count uint64) model.ReadOptions {
	return model.ReadOptions{
		Direction: direction,
		AllOptions: &model.AllOptions{
			Kind:            model.ReadAllKindPosition,
			PreparePosition: position,
			CommitPosition:  position,
		},
		Count: int64(count),
	}
}

func readAllOptsStart(direction model.Direction, count uint64) model.ReadOptions {
	opts := readAllOpts(direction, 0, count)
	opts.AllOptions.Kind = model.ReadAllKindStart
	return opts
}

func readAllOptsEnd(direction model.Direction, count uint64) model.ReadOptions {
	opts := readAllOpts(direction, 0, count)
	opts.AllOptions.Kind = model.ReadAllKindEnd
	return opts
}

func (s *ServiceSuite) TestReadNonExistentStream() {
	opts := readStreamOptsStart("test-stream", model.DirectionForwards, 10)
	events, err := s.readEvents(opts)
	s.Error(err, backend.ErrStreamNotExist)
	s.Len(events, 0)
}

func (s *ServiceSuite) TestReadStreamForwards() {
	res := s.createStreamWithEvents("test-stream", &bufferEventStream{Events: genEvents(100)})
	s.Equal(res, model.AppendResult{Revision: 99, PreparePosition: 1, CommitPosition: 1})

	opts := readStreamOptsStart("test-stream", model.DirectionForwards, 10)
	events, err := s.readEvents(opts)
	s.NoError(err)
	s.Len(events, 10)

	for i, e := range events {
		s.Equal(e.StreamRevision, uint64(i))
		s.Equal(e.GlobalPosition, uint64(i+1))
		s.Equal(e.StreamIdentifier, "test-stream")
	}

	events, err = s.readEvents(readStreamOpts("test-stream", model.DirectionForwards, 50, 10))
	s.NoError(err)
	s.Len(events, 10)

	for i, e := range events {
		s.Equal(e.StreamRevision, uint64(50+i))
		s.Equal(e.GlobalPosition, uint64(51+i))
		s.Equal(e.StreamIdentifier, "test-stream")
	}

	events, err = s.readEvents(readStreamOptsEnd("test-stream", model.DirectionForwards, 10))
	s.NoError(err)
	s.Len(events, 1)

	e := events[0]
	s.Equal(e.StreamRevision, uint64(99))
	s.Equal(e.GlobalPosition, uint64(100))
	s.Equal(e.StreamIdentifier, "test-stream")
}

func (s *ServiceSuite) TestReadStreamBackwards() {
	res := s.createStreamWithEvents("test-stream", &bufferEventStream{Events: genEvents(100)})
	s.Equal(res, model.AppendResult{Revision: 99, PreparePosition: 1, CommitPosition: 1})

	opts := readStreamOptsStart("test-stream", model.DirectionBackwards, 10)
	events, err := s.readEvents(opts)
	s.NoError(err)
	s.Len(events, 1)

	e := events[0]
	s.Equal(e.StreamRevision, uint64(0))
	s.Equal(e.GlobalPosition, uint64(1))
	s.Equal(e.StreamIdentifier, "test-stream")

	events, err = s.readEvents(readStreamOpts("test-stream", model.DirectionBackwards, 50, 10))
	s.NoError(err)
	s.Len(events, 10)

	for i, e := range events {
		s.Equal(e.StreamRevision, uint64(50-i))
		s.Equal(e.GlobalPosition, uint64(51-i))
		s.Equal(e.StreamIdentifier, "test-stream")
	}

	events, err = s.readEvents(readStreamOptsEnd("test-stream", model.DirectionBackwards, 10))
	s.NoError(err)
	s.Len(events, 10)

	for i, e := range events {
		s.Equal(e.StreamRevision, uint64(99-i))
		s.Equal(e.GlobalPosition, uint64(100-i))
		s.Equal(e.StreamIdentifier, "test-stream")
	}
}

func (s *ServiceSuite) TestReadAllFromStart() {
	for i := 0; i < 100; i++ {
		res := s.createStreamWithEvents(fmt.Sprintf("test-stream-%d", i), &bufferEventStream{Events: genEvents(1)})
		s.Equal(res, model.AppendResult{Revision: 0, PreparePosition: uint64(i + 1), CommitPosition: uint64(i + 1)})
	}

	events, err := s.readEvents(readAllOptsStart(model.DirectionForwards, 100))
	s.NoError(err)
	s.Len(events, 100)

	for i, e := range events {
		s.Equal(e.StreamIdentifier, fmt.Sprintf("test-stream-%d", i))
		s.Equal(e.StreamRevision, uint64(0))
		s.Equal(e.GlobalPosition, uint64(i+1))
	}
}

func (s *ServiceSuite) TestReadAllFromEnd() {
	for i := 0; i < 100; i++ {
		res := s.createStreamWithEvents(fmt.Sprintf("test-stream-%d", i), &bufferEventStream{Events: genEvents(1)})
		s.Equal(res, model.AppendResult{Revision: 0, PreparePosition: uint64(i + 1), CommitPosition: uint64(i + 1)})
	}

	events, err := s.readEvents(readAllOptsEnd(model.DirectionBackwards, 100))
	s.NoError(err)
	s.Len(events, 100)

	for i, e := range events {
		s.Equal(e.StreamIdentifier, fmt.Sprintf("test-stream-%d", 100-i-1))
		s.Equal(e.StreamRevision, uint64(0))
		s.Equal(e.GlobalPosition, uint64(100-i))
	}
}

func (s *ServiceSuite) TestReadAllFromPosition() {
	for i := 0; i < 100; i++ {
		res := s.createStreamWithEvents(fmt.Sprintf("test-stream-%d", i), &bufferEventStream{Events: genEvents(1)})
		s.Equal(res, model.AppendResult{Revision: 0, PreparePosition: uint64(i + 1), CommitPosition: uint64(i + 1)})
	}

	events, err := s.readEvents(readAllOpts(model.DirectionBackwards, 50, 100))
	s.NoError(err)
	s.Len(events, 50)

	for i, e := range events {
		s.Equal(e.StreamIdentifier, fmt.Sprintf("test-stream-%d", 50-i-1))
		s.Equal(e.StreamRevision, uint64(0))
		s.Equal(e.GlobalPosition, uint64(50-i))
	}

	events, err = s.readEvents(readAllOpts(model.DirectionForwards, 51, 100))
	s.NoError(err)
	s.Len(events, 50)

	for i, e := range events {
		s.Equal(e.StreamIdentifier, fmt.Sprintf("test-stream-%d", 50+i))
		s.Equal(e.StreamRevision, uint64(0))
		s.Equal(e.GlobalPosition, uint64(51+i))
	}
}

func (s *ServiceSuite) TestFilterEventTypeByPrefix() {
	events := genEvents(100)
	for i, e := range events {
		e.Metadata.SetEventType(fmt.Sprintf("type-%d", i))
	}
	_, err := s.svc.Append(context.Background(), "test-stream", &bufferEventStream{Events: events}, model.AppendOptions{Kind: model.RevisionAppendKindAny})
	s.NoError(err)

	opts := readAllOptsStart(model.DirectionForwards, 100)
	opts.AllOptions.Filter = model.FilterOptions{
		Kind: model.FilterKindEventType,
		Expr: model.FilterExpression{
			Prefix: []string{
				"type-0", "type-1", "type-5",
			},
		},
	}

	readEvents := make([]*model.Event, 0)
	err = s.svc.Read(context.Background(), func(e *model.Event) error {
		readEvents = append(readEvents, e)
		return nil
	}, opts)
	s.NoError(err)

	s.Len(readEvents, 23)

	for _, e := range readEvents {
		et := e.Metadata.EventType()
		s.True(strings.HasPrefix(et, "type-0") ||
			strings.HasPrefix(et, "type-1") ||
			strings.HasPrefix(et, "type-5"))
	}
}

func (s *ServiceSuite) TestFilterEventTypeByRegex() {
	events := genEvents(100)
	for i, e := range events {
		e.Metadata.SetEventType(fmt.Sprintf("type-%d", i))
	}
	_, err := s.svc.Append(context.Background(), "test-stream", &bufferEventStream{Events: events}, model.AppendOptions{Kind: model.RevisionAppendKindAny})
	s.NoError(err)

	opts := readAllOptsStart(model.DirectionForwards, 100)
	opts.AllOptions.Filter = model.FilterOptions{
		Kind: model.FilterKindEventType,
		Expr: model.FilterExpression{
			Regex: "^(type-0|type-1|type-2)$",
		},
	}

	readEvents := make([]*model.Event, 0)
	err = s.svc.Read(context.Background(), func(e *model.Event) error {
		readEvents = append(readEvents, e)
		return nil
	}, opts)
	s.NoError(err)

	s.Len(readEvents, 3)
}

func (s *ServiceSuite) TestFilterStreamIdentifierByPrefix() {
	for i := 0; i < 100; i++ {
		s.createStreamWithEvents(fmt.Sprintf("stream-%d", i), &bufferEventStream{Events: genEvents(1)})
	}

	opts := readAllOptsStart(model.DirectionForwards, 100)
	opts.AllOptions.Filter = model.FilterOptions{
		Kind: model.FilterKindStreamIdentifier,
		Expr: model.FilterExpression{
			Prefix: []string{
				"stream-0", "stream-1", "stream-2",
			},
		},
	}

	readEvents := make([]*model.Event, 0)
	err := s.svc.Read(context.Background(), func(e *model.Event) error {
		readEvents = append(readEvents, e)
		return nil
	}, opts)
	s.NoError(err)

	s.Len(readEvents, 23)

	for _, e := range readEvents {
		id := e.StreamIdentifier
		s.True(strings.HasPrefix(id, "stream-0") ||
			strings.HasPrefix(id, "stream-1") ||
			strings.HasPrefix(id, "stream-2"))
	}
}

func (s *ServiceSuite) TestFilterStreamIdentifierByRegex() {
	for i := 0; i < 100; i++ {
		s.createStreamWithEvents(fmt.Sprintf("stream-%d", i), &bufferEventStream{Events: genEvents(1)})
	}

	opts := readAllOptsStart(model.DirectionForwards, 100)
	opts.AllOptions.Filter = model.FilterOptions{
		Kind: model.FilterKindStreamIdentifier,
		Expr: model.FilterExpression{
			Regex: "^(stream-0|stream-1|stream-2)$",
		},
	}

	readEvents := make([]*model.Event, 0)
	err := s.svc.Read(context.Background(), func(e *model.Event) error {
		readEvents = append(readEvents, e)
		return nil
	}, opts)
	s.NoError(err)

	s.Len(readEvents, 3)
}

func (s *ServiceSuite) TestStreamSubscription() {
	s.createStreamWithEvents("test-stream", &bufferEventStream{Events: genEvents(10)})

	var wg sync.WaitGroup
	wg.Add(100)

	events := make([]*model.Event, 0)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := s.svc.Subscribe(ctx, func(se *model.Event) error {
			events = append(events, se)
			wg.Done()
			return nil
		}, model.ReadOptions{
			Direction: model.DirectionForwards,
			Count:     -1,
			StreamOptions: &model.StreamOptions{
				Identifier: "test-stream",
			},
		})
		s.NoError(err)
	}()

	_, err := s.svc.Append(context.TODO(), "test-stream", &bufferEventStream{Events: genEvents(90)}, model.AppendOptions{Kind: model.RevisionAppendKindAny})
	s.NoError(err)

	wg.Wait()

	cancel()

	s.Len(events, 100)

	for i, e := range events {
		s.Equal(e.StreamIdentifier, "test-stream")
		s.Equal(e.StreamRevision, uint64(i))
	}
}

func (s *ServiceSuite) readEvents(opts model.ReadOptions) ([]*model.Event, error) {
	readEvents := make([]*model.Event, 0, opts.Count)
	err := s.svc.Read(context.TODO(), func(e *model.Event) error {
		readEvents = append(readEvents, e)
		return nil
	}, opts)
	return readEvents, err
}

type bufferEventStream struct {
	next   int
	Events []*model.Event
}

func (s *bufferEventStream) Next() (*model.Event, error) {
	if s.next >= len(s.Events) {
		return nil, io.EOF
	}
	next := s.next
	s.next++
	return s.Events[next], nil
}
