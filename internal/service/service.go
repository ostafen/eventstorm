package service

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/ostafen/eventstorm/internal/backend"
	"github.com/ostafen/eventstorm/internal/model"
	"github.com/ostafen/eventstorm/internal/projections"
)

var (
	ErrInvalidStreamRevision = errors.New("invalid stream revision")
	ErrNoContentTypeMetadata = errors.New("missing content-type metdata")
	ErrNoEventTypeMetadata   = errors.New("missing event-type metdata")
)

type StreamService interface {
	Subscribe(ctx context.Context, onEvent func(*model.Event) error, opts model.ReadOptions) error
	Read(ctx context.Context, onEvent func(*model.Event) error, opts model.ReadOptions) error
	Append(ctx context.Context, name string, stream model.EventStream, opts model.AppendOptions) (model.AppendResult, error)
}

type Subscription struct {
	Stream   string
	NotifyCh chan *model.Event
}

type StreamInfo struct {
	Revision int64
	Exists   bool
}

type streamService struct {
	mtx     sync.RWMutex
	streams map[string]int64

	db                *sql.DB
	ProjectionRuntime *projections.Runtime
}

func (s *streamService) fetchRevision(name string) (int64, error) {
	s.mtx.RLock()

	revision, has := s.streams[name]
	if has {
		s.mtx.RUnlock()
		return revision, nil
	}
	s.mtx.RUnlock()

	r := backend.NewBackend(s.db)
	dbRevision, err := r.StreamRevision(name)
	if errors.Is(err, backend.ErrStreamNotExist) {
		revision = -1
		err = nil
	} else {
		revision = int64(dbRevision)
	}

	if err != nil {
		return -1, err
	}

	s.mtx.Lock()
	_, has = s.streams[name]
	if !has {
		s.streams[name] = revision
	}
	s.mtx.Unlock()

	return revision, nil
}

func (s *streamService) checkRevision(name string, opts model.AppendOptions) (int64, error) {
	revision, err := s.fetchRevision(name)
	if err != nil {
		return -1, err
	}

	ok := false
	switch opts.Kind {
	case model.RevisionAppendKindNoStream:
		ok = revision < 0
	case model.RevisionAppendKindExist:
		ok = revision >= 0
	case model.RevisionAppendKindAny:
		ok = true
	case model.RevisionAppendKindRevision:
		ok = uint64(revision) == opts.Revision
	}

	if !ok {
		return revision, ErrInvalidStreamRevision
	}
	return revision, nil
}

func (s *streamService) newBackend() (*sql.Tx, *backend.Backend, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	return tx, backend.NewBackend(tx), nil
}

func (s *streamService) append(ctx context.Context, backend *backend.Backend, name string, stream model.EventStream, opts model.AppendOptions) ([]model.Event, model.AppendResult, error) {
	currentRevision, err := s.checkRevision(name, opts)
	if err != nil {
		return nil, model.AppendResult{}, err
	}

	var startPos uint64
	for {
		e, err := stream.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, model.AppendResult{}, err
		}

		if errors.Is(err, io.EOF) {
			return nil, model.AppendResult{
				Revision:        uint64(currentRevision),
				PreparePosition: startPos,
				CommitPosition:  startPos,
			}, nil
		}

		if err := validateEvent(e); err != nil {
			return nil, model.AppendResult{}, err
		}

		e.StreamRevision = uint64(currentRevision) + 1
		e.Metadata[systemMetadataCreated] = strconv.FormatInt(time.Now().UTC().UnixNano()/100, 10)

		pos, err := backend.Append(ctx, name, e)
		if err != nil {
			return nil, model.AppendResult{}, err
		}
		currentRevision++

		if startPos == 0 {
			startPos = pos
		}
	}
}

func (s *streamService) updateStreamRevision(stream string, revision int64) {
	s.mtx.Lock()
	if revision > s.streams[stream] {
		s.streams[stream] = revision
	}
	s.mtx.Unlock()
}

func (s *streamService) Append(ctx context.Context, name string, stream model.EventStream, opts model.AppendOptions) (model.AppendResult, error) {
	tx, backend, err := s.newBackend()
	if err != nil {
		return model.AppendResult{}, err
	}
	defer tx.Rollback()

	_, res, err := s.append(ctx, backend, name, stream, opts)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return res, err
		}
		s.updateStreamRevision(name, int64(res.Revision))
	}
	return res, err
}

func validateEvent(e *model.Event) error {
	meta := e.Metadata

	if _, has := meta[systemMetadataContentType]; !has {
		return ErrNoContentTypeMetadata
	}

	if _, has := meta[systemMetadataEventType]; !has {
		return ErrNoEventTypeMetadata
	}

	return nil
}

const (
	systemMetadataCreated     = "created"
	systemMetadataContentType = "content-type"
	systemMetadataEventType   = "type"
)

func (s *streamService) Read(ctx context.Context, onEvent func(*model.Event) error, opts model.ReadOptions) error {
	r := backend.NewBackend(s.db)
	return r.ReadStream(ctx, opts, onEvent)
}

func (s *streamService) Subscribe(ctx context.Context, onEvent func(*model.Event) error, opts model.ReadOptions) error {
	r := backend.NewBackend(s.db)
	ch, err := r.Subscribe(ctx, opts, false)
	if err != nil {
		return err
	}

	if err := s.Read(ctx, onEvent, opts); err != nil {
		return err
	}

	for {
		e := <-ch
		if e == nil {
			return io.EOF
		}

		if err := onEvent(e); err != nil {
			return err
		}
	}
}

type CreateProjectionOptions struct {
	Name  string
	Query string
}

func (s *streamService) CreateProjection(ctx context.Context, opts CreateProjectionOptions) error {
	r := backend.NewBackend(s.db)

	if err := s.ProjectionRuntime.Register(opts.Query, opts.Name); err != nil {
		return err
	}

	return r.SaveProjection(ctx, opts.Name, opts.Query)
}

func (s *streamService) UpdateProjection(ctx context.Context, opts CreateProjectionOptions) error {
	return nil
}

func NewSteamsService(db *sql.DB) StreamService {
	s := &streamService{
		streams: make(map[string]int64),
		db:      db,
	}

	r := backend.NewBackend(db)
	if err := r.Init(); err != nil {
		log.Fatal(err)
	}
	return s
}
