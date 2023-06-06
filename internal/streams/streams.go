package streams

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ostafen/eventstorm/internal/backend"
	"github.com/ostafen/eventstorm/internal/model"
)

var (
	ErrStreamNotExist        = errors.New("stream not exist")
	ErrInvalidStreamRevision = errors.New("invalid stream revision")
	ErrNoContentTypeMetadata = errors.New("missing content-type metdata")
	ErrNoEventTypeMetadata   = errors.New("missing event-type metdata")
)

type StreamService interface {
	Subscribe(ctx context.Context, opts model.ReadOptions) (*Subscription, error)
	Read(ctx context.Context, onEvent func(*model.Event) error, opts model.ReadOptions) error
	Append(ctx context.Context, name string, stream model.EventStream, opts model.AppendOptions) (model.AppendResult, error)
}

type Subscription struct {
	Id       string
	Stream   string
	EventCh  chan *model.SubscriptionEvent
	SignalCh chan struct{}
}

func (s *Subscription) ForStream() bool {
	return s.Stream != ""
}

type StreamInfo struct {
	Revision int64
	Exists   bool
}

type subscriptions struct {
	allSubscriptions    map[string]*Subscription
	streamSubscriptions map[string]*Subscription
}

type streamService struct {
	mtx           sync.RWMutex
	streams       map[string]int64
	subscriptions subscriptions

	db *sql.DB
	b  *backend.Backend
}

func (s *streamService) getRevision(name string) (int64, error) {
	s.mtx.RLock()

	revision, has := s.streams[name]
	if has {
		s.mtx.RUnlock()
		return revision, nil
	}
	s.mtx.RUnlock()

	dbRevision, err := s.b.StreamRevision(name)
	if errors.Is(err, backend.ErrNoRows) {
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
	revision, err := s.getRevision(name)
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
	return tx, s.b.WithTx(tx), nil
}

func (s *streamService) append(ctx context.Context, backend *backend.Backend, name string, stream model.EventStream, opts model.AppendOptions) (model.AppendResult, error) {
	currentRevision, err := s.checkRevision(name, opts)
	if err != nil {
		return model.AppendResult{}, err
	}

	var startPos uint64
	for {
		e, err := stream.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			return model.AppendResult{}, err
		}

		if errors.Is(err, io.EOF) {
			return model.AppendResult{
				Revision:        uint64(currentRevision),
				PreparePosition: startPos,
				CommitPosition:  startPos,
			}, nil
		}

		if err := validateEvent(e); err != nil {
			return model.AppendResult{}, err
		}

		e.StreamRevision = uint64(currentRevision) + 1
		e.Metadata[systemMetadataCreated] = strconv.FormatInt(time.Now().UTC().UnixNano()/100, 10)

		pos, err := backend.Append(ctx, name, e)
		if err != nil {
			return model.AppendResult{}, err
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

	res, err := s.append(ctx, backend, name, stream, opts)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return res, err
		}
		s.updateStreamRevision(name, int64(res.Revision))
		s.notifySubscriptions(name, int64(res.Revision))
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
	if opts.StreamOptions != nil {
		revision, err := s.getRevision(opts.StreamOptions.Identifier)
		if err != nil {
			return err
		}
		if revision < 0 {
			return ErrStreamNotExist
		}
	}
	return s.b.ReadStream(ctx, opts, onEvent)
}

func (s *streamService) Subscribe(ctx context.Context, opts model.ReadOptions) (*Subscription, error) {
	isAll := opts.AllOptions != nil

	signalCh := make(chan struct{}, 1)

	subscription := &Subscription{
		Id:       uuid.NewString(),
		SignalCh: signalCh,
		EventCh:  make(chan *model.SubscriptionEvent, 100),
	}

	if !isAll {
		subscription.Stream = opts.StreamOptions.Identifier
	}

	s.addSubscription(subscription)

	go func() {
		defer s.removeSubscription(opts.StreamOptions.Identifier)

		lastPositionOrRevision := s.readAndSendToSubscription(ctx, subscription, opts, -1)
		for {
			select {
			case <-ctx.Done():
				return
			case <-signalCh:
				lastPositionOrRevision = s.readAndSendToSubscription(ctx, subscription, opts, lastPositionOrRevision)
			}
		}
	}()
	return subscription, nil
}

func (s *streamService) getSubscriptionReadOpts(opts model.ReadOptions, lastPositionOrRevision int64) model.ReadOptions {
	o := model.ReadOptions{
		Direction: model.DirectionForwards,
		Count:     -1,
	}

	if opts.AllOptions != nil {
		o.AllOptions = &model.AllOptions{
			Filter:          o.AllOptions.Filter,
			Kind:            model.ReadAllKindPosition,
			PreparePosition: uint64(lastPositionOrRevision + 1),
			CommitPosition:  uint64(lastPositionOrRevision + 1),
		}
	} else {
		o.StreamOptions = &model.StreamOptions{
			Identifier:   opts.StreamOptions.Identifier,
			RevisionKind: model.RevisionReadKindRevision,
			Revision:     uint64(lastPositionOrRevision + 1),
		}
	}
	return o
}

func (s *streamService) readAndSendToSubscription(ctx context.Context, subscription *Subscription, opts model.ReadOptions, lastPositionOrRevision int64) int64 {
	isAll := opts.AllOptions != nil
	readOpts := s.getSubscriptionReadOpts(opts, lastPositionOrRevision)

	if err := s.Read(ctx, func(e *model.Event) error {
		subscription.EventCh <- &model.SubscriptionEvent{
			Event: e,
			Err:   nil,
		}
		if isAll {
			lastPositionOrRevision = int64(e.GlobalPosition)
		} else {
			lastPositionOrRevision = int64(e.StreamRevision)
		}
		return nil
	}, readOpts); err != nil {
		subscription.EventCh <- &model.SubscriptionEvent{
			Err: err,
		}
	}
	return lastPositionOrRevision
}
func (s *streamService) addSubscription(sub *Subscription) {
	s.mtx.Lock()

	if sub.ForStream() {
		s.subscriptions.streamSubscriptions[sub.Id] = sub
	} else {
		s.subscriptions.allSubscriptions[sub.Id] = sub
	}

	s.mtx.Unlock()
}

func (s *streamService) removeSubscription(id string) {
	s.mtx.Lock()
	delete(s.subscriptions.streamSubscriptions, id)
	delete(s.subscriptions.allSubscriptions, id)
	s.mtx.Unlock()
}

func (s *streamService) notifySubscriptions(stream string, revision int64) {
	notify := func(s *Subscription) {
		select {
		case s.SignalCh <- struct{}{}:

		default:
		}
	}

	s.mtx.RLock()
	for _, subscription := range s.subscriptions.streamSubscriptions {
		if subscription.Stream == stream {
			notify(subscription)
		}
	}

	for _, subscription := range s.subscriptions.allSubscriptions {
		notify(subscription)
	}
	s.mtx.RUnlock()
}

func NewSteamsService(db *sql.DB) StreamService {
	s := &streamService{
		streams: make(map[string]int64),
		subscriptions: subscriptions{
			allSubscriptions:    map[string]*Subscription{},
			streamSubscriptions: map[string]*Subscription{},
		},
		db: db,
		b:  backend.NewBackend(db),
	}

	if err := s.b.Init(); err != nil {
		log.Fatal(err)
	}
	return s
}
