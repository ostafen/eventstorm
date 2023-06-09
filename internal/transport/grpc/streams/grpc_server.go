package streams

import (
	context "context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/ostafen/eventstorm/internal/model"
	"github.com/ostafen/eventstorm/internal/streams"
	shared "github.com/ostafen/eventstorm/internal/transport/grpc/shared"
)

type grpcServer struct {
	svc streams.StreamService
	UnimplementedStreamsServer
}

func NewStreamsServer(svc streams.StreamService) *grpcServer {
	return &grpcServer{
		svc: svc,
	}
}

type eventIterator struct {
	server Streams_AppendServer
}

func (it *eventIterator) Next() (*model.Event, error) {
	req, err := it.server.Recv()
	if err != nil {
		return nil, err
	}

	msg := req.GetProposedMessage()
	if msg == nil {
		return nil, fmt.Errorf("no proposed message")
	}

	var parsedId string
	switch id := msg.Id.Value.(type) {
	case *shared.UUID_String_:
		parsedId = id.String_
	case *shared.UUID_Structured_:
		raw := [16]byte{}
		binary.BigEndian.PutUint64(raw[:], uint64(id.Structured.GetMostSignificantBits()))
		binary.BigEndian.PutUint64(raw[8:], uint64(id.Structured.GetMostSignificantBits()))

		parsedId = uuid.FromBytesOrNil(raw[:]).String()
	}

	return &model.Event{
		UUID:           parsedId,
		Data:           msg.Data,
		Metadata:       msg.Metadata,
		CustomMetadata: msg.CustomMetadata,
	}, nil
}

func (s *grpcServer) Read(req *ReadReq, server Streams_ReadServer) error {
	opts, err := req.AsOptions()
	if err != nil {
		return err
	}

	if opts.IsSubscription() {
		return s.handleSubscriptionRequest(server, opts)
	}
	return s.handleReadRequest(server, opts)
}

func (s *grpcServer) handleReadRequest(server Streams_ReadServer, opts model.ReadOptions) error {
	err := s.svc.Read(server.Context(), func(e *model.Event) error {
		return s.sendEvent(server, e)
	}, opts)

	if errors.Is(err, streams.ErrStreamNotExist) {
		return s.sendStreamNotFound(server, opts.StreamOptions.Identifier)
	}
	return err
}

const (
	checkpointMod = 32
)

func (s *grpcServer) handleSubscriptionRequest(server Streams_ReadServer, opts model.ReadOptions) error {
	subscription, err := s.svc.Subscribe(server.Context(), opts)
	if err != nil {
		return err
	}

	if err := s.sendSubscriptionConfirmation(server, subscription.Id); err != nil {
		return err
	}

	nSent := 0
	for {
		select {
		case <-server.Context().Done():
			return nil
		case se := <-subscription.EventCh:
			if se.Err != nil {
				// TODO: send subscription dropped
				return err
			}
			e := se.Event

			if nSent%checkpointMod == 0 {
				if err := s.sendSubscriptionCheckpoint(server, e.GlobalPosition); err != nil {
					return err
				}
			}
			nSent++

			if err := s.sendEvent(server, e); err != nil {
				return err
			}
		}
	}
}

func (s *grpcServer) sendStreamNotFound(server Streams_ReadServer, stream string) error {
	return server.Send(&ReadResp{
		Content: &ReadResp_StreamNotFound_{
			StreamNotFound: &ReadResp_StreamNotFound{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte(stream),
				},
			},
		},
	})
}

func (s *grpcServer) sendSubscriptionConfirmation(server Streams_ReadServer, id string) error {
	return server.Send(&ReadResp{
		Content: &ReadResp_Confirmation{
			Confirmation: &ReadResp_SubscriptionConfirmation{
				SubscriptionId: id,
			},
		},
	})
}

func (s *grpcServer) sendSubscriptionCheckpoint(server Streams_ReadServer, position uint64) error {
	return server.Send(&ReadResp{
		Content: &ReadResp_Checkpoint_{
			Checkpoint: &ReadResp_Checkpoint{
				PreparePosition: position,
				CommitPosition:  position,
			},
		},
	})
}

func (s *grpcServer) sendEvent(server Streams_ReadServer, e *model.Event) error {
	return server.Send(&ReadResp{
		Content: &ReadResp_Event{
			Event: &ReadResp_ReadEvent{
				Event: &ReadResp_ReadEvent_RecordedEvent{
					Id:               &shared.UUID{Value: &shared.UUID_String_{String_: e.UUID}},
					StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(e.StreamIdentifier)},
					StreamRevision:   e.StreamRevision,
					CommitPosition:   e.StreamRevision - 1,
					PreparePosition:  e.StreamRevision - 1,
					Metadata:         e.Metadata,
					CustomMetadata:   e.CustomMetadata,
					Data:             e.Data,
				},
			},
		},
	})
}

func (s *grpcServer) Append(server Streams_AppendServer) error {
	req, err := server.Recv()
	if err != nil {
		return err
	}

	opts := req.GetOptions()
	if opts == nil {
		return errors.New("no options provided")
	}

	appendOptions := model.AppendOptions{
		Kind:     model.RevisionAppendKindAny,
		Revision: 0,
	}

	switch rev := opts.GetExpectedStreamRevision().(type) {
	case *AppendReq_Options_NoStream:
		appendOptions.Kind = model.RevisionAppendKindNoStream
	case *AppendReq_Options_Any:
		appendOptions.Kind = model.RevisionAppendKindAny
	case *AppendReq_Options_StreamExists:
		appendOptions.Kind = model.RevisionAppendKindExist
	case *AppendReq_Options_Revision:
		appendOptions.Kind = model.RevisionAppendKindRevision
		appendOptions.Revision = rev.Revision
	}

	stream := string(opts.GetStreamIdentifier().GetStreamName())

	it := &eventIterator{server: server}
	appendRes, err := s.svc.Append(server.Context(), string(stream), it, appendOptions)
	if errors.Is(err, streams.ErrInvalidStreamRevision) {
		return server.SendAndClose(&AppendResp{
			Result: wrongRevisionAnswer(appendRes, appendOptions),
		})
	}
	if err != nil {
		return err
	}

	return server.SendAndClose(&AppendResp{
		Result: appendSuccessResp(appendRes),
	})
}

func wrongRevisionAnswer(appendRes model.AppendResult, opts model.AppendOptions) isAppendResp_Result {
	res := &AppendResp_WrongExpectedVersion_{
		WrongExpectedVersion: &AppendResp_WrongExpectedVersion{
			CurrentRevisionOption:  &AppendResp_WrongExpectedVersion_CurrentRevision{CurrentRevision: appendRes.Revision},
			ExpectedRevisionOption: expectedRevisionOption(opts),
		},
	}

	if appendRes.Revision == 0 {
		res.WrongExpectedVersion.CurrentRevisionOption = &AppendResp_WrongExpectedVersion_CurrentNoStream{}
	}
	return res
}

func expectedRevisionOption(opts model.AppendOptions) isAppendResp_WrongExpectedVersion_ExpectedRevisionOption {
	switch opts.Kind {
	case model.RevisionAppendKindNoStream:
		return &AppendResp_WrongExpectedVersion_ExpectedNoStream{}
	case model.RevisionAppendKindExist:
		return &AppendResp_WrongExpectedVersion_ExpectedStreamExists{}
	case model.RevisionAppendKindAny:
		return &AppendResp_WrongExpectedVersion_ExpectedAny{}
	case model.RevisionAppendKindRevision:
		return &AppendResp_WrongExpectedVersion_ExpectedRevision{
			ExpectedRevision: opts.Revision,
		}
	}
	return nil
}

func appendSuccessResp(appendRes model.AppendResult) isAppendResp_Result {
	res := &AppendResp_Success_{
		Success: &AppendResp_Success{
			CurrentRevisionOption: &AppendResp_Success_CurrentRevision{CurrentRevision: appendRes.Revision},
			PositionOption: &AppendResp_Success_Position{
				Position: &AppendResp_Position{
					PreparePosition: appendRes.PreparePosition,
					CommitPosition:  appendRes.CommitPosition,
				},
			},
		},
	}

	if appendRes.Revision == 0 {
		res.Success.CurrentRevisionOption = &AppendResp_Success_NoStream{}
	}
	return res
}

func (s *grpcServer) Delete(ctx context.Context, req *DeleteReq) (*DeleteResp, error) {
	return nil, nil
}

func (s *grpcServer) Tombstone(ctx context.Context, req *TombstoneReq) (*TombstoneResp, error) {
	return nil, nil
}

func (s *grpcServer) BatchAppend(Streams_BatchAppendServer) error {
	return nil
}
