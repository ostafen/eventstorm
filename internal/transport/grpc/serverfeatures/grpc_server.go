package serverfeatures

import (
	context "context"

	shared "github.com/ostafen/eventstorm/internal/transport/grpc/shared"
)

const (
	serverVersion = "22.10.1"
)

type serverFeatures struct {
	UnimplementedServerFeaturesServer
}

func NewServerFeatures() *serverFeatures {
	return &serverFeatures{}
}

func (s *serverFeatures) GetSupportedMethods(ctx context.Context, empty *shared.Empty) (*SupportedMethods, error) {
	return &SupportedMethods{
		Methods:                 []*SupportedMethod{},
		EventStoreServerVersion: serverVersion,
	}, nil
}
