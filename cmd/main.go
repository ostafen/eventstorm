package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	"github.com/ostafen/eventstorm/internal/config"
	"github.com/ostafen/eventstorm/internal/streams"
	"github.com/ostafen/eventstorm/internal/transport/grpc/serverfeatures"
	grpcstreams "github.com/ostafen/eventstorm/internal/transport/grpc/streams"

	"google.golang.org/grpc"
)

func main() {
	conf, err := config.Read()
	if err != nil {
		log.Fatal(err)
	}

	db := openDB(conf.Engine.Postgres)
	streamService := streams.NewSteamsService(db)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Server.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	serverfeatures.RegisterServerFeaturesServer(server, serverfeatures.NewServerFeatures())
	grpcstreams.RegisterStreamsServer(server, grpcstreams.NewStreamsServer(streamService))
	server.Serve(listener)
}

func openDB(config config.Postgres) *sql.DB {
	db, err := sql.Open("postgres", config.ConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	return db
}
