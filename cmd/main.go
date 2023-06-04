package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	"github.com/ostafen/eventstorm/internal/streams"
	"github.com/ostafen/eventstorm/internal/transport/grpc/serverfeatures"
	grpcstreams "github.com/ostafen/eventstorm/internal/transport/grpc/streams"

	"google.golang.org/grpc"
)

const (
	defaultServerPort = 2113
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", defaultServerPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	connString := connectionString(address, user, port, password, db)
	db := openDB(connString)
	server := grpc.NewServer()

	serverfeatures.RegisterServerFeaturesServer(server, serverfeatures.NewServerFeatures())
	grpcstreams.RegisterStreamsServer(server, grpcstreams.NewStreamsServer(streams.NewSteamsService(db)))

	server.Serve(lis)
}

const (
	address  = "localhost"
	user     = "user"
	port     = "5432"
	password = "user"
	db       = "eventstorm"
)

func openDB(connectionString string) *sql.DB {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func connectionString(address, user, port, password, db string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", address, port, user, password, db)
}
