package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	"github.com/ostafen/eventstorm/internal/service"
	"github.com/ostafen/eventstorm/internal/transport/grpc/serverfeatures"
	grpcstreams "github.com/ostafen/eventstorm/internal/transport/grpc/streams"

	"google.golang.org/grpc"
)

const (
	serverPort = 8089
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	db := openDB()
	server := grpc.NewServer()

	serverfeatures.RegisterServerFeaturesServer(server, serverfeatures.NewServerFeatures())
	grpcstreams.RegisterStreamsServer(server, grpcstreams.NewStreamsServer(service.NewSteamsService(db)))

	server.Serve(lis)
}

const (
	address  = "localhost"
	user     = "user"
	port     = "5432"
	password = "user"
	db       = "eventstorm"
)

func openDB() *sql.DB {
	db, err := sql.Open("postgres", connectionString(address, user, port, password, db))
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func connectionString(address, user, port, password, db string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", address, port, user, password, db)
}
