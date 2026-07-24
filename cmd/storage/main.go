package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sagar0x0/stratum"
	"github.com/sagar0x0/stratum/internal/transport"
)

func main() {
	addr := flag.String("addr", ":9000", "Storage gRPC server address")
	dataDir := flag.String("dir", "data/storage", "Directory for storage data")
	flag.Parse()

	log.Printf("Starting Stratum Storage Node at %s", *addr)

	opts := stratum.DefaultOptions()
	opts.Dir = *dataDir

	db, err := stratum.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	grpcServer, lis, err := transport.StartGRPCServer(*addr, db)
	if err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down Storage Node...")
	grpcServer.GracefulStop()
	_ = lis.Close()
}
