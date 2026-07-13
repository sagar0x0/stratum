package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sagar0x0/stratum/internal/hlc"
	"github.com/sagar0x0/stratum/internal/mvcc"
	"github.com/sagar0x0/stratum/internal/transport"
)

func main() {
	clientAddr := flag.String("client-addr", ":8000", "Address to expose Client API")
	storageAddr := flag.String("storage-addr", "localhost:9000", "Address of the Storage node")
	flag.Parse()

	log.Printf("Starting Stratum Compute Node at %s", *clientAddr)
	log.Printf("Connecting to Storage Node at %s", *storageAddr)

	storageClient, err := transport.NewStorageClient(*storageAddr)
	if err != nil {
		log.Fatalf("Failed to connect to storage node: %v", err)
	}
	defer storageClient.Close()

	clock := hlc.NewClock()
	txnMgr := mvcc.NewTxnManager(clock)

	grpcServer, lis, err := transport.StartClientAPI(*clientAddr, storageClient, txnMgr)
	if err != nil {
		log.Fatalf("Failed to start Client API server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down Compute Node...")
	grpcServer.GracefulStop()
	lis.Close()
}
