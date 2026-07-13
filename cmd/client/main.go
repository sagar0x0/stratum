package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sagar0x0/stratum/proto/client"
)

func main() {
	addr := flag.String("addr", "localhost:8000", "Compute node Client API address")
	op := flag.String("op", "", "Operation to perform: put, get, delete")
	key := flag.String("key", "", "Key")
	val := flag.String("val", "", "Value for put")
	flag.Parse()

	if *op == "" || *key == "" {
		fmt.Println("Usage: client -op <put|get|delete> -key <key> [-val <value>]")
		os.Exit(1)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr, opts...)
	if err != nil {
		log.Fatalf("Failed to dial compute node %s: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewClientAPIClient(conn)

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rpcCancel()

	switch *op {
	case "put":
		res, err := client.Put(rpcCtx, &pb.PutRequest{Key: []byte(*key), Value: []byte(*val)})
		if err != nil {
			log.Fatalf("Put failed: %v", err)
		}
		if res.Success {
			fmt.Println("Put successful")
		}
	case "get":
		res, err := client.Get(rpcCtx, &pb.GetRequest{Key: []byte(*key)})
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}
		if res.Found {
			fmt.Printf("Get successful: %s\n", string(res.Value))
		} else {
			fmt.Println("Key not found")
		}
	case "delete":
		res, err := client.Delete(rpcCtx, &pb.DeleteRequest{Key: []byte(*key)})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}
		if res.Success {
			fmt.Println("Delete successful")
		}
	default:
		log.Fatalf("Unknown operation: %s", *op)
	}
}
