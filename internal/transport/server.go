package transport

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sagar0x0/stratum"
	"github.com/sagar0x0/stratum/internal/wal"
	pb "github.com/sagar0x0/stratum/proto/storage"
)

type StorageServer struct {
	pb.UnimplementedStorageEngineServer
	db *stratum.DB
}

func NewStorageServer(db *stratum.DB) *StorageServer {
	return &StorageServer{db: db}
}

func (s *StorageServer) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	batch := &wal.Batch{}
	for _, p := range req.Puts {
		batch.Put(p.Key, p.Value)
	}
	for _, d := range req.Deletes {
		batch.Delete(d)
	}

	if err := s.db.WriteBatch(batch); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write batch: %v", err)
	}

	return &pb.WriteBatchResponse{Success: true}, nil
}

func (s *StorageServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// TODO: Use req.Timestamp for MVCC snapshot read
	val, err := s.db.Get(req.Key)
	if err != nil {
		if err == stratum.ErrNotFound {
			return &pb.GetResponse{Found: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get key: %v", err)
	}
	return &pb.GetResponse{Value: val, Found: true}, nil
}

func (s *StorageServer) Scan(req *pb.ScanRequest, stream pb.StorageEngine_ScanServer) error {
	// P1 Scope: Not yet implemented in the core storage engine.
	return status.Errorf(codes.Unimplemented, "scan not implemented")
}

func (s *StorageServer) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	// P1/P2 Scope: Not yet implemented in the core storage engine.
	return nil, status.Errorf(codes.Unimplemented, "snapshot not implemented")
}

// StartGRPCServer starts the storage gRPC server on the given address.
func StartGRPCServer(addr string, db *stratum.DB) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterStorageEngineServer(grpcServer, NewStorageServer(db))
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc server failed to serve: %v", err)
		}
	}()
	
	return grpcServer, lis, nil
}
