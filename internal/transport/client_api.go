package transport

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sagar0x0/stratum/internal/mvcc"
	pb "github.com/sagar0x0/stratum/proto/client"
)

type ClientAPI struct {
	pb.UnimplementedClientAPIServer
	storage *StorageClient
	txnMgr  *mvcc.TxnManager
}

func NewClientAPI(storage *StorageClient, txnMgr *mvcc.TxnManager) *ClientAPI {
	return &ClientAPI{
		storage: storage,
		txnMgr:  txnMgr,
	}
}

func (c *ClientAPI) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// 1. Begin read-only Txn to get snapshot timestamp
	txn := c.txnMgr.Begin()
	defer txn.Abort() // not needed for read-only, but good practice
	ts := txn.SnapshotTS

	// 2. Fetch from Storage at Snapshot TS
	val, found, err := c.storage.Get(ctx, req.Key, ts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get from storage: %v", err)
	}

	return &pb.GetResponse{Value: val, Found: found}, nil
}

func (c *ClientAPI) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// 1. Begin Txn
	txn := c.txnMgr.Begin()

	// 2. Add to write set
	txn.AddWrite(req.Key)

	// 3. Commit Txn
	_, ok := txn.Commit()
	if !ok {
		return nil, status.Errorf(codes.Aborted, "transaction aborted due to conflict")
	}

	// 4. If commit succeeds, write to storage (In a real system, Raft would do this upon commit)
	puts := map[string][]byte{string(req.Key): req.Value}
	err := c.storage.WriteBatch(ctx, puts, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write to storage: %v", err)
	}

	return &pb.PutResponse{Success: true}, nil
}

func (c *ClientAPI) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	// 1. Begin Txn
	txn := c.txnMgr.Begin()

	// 2. Add to write set
	txn.AddWrite(req.Key)

	// 3. Commit Txn
	_, ok := txn.Commit()
	if !ok {
		return nil, status.Errorf(codes.Aborted, "transaction aborted due to conflict")
	}

	// 4. If commit succeeds, write to storage
	deletes := []string{string(req.Key)}
	err := c.storage.WriteBatch(ctx, nil, deletes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete from storage: %v", err)
	}

	return &pb.DeleteResponse{Success: true}, nil
}

func (c *ClientAPI) Scan(req *pb.ScanRequest, stream pb.ClientAPI_ScanServer) error {
	return status.Errorf(codes.Unimplemented, "scan not implemented")
}

func StartClientAPI(addr string, storage *StorageClient, txnMgr *mvcc.TxnManager) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterClientAPIServer(grpcServer, NewClientAPI(storage, txnMgr))

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc client API server failed to serve: %v", err)
		}
	}()

	return grpcServer, lis, nil
}
