package transport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sagar0x0/stratum/proto/storage"
)

// StorageClient provides a pooled/multiplexed connection to a storage node.
type StorageClient struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.StorageEngineClient
	mu     sync.RWMutex
}

func NewStorageClient(addr string) (*StorageClient, error) {
	// For multiplexing, grpc uses a single connection by default for all concurrent RPCs
	// which is generally sufficient. We can configure connection options if needed.
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial storage node %s: %w", addr, err)
	}

	return &StorageClient{
		addr:   addr,
		conn:   conn,
		client: pb.NewStorageEngineClient(conn),
	}, nil
}

func (c *StorageClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *StorageClient) WriteBatch(ctx context.Context, puts map[string][]byte, deletes []string) error {
	req := &pb.WriteBatchRequest{}
	for k, v := range puts {
		req.Puts = append(req.Puts, &pb.WriteBatchRequest_PutOp{
			Key:   []byte(k),
			Value: v,
		})
	}
	for _, k := range deletes {
		req.Deletes = append(req.Deletes, []byte(k))
	}

	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	res, err := client.WriteBatch(ctx, req)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("write batch failed on storage node")
	}
	return nil
}

func (c *StorageClient) Get(ctx context.Context, key []byte, timestamp uint64) ([]byte, bool, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	req := &pb.GetRequest{
		Key:       key,
		Timestamp: timestamp,
	}

	res, err := client.Get(ctx, req)
	if err != nil {
		return nil, false, err
	}

	return res.Value, res.Found, nil
}
