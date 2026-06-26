package transport

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sagar0x0/stratum/proto/raft"
)

// GRPCTransport implements the raft.Transport interface over gRPC.
type GRPCTransport struct {
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (t *GRPCTransport) getClient(peer string) (pb.RaftServiceClient, error) {
	if conn, ok := t.conns[peer]; ok {
		return pb.NewRaftServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	t.conns[peer] = conn
	return pb.NewRaftServiceClient(conn), nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, req)
}
