package chaos

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sagar0x0/stratum/proto/client"
)

// KvInput represents a Put or Get operation.
type KvInput struct {
	Op    string // "get" or "put"
	Value string // Value for put
}

// KvOutput represents the result.
type KvOutput struct {
	Value string // Value returned by get
}

// RegisterModel models a single register (key).
var RegisterModel = porcupine.Model{
	Init: func() interface{} {
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		inp := input.(KvInput)
		out := output.(KvOutput)
		if inp.Op == "get" {
			return out.Value == st, state
		} else if inp.Op == "put" {
			return true, inp.Value
		}
		return false, state
	},
}

func TestLinearizability(t *testing.T) {
	// This test assumes a cluster is running (compute node at :8000)
	// We will run concurrent clients and record operations.
	
	addr := "localhost:8000"
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		t.Skipf("Skipping test, compute node not available: %v", err)
	}
	defer conn.Close()

	client := pb.NewClientAPIClient(conn)
	key := []byte("chaos-key")
	
	// Reset the key
	_, _ = client.Put(context.Background(), &pb.PutRequest{Key: key, Value: []byte("")})

	var ops []porcupine.Operation
	var mu sync.Mutex

	numClients := 5
	opsPerClient := 10

	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < opsPerClient; j++ {
				isPut := (j % 2 == 0) // alternate put and get
				
				callStart := time.Now().UnixNano()
				var out KvOutput
				var inp KvInput
				
				if isPut {
					val := fmt.Sprintf("val-%d-%d", clientID, j)
					inp = KvInput{Op: "put", Value: val}
					
					_, err := client.Put(context.Background(), &pb.PutRequest{Key: key, Value: []byte(val)})
					if err != nil {
						continue // If failed, we don't record it in this simple harness
					}
				} else {
					inp = KvInput{Op: "get"}
					res, err := client.Get(context.Background(), &pb.GetRequest{Key: key})
					if err != nil {
						continue
					}
					out = KvOutput{Value: string(res.Value)}
				}
				
				callEnd := time.Now().UnixNano()
				
				op := porcupine.Operation{
					ClientId: clientID,
					Input:    inp,
					Call:     callStart,
					Output:   out,
					Return:   callEnd,
				}
				
				mu.Lock()
				ops = append(ops, op)
				mu.Unlock()
			}
		}(i)
	}
	
	wg.Wait()
	
	res, _ := porcupine.CheckOperationsVerbose(RegisterModel, ops, 0)
	if res == porcupine.Illegal {
		t.Fatalf("History is not linearizable")
	}
}
