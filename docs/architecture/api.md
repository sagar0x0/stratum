# Stratum API Documentation

## Overview

Stratum exposes three gRPC services across its disaggregated architecture:

1. **ClientAPI** — The public-facing API for application clients (Get, Put, Delete, Scan)
2. **StorageEngine** — Internal API between compute and storage layers
3. **RaftService** — Internal API for Raft consensus between nodes

---

## 1. Client API (`proto/client/client.proto`)

The Client API is the primary interface for applications to interact with Stratum. It is served by the **Compute Node** on port `8000` (default).

### Service Definition

```protobuf
service ClientAPI {
  // Get retrieves the value for a key. Returns the latest committed version.
  rpc Get(GetRequest) returns (GetResponse) {}

  // Put writes a key-value pair. The write is conflict-checked against
  // concurrent transactions using first-writer-wins MVCC.
  rpc Put(PutRequest) returns (PutResponse) {}

  // Delete removes a key by writing a tombstone marker.
  // The tombstone is garbage-collected during deepest-level compaction.
  rpc Delete(DeleteRequest) returns (DeleteResponse) {}

  // Scan performs a range scan over [start_key, end_key).
  // Returns results as a server-side stream for memory efficiency.
  rpc Scan(ScanRequest) returns (stream ScanResponse) {}
}
```

### Messages

| Message | Field | Type | Description |
|---------|-------|------|-------------|
| `GetRequest` | `key` | `bytes` | Key to look up |
| `GetResponse` | `value` | `bytes` | Value if found |
| | `found` | `bool` | Whether the key exists |
| `PutRequest` | `key` | `bytes` | Key to write |
| | `value` | `bytes` | Value to write |
| `PutResponse` | `success` | `bool` | Whether the write succeeded |
| `DeleteRequest` | `key` | `bytes` | Key to delete |
| `DeleteResponse` | `success` | `bool` | Whether the delete succeeded |
| `ScanRequest` | `start_key` | `bytes` | Inclusive start of range |
| | `end_key` | `bytes` | Exclusive end of range |
| `ScanResponse` | `key` | `bytes` | Key in the range |
| | `value` | `bytes` | Corresponding value |

### Client SDK Usage Examples

#### Go Client

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    pb "github.com/sagar0x0/stratum/proto/client"
)

func main() {
    // Connect to compute node
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    conn, err := grpc.DialContext(ctx, "localhost:8000",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithBlock(),
    )
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := pb.NewClientAPIClient(conn)

    // --- Put ---
    putResp, err := client.Put(context.Background(), &pb.PutRequest{
        Key:   []byte("user:1001"),
        Value: []byte(`{"name": "Sagar", "role": "intern"}`),
    })
    if err != nil {
        log.Fatalf("Put failed: %v", err)
    }
    fmt.Printf("Put success: %v\n", putResp.Success)

    // --- Get ---
    getResp, err := client.Get(context.Background(), &pb.GetRequest{
        Key: []byte("user:1001"),
    })
    if err != nil {
        log.Fatalf("Get failed: %v", err)
    }
    if getResp.Found {
        fmt.Printf("Value: %s\n", string(getResp.Value))
    } else {
        fmt.Println("Key not found")
    }

    // --- Delete ---
    delResp, err := client.Delete(context.Background(), &pb.DeleteRequest{
        Key: []byte("user:1001"),
    })
    if err != nil {
        log.Fatalf("Delete failed: %v", err)
    }
    fmt.Printf("Delete success: %v\n", delResp.Success)
}
```

#### CLI Client

```bash
# Put a key
go run ./cmd/client -op put -key "hello" -val "world"

# Get a key
go run ./cmd/client -op get -key "hello"

# Delete a key
go run ./cmd/client -op delete -key "hello"
```

---

## 2. Storage Engine API (`proto/storage/storage.proto`)

The Storage Engine API is the internal interface between compute and storage nodes. It is served by the **Storage Node** on port `9000` (default).

### Service Definition

```protobuf
service StorageEngine {
  // WriteBatch atomically applies a batch of put and delete operations.
  // The batch is first written to the WAL, then applied to the MemTable.
  rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse) {}

  // Get performs a point lookup at a specific MVCC timestamp.
  // The read path: MemTable → Immutable MemTable → L0 SSTables → L1+ SSTables.
  // Bloom filters are checked before reading each SSTable.
  rpc Get(GetRequest) returns (GetResponse) {}

  // Scan performs a range scan at a specific MVCC timestamp.
  // Results are streamed back to avoid buffering large result sets.
  rpc Scan(ScanRequest) returns (stream ScanResponse) {}

  // Snapshot returns a serialized snapshot of the storage state.
  // Used by Raft for snapshotting and follower catch-up.
  rpc Snapshot(SnapshotRequest) returns (SnapshotResponse) {}
}
```

### Messages

| Message | Field | Type | Description |
|---------|-------|------|-------------|
| `WriteBatchRequest` | `puts` | `repeated PutOp` | List of put operations |
| | `deletes` | `repeated bytes` | List of keys to delete |
| `WriteBatchRequest.PutOp` | `key` | `bytes` | Key to write |
| | `value` | `bytes` | Value to write |
| `WriteBatchResponse` | `success` | `bool` | Whether the batch succeeded |
| `GetRequest` | `key` | `bytes` | Key to look up |
| | `timestamp` | `uint64` | MVCC snapshot timestamp |
| `GetResponse` | `value` | `bytes` | Value if found |
| | `found` | `bool` | Whether the key exists at this timestamp |

---

## 3. Raft Service API (`proto/raft/raft.proto`)

The Raft Service API handles consensus communication between peer nodes.

### Service Definition

```protobuf
service RaftService {
  // RequestVote is sent by candidates during leader election.
  // Implements §5.2 of the Raft paper.
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse) {}

  // AppendEntries is sent by the leader to replicate log entries
  // and serve as heartbeats. Implements §5.3 of the Raft paper.
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse) {}

  // InstallSnapshot transfers a snapshot to a lagging follower.
  // Implements §7 of the Raft paper (P2 scope).
  rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse) {}
}
```

### Messages

| Message | Field | Type | Description |
|---------|-------|------|-------------|
| `RequestVoteRequest` | `term` | `uint64` | Candidate's term |
| | `candidateId` | `string` | Candidate requesting vote |
| | `lastLogIndex` | `uint64` | Index of candidate's last log entry |
| | `lastLogTerm` | `uint64` | Term of candidate's last log entry |
| `RequestVoteResponse` | `term` | `uint64` | Current term, for candidate to update |
| | `voteGranted` | `bool` | Whether vote was granted |
| `LogEntry` | `term` | `uint64` | Term when entry was created |
| | `index` | `uint64` | Position in the log |
| | `data` | `bytes` | Serialized WriteBatch payload |
| `AppendEntriesRequest` | `term` | `uint64` | Leader's term |
| | `leaderId` | `string` | So follower can redirect clients |
| | `prevLogIndex` | `uint64` | Index of log entry immediately preceding new ones |
| | `prevLogTerm` | `uint64` | Term of prevLogIndex entry |
| | `entries` | `repeated LogEntry` | Log entries to store (empty for heartbeat) |
| | `leaderCommit` | `uint64` | Leader's commitIndex |
| `AppendEntriesResponse` | `term` | `uint64` | Current term, for leader to update |
| | `success` | `bool` | True if follower contained entry matching prevLogIndex/prevLogTerm |

---

## Configuration Reference

### Storage Node (`cmd/storage`)

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:9000` | gRPC listen address |
| `-dir` | `data/storage` | Directory for WAL, SSTables, and manifest |

### Compute Node (`cmd/compute`)

| Flag | Default | Description |
|------|---------|-------------|
| `-client-addr` | `:8000` | Client API listen address |
| `-storage-addr` | `localhost:9000` | Storage node address to connect to |

### Storage Engine Options (Programmatic)

| Option | Default | Description |
|--------|---------|-------------|
| `MemTableSize` | 4 MB | Max size of the active MemTable before flush |
| `WALMaxBatchSize` | 100 | Max entries per WAL group commit batch |
| `WALMaxBatchDelay` | 10ms | Max delay before flushing a partial WAL batch |
| `SSTableBlockSize` | 4096 bytes | Target data block size in SSTables |
| `BloomBitsPerKey` | 10 | Bits per key in Bloom filters (~1% FPR) |
| `BlockCacheSize` | 8 MB | LRU block cache size for SSTable reads |
| `CompactionRateMB` | 50 MB/s | I/O rate limit for background compaction |
| `L0StallTrigger` | 12 | L0 file count threshold for write stall |
