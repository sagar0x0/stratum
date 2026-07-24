# Stratum Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Client SDK / CLI                           │
│              (gRPC, Retry, Leader Discovery)                    │
│  cmd/client/main.go                                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ gRPC (proto/client/client.proto)
                           │ Get / Put / Delete / Scan
┌──────────────────────────▼──────────────────────────────────────┐
│                   Compute Layer (Stateless)                      │
│   cmd/compute/main.go                                           │
│   ┌──────────────────┐  ┌───────────────────┐  ┌────────────┐  │
│   │ Transaction       │  │ MVCC Conflict     │  │ HLC        │  │
│   │ Coordinator       │  │ Detection         │  │ Timestamp  │  │
│   │ internal/         │  │ First-Writer-Wins │  │ Oracle     │  │
│   │  transport/       │  │ internal/mvcc/    │  │ internal/  │  │
│   │  client_api.go    │  │  txn.go           │  │  hlc/      │  │
│   └────────┬─────────┘  └────────┬──────────┘  │  hlc.go    │  │
│            │                     │              └────────────┘  │
└────────────┼─────────────────────┼─────────────────────────────┘
             │ Raft Propose        │ Read Path
             │                     │
┌────────────▼─────────────────────▼─────────────────────────────┐
│                   Consensus Layer (Raft)                         │
│   internal/raft/raft.go                                         │
│   ┌───────────────┐  ┌────────────────┐  ┌─────────────────┐  │
│   │ Log Replication│  │ Leader Lease   │  │ Raft WAL        │  │
│   │ (Pipeline,    │  │ (Bounded       │  │ internal/raft/  │  │
│   │  Batch)       │  │  Clock Skew)   │  │  wal.go         │  │
│   └───────┬───────┘  └────────────────┘  └─────────────────┘  │
│           │ gRPC Transport (internal/transport/grpc.go)         │
│           │ proto/raft/raft.proto                                │
└───────────┼────────────────────────────────────────────────────┘
            │ Apply committed entries
            │ gRPC (proto/storage/storage.proto)
            │ WriteBatch / Get / Scan / Snapshot
┌───────────▼────────────────────────────────────────────────────┐
│                   Storage Layer (LSM-Tree)                       │
│   cmd/storage/main.go                                           │
│   internal/transport/server.go                                  │
│                                                                  │
│   ┌─────────┐  ┌────────────┐  ┌───────────┐  ┌─────────────┐ │
│   │   WAL   │  │  MemTable  │  │  SSTables │  │   Bloom     │ │
│   │internal/│  │  (SkipList)│  │  (Leveled │  │   Filters   │ │
│   │wal/     │  │  internal/ │  │  Compaction│  │  internal/  │ │
│   │         │  │  memtable/ │  │  internal/ │  │  bloom/     │ │
│   │ writer  │  │            │  │  lsm/     │  │             │ │
│   │ reader  │  │  manager   │  │  sstable/ │  │  bloom.go   │ │
│   │ batch   │  │  skiplist  │  │           │  │             │ │
│   │ recover │  │            │  │           │  │             │ │
│   └─────────┘  └────────────┘  └───────────┘  └─────────────┘ │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │         MVCC Version Chains + GC                          │  │
│   │         internal/mvcc/key.go (encode/decode)              │  │
│   │         Key = userKey || ~timestamp (descending sort)     │  │
│   └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │  Compaction Engine                                        │  │
│   │  compaction.go  │  manifest.go  │  metrics.go             │  │
│   │  rate_limiter.go │  write_stall.go │ merge_iterator.go    │  │
│   └──────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## Mermaid Diagram

```mermaid
graph TD
    subgraph "Client Layer"
        CLI["CLI Client<br/>cmd/client/main.go"]
    end

    subgraph "Compute Layer (Stateless)"
        CA["Client API<br/>transport/client_api.go"]
        TXN["Txn Manager<br/>mvcc/txn.go"]
        HLC["HLC Clock<br/>hlc/hlc.go"]
    end

    subgraph "Consensus Layer"
        RAFT["Raft Node<br/>raft/raft.go"]
        RWAL["Raft WAL<br/>raft/wal.go"]
        GRPC["gRPC Transport<br/>transport/grpc.go"]
    end

    subgraph "Storage Layer (LSM-Tree)"
        SS["Storage Server<br/>transport/server.go"]
        WAL["WAL<br/>wal/writer.go"]
        MT["MemTable<br/>memtable/skiplist.go"]
        SST["SSTables<br/>sstable/reader.go<br/>sstable/writer.go"]
        BF["Bloom Filter<br/>bloom/bloom.go"]
        LSM["LSM Engine<br/>lsm/lsm.go"]
        COMP["Compaction<br/>lsm/compaction.go"]
        MAN["Manifest<br/>lsm/manifest.go"]
    end

    CLI -->|"gRPC<br/>Get/Put/Delete"| CA
    CA --> TXN
    CA --> HLC
    TXN -->|"Propose"| RAFT
    CA -->|"Read"| SS
    RAFT --> RWAL
    RAFT -->|"Replicate"| GRPC
    RAFT -->|"Apply"| SS
    SS --> WAL
    SS --> LSM
    LSM --> MT
    LSM --> SST
    SST --> BF
    LSM --> COMP
    COMP --> MAN
    MT -->|"Flush"| SST
