# Stratum

A disaggregated key-value store built from scratch in Go. Stratum cleanly separates compute (transaction processing) from storage (persistent state management) into independently scalable components.

[![CI](https://github.com/sagar0x0/stratum/actions/workflows/ci.yml/badge.svg)](https://github.com/sagar0x0/stratum/actions/workflows/ci.yml)

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Client (gRPC)                     │
├─────────────────────────────────────────────────────┤
│             Compute Layer (Stateless)               │
│         MVCC · Snapshot Isolation · HLC             │
├─────────────────────────────────────────────────────┤
│              Consensus Layer (Raft)                 │
│        Leader Election · Log Replication            │
├─────────────────────────────────────────────────────┤
│              Storage Layer (LSM-Tree)               │
│    WAL · MemTable · SSTable · Bloom · Compaction    │
└─────────────────────────────────────────────────────┘
```

**Compute** and **Storage** communicate over gRPC and can be deployed as separate processes.

## Features

- **LSM-Tree Storage Engine** — WAL with group commit, concurrent skip-list MemTable, block-based SSTables with LZ4 compression, partitioned Bloom filters, leveled compaction
- **Raft Consensus** — Leader election, log replication, leader leases for linearizable reads
- **MVCC Transactions** — Snapshot isolation with first-writer-wins conflict detection, hybrid logical clock timestamps
- **Disaggregated Architecture** — Stateless compute nodes, independently scalable storage nodes
- **Correctness Verified** — Porcupine linearizability checker integrated into the test suite

## Quick Start

### Prerequisites

- Go 1.25+
- Docker & Docker Compose (for cluster mode)

### Single Node

```bash
# Build
make build

# Run storage node
go run ./cmd/storage -addr :9000 -dir data/storage

# Run compute node (connects to storage)
go run ./cmd/compute -client-addr :8000 -storage-addr localhost:9000

# Use the CLI client
go run ./cmd/client -addr localhost:8000 -op put -key "hello" -val "world"
go run ./cmd/client -addr localhost:8000 -op get -key "hello"
go run ./cmd/client -addr localhost:8000 -op delete -key "hello"
```

### 3-Node Docker Cluster

```bash
docker compose up -d --build    # Start cluster
docker compose ps               # Check health
docker compose down              # Tear down
```

Exposes Client API on ports `8001`, `8002`, `8003`.

## Testing

```bash
make test          # All tests with race detector
make bench         # YCSB benchmarks (A/B/C/F)
make lint          # golangci-lint
make test-cover    # Tests with coverage report
```

## Benchmarks (Apple M1)

| Workload | Mix | Throughput |
|----------|-----|-----------|
| A | 50/50 read/write | 136 ops/s |
| B | 95/5 read/write | 1,365 ops/s |
| C | 100% read | **2,886,655 ops/s** |
| F | 50/50 read/RMW | 129 ops/s |

## Project Structure

```
stratum/
├── cmd/
│   ├── client/          # CLI client
│   ├── compute/         # Compute node binary
│   └── storage/         # Storage node binary
├── internal/
│   ├── bloom/           # Partitioned Bloom filters
│   ├── hlc/             # Hybrid Logical Clock
│   ├── lsm/             # LSM-tree (flush, compaction, manifest)
│   ├── memtable/        # Skip-list MemTable with manager
│   ├── mvcc/            # MVCC transaction manager
│   ├── raft/            # Raft consensus
│   ├── sstable/         # SSTable reader/writer, block cache
│   ├── transport/       # gRPC server/client wrappers
│   └── wal/             # Write-ahead log with group commit
├── proto/               # Protobuf service definitions
├── test/
│   ├── bench/           # YCSB benchmark harness
│   └── chaos/           # Linearizability tests (Porcupine)
├── docs/
│   ├── architecture/    # Architecture, API, and setup docs
│   └── weekly/          # Weekly progress reports
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

## Documentation

- [Architecture](docs/architecture/architecture.md)
- [API Reference](docs/architecture/api.md)
- [Setup & Deployment](docs/architecture/setup.md)

## License

MIT
