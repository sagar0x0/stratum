# Stratum: Setup & Deployment Guide

## Quick Start

### Prerequisites

- **Go 1.25+** ([install](https://go.dev/dl/))
- **Docker** & **Docker Compose** (for cluster deployment)
- **protoc** + Go plugins (only needed if regenerating proto files)

### Build from Source

```bash
git clone https://github.com/sagar0x0/stratum.git
cd stratum
go build ./...
```

### Run Tests

```bash
# All tests with race detector
make test

# Tests with coverage report
make test-cover

# Run linter
make lint
```

---

## Single-Node (Development)

### Start Storage Node

```bash
go run ./cmd/storage -addr :9000 -dir /tmp/stratum-data
```

### Start Compute Node

```bash
go run ./cmd/compute -client-addr :8000 -storage-addr localhost:9000
```

### Use the CLI Client

```bash
# Write a key
go run ./cmd/client -op put -key "hello" -val "world"

# Read a key
go run ./cmd/client -op get -key "hello"
# Output: Get successful: world

# Delete a key
go run ./cmd/client -op delete -key "hello"
```

---

## 3-Node Cluster (Docker Compose)

### One-Command Startup

```bash
# Build images and start the cluster
make docker-up

# Or manually:
docker compose up -d --build
```

This provisions:
- **3 storage nodes** (`storage-1`, `storage-2`, `storage-3`) with dedicated persistent volumes
- **3 compute nodes** (`compute-1`, `compute-2`, `compute-3`) with Client API exposed on ports `8001`, `8002`, `8003`
- Health checks ensure storage nodes are ready before compute nodes start

### Verify the Cluster

```bash
# Check all containers are running
docker compose ps

# Test against compute-1
go run ./cmd/client -addr localhost:8001 -op put -key "test" -val "cluster-works"
go run ./cmd/client -addr localhost:8001 -op get -key "test"
```

### Tear Down

```bash
make docker-down

# To also remove persistent volumes:
docker compose down -v
```

---

## Configuration Knobs

### Storage Engine (Programmatic via `stratum.Options`)

| Option | Default | Description | Tuning Guidance |
|--------|---------|-------------|-----------------|
| `MemTableSize` | 4 MB | Active MemTable capacity before freeze+flush | Increase for write-heavy workloads (16–64 MB) |
| `WALMaxBatchSize` | 100 | Max entries per group commit batch | Higher = better throughput, higher commit latency |
| `WALMaxBatchDelay` | 10 ms | Max wait time before flushing a partial batch | Lower = lower latency, higher fsync frequency |
| `SSTableBlockSize` | 4096 B | Target data block size in SSTables | Larger blocks = better compression, slower point lookups |
| `BloomBitsPerKey` | 10 | Bits per key in Bloom filters | 10 bits ≈ 1% FPR; 13 bits ≈ 0.1% FPR |
| `BlockCacheSize` | 8 MB | LRU block cache for SSTable reads | Size to fit the hot working set |
| `CompactionRateMB` | 50 MB/s | Background compaction I/O rate limit | Increase if compaction can't keep up |
| `L0StallTrigger` | 12 | L0 file count before write stall | Lower = more aggressive back-pressure |

### Command-Line Flags

#### Storage Node (`cmd/storage`)

```
-addr string     Storage gRPC server address (default ":9000")
-dir string      Directory for storage data (default "data/storage")
```

#### Compute Node (`cmd/compute`)

```
-client-addr string   Client API listen address (default ":8000")
-storage-addr string  Storage node address (default "localhost:9000")
```

#### CLI Client (`cmd/client`)

```
-addr string   Compute node address (default "localhost:8000")
-op string     Operation: put, get, delete
-key string    Key to operate on
-val string    Value for put operations
```

---

## Benchmarking

### Run YCSB Benchmarks

```bash
make bench
```

This runs workloads A (50/50), B (95/5), C (100% read), and F (read-modify-write) against the embedded storage engine.

### CPU / Memory Profiling

```bash
# CPU profile
make profile-cpu
go tool pprof cpu.prof

# Memory profile
make profile-mem
go tool pprof mem.prof
```

---

## Project Structure

```
stratum/
├── cmd/
│   ├── compute/       # Compute server entrypoint
│   ├── storage/       # Storage server entrypoint
│   └── client/        # CLI client
├── internal/
│   ├── wal/           # Write-ahead log (group commit, recovery)
│   ├── memtable/      # Skip-list MemTable with dual-buffer manager
│   ├── sstable/       # SSTable reader/writer with block cache
│   ├── lsm/           # LSM-tree (compaction, manifest, metrics)
│   ├── bloom/         # Partitioned Bloom filters
│   ├── raft/          # Raft consensus + leader leases
│   ├── mvcc/          # MVCC key encoding + transaction manager
│   ├── hlc/           # Hybrid logical clock
│   └── transport/     # gRPC transport (client API, storage server)
├── proto/             # Protobuf service definitions
│   ├── client/        # ClientAPI service
│   ├── storage/       # StorageEngine service
│   └── raft/          # RaftService
├── test/
│   ├── chaos/         # Porcupine linearizability tests
│   └── bench/         # YCSB benchmark harness
├── docs/
│   ├── weekly/        # Weekly progress reports (week-1 through week-8)
│   └── architecture/  # Architecture diagrams and API docs
├── docker-compose.yml # 3-node cluster orchestration
├── Dockerfile         # Multi-stage build for server binaries
├── Dockerfile.client  # Build for CLI client
├── Makefile           # Build, test, bench, docker targets
└── go.mod
```
