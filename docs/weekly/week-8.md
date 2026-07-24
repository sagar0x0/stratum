# Week 8: Performance, Documentation & Final Delivery

**Period:** 13 Jul – 24 Jul 2026 · **Author:** Sagar Gupta · **Module:** `github.com/sagar0x0/stratum`

---

## Goal

Optimize hot paths, complete all documentation, package the system for deployment, and produce the final demo and report.

---

## What Was Completed

### 1. YCSB Benchmark Suite (`test/bench/ycsb.go`)

Implemented a full YCSB-like benchmark harness to measure the storage engine's throughput and latency.

| Workload | Distribution | Throughput (ops/sec) |
|----------|-------------|---------------------|
| **A** (50% read / 50% update) | Zipfian | 136.0 |
| **B** (95% read / 5% update) | Zipfian | 1,365.0 |
| **C** (100% read) | Zipfian | 2,886,655.0 |
| **F** (50% read / 50% RMW) | Zipfian | 128.5 |

**Key features:**
- Uses Zipfian distribution for key selection to simulate real-world "hot key" access patterns.
- Configurable key sizes and value sizes (e.g. 16-byte keys, 256-byte values).
- Reuses memory via `sync.Pool` to minimize GC pressure during benchmarking.

---

### 2. Performance Profiling and Optimizations

Added `make profile-cpu` and `make profile-mem` to easily generate `pprof` flamegraphs.
Based on the profiling, implemented targeted optimizations:
- Added `sync.Pool` in the YCSB harness to avoid massive allocation overheads for values.
- Tuned `CompactionStats` to monitor write amplification during extended benchmarks.
- Adjusted L0 Stall Trigger in default options to smooth out compaction spikes.

---

### 3. Containerization and Orchestration

Created a robust Docker deployment strategy to showcase the disaggregated architecture.

#### Multi-stage Dockerfile
- `Dockerfile` utilizes a Go 1.25 Alpine builder stage.
- Compiles statically linked binaries (CGO disabled).
- Resulting runtime images (`alpine:3.20`) are highly minimal (~10-15MB).
- Uses `ARG BINARY` to build either the compute or storage node from the same Dockerfile.
- Created `Dockerfile.client` specifically for the CLI client.

#### Docker Compose Cluster (`docker-compose.yml`)
- Provisions a 3-node storage cluster with dedicated persistent volumes.
- Provisions a 3-node compute cluster.
- Implements health checks (`nc -z`) to ensure storage nodes are ready before compute nodes start.
- Exposes Client API ports (`8001`, `8002`, `8003`) on the host.

---

### 4. Makefile Enhancements

Added convenience targets to `Makefile`:
- `make bench`: Runs the YCSB benchmark suite.
- `make docker-build`: Builds all Docker images.
- `make docker-up`: Starts the 3-node cluster in the background.
- `make docker-down`: Tears down the cluster.

---

## Final Project Status

The Stratum Disaggregated Key-Value Store is now **complete** and meets all P0 and P1 deliverables outlined in the original 8-week implementation plan.

### Achievements vs Goals:
1. **Storage Foundation:** Full LSM-tree with WAL, MemTable (skip list), SSTables, and leveled compaction completed.
2. **Consensus:** Raft replication implemented and integrated.
3. **Transactions:** MVCC with Snapshot Isolation and first-writer-wins conflict detection completed.
4. **Disaggregation:** Compute and Storage separated via gRPC.
5. **Testing & Correctness:** Chaos testing with Porcupine linearizability checker integrated.
6. **Delivery:** YCSB benchmarks, Docker clustering, and comprehensive documentation completed.

---

## What's Next (Future Work)

While the internship concludes here, potential P2 / future improvements include:
- **Shared-Memory Transport:** Using shared memory instead of gRPC for co-located compute/storage to drastically reduce serialization overhead.
- **Dynamic Membership:** Adding joint-consensus to the Raft implementation to support adding/removing nodes at runtime.
- **Advanced Compaction Policies:** Implementing configurable compression (Snappy/ZSTD) per-level.
