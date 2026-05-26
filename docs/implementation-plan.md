# Disaggregated Key-Value Store: Implementation Plan

**Intern:** Sagar Gupta
**Duration:** 25 May 2026 – 24 July 2026 (8 weeks)  

---

## 1. Executive Summary

This project delivers a disaggregated key-value store that cleanly separates its compute (transaction processing) and storage (persistent state management) layers into independently scalable components. The storage engine is built from scratch around an LSM-tree with a write-ahead log, Bloom filters, and tiered compaction. State is replicated across nodes using Raft consensus with leader leases to provide linearizable reads without quorum round-trips. Concurrency is handled through multi-version concurrency control (MVCC) with snapshot isolation, enabling non-blocking reads against consistent point-in-time views. The system is validated end-to-end with a fault-injection harness and a linearizability checker (Jepsen-style history verification), ensuring correctness guarantees hold under node crashes, network partitions, and concurrent workloads. The final artifact is a fully containerized, tested, and documented distributed system suitable for demonstrating production-grade systems engineering.

---

## 2. Technology Stack

| Category | Choice | Rationale |
|---|---|---|
| **Language** | Go 1.22+ | First-class concurrency (goroutines, channels), strong systems ecosystem, native gRPC support, built-in race detector. |
| **RPC Framework** | `google.golang.org/grpc` + Protocol Buffers | Industry-standard for inter-service communication; streaming RPCs for `Scan` and snapshot transfer. |
| **Compression** | `github.com/klauspost/compress/lz4` | High-throughput LZ4 block compression for SSTables. |
| **Hashing** | `github.com/spaolacci/murmur3` | MurmurHash3 for Bloom filter hash functions. |
| **Linearizability Checker** | [Porcupine](https://github.com/anishathalye/porcupine) | Go-native linearizability checker; fits directly into `go test` workflows. |
| **Profiling** | `net/http/pprof`, `go tool pprof` | CPU and memory profiling with flamegraph generation. |
| **Benchmarking** | `testing.B` + custom YCSB harness | Go's built-in benchmark framework for micro-benchmarks; custom driver for end-to-end workloads. |
| **Linting / CI** | `golangci-lint`, GitHub Actions | Static analysis (`govet`, `staticcheck`, `errcheck`) and CI pipeline on every push. |
| **Containerization** | Multi-stage `Dockerfile` (Go builder → `scratch`/`alpine`) | Minimal image size (~10 MB), no runtime dependencies. |
| **Orchestration** | Docker Compose | 3-node cluster with persistent volumes and health checks. |

**Project Layout** (follows standard Go conventions):

```
disaggregated-kv/
├── cmd/
│   ├── compute/       # Compute server entrypoint
│   ├── storage/       # Storage server entrypoint
│   └── client/        # CLI client
├── internal/
│   ├── wal/           # Write-ahead log
│   ├── memtable/      # Skip-list MemTable
│   ├── sstable/       # SSTable reader/writer
│   ├── lsm/           # LSM-tree (compaction, manifest, read path)
│   ├── bloom/         # Bloom filter
│   ├── raft/          # Raft consensus + leader leases
│   ├── mvcc/          # MVCC version chains + transaction manager
│   ├── hlc/           # Hybrid logical clock
│   └── transport/     # gRPC transport layer
├── proto/             # Protobuf service definitions
├── test/
│   ├── chaos/         # Fault injection + linearizability tests
│   └── bench/         # YCSB benchmark harness
├── docs/
│   ├── weekly/        # Weekly progress reports
│   └── architecture/  # Diagrams and design docs
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── go.mod
└── go.sum
```

---

## 3. Technical Architecture Overview

The system is organized into four distinct layers, communicating over well-defined gRPC interfaces.

### 2.1 Client Interface Layer

Exposes a key-value API (`Get`, `Put`, `Delete`, `Scan`) over gRPC. Handles request routing: writes are forwarded to the current Raft leader; reads are served locally from any node holding a valid leader lease, or forwarded otherwise. Connection multiplexing and client-side retry logic with exponential backoff are handled here.

### 2.2 Compute Layer (Transaction Coordinator)

Manages the transaction lifecycle. Each transaction is assigned a monotonically increasing timestamp from a hybrid logical clock (HLC). The coordinator maintains a transaction table tracking active transactions and their read/write sets. On commit, it performs write-conflict detection against the MVCC version chain — if two concurrent transactions wrote to the same key, the later commit is aborted (first-writer-wins). Committed writes are serialized into a Raft log entry and proposed to the consensus layer. The compute layer is stateless beyond in-flight transaction state; all durable state lives in the storage layer.

### 2.3 Consensus Layer (Raft + Leader Leases)

A full Raft implementation providing:

- **Log replication** with pipelining and batched appends for throughput.
- **Leader leases** — the leader holds a time-bounded lease (renewed on successful heartbeats). During a valid lease, the leader serves linearizable reads locally without issuing a read-index quorum check, trading a bounded clock-skew assumption for single-node read latency.
- **Snapshotting** — periodic Raft snapshots of the LSM-tree's manifest to bound log growth and accelerate follower catch-up.
- **Membership changes** — single-node joint-consensus membership changes for adding/removing replicas.

### 2.4 Storage Layer (LSM-Tree Engine)

A persistent, append-oriented storage engine:

| Component | Detail |
|---|---|
| **Write-Ahead Log** | Append-only, `fsync`-per-batch for durability. Group commit to amortize I/O. |
| **MemTable** | Concurrent skip list (`sync.RWMutex`-guarded or lock-free via `sync/atomic`). Dual-buffer: active + immutable for flush overlap. |
| **SSTable** | Sorted, immutable on-disk files with block-based layout, per-block compression (LZ4), and a trailing index/filter block. |
| **Bloom Filters** | Per-SSTable partitioned Bloom filters (≈10 bits/key, ~1% FPR) to skip disk reads on negative lookups. |
| **Compaction** | Leveled compaction (RocksDB-style) with size-ratio triggers. Background compaction goroutines with rate limiting to bound write amplification and I/O interference. |
| **MVCC Version Chain** | Each key maps to a chain of `(timestamp, value)` pairs. `Get` at timestamp *t* binary-searches the chain for the latest version ≤ *t*. Garbage collection prunes versions older than the oldest active snapshot. |

### 2.5 Layer Interaction (Disaggregation Boundary)

The compute and storage layers communicate via a well-defined internal gRPC service:

```protobuf
service StorageEngine {
  rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse);
  rpc Get(GetRequest) returns (GetResponse);            // point lookup at timestamp
  rpc Scan(ScanRequest) returns (stream ScanResponse);   // range scan at timestamp
  rpc Snapshot(SnapshotRequest) returns (SnapshotResponse); // for Raft snapshots
}
```

This boundary means the compute layer can be scaled horizontally (multiple coordinators) independently of the storage layer, and the storage engine can be swapped or upgraded without changing transaction logic.

### 2.6 Architecture Diagram (Textual)

```
┌─────────────────────────────────────────────────────────────┐
│                      Client SDK / CLI                       │
│              (gRPC, Retry, Leader Discovery)                │
└──────────────────────────┬──────────────────────────────────┘
                           │ gRPC
┌──────────────────────────▼──────────────────────────────────┐
│                   Compute Layer (Stateless)                  │
│   ┌──────────────┐  ┌───────────────┐  ┌────────────────┐  │
│   │ Txn Coord.   │  │ MVCC Conflict │  │ HLC Timestamp  │  │
│   │ (Begin/      │  │ Detection     │  │ Oracle         │  │
│   │  Commit/     │  │ (Write-set    │  │                │  │
│   │  Abort)      │  │  Intersection)│  │                │  │
│   └──────┬───────┘  └───────┬───────┘  └────────────────┘  │
│          │                  │                                │
└──────────┼──────────────────┼────────────────────────────────┘
           │ Raft Propose     │ Read Path
┌──────────▼──────────────────▼────────────────────────────────┐
│                   Consensus Layer (Raft)                      │
│   ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│   │ Log Replic. │  │ Leader Lease │  │ Raft Snapshots    │  │
│   │ (Pipeline,  │  │ (Bounded     │  │ (LSM Manifest)    │  │
│   │  Batch)     │  │  Clock Skew) │  │                   │  │
│   └──────┬──────┘  └──────────────┘  └───────────────────┘  │
└──────────┼───────────────────────────────────────────────────┘
           │ Apply committed entries
┌──────────▼───────────────────────────────────────────────────┐
│                   Storage Layer (LSM-Tree)                    │
│   ┌───────┐  ┌──────────┐  ┌─────────┐  ┌───────────────┐  │
│   │  WAL  │  │ MemTable │  │ SSTables│  │ Bloom Filters │  │
│   │       │  │ (SkipList)│  │ (Leveled│  │ (Partitioned) │  │
│   │       │  │          │  │  Comp.) │  │               │  │
│   └───────┘  └──────────┘  └─────────┘  └───────────────┘  │
│   ┌──────────────────────────────────────────────────────┐  │
│   │           MVCC Version Chains + GC                   │  │
│   └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Phased Implementation Plan (8 Weeks)

### Week 1 (25 May – 31 May): Storage Foundation — WAL + MemTable

**Goals:** Establish the foundational write path. A client can write key-value pairs that survive process restarts.

**Deliverables:**
- Write-ahead log with `fsync`-per-batch group commit and CRC32 checksums per record
- Recovery routine that replays the WAL on startup, detecting and truncating torn writes
- Concurrent skip-list MemTable supporting `Put`, `Get`, `Delete`, and ordered iteration
- Dual-buffer MemTable scheme: active MemTable accepts writes while the immutable one is flushed in the background
- Unit tests: WAL corruption recovery, skip-list concurrency under contention, MemTable freeze semantics

**Technical Detail:**  
The WAL uses a fixed-size block format (32 KB blocks) with records spanning blocks via `FIRST/MIDDLE/LAST/FULL` record types, mirroring LevelDB's design. CRC32 checksums use Go's `hash/crc32` (Castagnoli polynomial, hardware-accelerated on amd64). The skip list targets 12–16 levels with probabilistic balancing (p=0.25) and uses `sync/atomic` for lock-free concurrent reads. MemTable capacity is configurable (default 64 MB); exceeding it triggers a freeze-and-flush.

---

### Week 2 (1 Jun – 7 Jun): SSTable Format + Flush + Read Path

**Goals:** Persist MemTable contents to disk as sorted, immutable SSTables. Enable point lookups across the MemTable and on-disk SSTables.

**Deliverables:**
- SSTable writer: block-based layout with configurable block size (4 KB default), per-block LZ4 compression, trailing index block and metadata block
- SSTable reader: binary search over the index block, block cache (sharded LRU with `sync.Mutex` per shard, configurable size)
- Bloom filter construction during SSTable write (partitioned, 10 bits/key)
- Bloom filter probe integrated into the read path to skip SSTables on negative lookups
- Flush pipeline: immutable MemTable → SSTable on disk, WAL truncation post-flush
- Integrated read path: check active MemTable → immutable MemTable → L0 SSTables (newest first) → deeper levels
- Unit tests: SSTable round-trip (write + read), Bloom filter false-positive rate validation, flush correctness

**Technical Detail:**  
SSTables are written with a footer containing the index block offset and Bloom filter offset. The block cache uses a sharded LRU (4 shards, `sync.Mutex` per shard) to reduce lock contention under concurrent `Get` calls. Bloom filters use double hashing with `murmur3`. Bloom filter correctness is validated empirically via `go test`: insert 100K keys, probe 100K non-existent keys, assert FPR < 1.5%.

---

### Week 3 (8 Jun – 14 Jun): Compaction + Full LSM-Tree

**Goals:** Implement background compaction to bound read amplification and space amplification. The LSM-tree is now feature-complete as a standalone embedded storage engine.

**Deliverables:**
- Leveled compaction: L0 → L1 with size-ratio trigger, L1+ with overlapping-range merge
- Compaction picker: selects candidate SSTables based on level size ratios and key-range overlap
- Compaction executor: multi-way merge of input SSTables, produces new SSTables, atomically updates the manifest
- Manifest file: tracks the current set of SSTables per level, supports atomic swaps via versioned manifest records
- Rate limiter on compaction I/O to prevent background work from starving foreground reads
- Write stall logic: back-pressure when L0 file count exceeds threshold
- Benchmarks: sequential and random write throughput, read latency percentiles (p50, p99), write amplification factor
- Integration test: write 1M keys, trigger multiple compaction cycles, verify all keys are readable and correct

**Technical Detail:**  
Compaction follows RocksDB's leveled strategy: L0 files are flushed MemTables (overlapping key ranges), L1+ files are non-overlapping within a level. The size ratio between adjacent levels is 10x. Compaction priority is determined by a score = (level_size / target_size), and the level with the highest score is compacted first. The manifest is a write-ahead log of `VersionEdit` records; recovery replays this log to reconstruct the level layout.

---

### Week 4 (15 Jun – 21 Jun): Raft Consensus with Leader Leases

**Goals:** Replicate the storage engine's state across a 3-node cluster with linearizable writes and lease-based linearizable reads.

**Deliverables:**
- Raft core: leader election with randomized timeouts, log replication with pipelining, commit index advancement on majority ack
- Leader leases: lease granted on successful heartbeat quorum, lease duration = election timeout × lease factor. Reads served locally during valid lease window
- Raft log persistence: entries stored in a dedicated WAL (separate from the storage engine's WAL)
- Raft snapshot: periodic snapshot of the LSM manifest + current MemTable state, snapshot transfer to lagging followers
- gRPC transport layer between Raft peers (`AppendEntries`, `RequestVote`, `InstallSnapshot`)
- Apply loop: committed log entries are applied to the local storage engine's write path
- Unit tests: leader election under network partition, log consistency after leader change, lease expiry triggers step-down

**Technical Detail:**  
Leader leases rely on a bounded clock-skew assumption: if the maximum clock skew across nodes is δ, the lease duration is set to `election_timeout - δ`. The leader steps down if it cannot confirm its lease before expiry. This is the TrueTime-lite approach used by CockroachDB. Raft log entries carry the transaction's `WriteBatch` as payload, so the apply loop simply calls `StorageEngine.WriteBatch`.

---

### Week 5 (22 Jun – 28 Jun): MVCC + Snapshot Isolation

**Goals:** Layer multi-version concurrency control on top of the storage engine. Transactions read from a consistent snapshot and detect write-write conflicts at commit time.

**Deliverables:**
- Hybrid logical clock (HLC) for timestamp generation: physical component (wall clock) + logical counter for causal ordering within the same millisecond
- MVCC version chain: each key stores a linked list of `(timestamp, value, is_tombstone)` tuples, ordered by timestamp descending
- Snapshot read: `Get(key, timestamp)` returns the latest version with `ts ≤ timestamp`
- Transaction manager: `Begin` acquires a snapshot timestamp, `Commit` acquires a commit timestamp, checks the write-set for conflicts (any key in the write-set was written by another committed transaction with a timestamp between the snapshot and commit timestamps → abort)
- Garbage collection: background thread prunes versions older than `min(active_snapshot_timestamps)`, runs on a configurable interval
- Read-only transactions: zero-overhead, never abort, no conflict detection needed
- Unit tests: snapshot isolation anomaly tests (write skew is permitted, dirty reads and non-repeatable reads are not), concurrent transaction conflict detection, GC correctness (no live version pruned)

**Technical Detail:**  
Write-conflict detection uses an in-memory committed transaction timestamp index: for each key, track the most recent commit timestamp. At commit time, for each key *k* in the write-set, check if `latest_commit_ts[k] > snapshot_ts`. If yes, abort. This is O(write-set size) per commit. The version chain is stored inline in the LSM-tree: the key is encoded as `(user_key, ~timestamp)` so that versions sort newest-first within the same user key. The `~` denotes bitwise inversion for descending sort in an ascending comparator.

---

### Week 6 (29 Jun – 5 Jul): Compute-Storage Disaggregation

**Goals:** Decouple the transaction coordinator from the storage engine into separate processes communicating over gRPC. Demonstrate independent scaling of compute and storage.

**Deliverables:**
- Storage server: standalone process exposing the `StorageEngine` gRPC service (WriteBatch, Get, Scan, Snapshot)
- Compute server: standalone process hosting the transaction coordinator, MVCC conflict detection, and Raft proposer
- Service discovery: compute nodes discover storage nodes via a static configuration file (sufficient for a 3-node prototype; extensible to etcd-based discovery)
- Connection pooling and multiplexing between compute and storage layers (gRPC channel with configurable max concurrent streams)
- Request routing: client → compute leader (via Raft leader discovery) → storage node (local to the Raft replica)
- Latency overhead measurement: compare single-process (co-located) vs. disaggregated deployment, quantify the gRPC serialization and network hop cost
- Integration tests: full transaction lifecycle (begin → put → get → commit) across disaggregated processes, follower read from storage node after leader commits

**Technical Detail:**  
Each Raft replica co-locates with a storage node (same machine, different process). The compute layer on the leader proposes a Raft entry; once committed, every replica's apply loop sends a `WriteBatch` RPC to its local storage node. This keeps the cross-network hop count to one (compute → storage, on localhost). In a production extension, storage nodes could be on remote machines with a replication protocol at the storage layer itself, but for this project the Raft layer handles replication.

---

### Week 7 (6 Jul – 12 Jul): Fault Injection + Linearizability Verification

**Goals:** Prove the system is correct under adversarial conditions. Inject faults and verify that the history of operations satisfies linearizability.

**Deliverables:**
- Fault injection framework:
  - **Network partitions**: iptables-based (or gRPC interceptor-based) partition of arbitrary node subsets, heal after configurable duration
  - **Node crashes**: SIGKILL + restart with WAL/Raft log recovery
  - **Disk faults**: inject I/O errors on SSTable reads to test graceful degradation
  - **Clock skew**: inject artificial clock offsets to test leader lease correctness
- Linearizability checker: record a history of all client operations `(invoke, ok/fail, key, value, timestamp)`, feed into a linearizability verifier (Wing & Gong algorithm or Porcupine-style checker)
- Test scenarios:
  - Leader crash during commit → verify no committed write is lost, no uncommitted write is visible
  - Network partition isolating the leader → verify new leader is elected, stale leader's lease expires, no split-brain reads
  - Concurrent transactions under partition → verify snapshot isolation invariants hold
  - Storage node crash and recovery → verify WAL replay restores consistent state
- Chaos test harness: automated test runner that cycles through fault scenarios, collects operation histories, and runs the linearizability checker
- Report: documented results for each fault scenario with pass/fail and latency impact

**Technical Detail:**  
The linearizability checker models each operation as an interval `[invocation_time, response_time]` and checks whether there exists a sequential ordering of operations consistent with their real-time ordering and return values. For a key-value store, the specification is simple: `Get(k)` returns the value of the most recent `Put(k, v)` in the linearization. The checker runs in O(n!) worst case but is tractable for histories of ~10K operations with pruning. [Porcupine](https://github.com/anishathalye/porcupine) is used as the linearizability checker — it is Go-native and integrates directly into `go test` as a test helper, with a `porcupine.KvModel` specification.

---

### Week 8 (13 Jul – 24 Jul): Performance, Documentation, Final Delivery

**Goals:** Optimize hot paths, complete all documentation, produce the final demo and report.

**Deliverables:**

**Performance (13 Jul – 17 Jul):**
- Benchmark suite: YCSB-like workloads (A: 50/50 read/write, B: 95/5 read/write, C: 100% read, F: read-modify-write)
- Profiling: CPU (`go tool pprof` flamegraphs) and I/O (`iostat`) profiling under each workload, with `runtime/trace` for goroutine scheduling analysis
- Targeted optimizations based on profiling results (candidates: batch commit coalescing, read-path bloom filter short-circuit, compaction scheduling tuning, `sync.Pool` for buffer reuse)
- Performance observations report: throughput (ops/sec), latency percentiles (p50, p95, p99), write amplification, space amplification, plotted across workloads and cluster sizes (1, 3 nodes)

**Documentation and Delivery (18 Jul – 24 Jul):**
- Architecture diagram (draw.io/Excalidraw export as SVG + PNG)
- API documentation: gRPC service definitions with protobuf comments, client SDK usage examples
- Setup and deployment guide: single-command Docker Compose for 3-node cluster, configuration knobs documented
- Final report (8–10 pages): problem statement, architecture, implementation details, correctness verification results, performance observations, future work
- Presentation (15–20 slides): system overview, demo walkthrough, key design decisions, results
- Demo video: 5-minute walkthrough showing cluster setup, write/read transactions, fault injection, and linearizability check pass

---

## 4. Scope Tiers (P0 / P1 / P2)

Given the 8-week timeline, features are classified into three priority tiers. P0 items define the minimum viable system that demonstrates all five architectural pillars. P1 items add production credibility. P2 items are stretch goals attempted only if the schedule permits. If a week runs over, P2 items are cut first, then P1 items within that week, preserving P0 delivery.

### P0 — Must Ship

These are non-negotiable. Without them, the project does not demonstrate the stated scope.

| Component | P0 Scope |
|---|---|
| **WAL** | Append-only log, `fsync`-per-batch, CRC32 checksums, crash recovery with torn-write detection |
| **MemTable** | Concurrent skip list, dual-buffer (active + immutable), `Put`/`Get`/`Delete` |
| **SSTable** | Block-based format, LZ4 compression, SSTable writer + reader, trailing index block |
| **Bloom Filters** | Per-SSTable partitioned Bloom filter, integrated into read path |
| **Compaction** | L0 → L1 compaction with size-ratio trigger, manifest tracking, basic multi-way merge |
| **Raft** | Leader election, log replication, commit on majority ack, persistent Raft log, gRPC transport |
| **MVCC** | HLC timestamps, versioned key encoding (`user_key \|\| ~timestamp`), point `Get` at snapshot timestamp, write-write conflict detection, transaction `Begin`/`Commit`/`Abort` |
| **Disaggregation** | Separate compute and storage processes, `StorageEngine` gRPC service, basic request routing |
| **Fault Injection** | Network partitions (gRPC interceptor), node crashes (SIGKILL + restart), linearizability verification with Porcupine |
| **Testing** | Unit tests per component, integration test for full transaction lifecycle, >80% coverage on core modules |
| **Docs** | README, Docker Compose for 3-node cluster, final report, demo video |

### P1 — Should Ship

These elevate the project from "correct prototype" to "credible systems engineering." Cut only if a P0 item slips by more than 2 days.

| Component | P1 Scope | Depends On |
|---|---|---|
| **Compaction** | Leveled compaction across L1+ (non-overlapping key ranges per level), compaction picker with score-based priority, rate limiter, write stall back-pressure | P0 Compaction |
| **Leader Leases** | Time-bounded lease on heartbeat quorum, local linearizable reads during valid lease, step-down on lease expiry | P0 Raft |
| **MVCC GC** | Background goroutine pruning versions older than oldest active snapshot, rate-limited | P0 MVCC |
| **Scan with MVCC** | Range scan at snapshot timestamp, skip old versions during iteration | P0 MVCC |
| **Disaggregation polish** | Connection pooling, multiplexed gRPC channels, latency overhead measurement (co-located vs. disaggregated) | P0 Disaggregation |
| **Block Cache** | Sharded LRU block cache for SSTable reads | P0 SSTable |
| **Clock Skew Injection** | Artificial clock offsets to test leader lease correctness under skew | P1 Leader Leases |
| **YCSB Benchmarks** | Workload A/B/C/F benchmarks with throughput and latency percentile reporting | P0 complete |

### P2 — Nice to Have

Attempted only if weeks 1–7 complete on schedule. Documented as "future work" if not delivered.

| Component | P2 Scope | Why Deferrable |
|---|---|---|
| **Raft `InstallSnapshot`** | Snapshot transfer to lagging followers | On a 3-node localhost cluster, followers rarely lag enough to need this. Log replay suffices. |
| **Raft Membership Changes** | Single-node joint-consensus add/remove | Fixed 3-node cluster for the prototype. Membership changes are a self-contained extension. |
| **Disk Fault Injection** | Injected I/O errors on SSTable reads | Network partitions + SIGKILL cover the critical correctness properties. Disk faults test graceful degradation, not safety. |
| **SSTable Compression Tuning** | Configurable compression (LZ4 vs. Snappy vs. ZSTD), per-level compression policies | LZ4-only is sufficient for the prototype. |
| **Shared-Memory Transport** | Replace gRPC-over-UDS with shared-memory for co-located compute-storage | Optimization that doesn't affect correctness. Document as a future work item with expected latency improvement. |

### Descope Protocol

If any week's P0 deliverables are not complete by end-of-day Friday:

1. **Saturday**: Complete remaining P0 items. Cut all P2 items from the current and subsequent weeks.
2. **Sunday weekly update**: Document what was cut and why. Adjust subsequent weeks' scope in the plan.
3. **If P0 slips into the next week**: Compress the next week by cutting its P1 items. Escalate in the weekly update with a revised timeline.

This ensures the final system is always **correct and complete across all five pillars**, even if some depth is traded for schedule.

---

## 5. Review and Submission Timeline

| Event | Date | Detail |
|---|---|---|
| Weekly progress update | Every Sunday | Written update: what was completed, what's blocked, plan for next week. Pushed to GitHub as `docs/weekly/week-N.md`. |
| Week 1 check-in | 31 May (Sun) | WAL + MemTable demo, code review of storage foundation |
| Week 4 mid-point review | 21 Jun (Sun) | Raft replication demo (3-node cluster, leader election, replicated writes). Architecture review with mentor. Mid-point self-assessment. |
| Week 7 pre-final review | 12 Jul (Sun) | Fault injection results, linearizability report. Identify remaining gaps. |
| Final submission | 23 Jul (Thu) | All deliverables: GitHub repo, report, presentation, demo video. |
| Final presentation | 24 Jul (Fri) | Live demo + Q&A. |

---

## 6. Expected Deliverables

| # | Deliverable | Description | Target Date |
|---|---|---|---|
| 1 | **GitHub Repository** | Monorepo with clean commit history, CI (GitHub Actions: build, test, lint), branch protection on `main`. | Continuous |
| 2 | **Architecture Diagram** | Multi-layer diagram (as described in §3.6), exported as SVG/PNG, embedded in README. | Week 8 |
| 3 | **Setup & Deployment Docs** | `README.md` with quickstart, `docker-compose.yml` for 3-node cluster, configuration reference. | Week 8 |
| 4 | **API Documentation** | Protobuf service definitions with inline documentation, generated HTML docs (protoc-gen-doc). | Week 8 |
| 5 | **Weekly Progress Updates** | 8 weekly markdown reports in `docs/weekly/`. | Every Sunday |
| 6 | **Final Report** | 8–10 page technical report covering architecture, implementation, correctness, and performance. | 23 Jul |
| 7 | **Final Presentation** | 15–20 slide deck. Sections: motivation, architecture, Raft + MVCC deep-dive, fault injection results, demo, future work. | 23 Jul |
| 8 | **Demo Walkthrough** | 5-minute recorded video: cluster bootstrap → transactions → fault injection → linearizability verification. | 23 Jul |
| 9 | **Test Suite** | Unit tests (per-component), integration tests (cross-layer), chaos tests (fault injection). Target: >80% line coverage on core modules. | Continuous |
| 10 | **Docker Deployment** | Multi-stage Dockerfile, `docker-compose.yml` with 3 replicas, persistent volumes, health checks. | Week 6 |
| 11 | **Performance Observations** | Benchmark results under YCSB workloads, latency/throughput charts, write amplification measurements. | Week 8 |

---

## 7. Risk Assessment

### Risk 1: Raft Implementation Complexity Exceeds Timeline

**Likelihood:** Medium  
**Impact:** High — Raft is on the critical path; delays cascade into MVCC and fault injection.  
**Mitigation:** The Raft implementation reuses architectural patterns from the candidate's prior Raft project (21K msgs/sec). The design will be scoped to single-decree membership changes (no joint consensus) and a fixed 3-node cluster. Pre-vote protocol is deferred to avoid scope creep. If the implementation falls behind by more than 3 days, a pre-built Raft library (`hashicorp/raft` or `etcd/raft`) will be integrated as a fallback, with documentation of the tradeoff.

### Risk 2: MVCC Garbage Collection Causes Latency Spikes

**Likelihood:** Medium  
**Impact:** Medium — GC pauses can cause p99 latency spikes during sustained write workloads.  
**Mitigation:** GC runs on a dedicated background goroutine with a rate limiter (max versions pruned per cycle). GC is triggered only when the version count for a key exceeds a threshold (default: 100) or the oldest active snapshot is older than 60 seconds. If GC becomes a bottleneck, it will be deferred to compaction (piggyback version pruning on SSTable rewrites), amortizing the cost.

### Risk 3: Linearizability Checker Performance on Large Histories

**Likelihood:** Low  
**Impact:** Medium — the checker may not terminate in reasonable time for histories with high concurrency.  
**Mitigation:** Histories are bounded to 10K operations per test run. The checker uses the Porcupine algorithm with O(n) best-case partitioning by key (each key's sub-history is checked independently, since the specification is per-key). For histories that exceed the checker's budget (30-second timeout), the test logs a warning and the history is manually inspected.

### Risk 4: Disaggregation Overhead Dominates Latency Budget

**Likelihood:** Medium  
**Impact:** Low — this is a design exploration project, not a latency-critical production system.  
**Mitigation:** Compute and storage processes are co-located on the same machine, communicating over Unix domain sockets (gRPC supports UDS) to minimize serialization overhead. The overhead is measured and documented as a data point. If the overhead exceeds 2x compared to the co-located baseline, the report will analyze the breakdown (serialization, context switch, network stack) and propose optimizations (shared-memory transport, batched RPCs).

---

## 8. Success Metrics

### 7.1 Correctness Metrics

| Metric | Target | Measurement Method |
|---|---|---|
| Linearizability | 100% pass on all recorded histories | Wing & Gong / Porcupine checker on operation histories under fault injection |
| Snapshot isolation | No dirty reads, no non-repeatable reads | Targeted anomaly tests (concurrent conflicting transactions) |
| WAL durability | Zero data loss after SIGKILL | Kill process mid-write, restart, verify all acknowledged writes are present |
| Raft safety | No committed entry is lost after leader failure | Kill leader after commit, verify new leader's log contains the entry |
| Bloom filter FPR | < 1.5% empirical | Insert N keys, probe N non-existent keys, measure false positive rate |

### 7.2 Performance Metrics

| Metric | Target | Measurement Method |
|---|---|---|
| Write throughput (single node) | > 50K ops/sec (256-byte values) | Sequential `Put` benchmark, batched commits |
| Read throughput (single node) | > 100K ops/sec (hot keys in block cache) | Sequential `Get` benchmark, pre-loaded dataset |
| Read latency (p99, single node) | < 5 ms | YCSB workload B, measured over 60 seconds |
| Write amplification | < 15x (leveled compaction) | Total bytes written to disk / total bytes written by client |
| Raft commit latency (3-node) | < 10 ms (p50) on localhost cluster | Measure time from `Propose` to `Apply` for 10K entries |
| Disaggregation overhead | < 2x latency vs. co-located baseline | Compare p50 `Put` latency in co-located vs. disaggregated deployment |

### 7.3 Engineering Quality Metrics

| Metric | Target |
|---|---|
| Test coverage (core modules) | > 80% line coverage (`go test -cover`) |
| CI pipeline | All tests pass on every push to `main` (including `go test -race`) |
| Documentation completeness | All public APIs documented, all configuration knobs documented |
| Code review | Self-reviewed; all weekly PRs have a written summary |

---
