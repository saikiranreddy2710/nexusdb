# NexusDB

A high-performance distributed database written in Rust. Combines the reliability of traditional SQL databases with modern storage engines, advanced caching, and built-in security.

## What is NexusDB?

NexusDB started as a learning project to understand how databases work from the ground up. It has grown into a serious database engine with an LSM-tree key-value store, research-grade caching algorithms, production-level security, and a full SQL query engine — all written from scratch in Rust.

The codebase currently passes **1,040+ tests** across 17 crates.

## Features

### Storage Engines

**SageTree** — A Bw-tree variant with delta chains and MVCC support for transactional workloads.

**LSM-Tree KV Store** — A RocksDB-style log-structured merge-tree for high write throughput:
- Concurrent skip list memtable with lock-free reads
- SSTable format with block-based layout, prefix compression, and bloom filters
- Leveled compaction with background scheduling
- Table cache and block cache for reduced disk I/O
- Manifest-based version management for atomic state transitions

**OS-Level Optimizations:**
- `io_uring` for batched async I/O on Linux (SQPOLL mode, registered buffers)
- NUMA-aware memory allocation via `mbind` and CPU-to-node mapping
- SIMD-accelerated key comparison (AVX2/SSE2/NEON), CRC32C, and bloom filter probing

### Caching System

Goes beyond basic LRU with research-grade algorithms:

| Algorithm | What it does |
|-----------|-------------|
| **W-TinyLFU** | Window + probation + protected segments with frequency-based admission (Caffeine-style) |
| **Tiered Cache** | Automatic L1/L2/L3 hot-warm-cold promotion and demotion |
| **ARC** | Adaptive Replacement Cache balancing recency and frequency |
| **LRU-K** | Eviction based on K-th most recent access (scan-resistant) |
| **Semantic Microcaching** | Query normalization and structural fingerprinting for AI agent workloads |
| **Predictive Prefetch** | Markov chain model predicting future page accesses |
| **LLM KV Cache** | Tensor-aware cache for transformer key-value states (RAG/inference) |

### Security

Zero Trust architecture with defense in depth:

- **Authentication:** Argon2id password hashing, JWT bearer tokens, user management
- **Authorization:** Role-based access control (RBAC), per-table permissions, row-level security policies
- **Encryption at rest:** AES-256-GCM via the `ring` crate, with nonce and AAD support
- **Audit logging:** Tamper-proof hash chain (SHA-256) — modifying any entry breaks the chain
- **TLS/mTLS:** Configuration for TLS 1.3 transport security and mutual certificate authentication

### SQL Engine

- Hand-written SQL parser (no generators)
- Cost-based optimizer with predicate pushdown and projection pruning
- Vectorized pull-based execution
- Plan caching with selective invalidation
- Result caching with automatic table-dependency tracking

### Distributed Consensus

- Full Raft implementation (leader election, log replication, membership changes)
- Leader leases for low-latency local reads
- TCP and in-memory transport backends

### Transactions

- MVCC with hybrid logical clocks for snapshot isolation
- Two-phase locking with wait-for-graph deadlock detection
- Serializable snapshot isolation validation

## Quick Start

```bash
# Clone and build
git clone https://github.com/saikiranreddy2710/nexusdb.git
cd nexusdb
cargo build --release

# Start the server
./target/release/nexusd

# Connect with the CLI (in another terminal)
./target/release/nexus
```

```sql
NexusDB> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT);
OK

NexusDB> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
Inserted 1 row

NexusDB> SELECT * FROM users WHERE id = 1;
id | name  | email
---+-------+------------------
1  | Alice | alice@example.com
(1 row, 0.42ms)
```

## Project Structure

```
crates/
├── nexus-common      Shared types, config, errors, NUMA allocator, SIMD utilities
├── nexus-kv          LSM-tree key-value store (memtable, SSTable, compaction, caches)
├── nexus-storage     SageTree B-tree, buffer pool, page layout, io_uring file I/O
├── nexus-cache       LRU/ARC/LRU-K/W-TinyLFU, bloom filters, semantic cache, prefetch, LLM KV
├── nexus-security    Authentication, authorization, AES-256-GCM encryption, audit logging, TLS
├── nexus-wal         Write-ahead log with group commit and checkpointing
├── nexus-txn         Transaction manager, 2PL locking, deadlock detection
├── nexus-mvcc        Multi-version concurrency control, hybrid logical clocks, GC
├── nexus-raft        Distributed consensus with leader leases
├── nexus-sql         SQL parser, logical/physical planner, optimizer, executor
├── nexus-query       Query planning and operator framework
├── nexus-server      Database server, session management, gRPC service
├── nexus-client      Rust client library
├── nexus-proto       Protocol Buffers / gRPC definitions
├── nexus-cli         Interactive command-line client (REPL)
├── nexus-test        Integration and end-to-end tests
└── nexus-bench       Criterion-based microbenchmarks
```

## Architecture

```
                          ┌──────────────────────┐
                          │     Client Layer      │
                          │  CLI · SDK · gRPC API │
                          └──────────┬───────────┘
                                     │
                          ┌──────────▼───────────┐
                          │      Security        │
                          │ TLS · Auth · RBAC    │
                          │ Encryption · Audit   │
                          └──────────┬───────────┘
                                     │
              ┌──────────────────────▼──────────────────────┐
              │                SQL Engine                    │
              │  Parser → Optimizer → Physical Planner      │
              │                  → Executor                  │
              └──────┬──────────────┬──────────────┬────────┘
                     │              │              │
              ┌──────▼──────┐ ┌────▼────┐ ┌───────▼───────┐
              │  Cache Layer │ │  Txn    │ │     Raft      │
              │  Plan/Result │ │ Manager │ │   Consensus   │
              │  W-TinyLFU   │ │ 2PL/SSI │ │  (distributed)│
              └──────┬──────┘ └────┬────┘ └───────────────┘
                     │             │
              ┌──────▼─────────────▼─────────────┐
              │         Storage Engines           │
              │  SageTree (B-tree) · LSM-KV       │
              │  Buffer Pool · Block/Table Cache  │
              └──────────────┬────────────────────┘
                             │
              ┌──────────────▼────────────────────┐
              │           OS Layer                 │
              │  io_uring · NUMA · SIMD · WAL     │
              └───────────────────────────────────┘
```

## SQL Support

**Supported:**
- `CREATE TABLE` / `DROP TABLE` with `IF EXISTS` / `IF NOT EXISTS`
- `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `WHERE` with `AND`, `OR`, comparisons, `IS NULL`, `IS NOT NULL`
- `ORDER BY`, `LIMIT`
- `GROUP BY` with `HAVING`
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `SHOW TABLES`, `SHOW DATABASES`, `DESCRIBE`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`
- Data types: `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`, `VARCHAR(n)`
- Primary key constraints

**Planned:**
- JOINs (parsed, execution in progress)
- Secondary indexes
- Subqueries and CTEs
- Window functions

## Running Tests

```bash
# All tests (1,040+)
cargo test --all

# Specific crate
cargo test -p nexus-kv
cargo test -p nexus-security
cargo test -p nexus-cache

# With output
cargo test -p nexus-kv -- --nocapture
```

## Configuration

Server configuration via `nexusdb.toml`:

```toml
[server]
host = "127.0.0.1"
port = 5432
max_connections = 100

[storage]
data_dir = "./data"
buffer_pool_size = 134217728  # 128 MB

[wal]
enabled = true
sync_mode = "fsync"
```

Or command line:

```bash
nexusd --port 5432 --data-dir ./data --memory 256
```

## Client Library

```rust
use nexus_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("localhost:5432").await?;

    client.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").await?;
    client.execute("INSERT INTO users VALUES (1, 'Bob')").await?;

    let rows = client.query("SELECT * FROM users").await?;
    for row in rows {
        println!("{:?}", row);
    }

    Ok(())
}
```

## Performance

Benchmarks run on an M-series MacBook (in-memory, `cargo bench -p nexus-bench`):

- SageTree point lookups: ~3.5-5.5M keys/sec
- SageTree sequential inserts: ~600K keys/sec
- End-to-end INSERT + SELECT: ~150-180K rows/sec

These are single-node in-memory figures. Real-world performance depends on workload, data size, and disk characteristics.

## Research References

Papers and resources that influenced the design:

- [The Bw-Tree](https://www.microsoft.com/en-us/research/publication/the-bw-tree-a-b-tree-for-new-hardware/) — Delta chain architecture for SageTree
- [Raft Consensus](https://raft.github.io/raft.pdf) — Distributed consensus implementation
- [Architecture of a Database System](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf) — Overall system design
- [A Critique of ANSI SQL Isolation Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf) — Transaction isolation
- [AlayaDB](https://arxiv.org/abs/2504.10326) — LLM KV cache management
- [Agent-First Database Design](https://arxiv.org/abs/2509.00997) — Semantic microcaching
- [W-TinyLFU](https://arxiv.org/abs/1512.00727) — Admission-optimized caching

## Contributing

Issues, bug reports, and suggestions are welcome. Open an issue or submit a pull request.

## License

Apache-2.0
