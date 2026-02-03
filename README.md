# NexusDB

A distributed NewSQL database written in Rust. Built for learning and experimentation.

## What is this?

NexusDB is my attempt at building a modern distributed database from scratch. It combines the ACID guarantees of traditional SQL databases with the horizontal scalability you'd expect from NoSQL systems.

The goal was to understand how databases really work under the hood — not just the theory, but the actual implementation details that make them fast and reliable.

## Features

**Storage Engine (SageTree)**
- SageTree B-tree variant with Bw-tree–style delta chains and fractional cascading
- MVCC-aware index structure designed for snapshot isolation
- Bloom filter integration for fast negative key lookups
- Buffer pool with clock-sweep eviction
- Write-ahead logging for durability

**Distributed Consensus (Raft++)**
- Full Raft implementation with leader election
- Parallel commit optimization for better P99 latency
- Leader leases to reduce read latency

**SQL Query Engine**
- Hand-written SQL parser (no parser generators)
- Cost-based query optimizer with rule-based rewrites
- Vectorized execution using a pull-based iterator model

**Transactions**
- MVCC for snapshot isolation
- Two-phase locking with deadlock detection
- Serializable isolation level support

## Quick Start

```bash
# Clone and build
git clone https://github.com/saikiranreddy2710/nexusdb.git
cd nexusdb
cargo build --release

# Start the server
./target/release/nexusd

# In another terminal, connect with the CLI
./target/release/nexus
```

Once connected:

```sql
NexusDB> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT);
OK

NexusDB> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
Inserted 1 row

NexusDB> SELECT * FROM users;
+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
| 1  | Alice | alice@example.com |
+----+-------+-------------------+
1 row (0.002s)
```

## Project Structure

```
crates/
├── nexus-common     # Shared types, config, errors, memory utilities
├── nexus-cache      # Bloom filters and in-memory caches
├── nexus-storage    # SageTree index, buffer pool, page layout/management
├── nexus-buffer     # Standalone buffer pool components
├── nexus-wal        # Write-ahead log
├── nexus-raft       # Distributed consensus
├── nexus-sql        # Parser, planner, optimizer, executor
├── nexus-query      # Logical/physical query planning and operators
├── nexus-txn        # Transaction manager (2PL, deadlock detection)
├── nexus-mvcc       # Multi-version concurrency control
├── nexus-server     # Database server and session management
├── nexus-client     # Rust client library
├── nexus-proto      # gRPC protocol definitions
├── nexus-cli        # Command-line interface
├── nexus-test       # Integration and end-to-end tests
└── nexus-bench      # Microbenchmarks and performance experiments
```

## Architecture

```
┌─────────────┐     gRPC      ┌──────────────┐
│   Client    │ ───────────── │    Server    │
│  (nexus)    │               │   (nexusd)   │
└─────────────┘               └──────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
               ┌────▼────┐     ┌─────▼─────┐    ┌─────▼─────┐
               │   SQL   │     │    Txn    │    │   Raft    │
               │ Engine  │     │  Manager  │    │ Consensus │
               └────┬────┘     └─────┬─────┘    └───────────┘
                    │                │
               ┌────▼────────────────▼────┐
               │     Storage Engine       │
               │  (SageTree + Buffer Pool)│
               └──────────────────────────┘
```

## What Works

- CREATE/DROP TABLE with IF EXISTS/IF NOT EXISTS
- INSERT, SELECT, UPDATE, DELETE
- WHERE clauses with AND/OR and comparison operators
- ORDER BY and LIMIT
- COUNT, SUM, AVG, MIN, MAX aggregations
- Primary key constraints
- Basic query optimization (predicate pushdown, projection pruning)

## What's Still Missing

- JOINs (parsed but not fully executed yet)
- Secondary indexes
- Disk persistence (currently in-memory only)
- Actual distributed deployment (Raft is implemented but not wired up)
- Authentication

## Running Tests

```bash
# Run all tests
cargo test --all

# Run with output
cargo test --all -- --nocapture

# Run specific test
cargo test -p nexus-sql test_select_where
```

Currently at **840+ passing tests**.

## Configuration

The server can be configured via `nexusdb.toml`:

```toml
[server]
host = "127.0.0.1"
port = 5432

[storage]
data_dir = "./data"
buffer_pool_size = 134217728  # 128MB

[wal]
enabled = true
sync_mode = "fsync"
```

Or via command line:

```bash
nexusd --port 5432 --data-dir ./data
```

## Using the Client Library

```rust
use nexus_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("localhost:5432").await?;
    
    client.execute("CREATE TABLE users (id INT, name TEXT)").await?;
    client.execute("INSERT INTO users VALUES (1, 'Bob')").await?;
    
    let rows = client.query("SELECT * FROM users").await?;
    for row in rows {
        println!("{:?}", row);
    }
    
    Ok(())
}
```

## Performance

`nexus-bench` contains Criterion-based microbenchmarks for the storage engine (`SageTree`), SQL/database layer, and caches (LRU/ARC/Bloom).

On a recent M‑series MacBook (bench profile, in-memory, `cargo bench -p nexus-bench -- --quick`), representative numbers are:

- SageTree point lookups: **3.5–5.5M keys/sec** (depending on tree size)
- SageTree sequential inserts: **~0.6M keys/sec**
- End-to-end `INSERT + SELECT` workload: **~150–180k rows/sec**

These are single-node, in-memory figures primarily useful for regression tracking — real-world performance will depend heavily on workload, data size, and deployment.

## Why Build This?

I wanted to really understand databases, not just use them. Building one from scratch forces you to confront all the hard problems:

- How do you make writes durable without killing performance?
- How do you handle concurrent transactions without corrupting data?
- How do you optimize queries when you don't know the data distribution?
- How do you keep multiple nodes in sync?

Reading papers and books helps, but implementing it yourself is different. You discover all the edge cases and tradeoffs that don't make it into the literature.

## References

Papers and resources that helped:

- [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Architecture of a Database System](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf)
- [A Critique of ANSI SQL Isolation Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
- [The Design and Implementation of Modern Column-Oriented Database Systems](https://stratos.seas.harvard.edu/files/stratos/files/columnstoresfntdbs.pdf)

## Contributing

This is primarily a learning project, but if you find bugs or have suggestions, feel free to open an issue.

## License

MIT
