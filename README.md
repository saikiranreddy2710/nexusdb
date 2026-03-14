# NexusDB

A research-grade next-generation database written in Rust, combining high performance, ACID reliability, military-grade security, and AI-native capabilities.

## What is this?

NexusDB is a modern distributed database built from scratch. It combines the ACID guarantees of traditional SQL databases with horizontal scalability, a Zero Trust security model, and foundations for AI-native workloads (vector search, learned indexes).

The goal: understand how databases really work under the hood -- not just the theory, but the actual implementation details that make them fast, reliable, and secure.

## Features

### Storage Engine (SageTree)
- SageTree B-tree variant with Bw-tree-style delta chains and fractional cascading
- MVCC-aware index structure designed for snapshot isolation
- Bloom filter integration for fast negative key lookups
- Buffer pool with clock-sweep eviction
- **Disk persistence** via `Pager` trait abstraction (`MemoryPager` / `FilePager`)
- Write-ahead logging for durability

### LSM-KV Engine
- Log-Structured Merge Tree key-value store with leveled compaction
- WAL with CRC32 checksummed records for crash recovery
- Manifest persistence and replay on startup
- Background compaction with condvar signaling

### Distributed Consensus (Raft++)
- Full Raft implementation with leader election
- Parallel commit optimization for better P99 latency
- Leader leases to reduce read latency

### SQL Query Engine
- Hand-written SQL parser (no parser generators)
- Cost-based query optimizer with rule-based rewrites
- Vectorized execution using a pull-based iterator model
- Query plan caching for repeated queries
- Result caching with automatic invalidation on DML/DDL

### Transactions
- MVCC for snapshot isolation
- Two-phase locking with deadlock detection
- Serializable isolation level support

### Security (Zero Trust)
- **Authentication**: PBKDF2-HMAC-SHA256 password hashing (10 000 iterations), API keys with O(1) reverse-index lookup, HMAC-SHA256 JWT tokens with constant-time signature verification
- **Authorization**: Role-Based Access Control (RBAC) with role inheritance, table/database/global permission granularity, built-in protected superuser role
- **Audit**: Tamper-proof hash-chained audit log (SHA-256), thread-safe with single-lock atomicity, integrity verification survives entry eviction, SIEM-exportable JSON
- **Transport**: TLS 1.2+ encryption, mutual TLS (mTLS) client certificate verification
- **Account security**: Automatic lockout after 5 failed attempts, password change support, empty password rejection

### gRPC Network Layer
- Full gRPC API: Execute, ExecuteStream, Prepare, BeginTransaction, Commit, Rollback, Ping, ServerInfo
- All endpoints authenticate requests (enforced or permissive mode)
- Proper error codes: `AUTHENTICATION_FAILED` (41), `AUTHORIZATION_FAILED` (42)
- Audit logging of all auth events with client IP extraction
- TLS/mTLS configurable from PEM files or bytes

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
├── nexus-cache      # Bloom filters, LRU/ARC caches, query result cache
├── nexus-storage    # SageTree index, buffer pool, page layout, io_uring
├── nexus-buffer     # Standalone buffer pool components
├── nexus-wal        # Write-ahead log with group commit
├── nexus-kv         # LSM-tree key-value engine with WAL + compaction
├── nexus-raft       # Distributed consensus (Raft++)
├── nexus-sql        # Parser, planner, optimizer, executor
├── nexus-query      # Logical/physical query planning and operators
├── nexus-txn        # Transaction manager (2PL, deadlock detection)
├── nexus-mvcc       # Multi-version concurrency control
├── nexus-security   # Authentication, RBAC authorization, audit log
├── nexus-server     # Database server, session management, gRPC API
├── nexus-client     # Rust client library
├── nexus-proto      # gRPC protocol definitions (protobuf)
├── nexus-cli        # Command-line interface
├── nexus-test       # Integration and end-to-end tests
└── nexus-bench      # Microbenchmarks and performance experiments
```

## Architecture

```
┌─────────────┐     gRPC/TLS     ┌──────────────────────────────────────┐
│   Client    │ ────────────────▶│           Server (nexusd)            │
│  (nexus)    │                  │                                      │
└─────────────┘                  │  ┌──────────────────────────────┐    │
                                 │  │     Security Layer            │    │
                                 │  │  Authentication (PBKDF2/JWT) │    │
                                 │  │  Authorization (RBAC)        │    │
                                 │  │  Audit Log (hash-chained)    │    │
                                 │  └──────────────┬───────────────┘    │
                                 │                 │                    │
                                 │  ┌──────────────▼───────────────┐    │
                                 │  │     SQL Engine               │    │
                                 │  │  Parser → Optimizer →        │    │
                                 │  │  Physical Plan → Executor    │    │
                                 │  └──────────────┬───────────────┘    │
                                 │                 │                    │
                                 │  ┌──────┬───────▼──────┬────────┐   │
                                 │  │ Txn  │   Storage    │  Raft  │   │
                                 │  │ Mgr  │   Engine     │Consensu│   │
                                 │  │(MVCC)│(SageTree+WAL)│   s    │   │
                                 │  └──────┴──────────────┴────────┘   │
                                 └──────────────────────────────────────┘
```

## What Works

- CREATE/DROP TABLE with IF EXISTS/IF NOT EXISTS
- INSERT, SELECT, UPDATE, DELETE
- WHERE clauses with AND/OR and comparison operators
- ORDER BY and LIMIT
- COUNT, SUM, AVG, MIN, MAX aggregations
- GROUP BY with HAVING
- Primary key constraints
- Multi-database support (CREATE/DROP/USE DATABASE, SHOW DATABASES)
- SHOW TABLES, DESCRIBE TABLE
- Basic query optimization (predicate pushdown, projection pruning)
- Query plan caching and result caching
- Disk persistence for SageTree (via FilePager) and LSM-KV
- Crash recovery (WAL replay for both engines)
- TLS/mTLS encrypted connections
- Authentication (PBKDF2 passwords, API keys, JWT tokens)
- RBAC authorization with role inheritance
- Tamper-proof audit logging

## Running Tests

```bash
# Run all tests
cargo test --all

# Run with output
cargo test --all -- --nocapture

# Run specific crate
cargo test -p nexus-security

# Run specific test
cargo test -p nexus-sql test_select_where
```

Currently at **1020+ passing tests** across 18 crates.

## Configuration

The server can be configured via `nexusdb.toml`:

```toml
host = "127.0.0.1"
port = 5432
data_dir = "./data"
buffer_pool_mb = 128
query_timeout_secs = 300

# TLS (optional)
tls_cert = "/path/to/server.crt"
tls_key = "/path/to/server.key"
tls_ca_cert = "/path/to/ca.crt"  # enables mTLS
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

## Security Model

NexusDB implements a Zero Trust security model:

1. **Never trust, always verify** -- every gRPC request is authenticated
2. **Least privilege** -- RBAC ensures users only access what they need
3. **Assume breach** -- tamper-proof audit trail detects post-compromise analysis

### Authentication

| Method | Description |
|--------|-------------|
| PBKDF2-HMAC-SHA256 | Password auth with 10 000 iterations, random 16-byte salt |
| API Keys | `nxk_` prefixed tokens, O(1) lookup via reverse index |
| JWT (HMAC-SHA256) | Stateless tokens with constant-time signature verification |

Account lockout triggers after 5 consecutive failed attempts.

### Authorization (RBAC)

Permissions can be granted at three levels:
- **Global**: server-wide (`Permission::Global`)
- **Database**: all tables in a database (`Permission::Database`)
- **Table**: specific table (`Permission::Table`)

Roles support inheritance. The built-in `superuser` role cannot be dropped.

### Audit Log

Every query, authentication event, schema change, and data modification is recorded in a hash-chained log. Each entry includes a SHA-256 hash linking it to the previous entry, making any tampering detectable via `verify_integrity()`.

## Performance

`nexus-bench` contains Criterion-based microbenchmarks for the storage engine (`SageTree`), SQL/database layer, and caches (LRU/ARC/Bloom).

On a recent M-series MacBook (bench profile, in-memory, `cargo bench -p nexus-bench -- --quick`), representative numbers are:

- SageTree point lookups: **3.5-5.5M keys/sec** (depending on tree size)
- SageTree sequential inserts: **~0.6M keys/sec**
- End-to-end `INSERT + SELECT` workload: **~150-180k rows/sec**

These are single-node, in-memory figures primarily useful for regression tracking.

## References

Papers and resources that helped:

- [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Architecture of a Database System](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf)
- [A Critique of ANSI SQL Isolation Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
- [The Design and Implementation of Modern Column-Oriented Database Systems](https://stratos.seas.harvard.edu/files/stratos/files/columnstoresfntdbs.pdf)
- [OWASP Key Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Key_Management_Cheat_Sheet.html)
- [PBKDF2 (RFC 8018)](https://datatracker.ietf.org/doc/html/rfc8018)

## Contributing

This is primarily a learning project, but if you find bugs or have suggestions, feel free to open an issue.

## License

MIT
