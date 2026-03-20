# NexusDB Production Roadmap

> A distributed NewSQL database written in Rust — from learning project to production-grade system.

---

## Table of Contents

1. [Current State Assessment](#current-state-assessment)
2. [Critical Gap Analysis](#critical-gap-analysis)
3. [Hybrid Storage Architecture (SageTree + LSM)](#hybrid-storage-architecture)
4. [Wire Protocol & Driver Strategy](#wire-protocol--driver-strategy)
5. [Docker Strategy](#docker-strategy)
6. [Phase 1: Disk Persistence](#phase-1-disk-persistence)
7. [Phase 2: PostgreSQL Wire Protocol](#phase-2-postgresql-wire-protocol)
8. [Phase 3: Query Engine Completeness](#phase-3-query-engine-completeness)
9. [Phase 4: Docker + Deployment](#phase-4-docker--deployment)
10. [Phase 5: Raft Integration (HA)](#phase-5-raft-integration)
11. [Phase 6: Observability + Production Polish](#phase-6-observability--production-polish)
12. [Phase 7: MySQL Wire Protocol + Native SDKs](#phase-7-mysql-wire-protocol--native-sdks)
13. [Execution Order](#execution-order)

---

## Current State Assessment

**Codebase**: ~87K lines of Rust, 19 crates, 1216+ tests passing.

### What's Solid (Production-Worthy)

| Area | Detail |
|------|--------|
| **Basic SQL** | SELECT / INSERT / UPDATE / DELETE with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT / OFFSET, DISTINCT |
| **JOINs** | HashJoin, MergeJoin, NestedLoopJoin — all 3 physical algorithms implemented |
| **Set Operations** | UNION / INTERSECT / EXCEPT (all 6 modes including ALL variants) |
| **Window Functions** | ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE + aggregate OVER() |
| **Authentication** | PBKDF2-HMAC-SHA256 (600K iterations), JWT, API keys, account lockout after 5 failures |
| **RBAC** | Role hierarchy with inheritance, table / database / global permission scopes, cycle detection |
| **Audit Logging** | SHA-256 hash-chained, tamper-proof, JSON export for SIEM integration |
| **TLS / mTLS** | Full TLS and mutual TLS via tonic / rustls |
| **EXPLAIN** | TEXT / VERBOSE / JSON / ANALYZE with per-operator metrics — better than many production DBs |
| **Raft Consensus** | Full protocol: leader election (pre-vote), log replication, heartbeats, leader leases, joint consensus membership changes |
| **Transactions** | BEGIN / COMMIT / ROLLBACK with undo-log rollback for INSERT / UPDATE / DELETE |
| **Constraints** | PRIMARY KEY, UNIQUE, NOT NULL, CHECK, DEFAULT — all enforced at runtime |
| **ALTER TABLE** | ADD / DROP / RENAME COLUMN, RENAME TABLE |
| **Vector Search** | HNSW index with cosine / L2 / inner-product metrics, full CRUD sync |

---

## Critical Gap Analysis

### Must-Fix (Blocking Production Use)

| # | Gap | Severity | Current State |
|---|-----|----------|---------------|
| 1 | ~~No disk persistence~~ | ~~CRITICAL~~ | **RESOLVED.** SageTree FilePager and LSM-KV now wired into the SQL layer. |
| 2 | ~~WAL not wired to DML~~ | ~~CRITICAL~~ | **RESOLVED.** WAL integrated with undo-log rollback for INSERT/UPDATE/DELETE. |
| 3 | **No wire protocol** | CRITICAL | Only gRPC. Cannot use `psql`, pgAdmin, Grafana, DBeaver, ORMs, or any standard database tool. |
| 4 | **Subqueries don't execute** | HIGH | `IN (subquery)`, `EXISTS`, scalar subqueries parsed but physical planner rejects them. Pending. |
| 5 | ~~BTree indexes unused~~ | ~~HIGH~~ | **RESOLVED.** BTree indexes wired into planner with `index_lookup` API. |
| 6 | **Raft not wired to database** | HIGH | Full Raft with TcpTransport, FileStorage, StateMachine trait. But server **never creates a RaftNode**. |

### Important Gaps (Required for Real Applications)

| # | Gap | Severity | Detail |
|---|-----|----------|--------|
| 7 | **Foreign keys not enforced** | MEDIUM | Parsed and stored, but never checked at runtime. No cascading. |
| 8 | **No views** | MEDIUM | No `CREATE VIEW` at any layer. |
| 9 | **CTEs not executed** | MEDIUM | Parsed, logical plan exists, physical planner ignores CTE definitions. |
| 10 | **No VACUUM** | MEDIUM | No dead-row cleanup. MVCC GC exists in nexus-mvcc but not wired. |
| 11 | **No prepared statements** | MEDIUM | gRPC `prepare()` returns "not yet implemented". |

### Nice-to-Have (PG / MySQL Parity)

- Stored procedures / triggers
- Full-text search (tsvector / tsquery / GIN indexes)
- JSON operators (`->`, `->>`, `@>`, `#>`)
- Row-level security policies
- `pg_catalog` / `information_schema` system tables
- Prometheus metrics export
- Slow query log (config field exists, no implementation)
- Server-side connection pooling
- `GRANT` / `REVOKE` SQL syntax (programmatic RBAC works, no SQL interface)

---

## Hybrid Storage Architecture

### Why Hybrid: SageTree (B+Tree) + LSM

NexusDB has **two complete storage engines** currently disconnected from SQL:

| Engine | Crate | Architecture | Best For |
|--------|-------|-------------|----------|
| **SageTree** | `nexus-storage` | B+Tree with delta chains, fractional cascading, FilePager (8KB pages, CRC32, write-through) | Point lookups, range scans, indexes |
| **LSM Engine** | `nexus-kv` | MemTable (lock-free skiplist) + SSTable + leveled compaction + bloom filters | High-throughput writes, sequential ingestion |

### Hybrid Design: "SageLSM"

```
                    ┌─────────────────────────────────────┐
                    │           SQL Engine                 │
                    │   (Parser → Planner → Executor)      │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │        Hybrid Storage API            │
                    │  (unified read / write interface)    │
                    └──┬───────────────────────────────┬──┘
                       │                               │
          ┌────────────▼────────────┐    ┌─────────────▼──────────────┐
          │     Write Path (LSM)    │    │     Read Path (SageTree)   │
          │                         │    │                            │
          │  ┌───────────────────┐  │    │  ┌──────────────────────┐  │
          │  │  Active MemTable  │  │    │  │   Clustered B+Tree   │  │
          │  │  (lock-free       │  │    │  │   (one per table,    │  │
          │  │   skiplist)       │  │    │  │    FilePager-backed)  │  │
          │  └────────┬──────────┘  │    │  └──────────────────────┘  │
          │           │ freeze      │    │                            │
          │  ┌────────▼──────────┐  │    │  ┌──────────────────────┐  │
          │  │ Immutable MemTable│──│────│──│  Secondary Indexes   │  │
          │  │  (flush to tree)  │  │    │  │  (one B+Tree per     │  │
          │  └───────────────────┘  │    │  │   CREATE INDEX)      │  │
          │                         │    │  └──────────────────────┘  │
          └─────────────────────────┘    └────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │   Durability Layer      │
          │                         │
          │  ┌───────────────────┐  │
          │  │    nexus-wal      │  │
          │  │  (ARIES-style,    │  │
          │  │   segmented,      │  │
          │  │   group commit)   │  │
          │  └───────────────────┘  │
          │                         │
          │  ┌───────────────────┐  │
          │  │   Buffer Pool     │  │
          │  │  (clock-sweep     │  │
          │  │   eviction,       │  │
          │  │   dirty tracking) │  │
          │  └───────────────────┘  │
          └─────────────────────────┘
```

### How the Hybrid Works

**Write Path (optimized for throughput):**
1. DML operation arrives (INSERT / UPDATE / DELETE)
2. Write WAL record (nexus-wal) for durability
3. Apply to LSM MemTable (lock-free skiplist — concurrent-safe, O(log n))
4. When MemTable reaches threshold (64MB default), freeze and flush
5. Flush merges MemTable entries into the SageTree B+Tree (not SSTable)
6. B+Tree pages written through FilePager with CRC32 integrity

**Read Path (optimized for latency):**
1. Query arrives (SELECT / point lookup / range scan)
2. Check active MemTable first (most recent writes)
3. Check immutable MemTables (in freeze/flush queue)
4. Check SageTree B+Tree (disk-backed, bloom filter fast-reject)
5. Results merged with newest-wins semantics using sequence numbers

### Why This Is Better Than Either Alone

| Property | Pure B+Tree | Pure LSM | Hybrid SageLSM |
|----------|------------|----------|-----------------|
| Write throughput | Moderate (random I/O) | Excellent (sequential) | **Excellent** (MemTable absorbs) |
| Read latency | Excellent (single tree) | Poor (check multiple levels) | **Excellent** (MemTable + single tree) |
| Space amplification | Low (~1x) | High (1.1-1.5x) | **Low** (no SSTable levels) |
| Write amplification | Moderate (page splits) | High (compaction rewrites) | **Low** (flush directly to tree) |
| Point lookups | O(log n) | O(log n) * L levels | **O(log n)** (bloom + tree) |
| Range scans | Excellent (leaf chain) | Poor (merge across levels) | **Excellent** (leaf chain) |

### Component Reuse Map

| Existing Component | Role in Hybrid | Changes Needed |
|-------------------|----------------|----------------|
| `SageTree` (nexus-storage) | Primary persistent storage per table | Wire FilePager, expose `flush()` |
| `MemTable` + `SkipList` (nexus-kv) | Write buffer (lock-free concurrent writes) | Extract from LsmEngine, use standalone |
| `nexus-wal` | Durability layer | Wire WAL records into DML path |
| `BufferPool` (nexus-storage) | Page cache for SageTree | Implement `Pager` trait backed by BufferPool |
| `FilePager` (nexus-storage) | Disk I/O for B+Tree pages | Already works, integrate with BufferPool |
| `RowEncoder` / `RowDecoder` (nexus-sql) | Row serialization for B+Tree KV storage | Already works, no changes |
| `BloomFilter` (nexus-cache) | Negative lookups on B+Tree | Already in SageTree, no changes |
| `Catalog` (nexus-sql) | Table metadata | Add Serialize / Deserialize, persist to JSON |

### Concurrency Model

- **Writes**: Serialized through single write mutex. WAL → MemTable → done.
- **Reads**: Lock-free. Clone `Arc<MemTable>`, scan skiplist with atomic pointer loads. B+Tree reads through `RwLock`.
- **Flush**: Background thread. Freeze MemTable (atomic swap), iterate sorted entries, batch-insert into SageTree.
- **MVCC**: Sequence numbers from MemTable. SageTree entries carry `(txn_id, lsn)` via `insert_with_txn()`.

---

## Wire Protocol & Driver Strategy

### Recommendation: PostgreSQL Wire Protocol First

**Via the `pgwire` Rust crate (v0.38+, 635K+ downloads, used by GreptimeDB, Databend, PeerDB).**

| Factor | PostgreSQL | MySQL |
|--------|-----------|-------|
| Industry momentum | #1 most admired DB, 3 years running | Declining mindshare |
| Tool support | Grafana, Metabase, Prisma, SQLAlchemy — first-class | Secondary |
| NewSQL trend | CockroachDB, YugabyteDB, QuestDB, GreptimeDB, Neon — all PG | Only TiDB chose MySQL |
| Rust crate | `pgwire`: mature, async, production-proven | `opensrv-mysql`: smaller |
| Free drivers | psycopg2, node-postgres, pgx, PgJDBC — all work automatically | Separate integration needed |

### PG Compatibility Tiers (implement in order)

| Tier | What | Why |
|------|------|-----|
| 1 | Wire protocol (message framing, startup, simple query, auth) | Minimum for `psql` connectivity |
| 2 | Extended query (Parse / Bind / Describe / Execute / Sync) | Required by psycopg3, pgx, node-postgres |
| 3 | Type system (NexusDB types → PG OIDs) | Correct data decoding in drivers |
| 4 | `pg_catalog` basics (pg_type, pg_class, pg_attribute) | Required by ORMs and admin tools |
| 5 | `information_schema` (tables, columns, constraints) | Schema introspection tools |

MySQL wire protocol via `opensrv-mysql` as Phase 7. Port `:3306` alongside PG `:5432`.

---

## Docker Strategy

### Production-Ready Container

```dockerfile
# Stage 1: Build
FROM rust:1.82-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --bin nexusd

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r nexusdb && useradd -r -g nexusdb nexusdb
COPY --from=builder /build/target/release/nexusd /usr/local/bin/nexusd
COPY docker-entrypoint.sh /usr/local/bin/
VOLUME ["/var/lib/nexusdb/data"]
USER nexusdb
EXPOSE 5432 5433
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD pg_isready -h localhost -p 5432 || exit 1
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["nexusd", "--data-dir=/var/lib/nexusdb/data", "--listen=0.0.0.0:5432"]
```

| Concern | Approach |
|---------|----------|
| **Base image** | `debian:bookworm-slim` (minimal attack surface) |
| **Non-root** | `USER nexusdb`, drop all capabilities |
| **Data** | Named volume at `/var/lib/nexusdb/data` |
| **Health** | `pg_isready` against PG wire port |
| **Init scripts** | `/docker-entrypoint-initdb.d/*.sql` (like official PG image) |
| **Config** | Env vars > CLI flags > config file > defaults |
| **Shutdown** | SIGTERM → flush WAL → checkpoint → close → exit |
| **Multi-arch** | GitHub Actions: amd64 + arm64, push to GHCR |
| **Ports** | 5432 (PG wire), 5433 (gRPC) |

---

## Phase 1: Disk Persistence

**Goal**: Data survives restarts. The single most critical gap.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 1.1 | **Catalog serialization** | Add `Serialize` / `Deserialize` to `TableInfo`, `IndexType`, `IndexInfo`. Handle `Arc<Schema>` with serde helper. Store `CheckConstraint` as SQL string (re-parse on load). Add `CATALOG_FILE` constant, `Catalog::to_json()` / `from_json()`. |
| 1.2 | **TableStore constructor** | Add `TableStore::with_tree(info, tree)` — accepts pre-built `SageTree` instead of always creating `SageTree::new()` (MemoryPager). |
| 1.3 | **StorageEngine data dir** | Add `data_dir: Option<PathBuf>` field. `with_data_dir(path)` constructor. `create_table()` creates `FilePager` → `SageTree::with_pager()` → `TableStore::with_tree()`. |
| 1.4 | **Startup recovery** | `with_data_dir()`: read `catalog.json` → for each table, `FilePager::open()` (auto-loads via `load_all()`) → reconstruct TableStore instances. |
| 1.5 | **flush_all()** | Serialize catalog to JSON, call `tree.flush()` on every table's SageTree. |
| 1.6 | **Database integration** | `Database::open_path()` passes data_dir to StorageEngine. `close()` calls `flush_all()`. `create_database()` creates subdirectory. |
| 1.7 | **Data directory layout** | `{data_dir}/{db_name}/catalog.json` + `{data_dir}/{db_name}/tables/{table}.sage` |
| 1.8 | **WAL integration** | Hook WAL writes into session DML. Each INSERT / UPDATE / DELETE writes WAL record before modifying tree. Implement `replay_wal()`. |
| 1.9 | **Checkpointing** | Wire `CheckpointManager` to flush dirty pages + write checkpoint WAL record. Background thread. |

### Deliverable

`INSERT INTO users VALUES (1, 'alice'); → restart server → SELECT * FROM users;` returns the data.

### Files Changed (~10)

- `nexus-sql/Cargo.toml` — add serde, serde_json
- `nexus-sql/src/storage/catalog.rs` — serde derives, to_json/from_json
- `nexus-sql/src/storage/table.rs` — `with_tree()` constructor
- `nexus-sql/src/storage/engine.rs` — data_dir, with_data_dir, flush_all, create_table update
- `nexus-sql/src/executor/value.rs` — Serialize/Deserialize on Value
- `nexus-server/src/database/engine.rs` — wire data_dir to StorageEngine, flush on close
- `nexus-server/src/database/session.rs` — WAL record writes in DML
- `nexus-storage/src/sagetree/tree.rs` — verify flush() is public

---

## Phase 2: PostgreSQL Wire Protocol

**Goal**: Connect with `psql`, Grafana, ORMs, and every PG driver in every language.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 2.1 | **Add `pgwire` crate** | Integrate pgwire v0.38+. Implement `StartupHandler` + `SimpleQueryHandler`. |
| 2.2 | **Simple Query protocol** | `Q` message → parse SQL → execute → return `RowDescription` + `DataRow` + `CommandComplete`. |
| 2.3 | **Type OID mapping** | Map NexusDB types (Integer, Float, String, Boolean, etc.) to PG type OIDs. |
| 2.4 | **Extended Query protocol** | Parse / Bind / Describe / Execute / Sync. Required by most drivers. |
| 2.5 | **Auth integration** | Wire PBKDF2 auth into PG's `AuthenticationCleartextPassword` or SCRAM-SHA-256. |
| 2.6 | **TLS integration** | pgwire TLS support + existing ServerConfig certs. |
| 2.7 | **Error mapping** | Map `DatabaseError` to PostgreSQL SQLSTATE codes (42P01, 23505, etc.). |
| 2.8 | **Dual-port listening** | PG wire on `:5432`, gRPC on `:5433`. |
| 2.9 | **Minimal pg_catalog** | `SELECT version()`, `SHOW server_version`, `SET client_encoding`, `pg_catalog.pg_type`. |

### Deliverable

```bash
psql -h localhost -p 5432 -U admin nexusdb
# works with psql, psycopg2, node-postgres, pgx, PgJDBC
```

---

## Phase 3: Query Engine Completeness

**Goal**: Close SQL gaps that break real applications.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 3.1 | **Subqueries** | Physical planning for `IN (subquery)` → HashSemiJoin, `EXISTS` → SemiJoin, scalar subqueries → single-row eval. Replace `SelectExpr` stub. |
| 3.2 | **CTE execution** | Wire `CteOperator` — materialize CTE results into temp storage, reference by name. |
| 3.3 | **BTree index scans** | Implement `IndexScan` operator. Cost model compares SeqScan vs IndexScan. Planner picks cheapest. |
| 3.4 | **Views** | `CREATE VIEW name AS select...` → store definition in catalog → expand during planning. |
| 3.5 | **Foreign key enforcement** | INSERT / UPDATE: check referenced table. DELETE: check referencing tables (RESTRICT / CASCADE / SET NULL). |
| 3.6 | **Prepared statements** | Server-side `PREPARE` / `EXECUTE`. Wire into gRPC prepare() and PG Extended Query. |

### Deliverable

Real-world SQL from ORMs (SQLAlchemy, Prisma, ActiveRecord) works.

---

## Phase 4: Docker + Deployment

**Goal**: `docker run nexusdb` works out of the box.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 4.1 | **Multi-stage Dockerfile** | Builder: `rust:1.82-bookworm`. Runtime: `debian:bookworm-slim` + binary + TLS libs. |
| 4.2 | **Non-root execution** | `nexusdb` user/group, `USER nexusdb`, drop capabilities. |
| 4.3 | **Named volume** | `VOLUME /var/lib/nexusdb/data` for persistence. |
| 4.4 | **Entrypoint script** | Init data dir on first run, `/docker-entrypoint-initdb.d/*.sql` support. |
| 4.5 | **Health check** | `pg_isready -h localhost -p 5432` (requires Phase 2). |
| 4.6 | **Graceful shutdown** | SIGTERM → flush WAL → checkpoint → close connections → exit. |
| 4.7 | **docker-compose.yml** | Dev compose: port mapping, volume, env vars (`NEXUSDB_PASSWORD`, `NEXUSDB_DATABASE`). |
| 4.8 | **CI/CD** | GitHub Actions: build + push to GHCR on tag. Multi-arch (amd64 + arm64). |
| 4.9 | **Config** | Env vars, CLI flags, `/etc/nexusdb/nexusdb.conf` mount. |

### Deliverable

```bash
docker run -d -p 5432:5432 -v nexusdb-data:/var/lib/nexusdb/data nexusdb/nexusdb
```

---

## Phase 5: Raft Integration

**Goal**: Multi-node cluster with automatic failover.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 5.1 | **StateMachine impl** | Implement `StateMachine` trait backed by SQL engine. `apply()` executes serialized operations. |
| 5.2 | **Route writes through Raft** | All DML → `raft_node.propose()` → committed → applied to local storage. |
| 5.3 | **Leader routing** | Non-leader nodes forward writes to leader (or return redirect). |
| 5.4 | **Read replicas** | Leader-lease reads on leader. Follower reads for eventual consistency. |
| 5.5 | **Cluster bootstrap** | `nexusd --cluster-init --peers=node1:7001,node2:7001,node3:7001` |
| 5.6 | **Membership changes** | `ALTER SYSTEM ADD NODE` / `REMOVE NODE` via Raft joint consensus. |
| 5.7 | **Snapshot transfer** | Full state snapshot for new nodes joining cluster. |

### Deliverable

3-node cluster. Kill leader → automatic failover → no data loss.

---

## Phase 6: Observability + Production Polish

**Goal**: Operable in production with monitoring and diagnostics.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 6.1 | **`information_schema`** | tables, columns, table_constraints, key_column_usage. Required by ORMs. |
| 6.2 | **`pg_catalog`** | pg_tables, pg_type, pg_class, pg_attribute, pg_namespace. Required by tools. |
| 6.3 | **Slow query log** | Wire `slow_query_threshold_ms` — log queries exceeding threshold with timing + plan. |
| 6.4 | **Prometheus metrics** | `/metrics` HTTP endpoint. Gauges: connections, buffer pool, WAL size. Counters: queries, txns, errors. Histograms: latency. |
| 6.5 | **VACUUM** | Dead row cleanup. Mark-and-sweep on MVCC versions. `VACUUM tablename` command. Background auto-vacuum. |
| 6.6 | **Connection pooling** | Server-side session reuse. Max connections limit. Idle timeout. |
| 6.7 | **`pg_stat_activity`** | Running queries, state, duration, client address. |

### Deliverable

Grafana dashboard showing NexusDB health. ORMs auto-discover schema via information_schema.

---

## Phase 7: MySQL Wire Protocol + Native SDKs

**Goal**: MySQL tool compatibility + optimized language SDKs.

### Tasks

| # | Task | Detail |
|---|------|--------|
| 7.1 | **MySQL wire protocol** | Via `opensrv-mysql` crate. COM_QUERY, COM_STMT_PREPARE/EXECUTE, handshake auth. Port `:3306`. |
| 7.2 | **Python SDK** | Thin wrapper over psycopg2 with NexusDB-specific bulk load, retry, telemetry. Published to PyPI. |
| 7.3 | **JavaScript SDK** | Wrapper over `pg` (node-postgres) with TypeScript types. Published to npm. |
| 7.4 | **Go SDK** | Wrapper over `pgx` with connection pooling helpers. |
| 7.5 | **JDBC driver** | For Java/Kotlin enterprise. Extends PgJDBC or thin custom driver. |

### Deliverable

```bash
mysql -h localhost -p 3306 nexusdb    # works
pip install nexusdb                    # works
```

---

## Execution Order

```
Phase 1 (Persistence)       ◄── START HERE — everything depends on this
  │
  ▼
Phase 2 (PG Wire Protocol)  ◄── Biggest ecosystem unlock
  │
  ▼
Phase 3 (SQL Completeness)  ◄── Fix what breaks when real apps connect
  │
  ▼
Phase 4 (Docker)             ◄── Package for easy deployment
  │
  ▼
Phase 5 (Raft / HA)         ◄── Distributed deployment
  │
  ▼
Phase 6 (Observability)     ◄── Production operations
  │
  ▼
Phase 7 (MySQL + SDKs)      ◄── Ecosystem expansion
```

Each phase is **independently shippable and testable**. No phase requires a later phase to be useful.

### Estimated Effort

| Phase | Scope | Estimated Effort |
|-------|-------|-----------------|
| Phase 1 | Disk persistence + WAL | 2-3 weeks |
| Phase 2 | PG wire protocol | 2 weeks |
| Phase 3 | SQL completeness | 2-3 weeks |
| Phase 4 | Docker + CI/CD | 3-4 days |
| Phase 5 | Raft integration | 2-3 weeks |
| Phase 6 | Observability | 1-2 weeks |
| Phase 7 | MySQL + SDKs | 2 weeks |

**Total to production-ready**: ~12-16 weeks of focused work.
