# AGENTS.md — NexusDB Agentic Coding Guide

## Project Overview

NexusDB is a research-grade next-generation database (~87K lines of Rust, 19 crates) combining:
- **High performance**: io_uring, NUMA-aware, zero-copy, SageTree B+Tree
- **Reliability**: ACID transactions with undo-log rollback, WAL, crash recovery
- **Security**: Zero Trust (RBAC, PBKDF2-600K auth, JWT, audit log)
- **AI-native**: HNSW vector search, LLM-ready EXPLAIN JSON, window functions

**Current state**: 1216 tests pass, 0 failures. Phases 1-4 complete (Storage, Cache, Security, Vector DB). Phase 5 (ML Integration) is next.

## Build / Test / Lint Commands

```bash
cargo build                          # Build all 19 crates
cargo build --release                # Release build (thin LTO, codegen-units=1)
cargo check                          # Fast type-check without codegen
cargo clippy --workspace             # Lint all crates
cargo test                           # Run ALL tests (~1216 tests, ~60s)
cargo test -p nexus-server           # Run tests for one crate
cargo test -p nexus-server -- test_session_count_distinct  # Single test by name
cargo test -p nexus-sql -- test_accumulator  # Tests matching prefix
cargo test -- --nocapture            # Show println output
cargo run --bin nexusd               # Start server daemon
cargo run --bin nexusd -- --memory   # Memory-only mode
```

**Before every commit**: run `cargo check && cargo test` — all 1216+ tests must pass, 0 failures.

## Workspace Crate Map

| Crate | Purpose | Key Types |
|-------|---------|-----------|
| `nexus-common` | Core types, errors, constants | `PageId`, `TxnId`, `Lsn`, `NexusError` |
| `nexus-storage` | SageTree B+Tree, buffer pool, io_uring | `SageTree`, `BufferPool`, `FilePager` |
| `nexus-wal` | Write-ahead log (ARIES-style) | `WalWriter`, `WalReader`, `WalRecord` |
| `nexus-kv` | LSM-tree key-value engine | `LsmEngine`, `MemTable`, `SSTable` |
| `nexus-sql` | SQL parser, optimizer, executor (Photon) | `Statement`, `LogicalOperator`, `PhysicalPlan` |
| `nexus-server` | Database engine + gRPC server | `Database`, `Session`, `NexusDbService` |
| `nexus-security` | RBAC, auth, audit | `Authenticator`, `Authorizer`, `AuditLog` |
| `nexus-hnsw` | HNSW vector index | `HnswIndex`, distance metrics |
| `nexus-cache` | LRU cache, plan cache, bloom filter | `ResultCache`, `PlanCache`, `BloomFilter` |
| `nexus-mvcc` | MVCC infrastructure (Phase 5) | `VersionStore`, `SnapshotManager`, `HLC` |
| `nexus-raft` | Raft++ consensus (Phase 5+) | `RaftNode`, `LogEntry` |

## Code Style

### Imports (3-group ordering)
```rust
use std::collections::HashMap;       // 1. std library
use std::sync::Arc;

use parking_lot::RwLock;             // 2. External crates
use thiserror::Error;

use crate::storage::TableInfo;       // 3. Internal (crate/super)
use super::error::DatabaseResult;
```

### Error Handling
Every crate defines `{Crate}Error` (via `thiserror`) and `{Crate}Result<T>`:
```rust
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("I/O error: {source}")]
    Io { #[from] source: std::io::Error },
}
pub type StorageResult<T> = Result<T, StorageError>;
```
Exception: `nexus-server` uses manual `Display` impl on `DatabaseError`.

### Naming Conventions
- Files/functions: `snake_case` — Structs/enums: `CamelCase`
- ID newtypes: `{Thing}Id` (e.g., `PageId`, `TxnId`, `SessionId`)
- Config structs: `{Component}Config` (e.g., `DatabaseConfig`, `WalConfig`)
- Constants: `SCREAMING_SNAKE_CASE`

### Documentation
- Every `lib.rs` has `#![warn(missing_docs)]` and `#![warn(clippy::all)]`
- All public items have `///` doc comments
- Module-level `//!` docs with ASCII architecture diagrams
- Section separators in large files: `// =========================================================================`

### Test Patterns
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn create_session() -> Session { /* test helper */ }

    #[test]
    fn test_feature_basic() { ... }

    #[test]
    fn test_feature_edge_case() { ... }
}
```
- Test configs use `pbkdf2_iterations: 1_000` (not 600K) for speed
- Use `Database::open_memory()` and `tempfile::tempdir()` for isolation
- Test passwords use `std::process::id()` to avoid CodeQL hardcoded-credential alerts

## Architecture: Query Pipeline

```
SQL string
  → Parser (sqlparser → our AST: Statement/Expr)
  → Logical Plan Builder (LogicalOperator tree)
  → Optimizer (5 rules: predicate/projection pushdown, constant folding, etc.)
  → Physical Planner (PhysicalOperator tree)
  → Executor (Operator trait: next_batch() → RecordBatch)
  → Session (result formatting, caching, audit)
```

## What's Been Done (Phases 1-4 Perfected)

### Audit (104 findings — ALL resolved)
6 Critical, 25 High, 31 Medium, 42 Low fixes across security, storage, concurrency.

### Command Fixes (14 items completed)
| # | Fix | Commit |
|---|-----|--------|
| 1 | COUNT(DISTINCT x) — accumulator + output_name fix | `824d4b9` |
| 2 | UNION/INTERSECT/EXCEPT — all 6 modes, streaming UNION ALL | `506d8d6` |
| 3 | INSERT...SELECT — full pipeline with column mapping | `43bff92` |
| 4 | ALTER TABLE — ADD/DROP/RENAME COLUMN, RENAME TABLE | `2f4aabf` |
| 5 | UPDATE SET expressions — full eval_expr (arithmetic, functions, CASE) | `360656e` |
| 6 | UPDATE/DELETE HNSW sync — undo stale/phantom vector entries | `2082b61` |
| 7 | Constraint enforcement — NOT NULL, UNIQUE, DEFAULT, CHECK | `4cdba97` |
| 8 | WHERE LIKE/IN/BETWEEN — parser + planner + executor + eval_expr | `37940ba` |
| 9 | Table-level PRIMARY KEY(a,b) — composite PK + table UNIQUE/CHECK | `f11da10` |
| 10 | Window functions — SUM/COUNT OVER(), PARTITION BY, ROW_NUMBER | `ae75ad9` |
| 11 | NULLS FIRST/LAST — fixed all 3 sort comparators | `0e6eec1` |
| 12 | Transactions — undo log ROLLBACK for INSERT/UPDATE/DELETE | `2373c75` |
| 13 | BTree indexes — metadata flow to planner, index_lookup API | `aaa3ce7` |
| 14 | IN/EXISTS subqueries — pending (last item) |

### EXPLAIN system
- TEXT, VERBOSE, JSON formats with per-operator metrics
- `explain_json_llm()` — machine-parseable JSON with warnings/suggestions for future LLM consumption

## What's Next

### Remaining Phase 4 Item
- **#14 IN/EXISTS subqueries** — physical planner rejects with hard errors

### Phase 5: ML Integration (upcoming)
- Learned cost model (replace `SimpleCostModel` with ML-trained predictor)
- LLM KV cache storage
- Embedding generation
- EXPLAIN → LLM analysis pipeline
- Index advisor (workload-driven recommendations)

### Phase 6-10 Roadmap
| Phase | Name | Key Features |
|-------|------|-------------|
| 6 | Agent API | AI agents query/mutate database autonomously |
| 7 | Learned Indexes | ML models replace B-Tree for O(1) lookups |
| 8 | Graph DB | Graph storage, traversal, path algorithms |
| 9 | NL Interface | Natural language → SQL via in-database LLM |
| 10 | Autonomous DB | Self-tuning indexes, configs, query plans via ML |

### LLM Integration Ideas (captured for Phase 5+)
- **Query optimization**: learned cost model, adaptive query rewriting, predictive cache warming
- **Reliability**: anomaly detection on queries, predictive failure detection, automatic error recovery
- **Semantic intelligence**: NL-to-SQL, data quality validation, query intent validation
- **Novel features**: conversational database agent (`ASK "why is this slow?"`), auto-materialized views, schema evolution assistant

## CLI Bugs (nexus-cli — ALL RESOLVED)

| # | Bug | Fix | Severity |
|---|-----|-----|----------|
| C1 | `\t` timing toggle always says "on" | Changed `ToggleTiming` to unit variant; REPL handler negates `self.timing` | LOW |
| C2 | `USE database` has no effect | Added `database_override` RwLock to Client; CLI intercepts USE and calls `set_database()`; database name sent per-request | HIGH |
| C3 | No `\c dbname` to switch database | Added `ConnectDatabase(String)` command; `\c` with args switches DB, without args shows conninfo | HIGH |
| C4 | Prompt doesn't show current database | `get_prompt()` now returns `nexus({db})> ` / `nexus({db})* > ` | LOW |
| C5 | `\d tablename` shows "OK" not columns | Fixed by C2 (database context now flows correctly); added `columns.is_empty()` branch in `print_result` to show `(0 rows)` | MEDIUM |
| C6 | Help says INSERT...SELECT unsupported | Moved INSERT...SELECT to "working" section in help text | LOW |
| C7 | Non-SQL input without `;` hangs forever | Validator checks first word against SQL keywords; non-SQL input accepted immediately; added max continuation line safety valve | LOW |
| C8 | `CREATE TABLE sai;` (no columns) succeeds | Added `columns.is_empty()` guard in `execute_create_table()` returning ExecutionError | MEDIUM |

## Key Discoveries (for agents)

- `Field` in `nexus-sql` uses `name()` method, NOT `name` field
- `StorageEngine::catalog()` returns `&Catalog` — bind `Arc<StorageEngine>` to a local variable
- Scalar functions dispatched by name string in `evaluate_scalar_function()`
- `Statement::Explain/ExplainAnalyze` are struct variants: `{ statement, format }`
- `ExplainFormat` enum: `Text`, `Verbose`, `Json`
- `DatabaseConfig` has `pbkdf2_iterations: u32` (0 = library default 600K)
- `Authenticator::new_for_test(enforce)` is `#[cfg(test)]` only
- Transaction IDs use `rand::random::<u64>()` in gRPC layer
- io_uring code gated behind `#[cfg(all(target_os = "linux", feature = "io-uring"))]`
- `nexus-query` and `nexus-buffer` crates are empty stubs
- The `nexus-mvcc` crate has full MVCC infrastructure but is NOT wired into storage yet

## Git Conventions

Conventional Commits format: `type: description` (lowercase, no period)
```
feat: implement window functions (SUM/COUNT/AVG OVER, PARTITION BY, ROW_NUMBER)
fix: sync HNSW vector indexes on UPDATE and DELETE
test: add coverage for all audit fix areas (8 new tests)
chore: fix 3 warnings from audit fixes
```

## Process for Each Fix

1. **Investigate**: use Task tool to explore the full pipeline (parser → builder → planner → executor)
2. **Plan**: create TodoWrite items for each sub-step
3. **Implement**: fix one layer at a time, `cargo check` after each change
4. **Test**: add comprehensive tests (unit + integration), cover edge cases
5. **Verify**: `cargo check && cargo test` — ALL tests must pass, 0 failures
6. **Commit**: descriptive conventional commit message, then `git push`
