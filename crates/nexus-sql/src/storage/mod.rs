//! Storage integration layer.
//!
//! This module bridges the SQL execution layer with the underlying SageTree
//! storage engine. It provides:
//!
//! - Table catalog management
//! - Row encoding/decoding
//! - Storage-backed query execution
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SQL Executor                              │
//! │  (executes physical plans, produces RecordBatches)          │
//! └─────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Storage Integration                         │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
//! │  │   Catalog   │  │   Encoder   │  │  TableStore │         │
//! │  │  (schemas)  │  │ (row<->kv)  │  │ (per-table) │         │
//! │  └─────────────┘  └─────────────┘  └─────────────┘         │
//! └─────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      SageTree                                │
//! │  (B-tree with delta chains, supports MVCC)                  │
//! └─────────────────────────────────────────────────────────────┘
//! ```

mod catalog;
mod encoder;
mod engine;
mod error;
mod table;

pub use catalog::{Catalog, TableInfo};
pub use encoder::{EncodingFormat, RowDecoder, RowEncoder};
pub use engine::{QuerySession, StorageEngine};
pub use error::{StorageError, StorageResult};
pub use table::{TableScanIterator, TableStore};
