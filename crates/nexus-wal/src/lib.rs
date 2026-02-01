//! # nexus-wal
//!
//! Write-ahead logging for NexusDB.
//!
//! This crate implements ARIES-style WAL with:
//! - Group commit for high throughput
//! - Redo and undo logging
//! - Checkpoint support
//! - Log truncation
//!
//! # Architecture
//!
//! The WAL is organized into segments (default 64 MB each). Each segment
//! contains a sequence of records, starting with a segment header record.
//!
//! ## Record Format
//!
//! Each record consists of a 40-byte header followed by a variable-length payload:
//!
//! ```text
//! +----------+----------+----------+------+-------+----------+--------+----------+
//! | LSN (8)  | Prev (8) | TxnId(8) | Type | Flags | Reserved | Length | Checksum |
//! +----------+----------+----------+------+-------+----------+--------+----------+
//! |                              Payload (variable)                              |
//! +------------------------------------------------------------------------------+
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

mod config;
mod error;

/// WAL record types and serialization.
pub mod record;

/// WAL segment management.
pub mod segment;

/// WAL writer for appending records.
pub mod writer;

/// WAL reader for recovery.
pub mod reader;

/// Group commit implementation.
pub mod group_commit;

/// Checkpoint management.
pub mod checkpoint;

/// Main WAL manager.
pub mod wal;

// Re-exports for convenience
pub use config::{SyncPolicy, WalConfig};
pub use error::{WalError, WalResult};
pub use record::{
    CheckpointBeginPayload, CheckpointEndPayload, CommitPayload, DeletePayload, InsertPayload,
    PageWritePayload, RecordFlags, RecordHeader, RecordType, UpdatePayload, WalRecord,
};
pub use wal::Wal;
