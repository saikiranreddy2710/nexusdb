//! # nexus-wal
//!
//! Write-ahead logging for NexusDB.
//!
//! This crate implements ARIES-style WAL with:
//! - Group commit for high throughput
//! - Redo and undo logging
//! - Checkpoint support
//! - Log truncation

#![warn(missing_docs)]
#![warn(clippy::all)]

/// WAL writer and reader
pub mod log;

/// Log record types
pub mod record;

/// Group commit implementation
pub mod group_commit;

/// Checkpoint management
pub mod checkpoint;
