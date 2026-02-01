//! Persistence layer for Raft consensus.
//!
//! This module provides durable storage for Raft state, enabling recovery
//! after crashes. It includes:
//!
//! - `Storage` trait: Abstract interface for persistent storage
//! - `RaftState`: Persistent metadata (term, voted_for)
//! - `PersistentLog`: WAL-backed Raft log
//! - `FileStorage`: Complete file-based storage implementation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                    RaftNode                         │
//! │                        │                            │
//! │            ┌───────────┴───────────┐               │
//! │            ▼                       ▼               │
//! │     ┌───────────┐          ┌─────────────┐        │
//! │     │ RaftState │          │ PersistentLog│        │
//! │     │ (metadata)│          │  (entries)   │        │
//! │     └─────┬─────┘          └──────┬──────┘        │
//! │           │                       │               │
//! │           └───────────┬───────────┘               │
//! │                       ▼                            │
//! │              ┌─────────────┐                       │
//! │              │ FileStorage │                       │
//! │              │  (on disk)  │                       │
//! │              └─────────────┘                       │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Recovery
//!
//! On startup, the storage layer:
//! 1. Reads `RaftState` from a metadata file
//! 2. Replays the WAL to reconstruct the log
//! 3. Applies any committed entries to the state machine
//! 4. Resumes normal operation
//!
//! # Durability Guarantees
//!
//! - State changes are synced before responding to RPCs
//! - Log entries are durable before sending acknowledgments
//! - Snapshots are atomic (written to temp file, then renamed)

mod file;
mod log;
mod state;

pub use file::FileStorage;
pub use log::PersistentLog;
pub use state::RaftState;

use std::path::Path;

use bytes::Bytes;
use thiserror::Error;

use crate::log::SnapshotMeta;
use crate::rpc::{LogEntry, NodeId, Term};
use crate::LogIndex;

/// Errors that can occur in storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Storage is corrupted.
    #[error("storage corrupted: {0}")]
    Corrupted(String),

    /// Entry not found.
    #[error("entry not found at index {0}")]
    EntryNotFound(LogIndex),

    /// Log compacted beyond requested index.
    #[error("log compacted, first available index is {0}")]
    LogCompacted(LogIndex),

    /// Snapshot error.
    #[error("snapshot error: {0}")]
    Snapshot(String),

    /// Storage is closed.
    #[error("storage closed")]
    Closed,
}

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// Configuration for persistent storage.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Directory for storage files.
    pub dir: std::path::PathBuf,
    /// Whether to sync writes immediately.
    pub sync_writes: bool,
    /// Maximum entries per WAL segment.
    pub max_entries_per_segment: usize,
    /// Snapshot threshold (entries since last snapshot).
    pub snapshot_threshold: usize,
}

impl StorageConfig {
    /// Creates a new storage configuration.
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            sync_writes: true,
            max_entries_per_segment: 10000,
            snapshot_threshold: 10000,
        }
    }

    /// Sets whether to sync writes immediately.
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
    }

    /// Sets the snapshot threshold.
    pub fn with_snapshot_threshold(mut self, threshold: usize) -> Self {
        self.snapshot_threshold = threshold;
        self
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new("./raft-storage")
    }
}

/// Trait for persistent Raft storage.
///
/// Implementations must ensure durability of all writes before returning.
pub trait Storage: Send + Sync {
    /// Returns the current term.
    fn current_term(&self) -> Term;

    /// Returns the node we voted for in the current term (if any).
    fn voted_for(&self) -> Option<NodeId>;

    /// Saves the current term and vote.
    ///
    /// This must be durable before returning (i.e., synced to disk).
    fn save_term_and_vote(&self, term: Term, voted_for: Option<NodeId>) -> StorageResult<()>;

    /// Returns the first log index (after compaction).
    fn first_index(&self) -> LogIndex;

    /// Returns the last log index.
    fn last_index(&self) -> LogIndex;

    /// Returns the term at the given index.
    fn term_at(&self, index: LogIndex) -> Option<Term>;

    /// Gets the entry at the given index.
    fn get_entry(&self, index: LogIndex) -> StorageResult<Option<LogEntry>>;

    /// Gets entries in the range [start, end).
    fn get_entries(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>>;

    /// Appends entries to the log.
    ///
    /// This must be durable before returning.
    fn append_entries(&self, entries: Vec<LogEntry>) -> StorageResult<()>;

    /// Truncates the log from the given index onwards.
    ///
    /// Used when receiving conflicting entries from a new leader.
    fn truncate_from(&self, from_index: LogIndex) -> StorageResult<()>;

    /// Returns snapshot metadata.
    fn snapshot_meta(&self) -> SnapshotMeta;

    /// Creates a snapshot at the given index.
    fn create_snapshot(
        &self,
        index: LogIndex,
        term: Term,
        data: Bytes,
    ) -> StorageResult<SnapshotMeta>;

    /// Installs a snapshot received from the leader.
    fn install_snapshot(&self, meta: SnapshotMeta, data: Bytes) -> StorageResult<()>;

    /// Returns the snapshot data.
    fn snapshot_data(&self) -> StorageResult<Option<Bytes>>;

    /// Syncs all pending writes to disk.
    fn sync(&self) -> StorageResult<()>;

    /// Closes the storage.
    fn close(&self) -> StorageResult<()>;
}

/// File names used by storage.
pub mod files {
    /// State file name (term, voted_for).
    pub const STATE_FILE: &str = "raft_state";
    /// State file backup during writes.
    pub const STATE_FILE_TMP: &str = "raft_state.tmp";
    /// Log directory.
    pub const LOG_DIR: &str = "log";
    /// Snapshot file name.
    pub const SNAPSHOT_FILE: &str = "snapshot";
    /// Snapshot metadata file.
    pub const SNAPSHOT_META_FILE: &str = "snapshot.meta";
    /// Snapshot temp file (during creation).
    pub const SNAPSHOT_TMP: &str = "snapshot.tmp";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config() {
        let config = StorageConfig::new("/tmp/raft")
            .with_sync_writes(false)
            .with_snapshot_threshold(5000);

        assert_eq!(config.dir.to_str().unwrap(), "/tmp/raft");
        assert!(!config.sync_writes);
        assert_eq!(config.snapshot_threshold, 5000);
    }

    #[test]
    fn test_storage_error_display() {
        let err = StorageError::EntryNotFound(42);
        assert!(err.to_string().contains("42"));

        let err = StorageError::LogCompacted(100);
        assert!(err.to_string().contains("100"));
    }
}
