//! File-based storage implementation.
//!
//! This module provides a complete `Storage` implementation that
//! combines `RaftState` and `PersistentLog` into a unified interface.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

use crate::log::SnapshotMeta;
use crate::rpc::{LogEntry, NodeId, Term};
use crate::LogIndex;

use super::log::PersistentLog;
use super::state::RaftState;
use super::{Storage, StorageConfig, StorageError, StorageResult};

/// File-based storage implementation.
///
/// Provides durable storage using the filesystem:
/// - `RaftState`: Stores term and voted_for in a metadata file
/// - `PersistentLog`: Stores log entries in append-only log files
///
/// # Directory Structure
///
/// ```text
/// <dir>/
/// ├── raft_state          # Current term and voted_for
/// ├── log/
/// │   └── entries.log     # Log entries
/// ├── snapshot            # Snapshot data
/// └── snapshot.meta       # Snapshot metadata
/// ```
#[derive(Debug)]
pub struct FileStorage {
    /// Raft state (term, voted_for).
    state: RaftState,
    /// Persistent log.
    log: PersistentLog,
    /// Configuration.
    config: StorageConfig,
    /// Whether storage is closed.
    closed: AtomicBool,
}

impl FileStorage {
    /// Creates a new file storage.
    ///
    /// If the directory exists, loads existing state. Otherwise, creates
    /// a new empty storage.
    pub fn new(config: StorageConfig) -> StorageResult<Self> {
        std::fs::create_dir_all(&config.dir)?;

        let state = RaftState::new(&config.dir, config.sync_writes)?;
        let log = PersistentLog::new(&config.dir, config.sync_writes)?;

        Ok(Self {
            state,
            log,
            config,
            closed: AtomicBool::new(false),
        })
    }

    /// Opens an existing storage or creates a new one.
    pub fn open<P: AsRef<Path>>(dir: P) -> StorageResult<Self> {
        Self::new(StorageConfig::new(dir))
    }

    /// Returns the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Checks if storage is closed.
    fn check_closed(&self) -> StorageResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(StorageError::Closed)
        } else {
            Ok(())
        }
    }
}

impl Storage for FileStorage {
    fn current_term(&self) -> Term {
        self.state.current_term()
    }

    fn voted_for(&self) -> Option<NodeId> {
        self.state.voted_for()
    }

    fn save_term_and_vote(&self, term: Term, voted_for: Option<NodeId>) -> StorageResult<()> {
        self.check_closed()?;
        self.state.save(term, voted_for)
    }

    fn first_index(&self) -> LogIndex {
        self.log.first_index()
    }

    fn last_index(&self) -> LogIndex {
        self.log.last_index()
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.log.term_at(index)
    }

    fn get_entry(&self, index: LogIndex) -> StorageResult<Option<LogEntry>> {
        self.check_closed()?;
        Ok(self.log.get(index))
    }

    fn get_entries(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>> {
        self.check_closed()?;
        self.log.get_range(start, end)
    }

    fn append_entries(&self, entries: Vec<LogEntry>) -> StorageResult<()> {
        self.check_closed()?;
        self.log.append(entries)
    }

    fn truncate_from(&self, from_index: LogIndex) -> StorageResult<()> {
        self.check_closed()?;
        self.log.truncate_from(from_index)
    }

    fn snapshot_meta(&self) -> SnapshotMeta {
        self.log.snapshot_meta()
    }

    fn create_snapshot(
        &self,
        index: LogIndex,
        term: Term,
        data: Bytes,
    ) -> StorageResult<SnapshotMeta> {
        self.check_closed()?;
        self.log.create_snapshot(index, term, data)
    }

    fn install_snapshot(&self, meta: SnapshotMeta, data: Bytes) -> StorageResult<()> {
        self.check_closed()?;
        self.log.install_snapshot(meta, data)
    }

    fn snapshot_data(&self) -> StorageResult<Option<Bytes>> {
        self.check_closed()?;
        self.log.snapshot_data()
    }

    fn sync(&self) -> StorageResult<()> {
        self.check_closed()?;
        self.log.sync()
    }

    fn close(&self) -> StorageResult<()> {
        self.closed.store(true, Ordering::Release);
        self.log.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_storage_new() {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());
        let storage = FileStorage::new(config).unwrap();

        assert_eq!(storage.current_term(), 0);
        assert_eq!(storage.voted_for(), None);
        assert_eq!(storage.first_index(), 1);
        assert_eq!(storage.last_index(), 0);
    }

    #[test]
    fn test_term_and_vote() {
        let tmp = TempDir::new().unwrap();
        let storage = FileStorage::open(tmp.path()).unwrap();

        storage.save_term_and_vote(5, Some(42)).unwrap();
        assert_eq!(storage.current_term(), 5);
        assert_eq!(storage.voted_for(), Some(42));

        // Clear vote on term change
        storage.save_term_and_vote(6, None).unwrap();
        assert_eq!(storage.current_term(), 6);
        assert_eq!(storage.voted_for(), None);
    }

    #[test]
    fn test_log_operations() {
        let tmp = TempDir::new().unwrap();
        let storage = FileStorage::open(tmp.path()).unwrap();

        let entries = vec![
            LogEntry::command(1, 1, Bytes::from("cmd1")),
            LogEntry::command(1, 2, Bytes::from("cmd2")),
            LogEntry::command(2, 3, Bytes::from("cmd3")),
        ];

        storage.append_entries(entries).unwrap();

        assert_eq!(storage.last_index(), 3);
        assert_eq!(storage.term_at(1), Some(1));
        assert_eq!(storage.term_at(3), Some(2));

        let entry = storage.get_entry(2).unwrap().unwrap();
        assert_eq!(entry.data.as_ref(), b"cmd2");

        let range = storage.get_entries(1, 3).unwrap();
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn test_persistence() {
        let tmp = TempDir::new().unwrap();

        // Write state
        {
            let storage = FileStorage::open(tmp.path()).unwrap();
            storage.save_term_and_vote(10, Some(99)).unwrap();
            storage
                .append_entries(vec![LogEntry::command(10, 1, Bytes::from("cmd"))])
                .unwrap();
            storage.close().unwrap();
        }

        // Reload and verify
        {
            let storage = FileStorage::open(tmp.path()).unwrap();
            assert_eq!(storage.current_term(), 10);
            assert_eq!(storage.voted_for(), Some(99));
            assert_eq!(storage.last_index(), 1);
        }
    }

    #[test]
    fn test_snapshot_operations() {
        let tmp = TempDir::new().unwrap();
        let storage = FileStorage::open(tmp.path()).unwrap();

        // Add entries
        let entries: Vec<_> = (1..=10)
            .map(|i| LogEntry::command(1, i, Bytes::from(format!("cmd{}", i))))
            .collect();
        storage.append_entries(entries).unwrap();

        // Create snapshot
        let meta = storage
            .create_snapshot(5, 1, Bytes::from("snapshot"))
            .unwrap();
        assert_eq!(meta.last_included_index, 5);

        // Verify compaction
        assert_eq!(storage.first_index(), 6);

        // Verify snapshot data
        let data = storage.snapshot_data().unwrap().unwrap();
        assert_eq!(data.as_ref(), b"snapshot");
    }

    #[test]
    fn test_closed_storage_errors() {
        let tmp = TempDir::new().unwrap();
        let storage = FileStorage::open(tmp.path()).unwrap();
        storage.close().unwrap();

        assert!(matches!(
            storage.append_entries(vec![]),
            Err(StorageError::Closed)
        ));
        assert!(matches!(storage.sync(), Err(StorageError::Closed)));
    }

    #[test]
    fn test_storage_trait_impl() {
        let tmp = TempDir::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(FileStorage::open(tmp.path()).unwrap());

        storage.save_term_and_vote(5, Some(1)).unwrap();
        assert_eq!(storage.current_term(), 5);
        assert_eq!(storage.voted_for(), Some(1));
    }
}
