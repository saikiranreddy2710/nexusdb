//! Main WAL manager.
//!
//! This module provides the main `Wal` struct that coordinates
//! all WAL operations including writing, reading, and recovery.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use nexus_common::types::{Lsn, TxnId};

use crate::config::{SyncPolicy, WalConfig};
use crate::error::{WalError, WalResult};
use crate::group_commit::GroupCommitManager;
use crate::reader::WalReader;
use crate::record::payload::{
    CheckpointBeginPayload, CheckpointEndPayload, DeletePayload, InsertPayload, Payload,
    UpdatePayload,
};
use crate::record::types::WalRecord;
use crate::segment::WalSegment;
use crate::writer::WalWriter;

/// WAL statistics.
#[derive(Debug, Default)]
pub struct WalStats {
    /// Total bytes written.
    pub bytes_written: AtomicU64,
    /// Total records written.
    pub records_written: AtomicU64,
    /// Number of segment rotations.
    pub segment_rotations: AtomicU64,
    /// Number of syncs performed.
    pub syncs: AtomicU64,
    /// Number of checkpoints.
    pub checkpoints: AtomicU64,
}

/// The main Write-Ahead Log manager.
pub struct Wal {
    /// Configuration.
    config: Arc<WalConfig>,
    /// WAL writer.
    writer: Arc<WalWriter>,
    /// WAL reader.
    reader: RwLock<WalReader>,
    /// Group commit manager (if enabled).
    group_commit: Option<Arc<GroupCommitManager>>,
    /// Per-transaction last LSN (for undo chain).
    txn_last_lsn: RwLock<HashMap<TxnId, Lsn>>,
    /// Last checkpoint LSN.
    last_checkpoint_lsn: AtomicU64,
    /// Minimum recovery LSN.
    min_recovery_lsn: AtomicU64,
    /// WAL statistics.
    stats: WalStats,
    /// Whether the WAL is closed.
    closed: AtomicBool,
}

impl Wal {
    /// Creates a new WAL with the given configuration.
    pub fn new(config: WalConfig) -> WalResult<Self> {
        config.validate().map_err(WalError::config_error)?;

        // Create WAL directory if it doesn't exist
        std::fs::create_dir_all(&config.dir)?;

        let config = Arc::new(config);
        let writer = Arc::new(WalWriter::new(Arc::clone(&config)));
        let reader = RwLock::new(WalReader::new(Arc::clone(&config)));

        let group_commit = match config.sync_policy {
            SyncPolicy::GroupCommit => Some(Arc::new(GroupCommitManager::new(
                Arc::clone(&config),
                Arc::clone(&writer),
            ))),
            _ => None,
        };

        Ok(Self {
            config,
            writer,
            reader,
            group_commit,
            txn_last_lsn: RwLock::new(HashMap::new()),
            last_checkpoint_lsn: AtomicU64::new(0),
            min_recovery_lsn: AtomicU64::new(0),
            stats: WalStats::default(),
            closed: AtomicBool::new(false),
        })
    }

    /// Opens an existing WAL, recovering from disk.
    pub fn open(config: WalConfig) -> WalResult<Self> {
        config.validate().map_err(WalError::config_error)?;

        let config = Arc::new(config);

        // Open and scan existing segments
        let mut reader = WalReader::new(Arc::clone(&config));
        reader.open_all_segments()?;

        // Find the next LSN based on existing segments
        let next_lsn = reader
            .max_lsn()
            .map(|lsn| Lsn::new(lsn.as_u64() + 1))
            .unwrap_or(Lsn::FIRST);

        // Get the active segment if any
        let active_segment = reader.segments().last().cloned();

        let writer = if let Some(segment) = active_segment {
            if segment.is_active() {
                Arc::new(WalWriter::with_segment(
                    Arc::clone(&config),
                    segment,
                    next_lsn,
                ))
            } else {
                Arc::new(WalWriter::new(Arc::clone(&config)))
            }
        } else {
            Arc::new(WalWriter::new(Arc::clone(&config)))
        };

        let group_commit = match config.sync_policy {
            SyncPolicy::GroupCommit => Some(Arc::new(GroupCommitManager::new(
                Arc::clone(&config),
                Arc::clone(&writer),
            ))),
            _ => None,
        };

        Ok(Self {
            config,
            writer,
            reader: RwLock::new(reader),
            group_commit,
            txn_last_lsn: RwLock::new(HashMap::new()),
            last_checkpoint_lsn: AtomicU64::new(0),
            min_recovery_lsn: AtomicU64::new(0),
            stats: WalStats::default(),
            closed: AtomicBool::new(false),
        })
    }

    /// Returns the configuration.
    pub fn config(&self) -> &WalConfig {
        &self.config
    }

    /// Returns the next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        self.writer.next_lsn()
    }

    /// Returns the last checkpoint LSN.
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        Lsn::new(self.last_checkpoint_lsn.load(Ordering::Acquire))
    }

    /// Returns the minimum recovery LSN.
    pub fn min_recovery_lsn(&self) -> Lsn {
        Lsn::new(self.min_recovery_lsn.load(Ordering::Acquire))
    }

    /// Logs an insert operation.
    pub fn log_insert(&self, txn_id: TxnId, payload: InsertPayload) -> WalResult<Lsn> {
        self.check_closed()?;

        let prev_lsn = self.get_txn_last_lsn(txn_id);
        let lsn = self.writer.log_insert(txn_id, prev_lsn, payload)?;
        self.set_txn_last_lsn(txn_id, lsn);
        self.update_stats();
        self.maybe_sync()?;

        Ok(lsn)
    }

    /// Logs an update operation.
    pub fn log_update(&self, txn_id: TxnId, payload: UpdatePayload) -> WalResult<Lsn> {
        self.check_closed()?;

        let prev_lsn = self.get_txn_last_lsn(txn_id);
        let lsn = self.writer.log_update(txn_id, prev_lsn, payload)?;
        self.set_txn_last_lsn(txn_id, lsn);
        self.update_stats();
        self.maybe_sync()?;

        Ok(lsn)
    }

    /// Logs a delete operation.
    pub fn log_delete(&self, txn_id: TxnId, payload: DeletePayload) -> WalResult<Lsn> {
        self.check_closed()?;

        let prev_lsn = self.get_txn_last_lsn(txn_id);
        let lsn = self.writer.log_delete(txn_id, prev_lsn, payload)?;
        self.set_txn_last_lsn(txn_id, lsn);
        self.update_stats();
        self.maybe_sync()?;

        Ok(lsn)
    }

    /// Logs a transaction commit.
    pub fn log_commit(&self, txn_id: TxnId, commit_timestamp: u64) -> WalResult<Lsn> {
        self.check_closed()?;

        let prev_lsn = self.get_txn_last_lsn(txn_id);
        let lsn = self.writer.log_commit(txn_id, prev_lsn, commit_timestamp)?;

        // Remove from txn_last_lsn map
        self.txn_last_lsn.write().remove(&txn_id);

        self.update_stats();

        // Commit always syncs (for durability)
        self.sync()?;

        Ok(lsn)
    }

    /// Logs a transaction abort.
    pub fn log_abort(&self, txn_id: TxnId) -> WalResult<Lsn> {
        self.check_closed()?;

        let prev_lsn = self.get_txn_last_lsn(txn_id);
        let lsn = self.writer.log_abort(txn_id, prev_lsn)?;

        // Remove from txn_last_lsn map
        self.txn_last_lsn.write().remove(&txn_id);

        self.update_stats();
        self.maybe_sync()?;

        Ok(lsn)
    }

    /// Logs a checkpoint begin.
    pub fn log_checkpoint_begin(&self, active_txns: Vec<TxnId>) -> WalResult<Lsn> {
        self.check_closed()?;

        let checkpoint_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let payload = CheckpointBeginPayload {
            checkpoint_id,
            active_txn_count: active_txns.len() as u32,
            active_txns,
        };

        let payload_bytes = payload.serialize()?;
        let lsn = self
            .writer
            .allocate_lsn(crate::record::header::RecordHeader::SIZE + payload_bytes.len());
        let record = WalRecord::checkpoint_begin(lsn, payload)?;
        let written_lsn = self.writer.append(&record)?;

        self.sync()?;
        self.stats.checkpoints.fetch_add(1, Ordering::Relaxed);

        Ok(written_lsn)
    }

    /// Logs a checkpoint end.
    pub fn log_checkpoint_end(
        &self,
        checkpoint_id: u64,
        begin_lsn: Lsn,
        min_recovery_lsn: Lsn,
    ) -> WalResult<Lsn> {
        self.check_closed()?;

        let payload = CheckpointEndPayload {
            checkpoint_id,
            begin_lsn,
            min_recovery_lsn,
        };

        let payload_bytes = payload.serialize()?;
        let lsn = self
            .writer
            .allocate_lsn(crate::record::header::RecordHeader::SIZE + payload_bytes.len());
        let record = WalRecord::checkpoint_end(lsn, payload)?;
        let written_lsn = self.writer.append(&record)?;

        // Update checkpoint LSN
        self.last_checkpoint_lsn
            .store(written_lsn.as_u64(), Ordering::Release);
        self.min_recovery_lsn
            .store(min_recovery_lsn.as_u64(), Ordering::Release);

        self.sync()?;

        Ok(written_lsn)
    }

    /// Syncs the WAL to disk.
    pub fn sync(&self) -> WalResult<()> {
        if let Some(ref gc) = self.group_commit {
            gc.flush()?;
        } else {
            self.writer.sync()?;
        }
        self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Conditionally syncs based on sync policy.
    fn maybe_sync(&self) -> WalResult<()> {
        match self.config.sync_policy {
            SyncPolicy::EveryWrite => self.writer.sync()?,
            SyncPolicy::GroupCommit => {
                // Group commit manager handles syncing
            }
            SyncPolicy::Periodic { .. } => {
                // Periodic sync is handled by a background thread
            }
            SyncPolicy::Never => {
                // No sync
            }
        }
        Ok(())
    }

    /// Returns an iterator over records starting from the given LSN.
    pub fn iter_from(&self, start_lsn: Lsn) -> WalResult<Vec<WalRecord>> {
        let reader = self.reader.read();
        reader.iter_from(start_lsn).collect()
    }

    /// Returns an iterator over all records.
    pub fn iter(&self) -> WalResult<Vec<WalRecord>> {
        self.iter_from(Lsn::INVALID)
    }

    /// Reads a single record at the given LSN.
    pub fn read(&self, lsn: Lsn) -> WalResult<WalRecord> {
        let reader = self.reader.read();
        reader.read(lsn)
    }

    /// Rotates to a new segment.
    pub fn rotate_segment(&self) -> WalResult<()> {
        let new_segment = self.writer.rotate_segment()?;
        self.reader.write().add_segment(new_segment);
        self.stats.segment_rotations.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Returns the active segment.
    pub fn active_segment(&self) -> Option<Arc<WalSegment>> {
        self.writer.active_segment()
    }

    /// Returns WAL statistics.
    pub fn stats(&self) -> &WalStats {
        &self.stats
    }

    /// Closes the WAL.
    pub fn close(&self) -> WalResult<()> {
        self.closed.store(true, Ordering::Release);

        if let Some(ref gc) = self.group_commit {
            gc.shutdown()?;
        }

        self.writer.close()?;
        Ok(())
    }

    /// Returns true if the WAL is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Checks if the WAL is closed and returns an error if so.
    fn check_closed(&self) -> WalResult<()> {
        if self.is_closed() {
            Err(WalError::Closed)
        } else {
            Ok(())
        }
    }

    /// Gets the last LSN for a transaction.
    fn get_txn_last_lsn(&self, txn_id: TxnId) -> Lsn {
        self.txn_last_lsn
            .read()
            .get(&txn_id)
            .copied()
            .unwrap_or(Lsn::INVALID)
    }

    /// Sets the last LSN for a transaction.
    fn set_txn_last_lsn(&self, txn_id: TxnId, lsn: Lsn) {
        self.txn_last_lsn.write().insert(txn_id, lsn);
    }

    /// Updates statistics from writer.
    fn update_stats(&self) {
        let writer_stats = self.writer.stats();
        self.stats.bytes_written.store(
            writer_stats.bytes_written.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.stats.records_written.store(
            writer_stats.records_written.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }

    /// Truncates WAL segments before the given LSN.
    pub fn truncate_before(&self, lsn: Lsn) -> WalResult<()> {
        let segment_id = self.config.segment_id_for_lsn(lsn.as_u64());

        // Remove old segment files
        let dir = &self.config.dir;
        if !dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(file_segment_id) = crate::reader::WalReader::parse_segment_id(&path) {
                if file_segment_id < segment_id {
                    std::fs::remove_file(&path)?;
                }
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("config", &self.config)
            .field("next_lsn", &self.next_lsn())
            .field("last_checkpoint_lsn", &self.last_checkpoint_lsn())
            .field("closed", &self.is_closed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use nexus_common::types::PageId;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> WalConfig {
        WalConfig::new(dir)
            .with_segment_size(1024 * 1024)
            .with_max_record_size(256 * 1024) // Must be <= segment_size / 2
            .with_sync_policy(SyncPolicy::EveryWrite)
            .with_preallocate_segments(false)
    }

    #[test]
    fn test_wal_creation() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let wal = Wal::new(config).unwrap();
        assert!(!wal.is_closed());
    }

    #[test]
    fn test_log_operations() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let wal = Wal::new(config).unwrap();
        let txn_id = TxnId::new(1);

        // Log insert
        let insert_payload = InsertPayload {
            page_id: PageId::new(1),
            slot_id: 0,
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        };
        let lsn1 = wal.log_insert(txn_id, insert_payload).unwrap();

        // Log update
        let update_payload = UpdatePayload {
            page_id: PageId::new(1),
            slot_id: 0,
            key: Bytes::from("key1"),
            old_value: Bytes::from("value1"),
            new_value: Bytes::from("value2"),
        };
        let lsn2 = wal.log_update(txn_id, update_payload).unwrap();

        // Log commit
        let lsn3 = wal.log_commit(txn_id, 12345).unwrap();

        assert!(lsn2 > lsn1);
        assert!(lsn3 > lsn2);
    }

    #[test]
    fn test_wal_close() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let wal = Wal::new(config).unwrap();
        let txn_id = TxnId::new(1);

        // Log something
        let insert_payload = InsertPayload {
            page_id: PageId::new(1),
            slot_id: 0,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };
        wal.log_insert(txn_id, insert_payload).unwrap();

        wal.close().unwrap();
        assert!(wal.is_closed());

        // Operations should fail
        let insert_payload = InsertPayload {
            page_id: PageId::new(2),
            slot_id: 0,
            key: Bytes::from("key2"),
            value: Bytes::from("value2"),
        };
        assert!(matches!(
            wal.log_insert(txn_id, insert_payload),
            Err(WalError::Closed)
        ));
    }

    #[test]
    fn test_wal_reopen() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        // Create and write
        let wal = Wal::new(config.clone()).unwrap();
        let txn_id = TxnId::new(1);

        let insert_payload = InsertPayload {
            page_id: PageId::new(1),
            slot_id: 0,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };
        wal.log_insert(txn_id, insert_payload).unwrap();
        wal.log_commit(txn_id, 12345).unwrap();
        let final_lsn = wal.next_lsn();
        wal.close().unwrap();

        // Reopen
        let wal2 = Wal::open(config).unwrap();
        assert!(wal2.next_lsn() >= final_lsn);
    }

    #[test]
    fn test_txn_lsn_chain() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let wal = Wal::new(config).unwrap();
        let txn_id = TxnId::new(1);

        // First insert - prev_lsn should be INVALID
        let insert1 = InsertPayload {
            page_id: PageId::new(1),
            slot_id: 0,
            key: Bytes::from("k1"),
            value: Bytes::from("v1"),
        };
        let lsn1 = wal.log_insert(txn_id, insert1).unwrap();

        // Second insert - prev_lsn should be lsn1
        let insert2 = InsertPayload {
            page_id: PageId::new(1),
            slot_id: 1,
            key: Bytes::from("k2"),
            value: Bytes::from("v2"),
        };
        let lsn2 = wal.log_insert(txn_id, insert2).unwrap();

        assert!(lsn2 > lsn1);

        // Commit clears the chain
        wal.log_commit(txn_id, 12345).unwrap();

        // Next operation for same txn should have INVALID prev_lsn
        let insert3 = InsertPayload {
            page_id: PageId::new(2),
            slot_id: 0,
            key: Bytes::from("k3"),
            value: Bytes::from("v3"),
        };
        let _ = wal.log_insert(txn_id, insert3).unwrap();
    }

    #[test]
    fn test_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        let wal = Wal::new(config).unwrap();

        // Log checkpoint begin
        let begin_lsn = wal.log_checkpoint_begin(vec![]).unwrap();

        // Log checkpoint end
        let min_recovery_lsn = begin_lsn;
        let checkpoint_id = 12345;
        let end_lsn = wal
            .log_checkpoint_end(checkpoint_id, begin_lsn, min_recovery_lsn)
            .unwrap();

        assert!(end_lsn > begin_lsn);
        assert_eq!(wal.last_checkpoint_lsn(), end_lsn);
        assert_eq!(wal.min_recovery_lsn(), min_recovery_lsn);
    }
}
