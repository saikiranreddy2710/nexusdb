//! WAL writer for appending records.
//!
//! This module provides the writer component that handles:
//! - Appending records to the current segment
//! - Managing segment rotation
//! - Coordinating with group commit

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use nexus_common::types::{Lsn, TxnId};

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::record::header::RecordHeader;
use crate::record::types::WalRecord;
use crate::segment::WalSegment;

/// WAL writer statistics.
#[derive(Debug, Default)]
pub struct WriterStats {
    /// Total bytes written.
    pub bytes_written: AtomicU64,
    /// Total records written.
    pub records_written: AtomicU64,
    /// Number of segment rotations.
    pub segment_rotations: AtomicU64,
    /// Number of syncs performed.
    pub syncs: AtomicU64,
}

/// WAL writer for appending records.
pub struct WalWriter {
    /// Configuration.
    config: Arc<WalConfig>,
    /// Current active segment.
    active_segment: RwLock<Option<Arc<WalSegment>>>,
    /// Current LSN counter.
    next_lsn: AtomicU64,
    /// Write buffer for batching.
    write_buffer: Mutex<Vec<u8>>,
    /// Writer statistics.
    stats: WriterStats,
    /// Whether the writer is closed.
    closed: AtomicBool,
}

impl WalWriter {
    /// Creates a new WAL writer.
    pub fn new(config: Arc<WalConfig>) -> Self {
        Self {
            config,
            active_segment: RwLock::new(None),
            next_lsn: AtomicU64::new(Lsn::FIRST.as_u64()),
            write_buffer: Mutex::new(Vec::with_capacity(64 * 1024)),
            stats: WriterStats::default(),
            closed: AtomicBool::new(false),
        }
    }

    /// Opens the writer with an existing segment.
    pub fn with_segment(config: Arc<WalConfig>, segment: Arc<WalSegment>, next_lsn: Lsn) -> Self {
        Self {
            config,
            active_segment: RwLock::new(Some(segment)),
            next_lsn: AtomicU64::new(next_lsn.as_u64()),
            write_buffer: Mutex::new(Vec::with_capacity(64 * 1024)),
            stats: WriterStats::default(),
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        Lsn::new(self.next_lsn.load(Ordering::Acquire))
    }

    /// Allocates an LSN for a record of the given size.
    pub fn allocate_lsn(&self, record_size: usize) -> Lsn {
        let lsn = self
            .next_lsn
            .fetch_add(record_size as u64, Ordering::AcqRel);
        Lsn::new(lsn)
    }

    /// Appends a record to the WAL.
    ///
    /// Returns the LSN where the record was written.
    pub fn append(&self, record: &WalRecord) -> WalResult<Lsn> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WalError::Closed);
        }

        let bytes = record.serialize()?;
        self.append_bytes(&bytes, record.header.lsn)
    }

    /// Appends raw record bytes to the WAL.
    pub fn append_bytes(&self, bytes: &[u8], lsn: Lsn) -> WalResult<Lsn> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WalError::Closed);
        }

        if bytes.len() > self.config.max_record_size {
            return Err(WalError::record_too_large(
                bytes.len(),
                self.config.max_record_size,
            ));
        }

        // Ensure we have an active segment with space
        self.ensure_segment_space(bytes.len())?;

        // Get the active segment
        let segment_guard = self.active_segment.read();
        let segment = segment_guard.as_ref().ok_or(WalError::NoActiveSegment)?;

        // Append to segment
        segment.append(bytes)?;

        // Update stats
        self.stats
            .bytes_written
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.stats.records_written.fetch_add(1, Ordering::Relaxed);

        Ok(lsn)
    }

    /// Ensures there's a segment with enough space for the given record size.
    fn ensure_segment_space(&self, record_size: usize) -> WalResult<()> {
        let segment_guard = self.active_segment.read();

        let needs_rotation = match segment_guard.as_ref() {
            None => true,
            Some(seg) => !seg.has_space_for(record_size),
        };

        drop(segment_guard);

        if needs_rotation {
            self.rotate_segment()?;
        }

        Ok(())
    }

    /// Rotates to a new segment.
    pub fn rotate_segment(&self) -> WalResult<Arc<WalSegment>> {
        let mut segment_guard = self.active_segment.write();

        // Seal the old segment if present
        let prev_last_lsn = if let Some(ref old_seg) = *segment_guard {
            old_seg.seal()?;
            old_seg.last_lsn()
        } else {
            Lsn::INVALID
        };

        // Calculate new segment ID
        let current_lsn = self.next_lsn.load(Ordering::Acquire);
        let segment_id = self.config.segment_id_for_lsn(current_lsn);
        let path = self.config.segment_path(segment_id);

        // Create new segment
        let new_segment = Arc::new(WalSegment::create(
            segment_id,
            &path,
            &self.config,
            prev_last_lsn,
        )?);

        *segment_guard = Some(Arc::clone(&new_segment));
        self.stats.segment_rotations.fetch_add(1, Ordering::Relaxed);

        Ok(new_segment)
    }

    /// Syncs the current segment to disk.
    pub fn sync(&self) -> WalResult<()> {
        let segment_guard = self.active_segment.read();
        if let Some(ref segment) = *segment_guard {
            segment.sync()?;
            self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Returns the active segment, if any.
    pub fn active_segment(&self) -> Option<Arc<WalSegment>> {
        self.active_segment.read().clone()
    }

    /// Returns writer statistics.
    pub fn stats(&self) -> &WriterStats {
        &self.stats
    }

    /// Closes the writer.
    pub fn close(&self) -> WalResult<()> {
        self.closed.store(true, Ordering::Release);

        // Seal and sync the active segment
        let segment_guard = self.active_segment.read();
        if let Some(ref segment) = *segment_guard {
            segment.seal()?;
            segment.sync()?;
        }

        Ok(())
    }

    /// Returns true if the writer is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Creates a new insert record and appends it.
    pub fn log_insert(
        &self,
        txn_id: TxnId,
        prev_lsn: Lsn,
        payload: crate::record::payload::InsertPayload,
    ) -> WalResult<Lsn> {
        let payload_bytes = payload.serialize()?;
        let lsn = self.allocate_lsn(RecordHeader::SIZE + payload_bytes.len());

        let record = WalRecord::insert(lsn, prev_lsn, txn_id, payload)?;
        self.append(&record)
    }

    /// Creates a new update record and appends it.
    pub fn log_update(
        &self,
        txn_id: TxnId,
        prev_lsn: Lsn,
        payload: crate::record::payload::UpdatePayload,
    ) -> WalResult<Lsn> {
        let payload_bytes = payload.serialize()?;
        let lsn = self.allocate_lsn(RecordHeader::SIZE + payload_bytes.len());

        let record = WalRecord::update(lsn, prev_lsn, txn_id, payload)?;
        self.append(&record)
    }

    /// Creates a new delete record and appends it.
    pub fn log_delete(
        &self,
        txn_id: TxnId,
        prev_lsn: Lsn,
        payload: crate::record::payload::DeletePayload,
    ) -> WalResult<Lsn> {
        let payload_bytes = payload.serialize()?;
        let lsn = self.allocate_lsn(RecordHeader::SIZE + payload_bytes.len());

        let record = WalRecord::delete(lsn, prev_lsn, txn_id, payload)?;
        self.append(&record)
    }

    /// Creates a new commit record and appends it.
    pub fn log_commit(
        &self,
        txn_id: TxnId,
        prev_lsn: Lsn,
        commit_timestamp: u64,
    ) -> WalResult<Lsn> {
        let lsn = self.allocate_lsn(RecordHeader::SIZE + 8);

        let record = WalRecord::commit(lsn, prev_lsn, txn_id, commit_timestamp);
        self.append(&record)
    }

    /// Creates a new abort record and appends it.
    pub fn log_abort(&self, txn_id: TxnId, prev_lsn: Lsn) -> WalResult<Lsn> {
        let lsn = self.allocate_lsn(RecordHeader::SIZE);

        let record = WalRecord::abort(lsn, prev_lsn, txn_id);
        self.append(&record)
    }
}

use crate::record::payload::Payload;

impl std::fmt::Debug for WalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalWriter")
            .field("next_lsn", &self.next_lsn())
            .field("closed", &self.is_closed())
            .field(
                "bytes_written",
                &self.stats.bytes_written.load(Ordering::Relaxed),
            )
            .field(
                "records_written",
                &self.stats.records_written.load(Ordering::Relaxed),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use nexus_common::types::PageId;
    use tempfile::TempDir;

    fn test_config(dir: &std::path::Path) -> Arc<WalConfig> {
        Arc::new(
            WalConfig::new(dir)
                .with_segment_size(1024 * 1024)
                .with_preallocate_segments(false),
        )
    }

    #[test]
    fn test_writer_creation() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = WalWriter::new(config);

        assert_eq!(writer.next_lsn(), Lsn::FIRST);
        assert!(!writer.is_closed());
    }

    #[test]
    fn test_append_commit() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = WalWriter::new(config);

        let lsn = writer
            .log_commit(TxnId::new(1), Lsn::INVALID, 12345)
            .unwrap();
        assert!(lsn >= Lsn::FIRST);

        assert!(writer.stats.records_written.load(Ordering::Relaxed) >= 1);
        assert!(writer.stats.bytes_written.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_append_insert() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = WalWriter::new(config);

        let payload = crate::record::payload::InsertPayload {
            page_id: PageId::new(1),
            slot_id: 0,
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        };

        let lsn = writer
            .log_insert(TxnId::new(1), Lsn::INVALID, payload)
            .unwrap();
        assert!(lsn >= Lsn::FIRST);
    }

    #[test]
    fn test_close_writer() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = WalWriter::new(config);

        // Write something first
        writer
            .log_commit(TxnId::new(1), Lsn::INVALID, 12345)
            .unwrap();

        writer.close().unwrap();
        assert!(writer.is_closed());

        // Appending should fail
        let result = writer.log_commit(TxnId::new(2), Lsn::INVALID, 12346);
        assert!(matches!(result, Err(WalError::Closed)));
    }

    #[test]
    fn test_sync() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = WalWriter::new(config);

        writer
            .log_commit(TxnId::new(1), Lsn::INVALID, 12345)
            .unwrap();
        writer.sync().unwrap();

        assert!(writer.stats.syncs.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn test_lsn_allocation() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = WalWriter::new(config);

        let lsn1 = writer.allocate_lsn(100);
        let lsn2 = writer.allocate_lsn(200);
        let lsn3 = writer.allocate_lsn(50);

        assert!(lsn2.as_u64() > lsn1.as_u64());
        assert!(lsn3.as_u64() > lsn2.as_u64());
        assert_eq!(lsn2.as_u64() - lsn1.as_u64(), 100);
        assert_eq!(lsn3.as_u64() - lsn2.as_u64(), 200);
    }
}
