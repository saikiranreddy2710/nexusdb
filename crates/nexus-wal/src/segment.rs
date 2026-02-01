//! WAL segment management.
//!
//! This module handles individual WAL segment files.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

use nexus_common::types::Lsn;

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::record::header::RecordHeader;
use crate::record::payload::{Payload, SegmentHeaderPayload};
use crate::record::types::{RecordFlags, RecordType, WalPayload, WalRecord};

/// Segment state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    /// Segment is open for writing.
    Active,
    /// Segment is sealed (no more writes).
    Sealed,
    /// Segment is being archived.
    Archiving,
    /// Segment has been archived.
    Archived,
}

/// A single WAL segment file.
pub struct WalSegment {
    /// Segment ID.
    segment_id: u64,
    /// Path to the segment file.
    path: PathBuf,
    /// File handle.
    file: Mutex<File>,
    /// Current write position.
    write_pos: AtomicU64,
    /// Segment size limit.
    size_limit: usize,
    /// First LSN in this segment.
    first_lsn: Lsn,
    /// Last LSN written to this segment.
    last_lsn: AtomicU64,
    /// Segment state.
    state: parking_lot::RwLock<SegmentState>,
}

impl WalSegment {
    /// Creates a new segment file.
    pub fn create(
        segment_id: u64,
        path: impl AsRef<Path>,
        config: &WalConfig,
        prev_segment_last_lsn: Lsn,
    ) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create and optionally preallocate the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // Preallocate if configured
        if config.preallocate_segments {
            file.set_len(config.segment_size as u64)?;
        }

        // Calculate first LSN for this segment
        let first_lsn = Lsn::new(config.start_lsn_for_segment(segment_id));

        let segment = Self {
            segment_id,
            path,
            file: Mutex::new(file),
            write_pos: AtomicU64::new(0),
            size_limit: config.segment_size,
            first_lsn,
            last_lsn: AtomicU64::new(first_lsn.as_u64()),
            state: parking_lot::RwLock::new(SegmentState::Active),
        };

        // Write segment header
        segment.write_header(prev_segment_last_lsn)?;

        Ok(segment)
    }

    /// Opens an existing segment file.
    pub fn open(segment_id: u64, path: impl AsRef<Path>, config: &WalConfig) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Read and validate the segment header
        let mut header_bytes = vec![0u8; RecordHeader::SIZE + 40]; // header + payload
        file.read_exact(&mut header_bytes)?;

        let header = RecordHeader::from_bytes(&header_bytes[..RecordHeader::SIZE])?;
        if header.record_type != RecordType::SegmentHeader {
            return Err(WalError::segment_corrupted(
                Lsn::INVALID,
                "First record is not a segment header",
            ));
        }

        let payload = SegmentHeaderPayload::deserialize(&header_bytes[RecordHeader::SIZE..])?;
        payload.validate()?;

        if payload.segment_id != segment_id {
            return Err(WalError::segment_corrupted(
                payload.first_lsn,
                format!(
                    "Segment ID mismatch: expected {}, found {}",
                    segment_id, payload.segment_id
                ),
            ));
        }

        // Find the write position by scanning to the end
        let write_pos = Self::find_end_position(&mut file, config)?;

        // Calculate first LSN
        let first_lsn = payload.first_lsn;

        // Calculate last LSN from write position
        // In our LSN scheme, LSNs correspond to file positions, so the last LSN
        // is approximately first_lsn + write_pos (minus 1 since write_pos points to next slot)
        let last_lsn = if write_pos > 0 {
            first_lsn.as_u64() + write_pos - 1
        } else {
            first_lsn.as_u64()
        };

        Ok(Self {
            segment_id,
            path,
            file: Mutex::new(file),
            write_pos: AtomicU64::new(write_pos),
            size_limit: config.segment_size,
            first_lsn,
            last_lsn: AtomicU64::new(last_lsn),
            state: parking_lot::RwLock::new(SegmentState::Active),
        })
    }

    /// Writes the segment header record.
    fn write_header(&self, prev_segment_last_lsn: Lsn) -> WalResult<()> {
        let payload =
            SegmentHeaderPayload::new(self.segment_id, self.first_lsn, prev_segment_last_lsn);
        let payload_bytes = payload.serialize()?;

        let mut header = RecordHeader::new(
            self.first_lsn,
            Lsn::INVALID,
            nexus_common::types::TxnId::INVALID,
            RecordType::SegmentHeader,
            RecordFlags::empty(),
            payload_bytes.len() as u32,
        );
        header.set_checksum(&payload_bytes);

        let header_bytes = header.to_bytes();
        let total_size = header_bytes.len() + payload_bytes.len();

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_bytes)?;
        file.write_all(&payload_bytes)?;
        file.flush()?;

        self.write_pos.store(total_size as u64, Ordering::Release);

        Ok(())
    }

    /// Finds the end position of valid records in the segment.
    fn find_end_position(file: &mut File, config: &WalConfig) -> WalResult<u64> {
        let file_len = file.metadata()?.len();
        let mut pos = 0u64;
        let mut header_buf = vec![0u8; RecordHeader::SIZE];

        file.seek(SeekFrom::Start(0))?;

        while pos + RecordHeader::SIZE as u64 <= file_len {
            file.seek(SeekFrom::Start(pos))?;
            if file.read_exact(&mut header_buf).is_err() {
                break;
            }

            let header = match RecordHeader::from_bytes(&header_buf) {
                Ok(h) => h,
                Err(_) => break,
            };

            // Check for zero LSN (unwritten space)
            if header.lsn == Lsn::INVALID && pos > 0 {
                break;
            }

            let total_size = header.total_size() as u64;
            if pos + total_size > config.segment_size as u64 {
                break;
            }

            pos += total_size;
        }

        Ok(pos)
    }

    /// Returns the segment ID.
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Returns the first LSN in this segment.
    pub fn first_lsn(&self) -> Lsn {
        self.first_lsn
    }

    /// Returns the last LSN written.
    pub fn last_lsn(&self) -> Lsn {
        Lsn::new(self.last_lsn.load(Ordering::Acquire))
    }

    /// Returns the current write position.
    pub fn write_position(&self) -> u64 {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Returns the remaining space in this segment.
    pub fn remaining_space(&self) -> usize {
        let pos = self.write_pos.load(Ordering::Acquire) as usize;
        self.size_limit.saturating_sub(pos)
    }

    /// Returns true if the segment has space for a record of the given size.
    pub fn has_space_for(&self, record_size: usize) -> bool {
        self.remaining_space() >= record_size
    }

    /// Returns true if this segment is active (accepting writes).
    pub fn is_active(&self) -> bool {
        *self.state.read() == SegmentState::Active
    }

    /// Returns the segment state.
    pub fn state(&self) -> SegmentState {
        *self.state.read()
    }

    /// Seals the segment, preventing further writes.
    pub fn seal(&self) -> WalResult<()> {
        let mut state = self.state.write();
        if *state == SegmentState::Active {
            *state = SegmentState::Sealed;
        }
        Ok(())
    }

    /// Appends a serialized record to the segment.
    ///
    /// Returns the LSN where the record was written.
    pub fn append(&self, record_bytes: &[u8]) -> WalResult<Lsn> {
        if !self.is_active() {
            return Err(WalError::SegmentFull {
                segment_id: self.segment_id,
            });
        }

        let record_size = record_bytes.len();
        let pos = self.write_pos.load(Ordering::Acquire);

        if pos as usize + record_size > self.size_limit {
            return Err(WalError::SegmentFull {
                segment_id: self.segment_id,
            });
        }

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(pos))?;
        file.write_all(record_bytes)?;

        let new_pos = pos + record_size as u64;
        self.write_pos.store(new_pos, Ordering::Release);

        // Parse LSN from the record header
        let lsn = if record_bytes.len() >= 8 {
            let lsn_bytes: [u8; 8] = record_bytes[..8].try_into().unwrap();
            Lsn::new(u64::from_be_bytes(lsn_bytes))
        } else {
            self.first_lsn
        };

        self.last_lsn.fetch_max(lsn.as_u64(), Ordering::AcqRel);

        Ok(lsn)
    }

    /// Syncs the segment to disk.
    pub fn sync(&self) -> WalResult<()> {
        let file = self.file.lock();
        file.sync_all()?;
        Ok(())
    }

    /// Reads a record at the given offset.
    pub fn read_at(&self, offset: u64) -> WalResult<WalRecord> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))?;

        // Read header
        let mut header_buf = vec![0u8; RecordHeader::SIZE];
        file.read_exact(&mut header_buf)?;
        let header = RecordHeader::from_bytes(&header_buf)?;

        // Read payload
        let mut payload_buf = vec![0u8; header.payload_length as usize];
        file.read_exact(&mut payload_buf)?;

        // Verify checksum
        if !header.verify_checksum(&payload_buf) {
            return Err(WalError::checksum_mismatch(
                header.lsn,
                header.checksum,
                header.compute_checksum(&payload_buf),
            ));
        }

        // Deserialize
        let mut full_buf = Vec::with_capacity(RecordHeader::SIZE + payload_buf.len());
        full_buf.extend_from_slice(&header_buf);
        full_buf.extend_from_slice(&payload_buf);

        WalRecord::deserialize(&mut full_buf.as_slice())
    }

    /// Returns the path to this segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl std::fmt::Debug for WalSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalSegment")
            .field("segment_id", &self.segment_id)
            .field("path", &self.path)
            .field("first_lsn", &self.first_lsn)
            .field("last_lsn", &self.last_lsn())
            .field("write_pos", &self.write_position())
            .field("state", &self.state())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> WalConfig {
        WalConfig::new(dir)
            .with_segment_size(1024 * 1024) // 1 MB for tests
            .with_preallocate_segments(false)
    }

    #[test]
    fn test_create_segment() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let path = config.segment_path(0);

        let segment = WalSegment::create(0, &path, &config, Lsn::INVALID).unwrap();

        assert_eq!(segment.segment_id(), 0);
        assert!(segment.is_active());
        assert!(segment.write_position() > 0); // Header was written
        assert!(path.exists());
    }

    #[test]
    fn test_open_segment() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let path = config.segment_path(0);

        // Create segment
        let segment = WalSegment::create(0, &path, &config, Lsn::INVALID).unwrap();
        let write_pos = segment.write_position();
        drop(segment);

        // Reopen
        let segment = WalSegment::open(0, &path, &config).unwrap();
        assert_eq!(segment.segment_id(), 0);
        assert_eq!(segment.write_position(), write_pos);
    }

    #[test]
    fn test_append_record() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let path = config.segment_path(0);

        let segment = WalSegment::create(0, &path, &config, Lsn::INVALID).unwrap();
        let initial_pos = segment.write_position();

        // Create a simple record
        let record = WalRecord::commit(
            Lsn::new(100),
            Lsn::INVALID,
            nexus_common::types::TxnId::new(1),
            12345,
        );
        let bytes = record.serialize().unwrap();

        let lsn = segment.append(&bytes).unwrap();
        assert!(segment.write_position() > initial_pos);
    }

    #[test]
    fn test_seal_segment() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let path = config.segment_path(0);

        let segment = WalSegment::create(0, &path, &config, Lsn::INVALID).unwrap();
        assert!(segment.is_active());

        segment.seal().unwrap();
        assert!(!segment.is_active());
        assert_eq!(segment.state(), SegmentState::Sealed);

        // Appending should fail
        let record = WalRecord::commit(
            Lsn::new(100),
            Lsn::INVALID,
            nexus_common::types::TxnId::new(1),
            12345,
        );
        let bytes = record.serialize().unwrap();
        assert!(segment.append(&bytes).is_err());
    }

    #[test]
    fn test_remaining_space() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let path = config.segment_path(0);

        let segment = WalSegment::create(0, &path, &config, Lsn::INVALID).unwrap();

        let remaining = segment.remaining_space();
        assert!(remaining < config.segment_size);
        assert!(remaining > 0);
    }
}
