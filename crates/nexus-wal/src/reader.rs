//! WAL reader for recovery.
//!
//! This module provides the reader component for:
//! - Reading records from WAL segments
//! - Iterating over records for recovery
//! - Validating record checksums

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use nexus_common::types::Lsn;

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::record::header::RecordHeader;
use crate::record::types::WalRecord;
use crate::segment::WalSegment;

/// WAL reader for recovery operations.
pub struct WalReader {
    /// Configuration.
    config: Arc<WalConfig>,
    /// Segments being read.
    segments: Vec<Arc<WalSegment>>,
}

impl WalReader {
    /// Creates a new WAL reader.
    pub fn new(config: Arc<WalConfig>) -> Self {
        Self {
            config,
            segments: Vec::new(),
        }
    }

    /// Opens all segments in the WAL directory.
    pub fn open_all_segments(&mut self) -> WalResult<()> {
        let dir = &self.config.dir;
        if !dir.exists() {
            return Ok(());
        }

        let mut segment_paths: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|ext| ext == "log").unwrap_or(false))
            .collect();

        segment_paths.sort();

        for path in segment_paths {
            if let Some(segment_id) = Self::parse_segment_id(&path) {
                match WalSegment::open(segment_id, &path, &self.config) {
                    Ok(segment) => {
                        self.segments.push(Arc::new(segment));
                    }
                    Err(e) => {
                        // Log error but continue with other segments
                        eprintln!("Warning: Failed to open segment {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Parses a segment ID from a file path.
    pub fn parse_segment_id(path: &Path) -> Option<u64> {
        let stem = path.file_stem()?.to_str()?;
        if !stem.starts_with("wal_") {
            return None;
        }
        let hex = &stem[4..];
        u64::from_str_radix(hex, 16).ok()
    }

    /// Returns an iterator over all records starting from the given LSN.
    pub fn iter_from(&self, start_lsn: Lsn) -> RecordIterator<'_> {
        RecordIterator::new(&self.segments, &self.config, start_lsn)
    }

    /// Returns an iterator over all records.
    pub fn iter(&self) -> RecordIterator<'_> {
        self.iter_from(Lsn::INVALID)
    }

    /// Reads a single record at the given LSN.
    pub fn read(&self, lsn: Lsn) -> WalResult<WalRecord> {
        let segment_id = self.config.segment_id_for_lsn(lsn.as_u64());

        let segment = self
            .segments
            .iter()
            .find(|s| s.segment_id() == segment_id)
            .ok_or(WalError::SegmentNotFound { segment_id })?;

        let offset = self.config.segment_offset_for_lsn(lsn.as_u64());
        segment.read_at(offset as u64)
    }

    /// Returns the segments being managed.
    pub fn segments(&self) -> &[Arc<WalSegment>] {
        &self.segments
    }

    /// Adds a segment to the reader.
    pub fn add_segment(&mut self, segment: Arc<WalSegment>) {
        self.segments.push(segment);
        self.segments.sort_by_key(|s| s.segment_id());
    }

    /// Finds the segment containing the given LSN.
    pub fn find_segment(&self, lsn: Lsn) -> Option<&Arc<WalSegment>> {
        let segment_id = self.config.segment_id_for_lsn(lsn.as_u64());
        self.segments.iter().find(|s| s.segment_id() == segment_id)
    }

    /// Returns the minimum LSN across all segments.
    pub fn min_lsn(&self) -> Option<Lsn> {
        self.segments.first().map(|s| s.first_lsn())
    }

    /// Returns the maximum LSN across all segments.
    pub fn max_lsn(&self) -> Option<Lsn> {
        self.segments.last().map(|s| s.last_lsn())
    }
}

/// Iterator over WAL records.
pub struct RecordIterator<'a> {
    /// Segments to iterate over.
    segments: &'a [Arc<WalSegment>],
    /// Configuration.
    config: &'a WalConfig,
    /// Current segment index.
    segment_idx: usize,
    /// Current offset within segment.
    offset: u64,
    /// File handle for current segment.
    current_file: Option<File>,
    /// Whether we've started iterating.
    started: bool,
    /// Start LSN.
    start_lsn: Lsn,
}

impl<'a> RecordIterator<'a> {
    /// Creates a new record iterator.
    fn new(segments: &'a [Arc<WalSegment>], config: &'a WalConfig, start_lsn: Lsn) -> Self {
        Self {
            segments,
            config,
            segment_idx: 0,
            offset: 0,
            current_file: None,
            started: false,
            start_lsn,
        }
    }

    /// Opens the file for the current segment.
    fn open_current_segment(&mut self) -> WalResult<bool> {
        if self.segment_idx >= self.segments.len() {
            return Ok(false);
        }

        let segment = &self.segments[self.segment_idx];
        let file = File::open(segment.path())?;
        self.current_file = Some(file);
        self.offset = 0;

        Ok(true)
    }

    /// Moves to the next segment.
    fn next_segment(&mut self) -> WalResult<bool> {
        self.segment_idx += 1;
        self.current_file = None;
        self.open_current_segment()
    }

    /// Reads the next record from the current position.
    fn read_next_record(&mut self) -> WalResult<Option<WalRecord>> {
        loop {
            // Ensure we have a file open
            if self.current_file.is_none() {
                if !self.open_current_segment()? {
                    return Ok(None);
                }
            }

            let file = self.current_file.as_mut().unwrap();
            let segment = &self.segments[self.segment_idx];

            // Check if we've reached the end of this segment
            if self.offset >= segment.write_position() {
                if !self.next_segment()? {
                    return Ok(None);
                }
                continue;
            }

            // Read header
            file.seek(SeekFrom::Start(self.offset))?;
            let mut header_buf = vec![0u8; RecordHeader::SIZE];
            match file.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    if !self.next_segment()? {
                        return Ok(None);
                    }
                    continue;
                }
                Err(e) => return Err(e.into()),
            }

            // Check for unwritten space (all zeros) - this indicates end of valid data
            // If the header bytes are all zeros, we've reached unwritten space
            if header_buf.iter().all(|&b| b == 0) {
                if !self.next_segment()? {
                    return Ok(None);
                }
                continue;
            }

            let header = RecordHeader::from_bytes(&header_buf)?;

            // Read payload
            let mut payload_buf = vec![0u8; header.payload_length as usize];
            file.read_exact(&mut payload_buf)?;

            // Update offset
            self.offset += header.total_size() as u64;

            // Verify checksum if configured
            if self.config.verify_checksums && !header.verify_checksum(&payload_buf) {
                return Err(WalError::checksum_mismatch(
                    header.lsn,
                    header.checksum,
                    header.compute_checksum(&payload_buf),
                ));
            }

            // Skip records before start_lsn
            if self.start_lsn.is_valid() && header.lsn < self.start_lsn {
                continue;
            }

            // Deserialize
            let mut full_buf = Vec::with_capacity(RecordHeader::SIZE + payload_buf.len());
            full_buf.extend_from_slice(&header_buf);
            full_buf.extend_from_slice(&payload_buf);

            let record = WalRecord::deserialize(&mut full_buf.as_slice())?;
            return Ok(Some(record));
        }
    }
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = WalResult<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Reads all records from a single segment file.
pub fn read_segment(path: impl AsRef<Path>, config: &WalConfig) -> WalResult<Vec<WalRecord>> {
    let path = path.as_ref();
    let segment_id = WalReader::parse_segment_id(path).unwrap_or(0);
    let segment = Arc::new(WalSegment::open(segment_id, path, config)?);

    let reader = WalReader {
        config: Arc::new(config.clone()),
        segments: vec![segment],
    };

    reader.iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::WalWriter;
    use nexus_common::types::TxnId;
    use tempfile::TempDir;

    fn test_config(dir: &std::path::Path) -> Arc<WalConfig> {
        Arc::new(
            WalConfig::new(dir)
                .with_segment_size(1024 * 1024)
                .with_max_record_size(256 * 1024)
                .with_preallocate_segments(false),
        )
    }

    #[test]
    fn test_reader_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let mut reader = WalReader::new(config);

        reader.open_all_segments().unwrap();
        assert!(reader.segments().is_empty());
    }

    #[test]
    fn test_read_written_records() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        // Write some records
        let writer = WalWriter::new(Arc::clone(&config));
        let lsn1 = writer.log_commit(TxnId::new(1), Lsn::INVALID, 100).unwrap();
        let lsn2 = writer.log_commit(TxnId::new(2), Lsn::INVALID, 200).unwrap();
        let lsn3 = writer.log_commit(TxnId::new(3), Lsn::INVALID, 300).unwrap();
        writer.sync().unwrap();
        writer.close().unwrap();

        // Read them back
        let mut reader = WalReader::new(config.clone());
        reader.open_all_segments().unwrap();

        // Debug: Check if files exist
        let dir_entries: Vec<_> = std::fs::read_dir(config.dir.clone())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        eprintln!("Directory contents: {:?}", dir_entries);
        eprintln!("Segments loaded: {}", reader.segments().len());

        let records: Vec<_> = reader.iter().collect::<Result<_, _>>().unwrap();
        eprintln!("Records read: {}", records.len());

        // Should have at least 3 records (plus segment header)
        assert!(
            records.len() >= 3,
            "Expected >= 3 records, got {}",
            records.len()
        );
    }

    #[test]
    fn test_iter_from_lsn() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());

        // Write some records
        let writer = WalWriter::new(Arc::clone(&config));
        writer.log_commit(TxnId::new(1), Lsn::INVALID, 100).unwrap();
        let lsn2 = writer.log_commit(TxnId::new(2), Lsn::INVALID, 200).unwrap();
        writer.log_commit(TxnId::new(3), Lsn::INVALID, 300).unwrap();
        writer.sync().unwrap();
        writer.close().unwrap();

        // Read from lsn2
        let mut reader = WalReader::new(config);
        reader.open_all_segments().unwrap();

        let records: Vec<_> = reader.iter_from(lsn2).collect::<Result<_, _>>().unwrap();

        // Should have at least 2 records (lsn2 and lsn3)
        assert!(records.len() >= 2);
        assert!(records[0].header.lsn >= lsn2);
    }

    #[test]
    fn test_parse_segment_id() {
        assert_eq!(
            WalReader::parse_segment_id(Path::new("/data/wal/wal_0000000000000000.log")),
            Some(0)
        );
        assert_eq!(
            WalReader::parse_segment_id(Path::new("/data/wal/wal_000000000000002a.log")),
            Some(42)
        );
        assert_eq!(
            WalReader::parse_segment_id(Path::new("/data/wal/other.txt")),
            None
        );
    }
}
