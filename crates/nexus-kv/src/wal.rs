//! Write-Ahead Log for the LSM-tree KV engine.
//!
//! A simple append-only log that records put/delete operations before they
//! are applied to the memtable. On crash, the WAL is replayed to recover
//! any data that was in the memtable but not yet flushed to SSTables.
//!
//! ## Record Format
//!
//! Each record is:
//! ```text
//! [crc32:4][length:4][type:1][key_len:4][key:N][value_len:4][value:M]
//! ```
//!
//! - `crc32`: CRC32C checksum of everything after the CRC field
//! - `length`: Total byte length of the payload (type + key_len + key + value_len + value)
//! - `type`: 1 = Put, 2 = Delete
//! - `key_len` + `key`: The user key
//! - `value_len` + `value`: The value (empty for Delete)
//!
//! ## File Naming
//!
//! WAL files are named `{number:06}.wal` inside the data directory.
//! Each memtable generation gets its own WAL file. When a memtable is
//! flushed to an SSTable, its WAL file can be deleted.

use crc32fast::Hasher;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{KvError, KvResult};

// ── Constants ───────────────────────────────────────────────────────────────

/// WAL file magic number: "NXKW" (NexusDB KV WAL)
const WAL_MAGIC: u32 = 0x4E584B57;

/// Size of the file header: magic(4) + version(4)
const HEADER_SIZE: usize = 8;

/// Size of the per-record envelope: crc(4) + length(4)
const RECORD_ENVELOPE_SIZE: usize = 8;

/// Current WAL format version.
const WAL_VERSION: u32 = 1;

// ── Record Types ────────────────────────────────────────────────────────────

/// The type of operation recorded in a WAL entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// Key-value put.
    Put = 1,
    /// Key deletion (tombstone).
    Delete = 2,
}

impl WalRecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// A single record recovered from a WAL file.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub record_type: WalRecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// ── WAL Writer ──────────────────────────────────────────────────────────────

/// Appends records to a WAL file.
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    file_number: u64,
    sync_writes: bool,
    bytes_written: u64,
}

impl WalWriter {
    /// Create a new WAL file with the given number.
    pub fn create(dir: &Path, file_number: u64, sync_writes: bool) -> KvResult<Self> {
        let path = wal_path(dir, file_number);
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        // Write file header
        writer.write_all(&WAL_MAGIC.to_le_bytes())?;
        writer.write_all(&WAL_VERSION.to_le_bytes())?;
        writer.flush()?;

        Ok(Self {
            writer,
            path,
            file_number,
            sync_writes,
            bytes_written: HEADER_SIZE as u64,
        })
    }

    /// Append a Put record.
    pub fn log_put(&mut self, key: &[u8], value: &[u8]) -> KvResult<()> {
        self.append_record(WalRecordType::Put, key, value)
    }

    /// Append a Delete record.
    pub fn log_delete(&mut self, key: &[u8]) -> KvResult<()> {
        self.append_record(WalRecordType::Delete, key, &[])
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> KvResult<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Returns the file number of this WAL.
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Returns the number of bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Returns the path of this WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush and close the writer.
    pub fn close(mut self) -> KvResult<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    // ── Internal ────────────────────────────────────────────────

    fn append_record(
        &mut self,
        record_type: WalRecordType,
        key: &[u8],
        value: &[u8],
    ) -> KvResult<()> {
        // Build payload: type(1) + key_len(4) + key(N) + value_len(4) + value(M)
        let payload_len = 1 + 4 + key.len() + 4 + value.len();
        let mut payload = Vec::with_capacity(payload_len);
        payload.push(record_type as u8);
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value);

        // Compute CRC over the payload
        let crc = compute_crc(&payload);

        // Write: crc(4) + length(4) + payload
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&(payload_len as u32).to_le_bytes())?;
        self.writer.write_all(&payload)?;

        self.bytes_written += (RECORD_ENVELOPE_SIZE + payload_len) as u64;

        if self.sync_writes {
            self.writer.flush()?;
            self.writer.get_ref().sync_data()?;
        }

        Ok(())
    }
}

// ── WAL Reader ──────────────────────────────────────────────────────────────

/// Reads and replays records from a WAL file.
pub struct WalReader {
    path: PathBuf,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn open(path: PathBuf) -> Self {
        Self { path }
    }

    /// Replay all valid records from the WAL file.
    ///
    /// Stops at the first corrupted or incomplete record (tail corruption
    /// is expected after a crash). Returns all records that were successfully
    /// read before the corruption point.
    pub fn replay(&self) -> KvResult<Vec<WalRecord>> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(KvError::Io(e)),
        };

        let file_len = file.metadata()?.len();
        if file_len < HEADER_SIZE as u64 {
            return Err(KvError::CorruptedManifest(format!(
                "WAL file too small: {} bytes",
                file_len
            )));
        }

        let mut reader = BufReader::new(file);

        // Validate header
        let mut header_buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;
        let magic = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

        if magic != WAL_MAGIC {
            return Err(KvError::CorruptedManifest(format!(
                "WAL bad magic: {:#010x}",
                magic
            )));
        }
        if version != WAL_VERSION {
            return Err(KvError::CorruptedManifest(format!(
                "WAL unsupported version: {}",
                version
            )));
        }

        // Read records until EOF or corruption
        let mut records = Vec::new();
        let mut envelope_buf = [0u8; RECORD_ENVELOPE_SIZE];

        loop {
            // Try to read the envelope (crc + length)
            match reader.read_exact(&mut envelope_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(KvError::Io(e)),
            }

            let expected_crc = u32::from_le_bytes(envelope_buf[0..4].try_into().unwrap());
            let payload_len = u32::from_le_bytes(envelope_buf[4..8].try_into().unwrap()) as usize;

            // Sanity check: payload should be at least 1 (type) + 4 (key_len) + 4 (value_len)
            if payload_len < 9 || payload_len > 32 * 1024 * 1024 {
                // Likely tail corruption, stop replay
                break;
            }

            // Read payload
            let mut payload = vec![0u8; payload_len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(KvError::Io(e)),
            }

            // Verify CRC
            let actual_crc = compute_crc(&payload);
            if actual_crc != expected_crc {
                // CRC mismatch: tail corruption, stop replay
                break;
            }

            // Parse record
            match parse_record(&payload) {
                Some(record) => records.push(record),
                None => break, // Malformed record, stop
            }
        }

        Ok(records)
    }
}

// ── WAL Management ──────────────────────────────────────────────────────────

/// Find all WAL files in a directory, sorted by file number.
pub fn find_wal_files(dir: &Path) -> KvResult<Vec<(u64, PathBuf)>> {
    let mut wals = Vec::new();

    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(KvError::Io(e)),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "wal" {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(num) = stem.parse::<u64>() {
                        wals.push((num, path));
                    }
                }
            }
        }
    }

    wals.sort_by_key(|(num, _)| *num);
    Ok(wals)
}

/// Delete a WAL file by number.
pub fn delete_wal(dir: &Path, file_number: u64) -> KvResult<()> {
    let path = wal_path(dir, file_number);
    if path.exists() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

/// Get the path for a WAL file.
pub fn wal_path(dir: &Path, file_number: u64) -> PathBuf {
    dir.join(format!("{:06}.wal", file_number))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn compute_crc(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn parse_record(payload: &[u8]) -> Option<WalRecord> {
    if payload.len() < 9 {
        return None;
    }
    let record_type = WalRecordType::from_u8(payload[0])?;

    let key_len = u32::from_le_bytes(payload[1..5].try_into().ok()?) as usize;
    if 5 + key_len + 4 > payload.len() {
        return None;
    }
    let key = payload[5..5 + key_len].to_vec();

    let vl_start = 5 + key_len;
    let value_len = u32::from_le_bytes(payload[vl_start..vl_start + 4].try_into().ok()?) as usize;
    let v_start = vl_start + 4;
    if v_start + value_len > payload.len() {
        return None;
    }
    let value = payload[v_start..v_start + value_len].to_vec();

    Some(WalRecord {
        record_type,
        key,
        value,
    })
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use tempfile::TempDir;

    #[test]
    fn test_wal_write_and_replay() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        // Write some records
        {
            let mut w = WalWriter::create(dir, 1, false).unwrap();
            w.log_put(b"hello", b"world").unwrap();
            w.log_put(b"foo", b"bar").unwrap();
            w.log_delete(b"hello").unwrap();
            w.close().unwrap();
        }

        // Replay
        let reader = WalReader::open(wal_path(dir, 1));
        let records = reader.replay().unwrap();
        assert_eq!(records.len(), 3);

        assert_eq!(records[0].record_type, WalRecordType::Put);
        assert_eq!(records[0].key, b"hello");
        assert_eq!(records[0].value, b"world");

        assert_eq!(records[1].record_type, WalRecordType::Put);
        assert_eq!(records[1].key, b"foo");

        assert_eq!(records[2].record_type, WalRecordType::Delete);
        assert_eq!(records[2].key, b"hello");
        assert!(records[2].value.is_empty());
    }

    #[test]
    fn test_wal_replay_empty() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        {
            let w = WalWriter::create(dir, 1, false).unwrap();
            w.close().unwrap();
        }

        let reader = WalReader::open(wal_path(dir, 1));
        let records = reader.replay().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_wal_replay_truncated() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        // Write records then truncate the file to simulate a crash
        {
            let mut w = WalWriter::create(dir, 1, false).unwrap();
            w.log_put(b"key1", b"val1").unwrap();
            w.log_put(b"key2", b"val2").unwrap();
            w.close().unwrap();
        }

        // Truncate the last few bytes
        let path = wal_path(dir, 1);
        let len = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 5).unwrap();

        let reader = WalReader::open(path);
        let records = reader.replay().unwrap();
        // First record should survive, second may be lost
        assert!(records.len() >= 1);
        assert_eq!(records[0].key, b"key1");
    }

    #[test]
    fn test_find_wal_files() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        WalWriter::create(dir, 3, false).unwrap().close().unwrap();
        WalWriter::create(dir, 1, false).unwrap().close().unwrap();
        WalWriter::create(dir, 5, false).unwrap().close().unwrap();

        let wals = find_wal_files(dir).unwrap();
        assert_eq!(wals.len(), 3);
        assert_eq!(wals[0].0, 1);
        assert_eq!(wals[1].0, 3);
        assert_eq!(wals[2].0, 5);
    }

    #[test]
    fn test_wal_nonexistent_dir() {
        let wals = find_wal_files(Path::new("/nonexistent/path/wal")).unwrap();
        assert!(wals.is_empty());
    }

    #[test]
    fn test_wal_large_values() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        let big_key = vec![0xABu8; 1024];
        let big_value = vec![0xCDu8; 64 * 1024];

        {
            let mut w = WalWriter::create(dir, 1, false).unwrap();
            w.log_put(&big_key, &big_value).unwrap();
            w.close().unwrap();
        }

        let reader = WalReader::open(wal_path(dir, 1));
        let records = reader.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, big_key);
        assert_eq!(records[0].value, big_value);
    }
}
