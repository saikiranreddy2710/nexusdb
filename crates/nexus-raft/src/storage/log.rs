//! Persistent Raft log backed by file storage.
//!
//! This module provides a durable log implementation that persists entries
//! to disk. It uses a simple append-only file format with indexing for
//! efficient random access.
//!
//! # File Format
//!
//! The log is stored in a series of segment files:
//! - `log/segment_00000000.log` - Log entries
//! - `log/segment_00000000.idx` - Index for fast lookup
//!
//! Each entry in the log file:
//! ```text
//! +--------+------+-------+----------+----------+----------+
//! | Magic  | Len  | Term  | Index    | Type     | Data     |
//! | (4)    | (4)  | (8)   | (8)      | (1)      | (Len-17) |
//! +--------+------+-------+----------+----------+----------+
//! ```

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;

use crate::log::SnapshotMeta;
use crate::rpc::{EntryType, LogEntry, Term};
use crate::LogIndex;

use super::files::{LOG_DIR, SNAPSHOT_FILE, SNAPSHOT_META_FILE, SNAPSHOT_TMP};
use super::{StorageError, StorageResult};

/// Magic number for log entries.
const ENTRY_MAGIC: u32 = 0x524C4F47; // "RLOG"

/// Size of entry header (magic + len + term + index + type).
const ENTRY_HEADER_SIZE: usize = 4 + 4 + 8 + 8 + 1;

/// Persistent Raft log.
///
/// Stores log entries durably on disk with support for snapshots
/// and log compaction.
#[derive(Debug)]
pub struct PersistentLog {
    /// Directory for log files.
    dir: PathBuf,
    /// Log entries (in-memory cache of recent entries).
    entries: RwLock<VecDeque<LogEntry>>,
    /// Offset for index calculation (entries before this are in snapshot).
    offset: AtomicU64,
    /// Snapshot metadata.
    snapshot_meta: RwLock<SnapshotMeta>,
    /// Current log file.
    log_file: RwLock<Option<File>>,
    /// Whether to sync writes.
    sync_writes: bool,
    /// Whether the log is closed.
    closed: AtomicBool,
}

impl PersistentLog {
    /// Creates a new persistent log.
    pub fn new<P: AsRef<Path>>(dir: P, sync_writes: bool) -> StorageResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        let log_dir = dir.join(LOG_DIR);
        fs::create_dir_all(&log_dir)?;

        let log = Self {
            dir: dir.clone(),
            entries: RwLock::new(VecDeque::new()),
            offset: AtomicU64::new(0),
            snapshot_meta: RwLock::new(SnapshotMeta::default()),
            log_file: RwLock::new(None),
            sync_writes,
            closed: AtomicBool::new(false),
        };

        // Load snapshot metadata if it exists
        log.load_snapshot_meta()?;

        // Replay log from disk
        log.replay()?;

        // Open log file for appending
        log.open_log_file()?;

        Ok(log)
    }

    /// Returns the first log index.
    pub fn first_index(&self) -> LogIndex {
        let offset = self.offset.load(Ordering::Acquire);
        if offset == 0 {
            1
        } else {
            offset + 1
        }
    }

    /// Returns the last log index.
    pub fn last_index(&self) -> LogIndex {
        let entries = self.entries.read();
        let offset = self.offset.load(Ordering::Acquire);

        if entries.is_empty() {
            offset
        } else {
            entries.back().map(|e| e.index).unwrap_or(offset)
        }
    }

    /// Returns the last term.
    pub fn last_term(&self) -> Term {
        let entries = self.entries.read();

        if entries.is_empty() {
            self.snapshot_meta.read().last_included_term
        } else {
            entries.back().map(|e| e.term).unwrap_or(0)
        }
    }

    /// Returns the term at the given index.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }

        let offset = self.offset.load(Ordering::Acquire);

        // Check if this is the snapshot index
        if index == offset {
            let meta = self.snapshot_meta.read();
            if meta.last_included_index == index {
                return Some(meta.last_included_term);
            }
        }

        // Check if compacted
        if index <= offset && offset > 0 {
            return None;
        }

        let entries = self.entries.read();
        let physical_index = if offset == 0 {
            index.checked_sub(1)?
        } else {
            index.checked_sub(offset + 1)?
        };

        entries.get(physical_index as usize).map(|e| e.term)
    }

    /// Gets the entry at the given index.
    pub fn get(&self, index: LogIndex) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }

        let offset = self.offset.load(Ordering::Acquire);

        // Check if compacted
        if index <= offset && offset > 0 {
            return None;
        }

        let entries = self.entries.read();
        let physical_index = if offset == 0 {
            index.checked_sub(1)?
        } else {
            index.checked_sub(offset + 1)?
        };

        entries.get(physical_index as usize).cloned()
    }

    /// Gets entries in the range [start, end).
    pub fn get_range(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>> {
        if start >= end {
            return Ok(Vec::new());
        }

        let offset = self.offset.load(Ordering::Acquire);
        let first = self.first_index();

        if start < first && offset > 0 {
            return Err(StorageError::LogCompacted(first));
        }

        let entries = self.entries.read();
        let mut result = Vec::with_capacity((end - start) as usize);

        for index in start..end {
            let physical_index = if offset == 0 {
                index.saturating_sub(1)
            } else {
                index.saturating_sub(offset + 1)
            };

            if let Some(entry) = entries.get(physical_index as usize) {
                result.push(entry.clone());
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Appends entries to the log.
    pub fn append(&self, new_entries: Vec<LogEntry>) -> StorageResult<()> {
        if new_entries.is_empty() {
            return Ok(());
        }

        if self.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed);
        }

        let offset = self.offset.load(Ordering::Acquire);
        let mut entries = self.entries.write();

        // Write entries to disk first
        {
            let mut log_file = self.log_file.write();
            let file = log_file.as_mut().ok_or(StorageError::Closed)?;

            for entry in &new_entries {
                Self::write_entry(file, entry)?;
            }

            if self.sync_writes {
                file.sync_all()?;
            }
        }

        // Then update in-memory state
        for entry in new_entries {
            let physical_index = if offset == 0 {
                entry.index.saturating_sub(1)
            } else {
                entry.index.saturating_sub(offset + 1)
            };

            if physical_index < entries.len() as u64 {
                // Check for conflict
                let existing = &entries[physical_index as usize];
                if existing.term != entry.term {
                    // Conflict! Truncate from this point
                    entries.truncate(physical_index as usize);
                    entries.push_back(entry);
                }
                // If terms match, entry is already there (idempotent)
            } else if physical_index == entries.len() as u64 {
                // Append new entry
                entries.push_back(entry);
            } else {
                // Gap in log - should not happen
                return Err(StorageError::Corrupted(format!(
                    "log gap at index {}",
                    entry.index
                )));
            }
        }

        Ok(())
    }

    /// Truncates the log from the given index onwards.
    pub fn truncate_from(&self, from_index: LogIndex) -> StorageResult<()> {
        let mut entries = self.entries.write();
        let offset = self.offset.load(Ordering::Acquire);
        let first = if offset == 0 { 1 } else { offset + 1 };

        if from_index < first {
            return Err(StorageError::LogCompacted(first));
        }

        let physical_index = if offset == 0 {
            from_index.saturating_sub(1)
        } else {
            from_index.saturating_sub(offset + 1)
        };

        if physical_index < entries.len() as u64 {
            entries.truncate(physical_index as usize);

            // Rewrite log file (simple approach - could be optimized)
            self.rewrite_log_file(&entries)?;
        }

        Ok(())
    }

    /// Returns snapshot metadata.
    pub fn snapshot_meta(&self) -> SnapshotMeta {
        self.snapshot_meta.read().clone()
    }

    /// Creates a snapshot at the given index.
    pub fn create_snapshot(
        &self,
        index: LogIndex,
        term: Term,
        data: Bytes,
    ) -> StorageResult<SnapshotMeta> {
        // Write snapshot data to temp file
        let snapshot_tmp = self.dir.join(SNAPSHOT_TMP);
        let snapshot_path = self.dir.join(SNAPSHOT_FILE);
        let _meta_path = self.dir.join(SNAPSHOT_META_FILE);

        // Write data
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&snapshot_tmp)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }

        // Atomic rename
        fs::rename(&snapshot_tmp, &snapshot_path)?;

        // Write metadata
        let meta = SnapshotMeta {
            last_included_index: index,
            last_included_term: term,
            size: data.len() as u64,
        };
        self.save_snapshot_meta(&meta)?;

        // Compact the log
        self.compact(index)?;

        // Sync directory
        if let Ok(dir) = File::open(&self.dir) {
            let _ = dir.sync_all();
        }

        Ok(meta)
    }

    /// Installs a snapshot from the leader.
    pub fn install_snapshot(&self, meta: SnapshotMeta, data: Bytes) -> StorageResult<()> {
        let snapshot_tmp = self.dir.join(SNAPSHOT_TMP);
        let snapshot_path = self.dir.join(SNAPSHOT_FILE);

        // Write data
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&snapshot_tmp)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }

        // Atomic rename
        fs::rename(&snapshot_tmp, &snapshot_path)?;

        // Save metadata
        self.save_snapshot_meta(&meta)?;

        // Clear log and reset offset
        {
            let mut entries = self.entries.write();
            entries.clear();
        }
        self.offset
            .store(meta.last_included_index, Ordering::Release);
        *self.snapshot_meta.write() = meta;

        // Clear and reopen log file
        self.clear_log_file()?;
        self.open_log_file()?;

        Ok(())
    }

    /// Returns the snapshot data.
    pub fn snapshot_data(&self) -> StorageResult<Option<Bytes>> {
        let snapshot_path = self.dir.join(SNAPSHOT_FILE);
        if !snapshot_path.exists() {
            return Ok(None);
        }

        let data = fs::read(&snapshot_path)?;
        Ok(Some(Bytes::from(data)))
    }

    /// Syncs all pending writes.
    pub fn sync(&self) -> StorageResult<()> {
        let log_file = self.log_file.read();
        if let Some(ref file) = *log_file {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Closes the log.
    pub fn close(&self) -> StorageResult<()> {
        self.closed.store(true, Ordering::Release);
        self.sync()?;
        *self.log_file.write() = None;
        Ok(())
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    // --- Private methods ---

    fn log_file_path(&self) -> PathBuf {
        self.dir.join(LOG_DIR).join("entries.log")
    }

    fn open_log_file(&self) -> StorageResult<()> {
        let path = self.log_file_path();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        *self.log_file.write() = Some(file);
        Ok(())
    }

    fn clear_log_file(&self) -> StorageResult<()> {
        let path = self.log_file_path();
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn rewrite_log_file(&self, entries: &VecDeque<LogEntry>) -> StorageResult<()> {
        let path = self.log_file_path();
        let tmp_path = path.with_extension("tmp");

        // Write to temp file
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;

            for entry in entries {
                Self::write_entry(&mut file, entry)?;
            }

            file.sync_all()?;
        }

        // Atomic rename
        fs::rename(&tmp_path, &path)?;

        // Reopen for appending
        self.open_log_file()?;

        Ok(())
    }

    fn write_entry(file: &mut File, entry: &LogEntry) -> StorageResult<()> {
        let data_len = entry.data.len();
        let total_len = ENTRY_HEADER_SIZE + data_len;

        let mut buf = BytesMut::with_capacity(total_len);

        // Magic
        buf.put_u32_le(ENTRY_MAGIC);
        // Length (excluding magic and length itself)
        buf.put_u32_le((total_len - 8) as u32);
        // Term
        buf.put_u64_le(entry.term);
        // Index
        buf.put_u64_le(entry.index);
        // Entry type
        buf.put_u8(match entry.entry_type {
            EntryType::Command => 0,
            EntryType::Noop => 1,
            EntryType::Config => 2,
        });
        // Data
        buf.put_slice(&entry.data);

        file.write_all(&buf)?;
        Ok(())
    }

    fn read_entry(file: &mut File) -> StorageResult<Option<LogEntry>> {
        let mut header = [0u8; ENTRY_HEADER_SIZE];

        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        }

        let mut cursor = &header[..];

        // Magic
        let magic = cursor.get_u32_le();
        if magic != ENTRY_MAGIC {
            return Err(StorageError::Corrupted(format!(
                "invalid entry magic: {:08x}",
                magic
            )));
        }

        // Length
        let len = cursor.get_u32_le() as usize;

        // Term
        let term = cursor.get_u64_le();

        // Index
        let index = cursor.get_u64_le();

        // Entry type
        let entry_type_byte = cursor.get_u8();
        let entry_type = match entry_type_byte {
            0 => EntryType::Command,
            1 => EntryType::Noop,
            2 => EntryType::Config,
            _ => {
                return Err(StorageError::Corrupted(format!(
                    "invalid entry type: {}",
                    entry_type_byte
                )))
            }
        };

        // Data
        let data_len = len - 17; // len - (term + index + type)
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data)?;

        Ok(Some(LogEntry {
            term,
            index,
            entry_type,
            data: Bytes::from(data),
        }))
    }

    fn replay(&self) -> StorageResult<()> {
        let path = self.log_file_path();
        if !path.exists() {
            return Ok(());
        }

        let mut file = File::open(&path)?;
        let mut entries = self.entries.write();

        while let Some(entry) = Self::read_entry(&mut file)? {
            // Skip entries before snapshot
            let offset = self.offset.load(Ordering::Acquire);
            if entry.index > offset {
                entries.push_back(entry);
            }
        }

        Ok(())
    }

    fn compact(&self, compact_index: LogIndex) -> StorageResult<()> {
        let mut entries = self.entries.write();
        let offset = self.offset.load(Ordering::Acquire);
        let current_first = if offset == 0 { 1 } else { offset + 1 };

        if compact_index >= current_first {
            let entries_to_remove = (compact_index - current_first + 1) as usize;
            for _ in 0..entries_to_remove.min(entries.len()) {
                entries.pop_front();
            }
        }

        self.offset.store(compact_index, Ordering::Release);

        // Rewrite log file with remaining entries
        self.rewrite_log_file(&entries)?;

        Ok(())
    }

    fn load_snapshot_meta(&self) -> StorageResult<()> {
        let meta_path = self.dir.join(SNAPSHOT_META_FILE);
        if !meta_path.exists() {
            return Ok(());
        }

        let data = fs::read(&meta_path)?;
        if data.len() < 24 {
            return Err(StorageError::Corrupted(
                "snapshot meta too short".to_string(),
            ));
        }

        let mut cursor = &data[..];
        let last_included_index = cursor.get_u64_le();
        let last_included_term = cursor.get_u64_le();
        let size = cursor.get_u64_le();

        let meta = SnapshotMeta {
            last_included_index,
            last_included_term,
            size,
        };

        self.offset.store(last_included_index, Ordering::Release);
        *self.snapshot_meta.write() = meta;

        Ok(())
    }

    fn save_snapshot_meta(&self, meta: &SnapshotMeta) -> StorageResult<()> {
        let meta_path = self.dir.join(SNAPSHOT_META_FILE);
        let tmp_path = meta_path.with_extension("tmp");

        let mut buf = BytesMut::with_capacity(24);
        buf.put_u64_le(meta.last_included_index);
        buf.put_u64_le(meta.last_included_term);
        buf.put_u64_le(meta.size);

        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }

        fs::rename(&tmp_path, &meta_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_log() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        assert_eq!(log.first_index(), 1);
        assert_eq!(log.last_index(), 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_append_and_get() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        let entries = vec![
            LogEntry::command(1, 1, Bytes::from("cmd1")),
            LogEntry::command(1, 2, Bytes::from("cmd2")),
            LogEntry::command(2, 3, Bytes::from("cmd3")),
        ];

        log.append(entries).unwrap();

        assert_eq!(log.len(), 3);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);

        let entry = log.get(1).unwrap();
        assert_eq!(entry.term, 1);
        assert_eq!(entry.index, 1);
        assert_eq!(entry.data.as_ref(), b"cmd1");

        let entry = log.get(3).unwrap();
        assert_eq!(entry.term, 2);
        assert_eq!(entry.data.as_ref(), b"cmd3");
    }

    #[test]
    fn test_get_range() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        let entries: Vec<_> = (1..=5)
            .map(|i| LogEntry::command(1, i, Bytes::from(format!("cmd{}", i))))
            .collect();

        log.append(entries).unwrap();

        let range = log.get_range(2, 5).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].index, 2);
        assert_eq!(range[2].index, 4);
    }

    #[test]
    fn test_persistence() {
        let tmp = TempDir::new().unwrap();

        // Write entries
        {
            let log = PersistentLog::new(tmp.path(), true).unwrap();
            let entries = vec![
                LogEntry::command(1, 1, Bytes::from("cmd1")),
                LogEntry::command(2, 2, Bytes::from("cmd2")),
            ];
            log.append(entries).unwrap();
            log.close().unwrap();
        }

        // Reload and verify
        {
            let log = PersistentLog::new(tmp.path(), true).unwrap();
            assert_eq!(log.len(), 2);
            assert_eq!(log.last_index(), 2);

            let entry = log.get(1).unwrap();
            assert_eq!(entry.data.as_ref(), b"cmd1");
        }
    }

    #[test]
    fn test_truncate() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        let entries: Vec<_> = (1..=5)
            .map(|i| LogEntry::command(1, i, Bytes::from(format!("cmd{}", i))))
            .collect();

        log.append(entries).unwrap();
        log.truncate_from(3).unwrap();

        assert_eq!(log.len(), 2);
        assert_eq!(log.last_index(), 2);
        assert!(log.get(3).is_none());
    }

    #[test]
    fn test_snapshot() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        let entries: Vec<_> = (1..=10)
            .map(|i| LogEntry::command(1, i, Bytes::from(format!("cmd{}", i))))
            .collect();

        log.append(entries).unwrap();

        // Create snapshot at index 5
        let snapshot_data = Bytes::from("snapshot state");
        let meta = log.create_snapshot(5, 1, snapshot_data.clone()).unwrap();

        assert_eq!(meta.last_included_index, 5);
        assert_eq!(meta.last_included_term, 1);

        // Log should be compacted
        assert_eq!(log.first_index(), 6);
        assert_eq!(log.len(), 5); // entries 6-10

        // Snapshot data should be retrievable
        let data = log.snapshot_data().unwrap().unwrap();
        assert_eq!(data.as_ref(), snapshot_data.as_ref());
    }

    #[test]
    fn test_snapshot_persistence() {
        let tmp = TempDir::new().unwrap();

        // Create log with snapshot
        {
            let log = PersistentLog::new(tmp.path(), true).unwrap();
            let entries: Vec<_> = (1..=10)
                .map(|i| LogEntry::command(1, i, Bytes::from(format!("cmd{}", i))))
                .collect();
            log.append(entries).unwrap();
            log.create_snapshot(5, 1, Bytes::from("snapshot")).unwrap();
            log.close().unwrap();
        }

        // Reload and verify
        {
            let log = PersistentLog::new(tmp.path(), true).unwrap();
            assert_eq!(log.first_index(), 6);
            let meta = log.snapshot_meta();
            assert_eq!(meta.last_included_index, 5);
        }
    }

    #[test]
    fn test_term_at() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        let entries = vec![
            LogEntry::command(1, 1, Bytes::from("cmd1")),
            LogEntry::command(2, 2, Bytes::from("cmd2")),
            LogEntry::command(2, 3, Bytes::from("cmd3")),
        ];

        log.append(entries).unwrap();

        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), None);
    }

    #[test]
    fn test_install_snapshot() {
        let tmp = TempDir::new().unwrap();
        let log = PersistentLog::new(tmp.path(), true).unwrap();

        // Add some entries
        let entries = vec![
            LogEntry::command(1, 1, Bytes::from("cmd1")),
            LogEntry::command(1, 2, Bytes::from("cmd2")),
        ];
        log.append(entries).unwrap();

        // Install snapshot from leader
        let meta = SnapshotMeta {
            last_included_index: 100,
            last_included_term: 5,
            size: 1024,
        };
        let data = Bytes::from("leader snapshot data");
        log.install_snapshot(meta.clone(), data.clone()).unwrap();

        // Verify
        assert_eq!(log.first_index(), 101);
        assert_eq!(log.last_index(), 100);
        assert!(log.is_empty());

        let loaded_meta = log.snapshot_meta();
        assert_eq!(loaded_meta.last_included_index, 100);
        assert_eq!(loaded_meta.last_included_term, 5);

        let loaded_data = log.snapshot_data().unwrap().unwrap();
        assert_eq!(loaded_data.as_ref(), data.as_ref());
    }
}
