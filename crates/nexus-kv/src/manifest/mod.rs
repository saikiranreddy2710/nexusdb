//! Manifest: version management for the LSM-tree.
//!
//! The manifest tracks the set of live SSTable files at each level,
//! enabling atomic state transitions during compaction and flush.
//! It is persisted to disk as a sequence of version edits, allowing
//! crash recovery to restore the latest consistent state.
//!
//! ## Version Edit
//!
//! Each state change (flush or compaction) is recorded as a `VersionEdit`:
//! - Files added at a specific level
//! - Files removed from a specific level
//! - Updated sequence numbers and log numbers
//!
//! ## Version
//!
//! A `Version` is an immutable snapshot of the LSM-tree state (which
//! files exist at each level). Versions are reference-counted so that
//! ongoing reads can continue even as new versions are created.

use crate::compaction::LevelInfo;
use crate::config::LsmConfig;
use crate::error::{KvError, KvResult};
use crate::sstable::SSTableInfo;
use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

/// A single edit to the version state.
#[derive(Debug, Clone)]
pub struct VersionEdit {
    /// New files to add, keyed by level.
    pub added_files: Vec<(usize, SSTableInfo)>,
    /// Files to remove, keyed by level and file ID.
    pub removed_files: Vec<(usize, u64)>,
    /// Updated last sequence number (if changed).
    pub last_sequence: Option<u64>,
    /// Updated next file number (if changed).
    pub next_file_number: Option<u64>,
    /// Updated log number (if changed).
    pub log_number: Option<u64>,
}

impl VersionEdit {
    /// Create an empty version edit.
    pub fn new() -> Self {
        Self {
            added_files: Vec::new(),
            removed_files: Vec::new(),
            last_sequence: None,
            next_file_number: None,
            log_number: None,
        }
    }

    /// Add a file at the specified level.
    pub fn add_file(&mut self, level: usize, file: SSTableInfo) {
        self.added_files.push((level, file));
    }

    /// Remove a file from the specified level.
    pub fn remove_file(&mut self, level: usize, file_id: u64) {
        self.removed_files.push((level, file_id));
    }

    /// Set the last sequence number.
    pub fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = Some(seq);
    }

    /// Set the next file number.
    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    /// Encode the version edit to bytes for persistence.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Tag 1: last_sequence
        if let Some(seq) = self.last_sequence {
            buf.push(1);
            buf.extend_from_slice(&seq.to_le_bytes());
        }

        // Tag 2: next_file_number
        if let Some(num) = self.next_file_number {
            buf.push(2);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        // Tag 3: log_number
        if let Some(num) = self.log_number {
            buf.push(3);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        // Tag 4: added files
        for (level, file) in &self.added_files {
            buf.push(4);
            buf.extend_from_slice(&(*level as u32).to_le_bytes());
            buf.extend_from_slice(&file.id.to_le_bytes());
            buf.extend_from_slice(&file.file_size.to_le_bytes());
            buf.extend_from_slice(&file.entry_count.to_le_bytes());
            buf.extend_from_slice(&file.min_sequence.to_le_bytes());
            buf.extend_from_slice(&file.max_sequence.to_le_bytes());
            // Smallest key
            buf.extend_from_slice(&(file.smallest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&file.smallest_key);
            // Largest key
            buf.extend_from_slice(&(file.largest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&file.largest_key);
        }

        // Tag 5: removed files
        for (level, file_id) in &self.removed_files {
            buf.push(5);
            buf.extend_from_slice(&(*level as u32).to_le_bytes());
            buf.extend_from_slice(&file_id.to_le_bytes());
        }

        // Tag 0: end marker
        buf.push(0);

        buf
    }

    /// Decode a version edit from bytes.
    pub fn decode(data: &[u8]) -> KvResult<Self> {
        let mut edit = Self::new();
        let mut pos = 0;

        while pos < data.len() {
            let tag = data[pos];
            pos += 1;

            match tag {
                0 => break, // End marker
                1 => {
                    // last_sequence
                    if pos + 8 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated sequence".into()));
                    }
                    let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    edit.last_sequence = Some(seq);
                    pos += 8;
                }
                2 => {
                    // next_file_number
                    if pos + 8 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated file number".into()));
                    }
                    let num = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    edit.next_file_number = Some(num);
                    pos += 8;
                }
                3 => {
                    // log_number
                    if pos + 8 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated log number".into()));
                    }
                    let num = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    edit.log_number = Some(num);
                    pos += 8;
                }
                4 => {
                    // added file
                    if pos + 4 + 8 + 8 + 8 + 8 + 8 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated file entry".into()));
                    }
                    let level = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    let id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let file_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let entry_count = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let min_seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let max_seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;

                    // Smallest key
                    if pos + 4 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated key len".into()));
                    }
                    let sk_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + sk_len > data.len() {
                        return Err(KvError::CorruptedManifest("truncated smallest key".into()));
                    }
                    let smallest_key = data[pos..pos + sk_len].to_vec();
                    pos += sk_len;

                    // Largest key
                    if pos + 4 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated key len".into()));
                    }
                    let lk_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + lk_len > data.len() {
                        return Err(KvError::CorruptedManifest("truncated largest key".into()));
                    }
                    let largest_key = data[pos..pos + lk_len].to_vec();
                    pos += lk_len;

                    edit.added_files.push((
                        level,
                        SSTableInfo {
                            id,
                            level,
                            smallest_key,
                            largest_key,
                            file_size,
                            entry_count,
                            min_sequence: min_seq,
                            max_sequence: max_seq,
                            data_block_count: 0,
                        },
                    ));
                }
                5 => {
                    // removed file
                    if pos + 4 + 8 > data.len() {
                        return Err(KvError::CorruptedManifest("truncated remove entry".into()));
                    }
                    let level = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    let file_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    edit.removed_files.push((level, file_id));
                }
                _ => {
                    return Err(KvError::CorruptedManifest(format!("unknown tag: {}", tag)));
                }
            }
        }

        Ok(edit)
    }
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self::new()
    }
}

/// An immutable snapshot of the LSM-tree's file state.
#[derive(Debug, Clone)]
pub struct Version {
    /// Files at each level.
    pub levels: Vec<LevelInfo>,
    /// Version number (monotonically increasing).
    pub version_number: u64,
}

impl Version {
    /// Create a new empty version.
    pub fn new(config: &LsmConfig) -> Self {
        let mut levels = Vec::with_capacity(config.max_levels);
        for i in 0..config.max_levels {
            let max_size = config.max_bytes_for_level(i);
            levels.push(LevelInfo::new(i, max_size));
        }
        Self {
            levels,
            version_number: 0,
        }
    }

    /// Apply a version edit to create a new version.
    pub fn apply(&self, edit: &VersionEdit) -> Version {
        let mut new_version = self.clone();
        new_version.version_number += 1;

        // Remove files
        for (level, file_id) in &edit.removed_files {
            if *level < new_version.levels.len() {
                new_version.levels[*level].remove_file(*file_id);
            }
        }

        // Add files
        for (level, file) in &edit.added_files {
            if *level < new_version.levels.len() {
                new_version.levels[*level].add_file(file.clone());
            }
        }

        // Sort non-L0 levels by key
        for level in new_version.levels.iter_mut().skip(1) {
            level.sort_by_key();
        }

        new_version
    }

    /// Find the SSTable files that might contain the given key.
    pub fn files_for_key(&self, key: &[u8]) -> Vec<(usize, &SSTableInfo)> {
        let mut result = Vec::new();

        // L0: all files might contain the key (overlapping ranges)
        for file in &self.levels[0].files {
            if file.might_contain_key(key) {
                result.push((0, file));
            }
        }

        // L1+: binary search for the right file (non-overlapping ranges)
        for level in &self.levels[1..] {
            if level.is_empty() {
                continue;
            }
            // Binary search for the file that might contain this key
            let idx = level
                .files
                .partition_point(|f| f.largest_key.as_slice() < key);
            if idx < level.files.len() && level.files[idx].might_contain_key(key) {
                result.push((level.level, &level.files[idx]));
            }
        }

        result
    }

    /// Total number of SSTable files across all levels.
    pub fn total_file_count(&self) -> usize {
        self.levels.iter().map(|l| l.file_count()).sum()
    }

    /// Total size of all SSTable files across all levels.
    pub fn total_size(&self) -> u64 {
        self.levels.iter().map(|l| l.total_size).sum()
    }
}

// ── Manifest File Constants ─────────────────────────────────────────────────

/// Manifest file magic: "NXMF" (NexusDB Manifest)
const MANIFEST_MAGIC: u32 = 0x4E584D46;
/// Current manifest format version.
const MANIFEST_VERSION: u32 = 1;
/// File header size: magic(4) + version(4)
const MANIFEST_HEADER_SIZE: usize = 8;

// ── Manifest Writer ─────────────────────────────────────────────────────────

/// Appends encoded `VersionEdit`s to a manifest file on disk.
struct ManifestWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl ManifestWriter {
    /// Create (or truncate) a new manifest file and write its header.
    fn create(path: &Path) -> KvResult<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&MANIFEST_MAGIC.to_le_bytes())?;
        writer.write_all(&MANIFEST_VERSION.to_le_bytes())?;
        writer.flush()?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
        })
    }

    /// Open an existing manifest file for appending.
    fn open_append(path: &Path) -> KvResult<Self> {
        let file = OpenOptions::new().append(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            path: path.to_path_buf(),
        })
    }

    /// Append a version edit record: [crc:4][length:4][edit_bytes:N]
    fn append(&mut self, edit: &VersionEdit) -> KvResult<()> {
        let data = edit.encode();
        let crc = manifest_crc(&data);

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&(data.len() as u32).to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }
}

// ── Manifest Reader ─────────────────────────────────────────────────────────

/// Reads all version edits from a manifest file.
fn read_manifest(path: &Path) -> KvResult<Vec<VersionEdit>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(KvError::Io(e)),
    };

    let file_len = file.metadata()?.len();
    if file_len < MANIFEST_HEADER_SIZE as u64 {
        return Err(KvError::CorruptedManifest("manifest file too small".into()));
    }

    let mut reader = BufReader::new(file);

    // Validate header
    let mut hdr = [0u8; MANIFEST_HEADER_SIZE];
    reader.read_exact(&mut hdr)?;
    let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    if magic != MANIFEST_MAGIC {
        return Err(KvError::CorruptedManifest(format!(
            "bad magic: {:#010x}",
            magic
        )));
    }
    if version != MANIFEST_VERSION {
        return Err(KvError::CorruptedManifest(format!(
            "unsupported version: {}",
            version
        )));
    }

    let mut edits = Vec::new();
    let mut envelope = [0u8; 8]; // crc(4) + length(4)

    loop {
        match reader.read_exact(&mut envelope) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(KvError::Io(e)),
        }

        let expected_crc = u32::from_le_bytes(envelope[0..4].try_into().unwrap());
        let data_len = u32::from_le_bytes(envelope[4..8].try_into().unwrap()) as usize;

        if data_len > 64 * 1024 * 1024 {
            // Sanity limit: 64 MB per edit record
            break;
        }

        let mut data = vec![0u8; data_len];
        match reader.read_exact(&mut data) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(KvError::Io(e)),
        }

        let actual_crc = manifest_crc(&data);
        if actual_crc != expected_crc {
            warn!("manifest CRC mismatch, stopping replay");
            break;
        }

        edits.push(VersionEdit::decode(&data)?);
    }

    Ok(edits)
}

fn manifest_crc(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

/// Path to the manifest file inside the data directory.
pub fn manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join("MANIFEST")
}

/// Path to the CURRENT file (points to the active manifest).
pub fn current_path(data_dir: &Path) -> PathBuf {
    data_dir.join("CURRENT")
}

// ── VersionSet ──────────────────────────────────────────────────────────────

/// Manages versions and persists version edits to the manifest file.
pub struct VersionSet {
    /// The current (latest) version.
    current: RwLock<Arc<Version>>,
    /// Next file number for new SSTables.
    next_file_number: AtomicU64,
    /// Last assigned sequence number.
    last_sequence: AtomicU64,
    /// Configuration.
    config: LsmConfig,
    /// Manifest file log number.
    manifest_number: AtomicU64,
    /// WAL log number: WALs with numbers <= this have been flushed to SSTables.
    log_number: AtomicU64,
    /// Manifest writer for persistent edits (None in memory-only mode).
    manifest_writer: Mutex<Option<ManifestWriter>>,
}

impl VersionSet {
    /// Create a new version set with an empty initial version (memory-only).
    pub fn new(config: LsmConfig) -> Self {
        let initial = Version::new(&config);
        Self {
            current: RwLock::new(Arc::new(initial)),
            next_file_number: AtomicU64::new(1),
            last_sequence: AtomicU64::new(0),
            config,
            manifest_number: AtomicU64::new(0),
            log_number: AtomicU64::new(0),
            manifest_writer: Mutex::new(None),
        }
    }

    /// Recover from an existing manifest file, or create a fresh one.
    ///
    /// This is the entry point for durable mode. It reads all version edits
    /// from the MANIFEST file, replays them to rebuild the current version,
    /// then opens the manifest for appending new edits.
    pub fn recover(config: LsmConfig) -> KvResult<Self> {
        let mpath = manifest_path(&config.data_dir);
        let edits = read_manifest(&mpath)?;

        let mut version = Version::new(&config);
        let mut last_seq = 0u64;
        let mut next_file = 1u64;
        let mut log_number = 0u64;

        for edit in &edits {
            version = version.apply(edit);
            if let Some(s) = edit.last_sequence {
                last_seq = last_seq.max(s);
            }
            if let Some(n) = edit.next_file_number {
                next_file = next_file.max(n);
            }
            if let Some(n) = edit.log_number {
                log_number = log_number.max(n);
            }
        }

        // Also scan for the highest file number among existing files
        for level in &version.levels {
            for f in &level.files {
                next_file = next_file.max(f.id + 1);
            }
        }

        info!(
            edits = edits.len(),
            version = version.version_number,
            files = version.total_file_count(),
            last_seq = last_seq,
            next_file = next_file,
            "manifest recovered"
        );

        // Re-open manifest for appending (or create if it didn't exist)
        let writer = if mpath.exists() {
            ManifestWriter::open_append(&mpath)?
        } else {
            let mut w = ManifestWriter::create(&mpath)?;
            // Write a full snapshot as the first edit so the manifest is self-contained
            let snapshot_edit = Self::snapshot_edit(&version, last_seq, next_file);
            w.append(&snapshot_edit)?;
            w
        };

        Ok(Self {
            current: RwLock::new(Arc::new(version)),
            next_file_number: AtomicU64::new(next_file),
            last_sequence: AtomicU64::new(last_seq),
            config,
            manifest_number: AtomicU64::new(0),
            log_number: AtomicU64::new(log_number),
            manifest_writer: Mutex::new(Some(writer)),
        })
    }

    /// Build a snapshot edit that captures the full state.
    fn snapshot_edit(version: &Version, last_seq: u64, next_file: u64) -> VersionEdit {
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(last_seq);
        edit.set_next_file_number(next_file);
        for level in &version.levels {
            for file in &level.files {
                edit.add_file(level.level, file.clone());
            }
        }
        edit
    }

    /// Get the current version.
    pub fn current(&self) -> Arc<Version> {
        self.current.read().clone()
    }

    /// Apply a version edit and make it the current version.
    ///
    /// If a manifest writer is present (durable mode), the edit is persisted
    /// to disk before the in-memory state is updated. This ensures that a
    /// crash after `apply_edit` returns always has the edit on disk.
    pub fn apply_edit(&self, edit: &VersionEdit) -> KvResult<Arc<Version>> {
        // Persist first (write-ahead)
        {
            let mut guard = self.manifest_writer.lock();
            if let Some(ref mut writer) = *guard {
                writer.append(edit)?;
            }
        }

        let current = self.current.read().clone();
        let new_version = Arc::new(current.apply(edit));

        // Update metadata
        if let Some(seq) = edit.last_sequence {
            self.last_sequence.store(seq, Ordering::Release);
        }
        if let Some(num) = edit.next_file_number {
            self.next_file_number.store(num, Ordering::Release);
        }
        if let Some(num) = edit.log_number {
            self.log_number.store(num, Ordering::Release);
        }

        // Install the new version
        *self.current.write() = new_version.clone();

        Ok(new_version)
    }

    /// Allocate a new file number.
    pub fn allocate_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the last sequence number.
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::Acquire)
    }

    /// Set the last sequence number.
    pub fn set_last_sequence(&self, seq: u64) {
        self.last_sequence.store(seq, Ordering::Release);
    }

    /// Get the next file number (without allocating).
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number.load(Ordering::Relaxed)
    }

    /// Get the WAL log number. WALs with numbers <= this have been flushed.
    pub fn log_number(&self) -> u64 {
        self.log_number.load(Ordering::Acquire)
    }

    /// Get the configuration.
    pub fn config(&self) -> &LsmConfig {
        &self.config
    }

    /// Get summary statistics.
    pub fn summary(&self) -> VersionSummary {
        let version = self.current();
        VersionSummary {
            version_number: version.version_number,
            levels: version
                .levels
                .iter()
                .map(|l| LevelSummary {
                    level: l.level,
                    file_count: l.file_count(),
                    total_size: l.total_size,
                    max_size: l.max_size,
                })
                .collect(),
            total_files: version.total_file_count(),
            total_size: version.total_size(),
            last_sequence: self.last_sequence(),
        }
    }
}

/// Summary of the current version state.
#[derive(Debug, Clone)]
pub struct VersionSummary {
    pub version_number: u64,
    pub levels: Vec<LevelSummary>,
    pub total_files: usize,
    pub total_size: u64,
    pub last_sequence: u64,
}

/// Summary of a single level.
#[derive(Debug, Clone)]
pub struct LevelSummary {
    pub level: usize,
    pub file_count: usize,
    pub total_size: u64,
    pub max_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_file(id: u64, smallest: &str, largest: &str, size: u64) -> SSTableInfo {
        SSTableInfo {
            id,
            level: 0,
            smallest_key: smallest.as_bytes().to_vec(),
            largest_key: largest.as_bytes().to_vec(),
            file_size: size,
            entry_count: 100,
            min_sequence: 1,
            max_sequence: 100,
            data_block_count: 10,
        }
    }

    #[test]
    fn test_version_edit_roundtrip() {
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(42);
        edit.set_next_file_number(10);
        edit.add_file(0, make_file(1, "aaa", "zzz", 1024));
        edit.add_file(1, make_file(2, "bbb", "mmm", 2048));
        edit.remove_file(0, 5);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.last_sequence, Some(42));
        assert_eq!(decoded.next_file_number, Some(10));
        assert_eq!(decoded.added_files.len(), 2);
        assert_eq!(decoded.removed_files.len(), 1);
        assert_eq!(decoded.removed_files[0], (0, 5));
    }

    #[test]
    fn test_version_apply() {
        let config = LsmConfig::for_testing(PathBuf::from("/tmp"));
        let version = Version::new(&config);

        let mut edit = VersionEdit::new();
        edit.add_file(0, make_file(1, "aaa", "mmm", 1024));
        edit.add_file(0, make_file(2, "nnn", "zzz", 2048));

        let v2 = version.apply(&edit);
        assert_eq!(v2.levels[0].file_count(), 2);
        assert_eq!(v2.levels[0].total_size, 3072);
        assert_eq!(v2.version_number, 1);

        // Remove a file
        let mut edit2 = VersionEdit::new();
        edit2.remove_file(0, 1);

        let v3 = v2.apply(&edit2);
        assert_eq!(v3.levels[0].file_count(), 1);
        assert_eq!(v3.levels[0].total_size, 2048);
    }

    #[test]
    fn test_version_files_for_key() {
        let config = LsmConfig::for_testing(PathBuf::from("/tmp"));
        let version = Version::new(&config);

        let mut edit = VersionEdit::new();
        edit.add_file(0, make_file(1, "aaa", "mmm", 1024));
        edit.add_file(1, make_file(2, "aaa", "fff", 2048));
        edit.add_file(1, make_file(3, "ggg", "zzz", 2048));

        let v = version.apply(&edit);

        // Key "bbb" should match L0 file 1 and L1 file 2
        let files = v.files_for_key(b"bbb");
        assert_eq!(files.len(), 2);

        // Key "hhh" should match L0 file 1 and L1 file 3
        let files = v.files_for_key(b"hhh");
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_version_set() {
        let config = LsmConfig::for_testing(PathBuf::from("/tmp"));
        let vs = VersionSet::new(config);

        assert_eq!(vs.last_sequence(), 0);
        let n1 = vs.allocate_file_number();
        let n2 = vs.allocate_file_number();
        assert_eq!(n1, 1);
        assert_eq!(n2, 2);

        let mut edit = VersionEdit::new();
        edit.set_last_sequence(100);
        edit.add_file(0, make_file(n1, "a", "z", 1024));

        vs.apply_edit(&edit).unwrap();

        assert_eq!(vs.last_sequence(), 100);
        let v = vs.current();
        assert_eq!(v.levels[0].file_count(), 1);
    }
}
