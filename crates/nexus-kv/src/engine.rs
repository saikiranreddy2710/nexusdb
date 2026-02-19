//! LSM-tree engine: the main entry point for the key-value store.
//!
//! The engine orchestrates all components:
//! - Memtable for buffering writes
//! - SSTables for persistent storage
//! - Compaction for space management
//! - Manifest for version tracking
//!
//! ## Write Path
//!
//! 1. Write to WAL (optional, for durability)
//! 2. Insert into active MemTable
//! 3. When MemTable is full, freeze it and create a new one
//! 4. Background thread flushes frozen MemTables to L0 SSTables
//! 5. Background compaction merges SSTables across levels
//!
//! ## Read Path
//!
//! 1. Check active MemTable
//! 2. Check frozen MemTables (newest first)
//! 3. Check L0 SSTables (all, since they overlap)
//! 4. Check L1+ SSTables (binary search, one per level)
//! 5. First match wins (newest data takes priority)

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::fs;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::cache::table_cache::TableCache;
use crate::compaction::leveled::LeveledCompaction;
use crate::compaction::scheduler::{CompactionCallback, CompactionScheduler};
use crate::compaction::CompactionStrategy;
use crate::config::LsmConfig;
use crate::error::{KvError, KvResult};
use crate::manifest::{VersionEdit, VersionSet};
use crate::memtable::skiplist::ValueType;
use crate::memtable::{ImmutableMemTable, MemTable, WriteBatch};
use crate::sstable::reader::SSTableReader;
use crate::sstable::writer::{SSTableWriter, SSTableWriterOptions};
use crate::sstable::SSTableInfo;

/// The main LSM-tree key-value storage engine.
pub struct LsmEngine {
    /// Configuration.
    config: Arc<LsmConfig>,
    /// Data directory path.
    data_dir: PathBuf,

    /// Active (mutable) memtable.
    active_memtable: RwLock<Arc<MemTable>>,
    /// Frozen memtables waiting to be flushed.
    immutable_memtables: RwLock<VecDeque<Arc<ImmutableMemTable>>>,

    /// Version management (tracks SSTable files at each level).
    versions: Arc<VersionSet>,
    /// Compaction strategy.
    compaction_strategy: Arc<dyn CompactionStrategy>,
    /// Cache of open SSTableReader handles (avoids re-opening files).
    table_cache: Arc<TableCache>,
    /// Background compaction scheduler.
    compaction_scheduler: Mutex<Option<CompactionScheduler>>,

    /// Next memtable ID.
    next_memtable_id: AtomicU64,
    /// Whether the engine is running.
    is_running: AtomicBool,
    /// Write serialization lock.
    write_mutex: Mutex<()>,
}

impl LsmEngine {
    /// Open or create an LSM-tree engine at the specified data directory.
    pub fn open(config: LsmConfig) -> KvResult<Self> {
        config
            .validate()
            .map_err(|e| KvError::InvalidConfig(e))?;

        let data_dir = config.data_dir.clone();

        // Create data directory if it doesn't exist
        fs::create_dir_all(&data_dir).map_err(|e| {
            KvError::DataDirectory(format!(
                "failed to create data directory {:?}: {}",
                data_dir, e
            ))
        })?;

        // Create subdirectories for SSTables
        fs::create_dir_all(data_dir.join("sst")).map_err(|e| {
            KvError::DataDirectory(format!(
                "failed to create sst directory {:?}: {}",
                data_dir.join("sst"),
                e
            ))
        })?;

        let config = Arc::new(config);
        let versions = Arc::new(VersionSet::new((*config).clone()));
        let compaction_strategy =
            Arc::new(LeveledCompaction::new((*config).clone()));

        // Table cache: keep up to 1024 SSTable readers open
        let table_cache = Arc::new(TableCache::new(data_dir.join("sst"), 1024));

        let initial_memtable = Arc::new(MemTable::new(
            1,
            config.memtable_size,
            versions.last_sequence() + 1,
        ));

        info!(
            data_dir = %data_dir.display(),
            memtable_size = config.memtable_size,
            max_levels = config.max_levels,
            "LSM engine opened"
        );

        let engine = Self {
            config,
            data_dir,
            active_memtable: RwLock::new(initial_memtable),
            immutable_memtables: RwLock::new(VecDeque::new()),
            versions,
            compaction_strategy,
            table_cache,
            compaction_scheduler: Mutex::new(None),
            next_memtable_id: AtomicU64::new(2),
            is_running: AtomicBool::new(true),
            write_mutex: Mutex::new(()),
        };

        Ok(engine)
    }

    /// Insert a key-value pair.
    #[must_use = "errors must not be silently ignored"]
    pub fn put(&self, key: Bytes, value: Bytes) -> KvResult<()> {
        self.check_running()?;
        self.check_key_value_size(&key, &value)?;

        let _guard = self.write_mutex.lock();
        self.maybe_freeze_memtable_locked()?;
        let memtable = self.active_memtable.read().clone();
        memtable.put(key, value)?;
        // Use next_sequence (one past last assigned) so all writes are visible
        self.versions.set_last_sequence(memtable.next_sequence());

        Ok(())
    }

    /// Delete a key.
    #[must_use = "errors must not be silently ignored"]
    pub fn delete(&self, key: Bytes) -> KvResult<()> {
        self.check_running()?;
        self.check_key_size(&key)?;

        let _guard = self.write_mutex.lock();
        self.maybe_freeze_memtable_locked()?;
        let memtable = self.active_memtable.read().clone();
        memtable.delete(key)?;
        self.versions.set_last_sequence(memtable.next_sequence());

        Ok(())
    }

    /// Apply a write batch atomically.
    #[must_use = "errors must not be silently ignored"]
    pub fn write(&self, batch: &WriteBatch) -> KvResult<()> {
        self.check_running()?;
        if batch.is_empty() {
            return Ok(());
        }

        let _guard = self.write_mutex.lock();
        self.maybe_freeze_memtable_locked()?;
        let memtable = self.active_memtable.read().clone();
        memtable.apply_batch(batch)?;
        self.versions.set_last_sequence(memtable.next_sequence());

        Ok(())
    }

    /// Look up the value for a key.
    ///
    /// Returns `None` if the key does not exist or has been deleted.
    pub fn get(&self, key: &[u8]) -> KvResult<Option<Bytes>> {
        self.check_running()?;
        let sequence = self.versions.last_sequence();

        // 1. Check active memtable
        {
            let memtable = self.active_memtable.read().clone();
            if let Some((vt, value)) = memtable.get(key, sequence) {
                return match vt {
                    ValueType::Value => Ok(Some(value)),
                    ValueType::Deletion => Ok(None),
                };
            }
        }

        // 2. Check immutable memtables (newest first)
        {
            let imm_tables = self.immutable_memtables.read();
            for imm in imm_tables.iter().rev() {
                if let Some((vt, value)) = imm.get(key, sequence) {
                    return match vt {
                        ValueType::Value => Ok(Some(value)),
                        ValueType::Deletion => Ok(None),
                    };
                }
            }
        }

        // 3. Check SSTables via table cache (file bytes cached in memory)
        let version = self.versions.current();
        let candidates = version.files_for_key(key);

        for (_level, file_info) in candidates {
            // Table cache provides in-memory file data (avoids disk I/O on hit).
            // We create a lightweight reader each time (parses footer/index/bloom
            // from memory in microseconds).
            let mut reader = match self.table_cache.open_reader(file_info.id) {
                Ok(r) => r,
                Err(_) => continue,
            };

            if let Some(raw_value) = reader.get(key)? {
                if raw_value.is_empty() {
                    continue;
                }
                match raw_value[0] {
                    0x00 => return Ok(None), // Deletion tombstone
                    0x01 => return Ok(Some(Bytes::copy_from_slice(&raw_value[1..]))),
                    _ => continue,
                }
            }
        }

        Ok(None)
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &[u8]) -> KvResult<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Flush the active memtable to an SSTable on disk.
    ///
    /// This is normally done automatically when the memtable is full,
    /// but can be triggered manually for testing or before shutdown.
    pub fn flush(&self) -> KvResult<()> {
        // Note: don't check_running here — shutdown calls flush after setting is_running=false
        {
            let _guard = self.write_mutex.lock();
            self.freeze_memtable_inner()?;
        }
        self.flush_immutable_memtables()?;
        Ok(())
    }

    /// Trigger a compaction if needed.
    pub fn maybe_compact(&self) -> KvResult<bool> {
        self.check_running()?;
        let version = self.versions.current();

        if let Some(job) = self.compaction_strategy.pick_compaction(&version.levels) {
            debug!(
                input_level = job.input_level,
                output_level = job.output_level,
                input_files = job.input_file_count(),
                "starting compaction"
            );
            self.execute_compaction(&job)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get engine statistics.
    pub fn stats(&self) -> EngineStats {
        let version = self.versions.current();
        let imm_count = self.immutable_memtables.read().len();
        let active_mem = self.active_memtable.read().approximate_memory_usage();

        EngineStats {
            memtable_size: active_mem,
            immutable_memtable_count: imm_count,
            total_sst_files: version.total_file_count(),
            total_sst_size: version.total_size(),
            level_stats: version
                .levels
                .iter()
                .map(|l| LevelStats {
                    level: l.level,
                    file_count: l.file_count(),
                    total_size: l.total_size,
                })
                .collect(),
            last_sequence: self.versions.last_sequence(),
        }
    }

    /// Shut down the engine gracefully.
    ///
    /// Stops background compaction, flushes remaining data, and returns
    /// an error if the final flush fails (potential data loss).
    pub fn shutdown(&self) -> KvResult<()> {
        if !self.is_running.swap(false, Ordering::SeqCst) {
            return Ok(()); // Already shut down
        }

        info!("shutting down LSM engine");

        // Stop background compaction first
        if let Some(mut scheduler) = self.compaction_scheduler.lock().take() {
            scheduler.shutdown();
        }

        // Flush any remaining data — propagate errors to caller
        self.flush().map_err(|e| {
            warn!("error during shutdown flush: {}", e);
            e
        })?;

        info!("LSM engine shut down complete");
        Ok(())
    }

    // ── Internal Methods ────────────────────────────────────────

    /// Check if the engine is running.
    fn check_running(&self) -> KvResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            Err(KvError::Shutdown)
        } else {
            Ok(())
        }
    }

    /// Validate key size.
    fn check_key_size(&self, key: &[u8]) -> KvResult<()> {
        const MAX_KEY_SIZE: usize = 16 * 1024;
        if key.len() > MAX_KEY_SIZE {
            return Err(KvError::KeyTooLarge {
                size: key.len(),
                max: MAX_KEY_SIZE,
            });
        }
        Ok(())
    }

    /// Validate key and value sizes.
    fn check_key_value_size(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        const MAX_VALUE_SIZE: usize = 1024 * 1024;
        self.check_key_size(key)?;
        if value.len() > MAX_VALUE_SIZE {
            return Err(KvError::ValueTooLarge {
                size: value.len(),
                max: MAX_VALUE_SIZE,
            });
        }
        Ok(())
    }

    /// Freeze the active memtable if it exceeds the size threshold.
    /// Must be called while holding `write_mutex`.
    fn maybe_freeze_memtable_locked(&self) -> KvResult<()> {
        let should_freeze = {
            let memtable = self.active_memtable.read();
            memtable.should_freeze()
        };

        if should_freeze {
            self.freeze_memtable_inner()?;
        }

        Ok(())
    }

    /// Freeze the active memtable and create a new one.
    fn freeze_memtable(&self) -> KvResult<()> {
        let _guard = self.write_mutex.lock();
        self.freeze_memtable_inner()?;
        self.flush_immutable_memtables()?;
        Ok(())
    }

    /// Inner freeze logic — assumes write_mutex is held.
    fn freeze_memtable_inner(&self) -> KvResult<()> {
        let old_memtable = {
            let mut active = self.active_memtable.write();
            let next_seq = active.next_sequence();
            let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
            let new_memtable = Arc::new(MemTable::new(
                new_id,
                self.config.memtable_size,
                next_seq,
            ));
            std::mem::replace(&mut *active, new_memtable)
        };

        // Only add to immutables if it has data
        if !old_memtable.is_empty() {
            // Wrap the Arc<MemTable> directly — no try_unwrap needed.
            // Concurrent readers holding a clone of the Arc keep it alive safely.
            let immutable = Arc::new(ImmutableMemTable::new(old_memtable));

            let mut imm_tables = self.immutable_memtables.write();
            imm_tables.push_back(immutable);

            debug!(
                immutable_count = imm_tables.len(),
                "memtable frozen"
            );

            if imm_tables.len() > self.config.max_immutable_memtables {
                warn!(
                    count = imm_tables.len(),
                    max = self.config.max_immutable_memtables,
                    "too many immutable memtables, triggering flush"
                );
            }
        }

        Ok(())
    }

    /// Flush all immutable memtables to L0 SSTables.
    fn flush_immutable_memtables(&self) -> KvResult<()> {
        loop {
            let imm = {
                let imm_tables = self.immutable_memtables.read();
                match imm_tables.front() {
                    Some(imm) if !imm.is_flushed() => imm.clone(),
                    _ => break,
                }
            };

            self.flush_memtable_to_l0(&imm)?;

            // Remove from the queue
            let mut imm_tables = self.immutable_memtables.write();
            if let Some(front) = imm_tables.front() {
                if front.is_flushed() {
                    imm_tables.pop_front();
                }
            }
        }

        Ok(())
    }

    /// Flush a single immutable memtable to an L0 SSTable.
    fn flush_memtable_to_l0(&self, imm: &ImmutableMemTable) -> KvResult<()> {
        if imm.is_empty() {
            imm.mark_flushed();
            return Ok(());
        }

        let file_number = self.versions.allocate_file_number();
        let file_path = self.sst_path(file_number);

        info!(
            file_number = file_number,
            entries = imm.len(),
            memory = imm.approximate_memory_usage(),
            "flushing memtable to L0 SSTable"
        );

        // Build the SSTable
        let file = fs::File::create(&file_path)?;
        let buf_writer = BufWriter::new(file);
        let options = SSTableWriterOptions {
            block_size: self.config.block_size,
            block_restart_interval: self.config.block_restart_interval,
            compression: self.config.compression,
            bloom_bits_per_key: if self.config.enable_bloom_filter {
                self.config.bloom_bits_per_key
            } else {
                0
            },
        };

        let mut writer = SSTableWriter::new(buf_writer, options);
        let mut entry_count = 0u64;

        // Iterate over the memtable and write entries.
        // We store (user_key, type_prefix + value) in the SSTable.
        // Type prefix: 0x01 = Put, 0x00 = Delete.
        // For duplicate user keys, we keep only the newest version.
        let mut iter = imm.iter();
        let mut last_user_key: Option<Vec<u8>> = None;
        loop {
            let (key, value) = match iter.current() {
                Some((k, v)) => (k.clone(), v.clone()),
                None => break,
            };

            // Skip older versions of the same user key
            // (InternalKey ordering puts newer versions first for same user_key)
            let user_key = key.user_key.to_vec();
            if last_user_key.as_deref() == Some(user_key.as_slice()) {
                iter.next();
                continue;
            }
            last_user_key = Some(user_key.clone());

            // Encode value with type prefix
            let mut encoded_value = Vec::with_capacity(1 + value.len());
            encoded_value.push(key.value_type as u8);
            encoded_value.extend_from_slice(&value);

            writer.add_with_seq(&user_key, &encoded_value, key.sequence)?;
            entry_count += 1;
            iter.next();
        }

        let result = writer.finish()?;

        // Create SSTable info
        let sst_info = SSTableInfo {
            id: file_number,
            level: 0,
            smallest_key: result.smallest_key,
            largest_key: result.largest_key,
            file_size: result.file_size,
            entry_count,
            min_sequence: result.min_sequence,
            max_sequence: result.max_sequence,
            data_block_count: result.block_count,
        };

        // Update the version
        let mut edit = VersionEdit::new();
        edit.add_file(0, sst_info);
        edit.set_last_sequence(self.versions.last_sequence());
        edit.set_next_file_number(self.versions.next_file_number());
        self.versions.apply_edit(&edit)?;

        imm.mark_flushed();

        info!(
            file_number = file_number,
            entries = entry_count,
            file_size = result.file_size,
            "memtable flushed to L0"
        );

        // Notify background compaction that there's a new L0 file
        self.notify_compaction();

        Ok(())
    }

    /// Notify the background compaction scheduler that work may be available.
    fn notify_compaction(&self) {
        if let Some(ref scheduler) = *self.compaction_scheduler.lock() {
            scheduler.notify();
        }
    }

    /// Start the background compaction scheduler.
    ///
    /// Must be called after creating the engine with `open()`.
    /// The engine itself is passed as the compaction callback via an `Arc`.
    pub fn start_compaction(self: &Arc<Self>) {
        let engine = self.clone();
        let scheduler = CompactionScheduler::start(
            engine,
            std::time::Duration::from_secs(5),
        );
        *self.compaction_scheduler.lock() = Some(scheduler);
        info!("background compaction scheduler started");
    }

    /// Execute a compaction job.
    fn execute_compaction(
        &self,
        job: &crate::compaction::CompactionJob,
    ) -> KvResult<()> {
        // Collect all input file IDs
        let mut input_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        // Read all entries from input files (input level)
        for file_info in &job.input_files {
            let path = self.sst_path(file_info.id);
            if path.exists() {
                let file = fs::File::open(&path)?;
                let mut reader = SSTableReader::open(file, path)?;
                let mut iter = reader.iter()?;
                while iter.is_valid() {
                    if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                        input_entries.push((k.to_vec(), v.to_vec()));
                    }
                    iter.next();
                }
            }
        }

        // Read all entries from output level overlapping files
        for file_info in &job.output_files {
            let path = self.sst_path(file_info.id);
            if path.exists() {
                let file = fs::File::open(&path)?;
                let mut reader = SSTableReader::open(file, path)?;
                let mut iter = reader.iter()?;
                while iter.is_valid() {
                    if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                        input_entries.push((k.to_vec(), v.to_vec()));
                    }
                    iter.next();
                }
            }
        }

        // Sort entries by key. We use stable sort so that for equal keys,
        // entries from files read later (output level = older data) come after
        // entries from files read first (input level = newer data).
        // IMPORTANT: must remain a stable sort to preserve insertion order for dedup.
        input_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Deduplicate: for same key, keep the first occurrence (newer data
        // from input level files, which were read first into the vec).
        input_entries.dedup_by(|b, a| a.0 == b.0);

        // Write the merged entries to new SSTable(s)
        let target_size = self.compaction_strategy.target_file_size(job.output_level);
        let mut new_files = Vec::new();
        let mut current_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut current_size = 0u64;

        for entry in &input_entries {
            current_size += (entry.0.len() + entry.1.len()) as u64;
            current_entries.push(entry.clone());

            if current_size >= target_size && !current_entries.is_empty() {
                let file_info = self.write_compaction_output(
                    job.output_level,
                    &current_entries,
                )?;
                new_files.push(file_info);
                current_entries.clear();
                current_size = 0;
            }
        }

        // Write remaining entries
        if !current_entries.is_empty() {
            let file_info = self.write_compaction_output(
                job.output_level,
                &current_entries,
            )?;
            new_files.push(file_info);
        }

        // Update version: remove old files, add new files
        let mut edit = VersionEdit::new();

        for file_info in &job.input_files {
            edit.remove_file(job.input_level, file_info.id);
        }
        for file_info in &job.output_files {
            edit.remove_file(job.output_level, file_info.id);
        }
        for file_info in &new_files {
            edit.add_file(job.output_level, file_info.clone());
        }

        self.versions.apply_edit(&edit)?;

        // Evict deleted files from table cache, then delete from disk
        for file_info in job.input_files.iter().chain(job.output_files.iter()) {
            self.table_cache.evict(file_info.id);
            let path = self.sst_path(file_info.id);
            if path.exists() {
                if let Err(e) = fs::remove_file(&path) {
                    warn!(
                        file_id = file_info.id,
                        error = %e,
                        "failed to delete old SSTable file during compaction"
                    );
                }
            }
        }

        info!(
            input_level = job.input_level,
            output_level = job.output_level,
            input_files = job.input_file_count(),
            output_files = new_files.len(),
            "compaction complete"
        );

        Ok(())
    }

    /// Write a set of entries to a new SSTable for compaction output.
    fn write_compaction_output(
        &self,
        level: usize,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> KvResult<SSTableInfo> {
        let file_number = self.versions.allocate_file_number();
        let file_path = self.sst_path(file_number);

        let file = fs::File::create(&file_path)?;
        let buf_writer = BufWriter::new(file);
        let options = SSTableWriterOptions {
            block_size: self.config.block_size,
            block_restart_interval: self.config.block_restart_interval,
            compression: self.config.compression,
            bloom_bits_per_key: if self.config.enable_bloom_filter {
                self.config.bloom_bits_per_key
            } else {
                0
            },
        };

        let mut writer = SSTableWriter::new(buf_writer, options);
        for (k, v) in entries {
            writer.add(k, v)?;
        }
        let result = writer.finish()?;

        Ok(SSTableInfo {
            id: file_number,
            level,
            smallest_key: result.smallest_key,
            largest_key: result.largest_key,
            file_size: result.file_size,
            entry_count: result.entry_count,
            min_sequence: result.min_sequence,
            max_sequence: result.max_sequence,
            data_block_count: result.block_count,
        })
    }

    /// Get the file path for an SSTable by its file number.
    fn sst_path(&self, file_number: u64) -> PathBuf {
        self.data_dir
            .join("sst")
            .join(format!("{:06}.sst", file_number))
    }
}

/// Implement CompactionCallback so the engine can be used with the
/// background compaction scheduler.
impl CompactionCallback for LsmEngine {
    fn try_compact(&self) -> Result<bool, String> {
        if !self.is_running.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.maybe_compact().map_err(|e| e.to_string())
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        // Stop compaction scheduler first
        if let Some(mut scheduler) = self.compaction_scheduler.lock().take() {
            scheduler.shutdown();
        }
        if self.is_running.load(Ordering::Relaxed) {
            let _ = self.shutdown();
        }
    }
}

/// Engine statistics.
#[derive(Debug, Clone)]
pub struct EngineStats {
    /// Active memtable size in bytes.
    pub memtable_size: usize,
    /// Number of immutable memtables pending flush.
    pub immutable_memtable_count: usize,
    /// Total number of SSTable files.
    pub total_sst_files: usize,
    /// Total size of all SSTable files.
    pub total_sst_size: u64,
    /// Per-level statistics.
    pub level_stats: Vec<LevelStats>,
    /// Last sequence number.
    pub last_sequence: u64,
}

/// Per-level statistics.
#[derive(Debug, Clone)]
pub struct LevelStats {
    pub level: usize,
    pub file_count: usize,
    pub total_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_engine() -> (LsmEngine, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = LsmConfig::for_testing(tmp.path().to_path_buf());
        let engine = LsmEngine::open(config).unwrap();
        (engine, tmp)
    }

    #[test]
    fn test_put_get() {
        let (engine, _tmp) = create_test_engine();

        engine
            .put(Bytes::from("hello"), Bytes::from("world"))
            .unwrap();

        let value = engine.get(b"hello").unwrap();
        assert_eq!(value, Some(Bytes::from("world")));
    }

    #[test]
    fn test_get_nonexistent() {
        let (engine, _tmp) = create_test_engine();
        assert_eq!(engine.get(b"missing").unwrap(), None);
    }

    #[test]
    fn test_overwrite() {
        let (engine, _tmp) = create_test_engine();

        engine
            .put(Bytes::from("key"), Bytes::from("v1"))
            .unwrap();
        engine
            .put(Bytes::from("key"), Bytes::from("v2"))
            .unwrap();

        let value = engine.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from("v2")));
    }

    #[test]
    fn test_delete() {
        let (engine, _tmp) = create_test_engine();

        engine
            .put(Bytes::from("key"), Bytes::from("value"))
            .unwrap();
        engine.delete(Bytes::from("key")).unwrap();

        assert_eq!(engine.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_write_batch() {
        let (engine, _tmp) = create_test_engine();

        let mut batch = WriteBatch::new();
        batch.put(Bytes::from("a"), Bytes::from("1"));
        batch.put(Bytes::from("b"), Bytes::from("2"));
        batch.put(Bytes::from("c"), Bytes::from("3"));

        engine.write(&batch).unwrap();

        assert_eq!(engine.get(b"a").unwrap(), Some(Bytes::from("1")));
        assert_eq!(engine.get(b"b").unwrap(), Some(Bytes::from("2")));
        assert_eq!(engine.get(b"c").unwrap(), Some(Bytes::from("3")));
    }

    #[test]
    fn test_many_writes() {
        let (engine, _tmp) = create_test_engine();

        for i in 0..500u32 {
            let key = format!("{:08}", i);
            let value = format!("value_{}", i);
            engine
                .put(Bytes::from(key), Bytes::from(value))
                .unwrap();
        }

        // Verify all keys
        for i in 0..500u32 {
            let key = format!("{:08}", i);
            let expected = format!("value_{}", i);
            let value = engine.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from(expected)));
        }
    }

    #[test]
    fn test_flush() {
        let (engine, _tmp) = create_test_engine();

        // Write enough data to trigger memtable freeze
        for i in 0..100u32 {
            let key = format!("{:08}", i);
            let value = format!("value_{}", i);
            engine
                .put(Bytes::from(key), Bytes::from(value))
                .unwrap();
        }

        engine.flush().unwrap();

        // Data should still be readable from SSTables
        for i in 0..100u32 {
            let key = format!("{:08}", i);
            let expected = format!("value_{}", i);
            let value = engine.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(Bytes::from(expected)), "key {} not found", key);
        }
    }

    #[test]
    fn test_engine_stats() {
        let (engine, _tmp) = create_test_engine();

        engine
            .put(Bytes::from("key"), Bytes::from("value"))
            .unwrap();

        let stats = engine.stats();
        assert!(stats.memtable_size > 0);
        assert_eq!(stats.immutable_memtable_count, 0);
    }

    #[test]
    fn test_shutdown() {
        let (engine, _tmp) = create_test_engine();

        engine
            .put(Bytes::from("key"), Bytes::from("value"))
            .unwrap();
        engine.shutdown().unwrap();

        // Operations after shutdown should fail
        assert!(engine.put(Bytes::from("k"), Bytes::from("v")).is_err());
    }
}
