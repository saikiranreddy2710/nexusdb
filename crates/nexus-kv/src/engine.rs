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
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::VecDeque;
use std::fs;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::compaction::leveled::LeveledCompaction;
use crate::compaction::CompactionStrategy;
use crate::config::LsmConfig;
use crate::error::{KvError, KvResult};
use crate::manifest::{VersionEdit, VersionSet};
use crate::memtable::skiplist::ValueType;
use crate::memtable::{ImmutableMemTable, MemTable, WriteBatch};
use crate::sstable::reader::SSTableReader;
use crate::sstable::writer::{SSTableWriter, SSTableWriterOptions};
use crate::sstable::SSTableInfo;
use crate::wal::{self, WalRecordType, WalWriter};

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

    /// Active WAL writer (None when WAL is disabled).
    wal_writer: Mutex<Option<WalWriter>>,
    /// WAL file number for the current active memtable.
    wal_file_number: AtomicU64,

    /// Next memtable ID.
    next_memtable_id: AtomicU64,
    /// Whether the engine is running (Arc so background thread can share it).
    is_running: Arc<AtomicBool>,
    /// Write serialization lock.
    write_mutex: Mutex<()>,

    /// Background compaction thread handle.
    bg_compaction_handle: Mutex<Option<thread::JoinHandle<()>>>,
    /// Condvar to wake the background compaction thread.
    bg_compaction_signal: Arc<(Mutex<bool>, Condvar)>,
}

impl LsmEngine {
    /// Open or create an LSM-tree engine at the specified data directory.
    ///
    /// If `enable_wal` is true, this will:
    /// 1. Recover the manifest to rebuild the SSTable version state
    /// 2. Replay any WAL files to recover unflushed memtable data
    /// 3. Open a new WAL for the active memtable
    /// 4. Start a background compaction thread
    pub fn open(config: LsmConfig) -> KvResult<Self> {
        config.validate().map_err(|e| KvError::InvalidConfig(e))?;

        let data_dir = config.data_dir.clone();
        let enable_wal = config.enable_wal;
        let sync_writes = config.sync_writes;

        // Create data directory if it doesn't exist
        fs::create_dir_all(&data_dir).map_err(|e| {
            KvError::DataDirectory(format!(
                "failed to create data directory {:?}: {}",
                data_dir, e
            ))
        })?;

        // Create subdirectories for SSTables
        fs::create_dir_all(data_dir.join("sst")).ok();

        let config = Arc::new(config);

        // ── Recover or create manifest ──────────────────────────
        let versions = if enable_wal {
            Arc::new(VersionSet::recover((*config).clone())?)
        } else {
            Arc::new(VersionSet::new((*config).clone()))
        };

        let compaction_strategy = Arc::new(LeveledCompaction::new((*config).clone()));

        let mut next_memtable_id = 2u64;

        // ── Replay WAL files ────────────────────────────────────
        let initial_memtable = if enable_wal {
            let wal_files = wal::find_wal_files(&data_dir)?;
            let memtable = MemTable::new(1, config.memtable_size, versions.last_sequence() + 1);

            let mut recovered_keys = 0u64;
            for (wal_num, wal_path) in &wal_files {
                let reader = wal::WalReader::open(wal_path.clone());
                let records = reader.replay()?;
                for record in &records {
                    match record.record_type {
                        WalRecordType::Put => {
                            memtable.put(
                                Bytes::copy_from_slice(&record.key),
                                Bytes::copy_from_slice(&record.value),
                            )?;
                            recovered_keys += 1;
                        }
                        WalRecordType::Delete => {
                            memtable.delete(Bytes::copy_from_slice(&record.key))?;
                            recovered_keys += 1;
                        }
                    }
                }
                next_memtable_id = next_memtable_id.max(wal_num + 1);
            }

            if recovered_keys > 0 {
                info!(
                    wal_files = wal_files.len(),
                    recovered_keys = recovered_keys,
                    "WAL recovery complete"
                );
            }

            // Update sequence from recovered memtable
            let seq = memtable.next_sequence();
            if seq > versions.last_sequence() {
                versions.set_last_sequence(seq);
            }

            Arc::new(memtable)
        } else {
            Arc::new(MemTable::new(
                1,
                config.memtable_size,
                versions.last_sequence() + 1,
            ))
        };

        // ── Open new WAL for the active memtable ────────────────
        let wal_number = next_memtable_id;
        let wal_writer = if enable_wal {
            let w = WalWriter::create(&data_dir, wal_number, sync_writes)?;
            Some(w)
        } else {
            None
        };

        info!(
            data_dir = %data_dir.display(),
            memtable_size = config.memtable_size,
            max_levels = config.max_levels,
            wal_enabled = enable_wal,
            "LSM engine opened"
        );

        let bg_signal = Arc::new((Mutex::new(false), Condvar::new()));

        let engine = Self {
            config,
            data_dir,
            active_memtable: RwLock::new(initial_memtable),
            immutable_memtables: RwLock::new(VecDeque::new()),
            versions,
            compaction_strategy,
            wal_writer: Mutex::new(wal_writer),
            wal_file_number: AtomicU64::new(wal_number),
            next_memtable_id: AtomicU64::new(next_memtable_id + 1),
            is_running: Arc::new(AtomicBool::new(true)),
            write_mutex: Mutex::new(()),
            bg_compaction_handle: Mutex::new(None),
            bg_compaction_signal: bg_signal,
        };

        // ── Start background compaction thread ──────────────────
        engine.start_bg_compaction();

        Ok(engine)
    }

    /// Insert a key-value pair.
    pub fn put(&self, key: Bytes, value: Bytes) -> KvResult<()> {
        self.check_running()?;
        self.check_key_value_size(&key, &value)?;
        self.maybe_freeze_memtable()?;

        let _guard = self.write_mutex.lock();

        // Write to WAL first (write-ahead)
        {
            let mut wal_guard = self.wal_writer.lock();
            if let Some(ref mut w) = *wal_guard {
                w.log_put(&key, &value)?;
            }
        }

        let memtable = self.active_memtable.read().clone();
        memtable.put(key, value)?;

        // Update the version set's sequence number
        let seq = memtable.next_sequence();
        self.versions.set_last_sequence(seq);

        Ok(())
    }

    /// Delete a key.
    pub fn delete(&self, key: Bytes) -> KvResult<()> {
        self.check_running()?;
        self.maybe_freeze_memtable()?;

        let _guard = self.write_mutex.lock();

        // Write to WAL first
        {
            let mut wal_guard = self.wal_writer.lock();
            if let Some(ref mut w) = *wal_guard {
                w.log_delete(&key)?;
            }
        }

        let memtable = self.active_memtable.read().clone();
        memtable.delete(key)?;

        let seq = memtable.next_sequence();
        self.versions.set_last_sequence(seq);

        Ok(())
    }

    /// Apply a write batch atomically.
    pub fn write(&self, batch: &WriteBatch) -> KvResult<()> {
        self.check_running()?;
        if batch.is_empty() {
            return Ok(());
        }
        self.maybe_freeze_memtable()?;

        let _guard = self.write_mutex.lock();

        // Write all batch entries to WAL first
        {
            let mut wal_guard = self.wal_writer.lock();
            if let Some(ref mut w) = *wal_guard {
                for entry in batch.iter() {
                    match entry.value_type {
                        ValueType::Value => w.log_put(&entry.key, &entry.value)?,
                        ValueType::Deletion => w.log_delete(&entry.key)?,
                    }
                }
            }
        }

        let memtable = self.active_memtable.read().clone();
        memtable.apply_batch(batch)?;

        let seq = memtable.next_sequence();
        self.versions.set_last_sequence(seq);

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

        // 3. Check SSTables
        let version = self.versions.current();
        let candidates = version.files_for_key(key);

        for (_level, file_info) in candidates {
            let file_path = self.sst_path(file_info.id);
            if !file_path.exists() {
                continue;
            }
            let file = fs::File::open(&file_path)?;
            let mut reader = SSTableReader::open(file, file_path)?;

            if let Some(raw_value) = reader.get(key)? {
                // Decode the type prefix: 0x01 = Put, 0x00 = Delete
                if raw_value.is_empty() {
                    continue; // Malformed entry, skip
                }
                match raw_value[0] {
                    0x00 => return Ok(None), // Deletion tombstone
                    0x01 => return Ok(Some(Bytes::copy_from_slice(&raw_value[1..]))),
                    _ => continue, // Unknown type, skip
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
        self.check_running()?;
        self.freeze_memtable()?;
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
    pub fn shutdown(&self) -> KvResult<()> {
        if !self.is_running.swap(false, Ordering::SeqCst) {
            return Ok(()); // Already shut down
        }

        info!("shutting down LSM engine");

        // Wake the background compaction thread so it exits
        {
            let (lock, cvar) = &*self.bg_compaction_signal;
            let mut triggered = lock.lock();
            *triggered = true;
            cvar.notify_one();
        }

        // Wait for background thread to finish
        {
            let mut handle = self.bg_compaction_handle.lock();
            if let Some(h) = handle.take() {
                let _ = h.join();
            }
        }

        // Flush any remaining data
        if let Err(e) = self.flush() {
            warn!("error during shutdown flush: {}", e);
        }

        // Close the WAL writer
        {
            let mut wal_guard = self.wal_writer.lock();
            if let Some(w) = wal_guard.take() {
                if let Err(e) = w.close() {
                    warn!("error closing WAL: {}", e);
                }
            }
        }

        info!("LSM engine shut down complete");
        Ok(())
    }

    // ── Internal Methods ────────────────────────────────────────

    /// Check if the engine is running.
    fn check_running(&self) -> KvResult<()> {
        if !self.is_running.load(Ordering::Relaxed) {
            Err(KvError::Shutdown)
        } else {
            Ok(())
        }
    }

    /// Validate key and value sizes.
    fn check_key_value_size(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        if key.len() > 16 * 1024 {
            return Err(KvError::KeyTooLarge {
                size: key.len(),
                max: 16 * 1024,
            });
        }
        if value.len() > 1024 * 1024 {
            return Err(KvError::ValueTooLarge {
                size: value.len(),
                max: 1024 * 1024,
            });
        }
        Ok(())
    }

    /// Freeze the active memtable if it exceeds the size threshold.
    fn maybe_freeze_memtable(&self) -> KvResult<()> {
        let should_freeze = {
            let memtable = self.active_memtable.read();
            memtable.should_freeze()
        };

        if should_freeze {
            self.freeze_memtable()?;
        }

        Ok(())
    }

    /// Freeze the active memtable and create a new one.
    ///
    /// Also rotates the WAL: closes the old WAL and opens a new one for
    /// the new active memtable. The old WAL file number is recorded so it
    /// can be deleted after the memtable is flushed to an SSTable.
    fn freeze_memtable(&self) -> KvResult<()> {
        let _guard = self.write_mutex.lock();

        // Track the WAL number of the frozen memtable so we can delete it after flush
        let old_wal_number = self.wal_file_number.load(Ordering::Relaxed);

        let old_memtable = {
            let mut active = self.active_memtable.write();
            let next_seq = active.next_sequence();
            let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
            let new_memtable = Arc::new(MemTable::new(new_id, self.config.memtable_size, next_seq));
            std::mem::replace(&mut *active, new_memtable)
        };

        // Rotate WAL: close old, open new
        {
            let mut wal_guard = self.wal_writer.lock();
            if wal_guard.is_some() {
                // Close old WAL (flush to disk)
                if let Some(old_w) = wal_guard.take() {
                    if let Err(e) = old_w.close() {
                        warn!("error closing old WAL: {}", e);
                    }
                }
                // Open new WAL for the new active memtable
                let new_wal_num = self.next_memtable_id.load(Ordering::Relaxed);
                self.wal_file_number.store(new_wal_num, Ordering::Relaxed);
                let new_w =
                    WalWriter::create(&self.data_dir, new_wal_num, self.config.sync_writes)?;
                *wal_guard = Some(new_w);
            }
        }

        // Only add to immutables if it has data
        if !old_memtable.is_empty() {
            // Convert Arc<MemTable> to MemTable, then to ImmutableMemTable
            let memtable = Arc::try_unwrap(old_memtable).unwrap_or_else(|_arc| {
                warn!("memtable has multiple references during freeze");
                MemTable::new(0, 0, 0)
            });

            let immutable = Arc::new(ImmutableMemTable::from(memtable));

            let mut imm_tables = self.immutable_memtables.write();
            imm_tables.push_back(immutable);

            debug!(
                immutable_count = imm_tables.len(),
                old_wal = old_wal_number,
                "memtable frozen"
            );

            // Check if we need to stall writes
            if imm_tables.len() > self.config.max_immutable_memtables {
                warn!(
                    count = imm_tables.len(),
                    max = self.config.max_immutable_memtables,
                    "too many immutable memtables, triggering flush"
                );
            }
        } else {
            // Empty memtable, clean up the old WAL immediately
            let _ = wal::delete_wal(&self.data_dir, old_wal_number);
        }

        // Try to flush immutable memtables
        drop(_guard); // Release write lock before flush
        self.flush_immutable_memtables()?;

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

        // Clean up old WAL files that are no longer needed.
        // All WAL files with numbers less than the current active WAL are safe to delete
        // because the data they contained has been flushed to SSTables.
        if self.config.enable_wal {
            let current_wal = self.wal_file_number.load(Ordering::Relaxed);
            if let Ok(wal_files) = wal::find_wal_files(&self.data_dir) {
                for (wal_num, _) in wal_files {
                    if wal_num < current_wal {
                        let _ = wal::delete_wal(&self.data_dir, wal_num);
                        debug!(wal_number = wal_num, "deleted old WAL file");
                    }
                }
            }
        }

        // Signal background compaction that new L0 files are available
        self.signal_compaction();

        info!(
            file_number = file_number,
            entries = entry_count,
            file_size = result.file_size,
            "memtable flushed to L0"
        );

        Ok(())
    }

    /// Execute a compaction job.
    fn execute_compaction(&self, job: &crate::compaction::CompactionJob) -> KvResult<()> {
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

        // Sort entries by key (merge sort)
        input_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Deduplicate: for same key, keep the newest (first occurrence after sort
        // since entries from newer files come first)
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
                let file_info = self.write_compaction_output(job.output_level, &current_entries)?;
                new_files.push(file_info);
                current_entries.clear();
                current_size = 0;
            }
        }

        // Write remaining entries
        if !current_entries.is_empty() {
            let file_info = self.write_compaction_output(job.output_level, &current_entries)?;
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

        // Delete old SSTable files
        for file_info in &job.input_files {
            let path = self.sst_path(file_info.id);
            if path.exists() {
                fs::remove_file(&path).ok();
            }
        }
        for file_info in &job.output_files {
            let path = self.sst_path(file_info.id);
            if path.exists() {
                fs::remove_file(&path).ok();
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

    /// Signal the background compaction thread to wake up.
    fn signal_compaction(&self) {
        let (lock, cvar) = &*self.bg_compaction_signal;
        let mut triggered = lock.lock();
        *triggered = true;
        cvar.notify_one();
    }
    /// Start the background compaction thread.
    ///
    /// The thread sleeps until signalled (after a flush) or until a periodic
    /// timer fires. It then checks if compaction is needed and runs it.
    fn start_bg_compaction(&self) {
        if self.config.max_background_compactions == 0 {
            return; // Background compaction disabled
        }

        // Clone Arcs for the background thread
        let versions = Arc::clone(&self.versions);
        let compaction_strategy = Arc::clone(&self.compaction_strategy);
        let config = Arc::clone(&self.config);
        let signal = Arc::clone(&self.bg_compaction_signal);
        let data_dir = self.data_dir.clone();
        let is_running = Arc::clone(&self.is_running);

        let handle = thread::Builder::new()
            .name("nexus-kv-compaction".to_string())
            .spawn(move || {
                loop {
                    // Wait for a signal or timeout (check every 5 seconds)
                    {
                        let (lock, cvar) = &*signal;
                        let mut triggered = lock.lock();
                        if !*triggered {
                            cvar.wait_for(&mut triggered, Duration::from_secs(5));
                        }
                        *triggered = false;
                    }

                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }

                    // Try compaction
                    let version = versions.current();
                    let job = compaction_strategy.pick_compaction(&version.levels);

                    if let Some(job) = job {
                        debug!(
                            input_level = job.input_level,
                            output_level = job.output_level,
                            input_files = job.input_file_count(),
                            "background compaction starting"
                        );

                        if let Err(e) = run_compaction_job(
                            &job,
                            &versions,
                            &compaction_strategy,
                            &config,
                            &data_dir,
                        ) {
                            warn!("background compaction failed: {}", e);
                        }
                    }
                }

                debug!("background compaction thread exiting");
            })
            .ok();

        if let Some(h) = handle {
            *self.bg_compaction_handle.lock() = Some(h);
        }
    }
}

/// Run a compaction job (free function for use by the background thread).
fn run_compaction_job(
    job: &crate::compaction::CompactionJob,
    versions: &VersionSet,
    compaction_strategy: &Arc<dyn CompactionStrategy>,
    config: &LsmConfig,
    data_dir: &PathBuf,
) -> KvResult<()> {
    let sst_dir = data_dir.join("sst");
    let sst_path = |id: u64| sst_dir.join(format!("{:06}.sst", id));

    // Read all entries from input files
    let mut input_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    for file_info in job.input_files.iter().chain(job.output_files.iter()) {
        let path = sst_path(file_info.id);
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

    input_entries.sort_by(|a, b| a.0.cmp(&b.0));
    input_entries.dedup_by(|b, a| a.0 == b.0);

    // Write merged entries to new SSTable(s)
    let target_size = compaction_strategy.target_file_size(job.output_level);
    let mut new_files = Vec::new();
    let mut current_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut current_size = 0u64;

    let write_output = |level: usize, entries: &[(Vec<u8>, Vec<u8>)]| -> KvResult<SSTableInfo> {
        let file_number = versions.allocate_file_number();
        let file_path = sst_path(file_number);
        let file = fs::File::create(&file_path)?;
        let buf_writer = BufWriter::new(file);
        let options = SSTableWriterOptions {
            block_size: config.block_size,
            block_restart_interval: config.block_restart_interval,
            compression: config.compression,
            bloom_bits_per_key: if config.enable_bloom_filter {
                config.bloom_bits_per_key
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
    };

    for entry in &input_entries {
        current_size += (entry.0.len() + entry.1.len()) as u64;
        current_entries.push(entry.clone());

        if current_size >= target_size && !current_entries.is_empty() {
            new_files.push(write_output(job.output_level, &current_entries)?);
            current_entries.clear();
            current_size = 0;
        }
    }
    if !current_entries.is_empty() {
        new_files.push(write_output(job.output_level, &current_entries)?);
    }

    // Update version
    let mut edit = VersionEdit::new();
    for f in &job.input_files {
        edit.remove_file(job.input_level, f.id);
    }
    for f in &job.output_files {
        edit.remove_file(job.output_level, f.id);
    }
    for f in &new_files {
        edit.add_file(job.output_level, f.clone());
    }
    versions.apply_edit(&edit)?;

    // Delete old files
    for f in job.input_files.iter().chain(job.output_files.iter()) {
        let path = sst_path(f.id);
        if path.exists() {
            fs::remove_file(&path).ok();
        }
    }

    info!(
        input_level = job.input_level,
        output_level = job.output_level,
        input_files = job.input_file_count(),
        output_files = new_files.len(),
        "background compaction complete"
    );

    Ok(())
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
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

        engine.put(Bytes::from("key"), Bytes::from("v1")).unwrap();
        engine.put(Bytes::from("key"), Bytes::from("v2")).unwrap();

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
            engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
            engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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

    // ── Durability tests (WAL + manifest) ───────────────────────

    fn create_durable_config(dir: &std::path::Path) -> LsmConfig {
        let mut cfg = LsmConfig::for_testing(dir.to_path_buf());
        cfg.enable_wal = true;
        cfg.sync_writes = false; // faster tests
        cfg
    }

    #[test]
    fn test_wal_recovery_after_crash() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        // Phase 1: Write data, then drop engine without flush (simulate crash)
        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            engine
                .put(Bytes::from("key1"), Bytes::from("val1"))
                .unwrap();
            engine
                .put(Bytes::from("key2"), Bytes::from("val2"))
                .unwrap();
            engine
                .put(Bytes::from("key3"), Bytes::from("val3"))
                .unwrap();
            // Intentionally do NOT flush/shutdown - simulate crash
            // (Drop will try to shutdown, but that's fine for this test)
        }

        // Phase 2: Reopen and verify data recovered from WAL
        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            assert_eq!(engine.get(b"key1").unwrap(), Some(Bytes::from("val1")));
            assert_eq!(engine.get(b"key2").unwrap(), Some(Bytes::from("val2")));
            assert_eq!(engine.get(b"key3").unwrap(), Some(Bytes::from("val3")));
        }
    }

    #[test]
    fn test_manifest_recovery_after_flush() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        // Phase 1: Write, flush to SSTable, then close
        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            for i in 0..50u32 {
                let key = format!("{:08}", i);
                let value = format!("value_{}", i);
                engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
            }
            engine.flush().unwrap();
            engine.shutdown().unwrap();
        }

        // Phase 2: Reopen and verify data survived via manifest + SSTables
        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            for i in 0..50u32 {
                let key = format!("{:08}", i);
                let expected = format!("value_{}", i);
                let value = engine.get(key.as_bytes()).unwrap();
                assert_eq!(
                    value,
                    Some(Bytes::from(expected)),
                    "key {} not found after recovery",
                    key
                );
            }
        }
    }

    #[test]
    fn test_wal_delete_recovery() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        // Phase 1: Write then delete, crash
        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            engine
                .put(Bytes::from("alive"), Bytes::from("yes"))
                .unwrap();
            engine
                .put(Bytes::from("dead"), Bytes::from("soon"))
                .unwrap();
            engine.delete(Bytes::from("dead")).unwrap();
        }

        // Phase 2: Verify delete was also recovered
        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            assert_eq!(engine.get(b"alive").unwrap(), Some(Bytes::from("yes")));
            assert_eq!(engine.get(b"dead").unwrap(), None);
        }
    }

    #[test]
    fn test_durable_write_batch_recovery() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            let mut batch = WriteBatch::new();
            batch.put(Bytes::from("batch_a"), Bytes::from("1"));
            batch.put(Bytes::from("batch_b"), Bytes::from("2"));
            batch.delete(Bytes::from("batch_a"));
            engine.write(&batch).unwrap();
        }

        {
            let config = create_durable_config(&dir);
            let engine = LsmEngine::open(config).unwrap();
            assert_eq!(engine.get(b"batch_a").unwrap(), None);
            assert_eq!(engine.get(b"batch_b").unwrap(), Some(Bytes::from("2")));
        }
    }
}
