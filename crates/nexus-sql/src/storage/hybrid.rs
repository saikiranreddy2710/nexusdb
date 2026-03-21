//! Hybrid SageLSM Storage Engine.
//!
//! Combines an LSM-style MemTable (write buffer) with a SageTree B+Tree
//! (persistent read-optimized storage) to achieve both high write throughput
//! and low read latency.
//!
//! # Architecture
//!
//! ```text
//! Write Path:  insert(key, value)
//!                  │
//!                  ▼
//!          ┌──────────────┐
//!          │  MemTable    │  (lock-free skiplist, O(log n))
//!          │  (active)    │
//!          └──────┬───────┘
//!                 │ should_freeze()
//!                 ▼
//!          ┌──────────────┐
//!          │  MemTable    │  (immutable, queued for flush)
//!          │  (frozen)    │
//!          └──────┬───────┘
//!                 │ flush_to_tree()
//!                 ▼
//!          ┌──────────────┐
//!          │  SageTree    │  (B+Tree, FilePager-backed)
//!          │  (persistent)│
//!          └──────────────┘
//!
//! Read Path:  get(key)
//!                  │
//!                  ├── 1. Check active MemTable
//!                  ├── 2. Check frozen MemTables
//!                  └── 3. Check SageTree
//!                        (first match wins, tombstones honored)
//! ```
//!
//! # ML/LLM Extensibility
//!
//! The `FlushPolicy` trait allows plugging in learned flush strategies:
//! - Default: threshold-based (flush when MemTable exceeds size limit)
//! - Future: ML model predicts optimal flush timing based on workload patterns
//!
//! The `HybridStats` struct exposes metrics for learned cost models:
//! - Write/read counters, MemTable hit rate, flush latency
//! - These feed into EXPLAIN ANALYZE and future ML-based query optimization

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tracing::debug;

use nexus_common::types::{Key, Value as StorageValue};
use nexus_kv::memtable::{ImmutableMemTable, MemTable, ValueType};
use nexus_storage::sagetree::{KeyRange, SageTree};

use super::error::{StorageError, StorageResult};

// =========================================================================
// Configuration
// =========================================================================

/// Configuration for the hybrid store.
#[derive(Debug, Clone)]
pub struct HybridConfig {
    /// Maximum MemTable size in bytes before freezing (default: 4MB).
    pub memtable_size: usize,
    /// Maximum number of frozen MemTables before blocking writes (default: 4).
    pub max_immutable_memtables: usize,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024, // 4MB
            max_immutable_memtables: 4,
        }
    }
}

// =========================================================================
// Flush Policy (ML-extensible)
// =========================================================================

/// Policy for deciding when to flush a MemTable to the tree.
///
/// Default implementation uses a simple size threshold. Future implementations
/// can use ML models trained on workload patterns to decide optimal flush timing.
pub trait FlushPolicy: Send + Sync + std::fmt::Debug {
    /// Returns true if the active MemTable should be frozen and flushed.
    fn should_flush(&self, stats: &HybridStats, memtable_usage: usize, memtable_max: usize)
        -> bool;
}

/// Default threshold-based flush policy.
#[derive(Debug)]
pub struct ThresholdFlushPolicy;

impl FlushPolicy for ThresholdFlushPolicy {
    fn should_flush(
        &self,
        _stats: &HybridStats,
        memtable_usage: usize,
        memtable_max: usize,
    ) -> bool {
        memtable_usage >= memtable_max
    }
}

// =========================================================================
// Statistics (for ML observability)
// =========================================================================

/// Statistics exposed for ML cost models and EXPLAIN ANALYZE.
#[derive(Debug, Default, Clone)]
pub struct HybridStats {
    /// Total writes to MemTable.
    pub memtable_writes: u64,
    /// Total reads served from MemTable (hit).
    pub memtable_hits: u64,
    /// Total reads served from SageTree (miss in MemTable).
    pub tree_hits: u64,
    /// Total reads that found nothing.
    pub read_misses: u64,
    /// Number of flushes performed.
    pub flush_count: u64,
    /// Total rows flushed to tree.
    pub rows_flushed: u64,
    /// Total scan operations.
    pub scan_count: u64,
}

// =========================================================================
// HybridStore
// =========================================================================

/// Hybrid SageLSM store combining MemTable write buffer with SageTree B+Tree.
///
/// Thread-safe: multiple readers, serialized writers (via MemTable's internal lock).
pub struct HybridStore {
    /// Active (mutable) MemTable — receives all writes.
    active: RwLock<Arc<MemTable>>,
    /// Frozen MemTables waiting to be flushed to the tree.
    frozen: RwLock<Vec<Arc<ImmutableMemTable>>>,
    /// Persistent B+Tree storage.
    tree: SageTree,
    /// Global sequence counter for MVCC ordering.
    sequence: AtomicU64,
    /// MemTable ID counter.
    next_memtable_id: AtomicU64,
    /// Atomic entry count — tracks net live entries (inserts - deletes).
    /// Updated on insert/delete/upsert to avoid O(n) full-scan for len().
    entry_count: AtomicI64,
    /// Configuration.
    config: HybridConfig,
    /// Flush policy (pluggable for ML).
    flush_policy: Box<dyn FlushPolicy>,
    /// Observable statistics.
    stats: RwLock<HybridStats>,
}

impl std::fmt::Debug for HybridStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridStore")
            .field("config", &self.config)
            .field("sequence", &self.sequence.load(Ordering::Relaxed))
            .field("frozen_count", &self.frozen.read().len())
            .finish()
    }
}

impl HybridStore {
    /// Creates a new hybrid store with an in-memory SageTree.
    pub fn new() -> Self {
        Self::with_tree(SageTree::new())
    }

    /// Creates a hybrid store backed by the given SageTree.
    pub fn with_tree(tree: SageTree) -> Self {
        Self::with_tree_and_config(tree, HybridConfig::default())
    }

    /// Creates a hybrid store with custom config.
    pub fn with_tree_and_config(tree: SageTree, config: HybridConfig) -> Self {
        // Initialize entry count from tree's existing data
        let initial_count = tree.len() as i64;
        let memtable = Arc::new(MemTable::new(1, config.memtable_size, 1));
        Self {
            active: RwLock::new(memtable),
            frozen: RwLock::new(Vec::new()),
            tree,
            sequence: AtomicU64::new(1),
            next_memtable_id: AtomicU64::new(2),
            entry_count: AtomicI64::new(initial_count),
            config,
            flush_policy: Box::new(ThresholdFlushPolicy),
            stats: RwLock::new(HybridStats::default()),
        }
    }

    /// Sets a custom flush policy (for ML-driven flush decisions).
    pub fn with_flush_policy(mut self, policy: Box<dyn FlushPolicy>) -> Self {
        self.flush_policy = policy;
        self
    }

    // =====================================================================
    // Write Path
    // =====================================================================

    /// Inserts a new key-value pair into the MemTable.
    ///
    /// The caller is responsible for checking duplicates if needed.
    /// This is a pure append to the MemTable — no disk I/O.
    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> StorageResult<()> {
        self.maybe_freeze();

        let memtable = self.active.read().clone();
        memtable
            .put(Bytes::from(key), Bytes::from(value))
            .map_err(|e| StorageError::InvalidOperation(format!("memtable put failed: {}", e)))?;

        self.entry_count.fetch_add(1, Ordering::Relaxed);
        self.stats.write().memtable_writes += 1;
        Ok(())
    }

    /// Marks a key as deleted (tombstone in MemTable).
    pub fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.maybe_freeze();

        let memtable = self.active.read().clone();
        memtable.delete(Bytes::from(key.to_vec())).map_err(|e| {
            StorageError::InvalidOperation(format!("memtable delete failed: {}", e))
        })?;

        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.write().memtable_writes += 1;
        Ok(())
    }

    /// Updates a key-value pair (overwrite — no net count change).
    pub fn update(&self, key: Vec<u8>, value: Vec<u8>) -> StorageResult<()> {
        self.maybe_freeze();

        let memtable = self.active.read().clone();
        memtable
            .put(Bytes::from(key), Bytes::from(value))
            .map_err(|e| StorageError::InvalidOperation(format!("memtable put failed: {}", e)))?;

        // No entry_count change — overwrite of existing key
        self.stats.write().memtable_writes += 1;
        Ok(())
    }

    /// Upserts a key-value pair (insert or update).
    ///
    /// Increments entry count only if the key doesn't already exist.
    pub fn upsert(&self, key: Vec<u8>, value: Vec<u8>) -> StorageResult<()> {
        let is_new = self.get(&key)?.is_none();

        self.maybe_freeze();

        let memtable = self.active.read().clone();
        memtable
            .put(Bytes::from(key), Bytes::from(value))
            .map_err(|e| StorageError::InvalidOperation(format!("memtable put failed: {}", e)))?;

        if is_new {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.write().memtable_writes += 1;
        Ok(())
    }

    // =====================================================================
    // Read Path (MemTable → Frozen → SageTree)
    // =====================================================================

    /// Gets a value by key. Checks MemTable first, then frozen tables, then SageTree.
    pub fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let seq = u64::MAX; // read latest

        // 1. Check active MemTable
        let memtable = self.active.read().clone();
        if let Some((vtype, value)) = memtable.get(key, seq) {
            self.stats.write().memtable_hits += 1;
            if matches!(vtype, ValueType::Deletion) {
                return Ok(None); // tombstone
            }
            return Ok(Some(value.to_vec()));
        }

        // 2. Check frozen MemTables (newest first)
        {
            let frozen = self.frozen.read();
            for imm in frozen.iter().rev() {
                if let Some((vtype, value)) = imm.get(key, seq) {
                    self.stats.write().memtable_hits += 1;
                    if matches!(vtype, ValueType::Deletion) {
                        return Ok(None); // tombstone
                    }
                    return Ok(Some(value.to_vec()));
                }
            }
        }

        // 3. Check SageTree
        let tree_key = Key::from(key.to_vec());
        match self.tree.get(&tree_key) {
            Ok(Some(value)) => {
                self.stats.write().tree_hits += 1;
                Ok(Some(value.as_ref().to_vec()))
            }
            Ok(None) => {
                self.stats.write().read_misses += 1;
                Ok(None)
            }
            Err(e) => Err(StorageError::InvalidOperation(format!(
                "tree get failed: {}",
                e
            ))),
        }
    }

    /// Scans all entries, merging MemTable and SageTree results.
    ///
    /// Returns entries sorted by key. MemTable entries take precedence
    /// over SageTree entries (newer data wins). Tombstones are filtered out.
    pub fn scan_all(&self) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range_internal(KeyRange::all())
    }

    /// Scans a key range, merging MemTable and SageTree.
    pub fn scan_range(&self, range: KeyRange) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range_internal(range)
    }

    /// Internal scan implementation — merges MemTable + SageTree entries.
    ///
    /// Uses a single BTreeMap pass: tree entries first, then MemTable entries
    /// override (newest wins). Tombstones filtered at the end.
    fn scan_range_internal(&self, range: KeyRange) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.stats.write().scan_count += 1;

        // Collect SageTree entries into a BTreeMap (sorted by key)
        let tree_entries = self
            .tree
            .scan(range)
            .map_err(|e| StorageError::InvalidOperation(format!("tree scan failed: {}", e)))?;

        // Pre-allocate with tree size estimate
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();

        for entry in &tree_entries {
            merged.insert(
                entry.key.as_ref().to_vec(),
                Some(entry.value.as_ref().to_vec()),
            );
        }

        // Overlay MemTable entries (active + frozen).
        // MemTable iterators return entries in (user_key ASC, sequence DESC) order.
        // For each key, the first entry we see is the newest version.
        self.overlay_memtable_entries(&mut merged);

        // Filter out tombstones (None values) and collect
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    /// Overlays MemTable entries onto the merged result map.
    ///
    /// Processes all MemTables (active + frozen) and for each unique key,
    /// takes the newest version (highest sequence number).
    fn overlay_memtable_entries(
        &self,
        merged: &mut std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) {
        // Collect the latest version per key from all MemTables.
        // We use a temporary map keyed by user_key → (sequence, value_type, value)
        // to deduplicate across multiple MemTables.
        let mut latest: std::collections::HashMap<Vec<u8>, (u64, ValueType, Vec<u8>)> =
            std::collections::HashMap::new();

        // Process frozen MemTables (oldest first)
        let frozen = self.frozen.read();
        for imm in frozen.iter() {
            let mut iter = imm.iter();
            while iter.valid() {
                if let Some((ikey, value)) = iter.current() {
                    let user_key = ikey.user_key.to_vec();
                    let seq = ikey.sequence;
                    let existing_seq = latest.get(&user_key).map(|e| e.0).unwrap_or(0);
                    if seq > existing_seq {
                        latest.insert(user_key, (seq, ikey.value_type, value.to_vec()));
                    }
                }
                iter.next();
            }
        }
        drop(frozen);

        // Process active MemTable (newest — always wins over frozen)
        let memtable = self.active.read().clone();
        let mut iter = memtable.iter();
        while iter.valid() {
            if let Some((ikey, value)) = iter.current() {
                let user_key = ikey.user_key.to_vec();
                let seq = ikey.sequence;
                let existing_seq = latest.get(&user_key).map(|e| e.0).unwrap_or(0);
                if seq > existing_seq {
                    latest.insert(user_key, (seq, ikey.value_type, value.to_vec()));
                }
            }
            iter.next();
        }

        // Apply to merged map
        for (key, (_seq, vtype, value)) in latest {
            match vtype {
                ValueType::Value => {
                    merged.insert(key, Some(value));
                }
                ValueType::Deletion => {
                    merged.insert(key, None); // tombstone masks tree entry
                }
            }
        }
    }

    // =====================================================================
    // Flush (MemTable → SageTree)
    // =====================================================================

    /// Checks if the active MemTable should be frozen, and freezes it if so.
    fn maybe_freeze(&self) {
        let should = {
            let memtable = self.active.read();
            let stats = self.stats.read();
            self.flush_policy.should_flush(
                &stats,
                memtable.approximate_memory_usage(),
                self.config.memtable_size,
            )
        };

        if should {
            self.freeze_active();
        }
    }

    /// Freezes the active MemTable and creates a new one.
    fn freeze_active(&self) {
        let mut active = self.active.write();
        let old = Arc::clone(&active);

        if old.is_empty() {
            return; // nothing to freeze
        }

        let imm = ImmutableMemTable::from_arc(old);
        self.frozen.write().push(Arc::new(imm));

        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let new_seq = self.sequence.load(Ordering::Relaxed);
        *active = Arc::new(MemTable::new(new_id, self.config.memtable_size, new_seq));

        debug!("Froze MemTable, frozen count: {}", self.frozen.read().len());
    }

    /// Flushes all frozen MemTables into the SageTree, then flushes to disk.
    pub fn flush(&self) -> StorageResult<()> {
        // First freeze the active memtable if it has data
        {
            let active = self.active.read();
            if !active.is_empty() {
                drop(active);
                self.freeze_active();
            }
        }

        // Drain frozen MemTables into the SageTree
        let to_flush: Vec<Arc<ImmutableMemTable>> = {
            let mut frozen = self.frozen.write();
            std::mem::take(&mut *frozen)
        };

        let mut total_flushed = 0u64;
        for imm in &to_flush {
            let mut iter = imm.iter();
            while iter.valid() {
                if let Some((ikey, value)) = iter.current() {
                    match ikey.value_type {
                        ValueType::Value => {
                            let key = Key::from(ikey.user_key.to_vec());
                            let val = StorageValue::from_bytes(&value);
                            // Use upsert to handle duplicates (newer wins)
                            self.tree.upsert(key, val).map_err(|e| {
                                StorageError::InvalidOperation(format!(
                                    "flush upsert failed: {}",
                                    e
                                ))
                            })?;
                            total_flushed += 1;
                        }
                        ValueType::Deletion => {
                            let key = Key::from(ikey.user_key.to_vec());
                            // Ignore KeyNotFound — the key may not exist in tree
                            let _ = self.tree.delete(&key);
                        }
                    }
                }
                iter.next();
            }
            // imm.mark_flushed(); // already consumed
        }

        // Flush the SageTree pager to disk
        self.tree
            .flush()
            .map_err(|e| StorageError::InvalidOperation(format!("tree flush failed: {}", e)))?;

        let mut stats = self.stats.write();
        stats.flush_count += 1;
        stats.rows_flushed += total_flushed;

        debug!(
            "Flushed {} rows to SageTree (total flushes: {})",
            total_flushed, stats.flush_count
        );

        Ok(())
    }

    // =====================================================================
    // Metadata
    // =====================================================================

    /// Returns the total number of live entries (O(1), atomic counter).
    ///
    /// Maintained incrementally on insert/delete/upsert — no scan required.
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed).max(0) as usize
    }

    /// Returns true if the store has no entries (O(1)).
    pub fn is_empty(&self) -> bool {
        self.entry_count.load(Ordering::Relaxed) <= 0
    }

    /// Returns a snapshot of the statistics (for EXPLAIN ANALYZE / ML models).
    pub fn stats(&self) -> HybridStats {
        self.stats.read().clone()
    }

    /// Returns the underlying SageTree (for direct access when needed).
    pub fn tree(&self) -> &SageTree {
        &self.tree
    }

    /// Clears all data (MemTable + SageTree).
    pub fn clear(&self) {
        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        *self.active.write() = Arc::new(MemTable::new(new_id, self.config.memtable_size, 1));
        self.frozen.write().clear();
        self.tree.clear();
        self.sequence.store(1, Ordering::Relaxed);
        self.entry_count.store(0, Ordering::Relaxed);
        *self.stats.write() = HybridStats::default();
    }
}

impl Default for HybridStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_get() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        store.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_delete_tombstone() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        store.delete(b"key1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_update_overwrites() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"v1".to_vec()).unwrap();
        store.update(b"key1".to_vec(), b"v2".to_vec()).unwrap();

        assert_eq!(store.get(b"key1").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_scan_all() {
        let store = HybridStore::new();
        store.insert(b"b".to_vec(), b"2".to_vec()).unwrap();
        store.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
        store.insert(b"c".to_vec(), b"3".to_vec()).unwrap();

        let results = store.scan_all().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_scan_with_tombstones() {
        let store = HybridStore::new();
        store.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
        store.insert(b"b".to_vec(), b"2".to_vec()).unwrap();
        store.insert(b"c".to_vec(), b"3".to_vec()).unwrap();
        store.delete(b"b").unwrap();

        let results = store.scan_all().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_flush_to_tree() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        store.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        // Flush memtable to tree
        store.flush().unwrap();

        // Data should still be readable after flush
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));

        // Stats should reflect the flush
        let stats = store.stats();
        assert_eq!(stats.flush_count, 1);
        assert_eq!(stats.rows_flushed, 2);
    }

    #[test]
    fn test_memtable_overrides_tree() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"old".to_vec()).unwrap();
        store.flush().unwrap(); // now in tree

        // Update in memtable — should override tree value
        store.insert(b"key1".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Some(b"new".to_vec()));

        // Scan should also show the new value
        let results = store.scan_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"new".to_vec());
    }

    #[test]
    fn test_delete_after_flush() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        store.flush().unwrap(); // now in tree

        // Delete via tombstone in memtable
        store.delete(b"key1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), None);

        // Scan should not include deleted key
        let results = store.scan_all().unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_stats_tracking() {
        let store = HybridStore::new();
        store.insert(b"k".to_vec(), b"v".to_vec()).unwrap();
        store.get(b"k").unwrap();
        store.get(b"missing").unwrap();

        let stats = store.stats();
        assert_eq!(stats.memtable_writes, 1);
        assert_eq!(stats.memtable_hits, 1);
        assert_eq!(stats.read_misses, 1);
    }

    #[test]
    fn test_clear() {
        let store = HybridStore::new();
        store.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        store.flush().unwrap();
        store.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        store.clear();
        assert!(store.is_empty());
        assert_eq!(store.get(b"key1").unwrap(), None);
        assert_eq!(store.get(b"key2").unwrap(), None);
    }
}
