//! Table cache: LRU cache of open SSTableReader handles.
//!
//! Opening an SSTable involves reading the footer, index block, and bloom
//! filter from disk. The table cache keeps recently-accessed readers open
//! so that repeated lookups to the same SSTable avoid this overhead.
//!
//! ## Usage
//!
//! The engine calls `table_cache.get_or_open(file_id)` on every SSTable
//! read. The cache returns a shared `Arc<SSTableReader>` that can be used
//! concurrently by multiple readers.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{KvError, KvResult};
use crate::sstable::reader::SSTableReader;

/// Cached SSTable file data (read into memory once, reused for all lookups).
pub type SharedData = Arc<Vec<u8>>;

/// LRU cache of SSTable file data.
///
/// Stores the raw bytes of SSTable files in memory to avoid repeated
/// disk reads. The engine creates lightweight `SSTableReader` instances
/// from the cached bytes for each lookup (parsing footer/index/bloom
/// from memory is microseconds vs milliseconds from disk).
///
/// Thread-safe via internal Mutex. Capacity is bounded by number of
/// cached files.
pub struct TableCache {
    inner: Mutex<TableCacheInner>,
    /// Directory containing SSTable files.
    sst_dir: PathBuf,
    /// Maximum number of cached files.
    max_open: usize,
    // Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

struct TableCacheInner {
    /// Map from file_id to cached file data.
    data: HashMap<u64, SharedData>,
    /// LRU order: most recently used at the end.
    order: Vec<u64>,
}

impl TableCacheInner {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            order: Vec::new(),
        }
    }

    fn touch(&mut self, file_id: u64) {
        if let Some(pos) = self.order.iter().position(|&id| id == file_id) {
            self.order.remove(pos);
        }
        self.order.push(file_id);
    }

    fn evict_lru(&mut self) -> Option<u64> {
        if self.order.is_empty() {
            return None;
        }
        let file_id = self.order.remove(0);
        self.data.remove(&file_id);
        Some(file_id)
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

impl TableCache {
    /// Create a new table cache.
    ///
    /// - `sst_dir`: Directory containing `{file_id:06}.sst` files.
    /// - `max_open`: Maximum number of SSTable readers to keep open.
    pub fn new(sst_dir: PathBuf, max_open: usize) -> Self {
        Self {
            inner: Mutex::new(TableCacheInner::new()),
            sst_dir,
            max_open: max_open.max(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Get cached file data for the given SSTable file ID.
    ///
    /// On cache hit, returns the in-memory bytes immediately.
    /// On cache miss, reads the file from disk and caches the bytes.
    ///
    /// The caller creates an `SSTableReader` from the returned bytes
    /// using `Cursor::new((*data).clone())`.
    pub fn get_data(&self, file_id: u64) -> KvResult<SharedData> {
        let mut inner = self.inner.lock();

        // Check cache
        if inner.data.contains_key(&file_id) {
            inner.touch(file_id);
            let data = inner.data.get(&file_id).unwrap().clone();
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(data);
        }

        // Cache miss — read from disk
        self.misses.fetch_add(1, Ordering::Relaxed);

        // Evict if at capacity
        while inner.len() >= self.max_open {
            if inner.evict_lru().is_some() {
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        let path = self.sst_path(file_id);
        let raw_data = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                KvError::FileNotFound(path.clone())
            } else {
                KvError::Io(e)
            }
        })?;

        let shared = Arc::new(raw_data);
        inner.data.insert(file_id, shared.clone());
        inner.touch(file_id);

        Ok(shared)
    }

    /// Create an SSTableReader from cached file data.
    ///
    /// This is a convenience method that combines `get_data()` with
    /// reader construction. The reader parses footer/index/bloom from
    /// the in-memory bytes (microseconds, no disk I/O).
    pub fn open_reader(&self, file_id: u64) -> KvResult<SSTableReader<Cursor<Vec<u8>>>> {
        let data = self.get_data(file_id)?;
        let path = self.sst_path(file_id);
        let cursor = Cursor::new((*data).clone());
        SSTableReader::open(cursor, path)
    }

    /// Remove a specific file from the cache.
    /// Used when an SSTable is deleted during compaction.
    pub fn evict(&self, file_id: u64) {
        let mut inner = self.inner.lock();
        inner.data.remove(&file_id);
        if let Some(pos) = inner.order.iter().position(|&id| id == file_id) {
            inner.order.remove(pos);
        }
    }

    /// Remove all entries from the cache.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.data.clear();
        inner.order.clear();
    }

    /// Number of currently cached readers.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cache statistics.
    pub fn stats(&self) -> TableCacheStats {
        TableCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            open_readers: self.len(),
            max_open: self.max_open,
        }
    }

    /// Build the file path for an SSTable.
    fn sst_path(&self, file_id: u64) -> PathBuf {
        self.sst_dir.join(format!("{:06}.sst", file_id))
    }
}

/// Table cache statistics snapshot.
#[derive(Debug, Clone)]
pub struct TableCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub open_readers: usize,
    pub max_open: usize,
}

impl TableCacheStats {
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionType;
    use crate::sstable::writer::{SSTableWriter, SSTableWriterOptions};
    use std::path::Path;
    use tempfile::TempDir;

    fn create_test_sst(dir: &Path, file_id: u64, entries: &[(&str, &str)]) {
        let path = dir.join(format!("{:06}.sst", file_id));
        let file = fs::File::create(&path).unwrap();
        let options = SSTableWriterOptions {
            block_size: 128,
            block_restart_interval: 4,
            compression: CompressionType::None,
            bloom_bits_per_key: 10,
        };
        let mut writer = SSTableWriter::new(file, options);
        for (k, v) in entries {
            writer.add(k.as_bytes(), v.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }

    #[test]
    fn test_table_cache_basic() {
        let tmp = TempDir::new().unwrap();
        let sst_dir = tmp.path().to_path_buf();

        create_test_sst(&sst_dir, 1, &[("a", "1"), ("b", "2"), ("c", "3")]);

        let cache = TableCache::new(sst_dir, 10);

        // First access: cache miss
        let _data = cache.get_data(1).unwrap();
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 0);

        // Second access: cache hit
        let _data2 = cache.get_data(1).unwrap();
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn test_table_cache_open_reader() {
        let tmp = TempDir::new().unwrap();
        let sst_dir = tmp.path().to_path_buf();

        create_test_sst(&sst_dir, 1, &[("a", "1"), ("b", "2"), ("c", "3")]);

        let cache = TableCache::new(sst_dir, 10);

        // Open a reader and perform a lookup
        let mut reader = cache.open_reader(1).unwrap();
        let val = reader.get(b"b").unwrap();
        assert_eq!(val, Some(b"2".to_vec()));
    }

    #[test]
    fn test_table_cache_eviction() {
        let tmp = TempDir::new().unwrap();
        let sst_dir = tmp.path().to_path_buf();

        for i in 1..=5u64 {
            create_test_sst(
                &sst_dir,
                i,
                &[(
                    &format!("key_{}", i),
                    &format!("val_{}", i),
                )],
            );
        }

        let cache = TableCache::new(sst_dir, 3); // Max 3 cached

        cache.get_data(1).unwrap();
        cache.get_data(2).unwrap();
        cache.get_data(3).unwrap();
        assert_eq!(cache.len(), 3);

        // Opening 4th should evict file 1 (LRU)
        cache.get_data(4).unwrap();
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn test_table_cache_evict_specific() {
        let tmp = TempDir::new().unwrap();
        let sst_dir = tmp.path().to_path_buf();

        create_test_sst(&sst_dir, 1, &[("a", "1")]);
        create_test_sst(&sst_dir, 2, &[("b", "2")]);

        let cache = TableCache::new(sst_dir, 10);
        cache.get_data(1).unwrap();
        cache.get_data(2).unwrap();
        assert_eq!(cache.len(), 2);

        cache.evict(1);
        assert_eq!(cache.len(), 1);

        // Re-reading file 1 should be a miss
        cache.get_data(1).unwrap();
        assert_eq!(cache.stats().misses, 3); // 2 initial + 1 re-read
    }

    #[test]
    fn test_table_cache_file_not_found() {
        let tmp = TempDir::new().unwrap();
        let cache = TableCache::new(tmp.path().to_path_buf(), 10);

        let result = cache.get_data(999);
        assert!(result.is_err());
    }
}
