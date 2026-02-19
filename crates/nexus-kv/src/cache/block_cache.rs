//! Sharded block cache for SSTable data blocks.
//!
//! Caches recently-read data blocks to avoid repeated disk I/O. Uses
//! sharding (multiple independent LRU partitions) to reduce lock contention
//! under concurrent reads.
//!
//! ## Design
//!
//! ```text
//! BlockCache (capacity = 512 MB, shards = 16)
//!   ├── Shard 0:  LRU { (file_id=1, offset=0) -> Block, ... }
//!   ├── Shard 1:  LRU { (file_id=3, offset=4096) -> Block, ... }
//!   ├── ...
//!   └── Shard 15: LRU { ... }
//!
//! Shard selection: hash(file_id, offset) % num_shards
//! ```
//!
//! Each shard is an independent LRU with its own Mutex, so reads to
//! different shards never contend.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Key for a cached block: (SSTable file ID, block offset within file).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockCacheKey {
    pub file_id: u64,
    pub offset: u64,
}

impl BlockCacheKey {
    pub fn new(file_id: u64, offset: u64) -> Self {
        Self { file_id, offset }
    }

    /// Hash for shard selection. Uses FxHash-style mixing.
    fn shard_hash(&self) -> u64 {
        let mut h = self.file_id.wrapping_mul(0x517cc1b727220a95);
        h ^= self.offset.wrapping_mul(0x6c62272e07bb0142);
        h
    }
}

/// A cached data block.
#[derive(Debug, Clone)]
pub struct CachedBlock {
    /// The raw block data (entries + restart array + restart count).
    pub data: Vec<u8>,
}

impl CachedBlock {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Size of this cached entry in bytes (for capacity accounting).
    pub fn charge(&self) -> usize {
        self.data.len() + std::mem::size_of::<Self>()
    }
}

/// Thread-safe, sharded LRU block cache.
///
/// Each shard is an independent LRU partition protected by its own Mutex.
/// Shard selection is deterministic based on (file_id, offset), so the
/// same block always maps to the same shard.
pub struct BlockCache {
    shards: Vec<Mutex<CacheShard>>,
    num_shards: usize,
    /// Total capacity in bytes across all shards.
    capacity: usize,
    // Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

/// A single LRU shard. Uses a `HashMap` + doubly-linked-list approach
/// implemented via generation-indexed entries for simplicity and correctness.
struct CacheShard {
    /// Map from key to entry index in `entries`.
    map: HashMap<BlockCacheKey, usize>,
    /// LRU-ordered entries. Index 0 = most recently used.
    entries: Vec<ShardEntry>,
    /// Current total size of cached data in this shard.
    usage: usize,
    /// Maximum size for this shard.
    capacity: usize,
}

struct ShardEntry {
    key: BlockCacheKey,
    block: CachedBlock,
    charge: usize,
}

impl CacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            entries: Vec::new(),
            usage: 0,
            capacity,
        }
    }

    /// Look up a block. On hit, moves the entry to the front (most recent).
    fn get(&mut self, key: &BlockCacheKey) -> Option<&CachedBlock> {
        let idx = *self.map.get(key)?;
        // Move to front (most recently used)
        if idx != 0 {
            let entry = self.entries.remove(idx);
            self.entries.insert(0, entry);
            // Rebuild index mappings for shifted entries
            for (i, e) in self.entries.iter().enumerate() {
                self.map.insert(e.key, i);
            }
        }
        Some(&self.entries[0].block)
    }

    /// Insert a block. Evicts LRU entries if over capacity.
    /// Returns the number of entries evicted.
    fn insert(&mut self, key: BlockCacheKey, block: CachedBlock) -> usize {
        let charge = block.charge();

        // Remove existing entry for this key if present
        if let Some(idx) = self.map.remove(&key) {
            let old = self.entries.remove(idx);
            self.usage -= old.charge;
            // Rebuild indices
            for (i, e) in self.entries.iter().enumerate() {
                self.map.insert(e.key, i);
            }
        }

        // Evict LRU entries until we have room
        let mut evicted = 0;
        while self.usage + charge > self.capacity && !self.entries.is_empty() {
            let victim = self.entries.pop().unwrap();
            self.map.remove(&victim.key);
            self.usage -= victim.charge;
            evicted += 1;
        }

        // Insert at front (most recently used)
        let entry = ShardEntry {
            key,
            block,
            charge,
        };
        self.entries.insert(0, entry);
        self.usage += charge;
        // Rebuild indices
        for (i, e) in self.entries.iter().enumerate() {
            self.map.insert(e.key, i);
        }

        evicted
    }

    /// Remove a specific block.
    fn remove(&mut self, key: &BlockCacheKey) -> bool {
        if let Some(idx) = self.map.remove(key) {
            let entry = self.entries.remove(idx);
            self.usage -= entry.charge;
            for (i, e) in self.entries.iter().enumerate() {
                self.map.insert(e.key, i);
            }
            true
        } else {
            false
        }
    }

    /// Remove all entries for a given file.
    fn invalidate_file(&mut self, file_id: u64) {
        let keys_to_remove: Vec<BlockCacheKey> = self
            .entries
            .iter()
            .filter(|e| e.key.file_id == file_id)
            .map(|e| e.key)
            .collect();
        for key in keys_to_remove {
            self.remove(&key);
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn usage(&self) -> usize {
        self.usage
    }
}

// ── BlockCache Public API ───────────────────────────────────────

/// Default number of shards (should be power of 2).
const DEFAULT_NUM_SHARDS: usize = 16;

impl BlockCache {
    /// Create a new block cache with the given total capacity in bytes.
    pub fn new(capacity: usize) -> Self {
        Self::with_shards(capacity, DEFAULT_NUM_SHARDS)
    }

    /// Create a block cache with a specific number of shards.
    pub fn with_shards(capacity: usize, num_shards: usize) -> Self {
        let num_shards = num_shards.max(1);
        let per_shard = capacity / num_shards;

        let shards = (0..num_shards)
            .map(|_| Mutex::new(CacheShard::new(per_shard)))
            .collect();

        Self {
            shards,
            num_shards,
            capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Look up a cached block.
    pub fn get(&self, key: &BlockCacheKey) -> Option<CachedBlock> {
        let shard_idx = self.shard_for(key);
        let mut shard = self.shards[shard_idx].lock();
        match shard.get(key) {
            Some(block) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(block.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a block into the cache.
    pub fn insert(&self, key: BlockCacheKey, block: CachedBlock) {
        let shard_idx = self.shard_for(&key);
        let mut shard = self.shards[shard_idx].lock();
        let evicted = shard.insert(key, block);
        self.inserts.fetch_add(1, Ordering::Relaxed);
        if evicted > 0 {
            self.evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }
    }

    /// Remove a block from the cache.
    pub fn remove(&self, key: &BlockCacheKey) -> bool {
        let shard_idx = self.shard_for(key);
        let mut shard = self.shards[shard_idx].lock();
        shard.remove(key)
    }

    /// Invalidate all cached blocks for a given SSTable file.
    /// Used after compaction deletes an SSTable.
    pub fn invalidate_file(&self, file_id: u64) {
        for shard in &self.shards {
            shard.lock().invalidate_file(file_id);
        }
    }

    /// Total capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current total usage across all shards.
    pub fn usage(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().usage())
            .sum()
    }

    /// Total number of cached blocks.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cache statistics.
    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            usage: self.usage(),
            capacity: self.capacity,
        }
    }

    /// Determine which shard a key belongs to.
    fn shard_for(&self, key: &BlockCacheKey) -> usize {
        (key.shard_hash() as usize) % self.num_shards
    }
}

/// Block cache statistics snapshot.
#[derive(Debug, Clone)]
pub struct BlockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub usage: usize,
    pub capacity: usize,
}

impl BlockCacheStats {
    /// Hit ratio (0.0 to 1.0).
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

    #[test]
    fn test_block_cache_basic() {
        let cache = BlockCache::new(1024 * 1024); // 1 MB
        let key = BlockCacheKey::new(1, 0);
        let block = CachedBlock::new(vec![0u8; 100]);

        assert!(cache.get(&key).is_none());
        cache.insert(key, block.clone());
        assert!(cache.get(&key).is_some());
        assert_eq!(cache.get(&key).unwrap().data.len(), 100);
    }

    #[test]
    fn test_block_cache_eviction() {
        let cache = BlockCache::with_shards(256, 1); // 256 bytes, 1 shard

        // Insert blocks until we exceed capacity
        for i in 0..10u64 {
            let key = BlockCacheKey::new(1, i * 4096);
            let block = CachedBlock::new(vec![i as u8; 50]);
            cache.insert(key, block);
        }

        // Older entries should have been evicted
        let stats = cache.stats();
        assert!(stats.evictions > 0);
        assert!(cache.usage() <= 256);
    }

    #[test]
    fn test_block_cache_lru_ordering() {
        let cache = BlockCache::with_shards(512, 1);

        let k1 = BlockCacheKey::new(1, 0);
        let k2 = BlockCacheKey::new(1, 4096);
        let k3 = BlockCacheKey::new(1, 8192);

        cache.insert(k1, CachedBlock::new(vec![0; 100]));
        cache.insert(k2, CachedBlock::new(vec![0; 100]));
        cache.insert(k3, CachedBlock::new(vec![0; 100]));

        // Access k1 to make it most recently used
        cache.get(&k1);

        // Insert more to force eviction — k2 should be evicted (oldest)
        let k4 = BlockCacheKey::new(1, 12288);
        cache.insert(k4, CachedBlock::new(vec![0; 200]));

        // k1 should still be present (recently accessed)
        assert!(cache.get(&k1).is_some());
    }

    #[test]
    fn test_block_cache_remove() {
        let cache = BlockCache::new(1024 * 1024);
        let key = BlockCacheKey::new(1, 0);
        cache.insert(key, CachedBlock::new(vec![0; 100]));

        assert!(cache.remove(&key));
        assert!(cache.get(&key).is_none());
        assert!(!cache.remove(&key)); // Already removed
    }

    #[test]
    fn test_block_cache_invalidate_file() {
        let cache = BlockCache::new(1024 * 1024);

        // Insert blocks from two files
        for i in 0..5u64 {
            cache.insert(
                BlockCacheKey::new(1, i * 4096),
                CachedBlock::new(vec![0; 50]),
            );
            cache.insert(
                BlockCacheKey::new(2, i * 4096),
                CachedBlock::new(vec![0; 50]),
            );
        }
        assert_eq!(cache.len(), 10);

        // Invalidate file 1
        cache.invalidate_file(1);

        // File 1 blocks should be gone, file 2 blocks remain
        for i in 0..5u64 {
            assert!(cache.get(&BlockCacheKey::new(1, i * 4096)).is_none());
            assert!(cache.get(&BlockCacheKey::new(2, i * 4096)).is_some());
        }
    }

    #[test]
    fn test_block_cache_stats() {
        let cache = BlockCache::new(1024 * 1024);
        let key = BlockCacheKey::new(1, 0);

        cache.get(&key); // miss
        cache.insert(key, CachedBlock::new(vec![0; 100]));
        cache.get(&key); // hit
        cache.get(&key); // hit

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.hit_ratio(), 2.0 / 3.0);
    }

    #[test]
    fn test_block_cache_replace_existing() {
        let cache = BlockCache::new(1024 * 1024);
        let key = BlockCacheKey::new(1, 0);

        cache.insert(key, CachedBlock::new(vec![1; 100]));
        cache.insert(key, CachedBlock::new(vec![2; 200]));

        let block = cache.get(&key).unwrap();
        assert_eq!(block.data.len(), 200);
        assert_eq!(block.data[0], 2);
    }

    #[test]
    fn test_block_cache_sharding() {
        let cache = BlockCache::with_shards(1024 * 1024, 8);

        // Insert keys that should distribute across shards
        for i in 0..100u64 {
            cache.insert(
                BlockCacheKey::new(i, 0),
                CachedBlock::new(vec![0; 10]),
            );
        }
        assert_eq!(cache.len(), 100);

        // All should be retrievable
        for i in 0..100u64 {
            assert!(cache.get(&BlockCacheKey::new(i, 0)).is_some());
        }
    }
}
