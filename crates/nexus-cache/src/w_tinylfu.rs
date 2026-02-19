//! W-TinyLFU: Window Tiny Least Frequently Used cache.
//!
//! A high-performance eviction policy combining frequency and recency,
//! used by Java's Caffeine (the best-in-class caching library).
//!
//! ## Architecture
//!
//! ```text
//! ┌───────────┐     ┌────────────────────────────────────────────┐
//! │  Window   │     │              Main Cache                    │
//! │  (1% cap) │────►│  Probation (20%)  │  Protected (80%)      │
//! │  LRU      │     │  LRU              │  LRU                  │
//! └───────────┘     └────────────────────────────────────────────┘
//!       │                     │
//!       └── admission ◄──────┘
//!            filter
//!       (CountMinSketch)
//! ```
//!
//! New entries go to the Window. When Window is full, the victim competes
//! with the Main probation victim via the TinyLFU admission filter. The
//! winner stays; the loser is evicted.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::stats::CacheStats;

/// Count-Min Sketch for approximate frequency counting.
///
/// Uses 4 hash functions and a fixed-size counter matrix to estimate
/// how many times each item has been seen. Space-efficient: uses
/// 4 * width * 4 bytes (typically ~64KB for millions of items).
struct CountMinSketch {
    /// Counter matrix: [num_hashes][width].
    counters: Vec<Vec<u8>>,
    /// Width of each row.
    width: usize,
    /// Number of hash functions (rows).
    depth: usize,
    /// Total additions (for periodic reset).
    additions: u64,
    /// Reset threshold (typically 10x the capacity).
    reset_threshold: u64,
}

impl CountMinSketch {
    fn new(capacity: usize) -> Self {
        let width = (capacity * 8).next_power_of_two().max(64);
        let depth = 4;
        Self {
            counters: vec![vec![0u8; width]; depth],
            width,
            depth,
            additions: 0,
            reset_threshold: capacity as u64 * 10,
        }
    }

    /// Increment the frequency count for an item.
    fn increment(&mut self, hash: u64) {
        for i in 0..self.depth {
            let idx = self.index(hash, i);
            self.counters[i][idx] = self.counters[i][idx].saturating_add(1);
        }
        self.additions += 1;

        // Periodic halving to adapt to changing access patterns
        if self.additions >= self.reset_threshold {
            self.halve();
        }
    }

    /// Estimate the frequency of an item.
    fn estimate(&self, hash: u64) -> u8 {
        let mut min = u8::MAX;
        for i in 0..self.depth {
            let idx = self.index(hash, i);
            min = min.min(self.counters[i][idx]);
        }
        min
    }

    /// Halve all counters (aging mechanism).
    fn halve(&mut self) {
        for row in &mut self.counters {
            for counter in row.iter_mut() {
                *counter >>= 1;
            }
        }
        self.additions /= 2;
    }

    /// Get the index for a given hash and row.
    fn index(&self, hash: u64, row: usize) -> usize {
        let h = hash.wrapping_add((row as u64).wrapping_mul(0x9E3779B97F4A7C15));
        (h as usize) % self.width
    }
}

/// A simple LRU deque for the window/probation/protected segments.
struct LruDeque<K: Clone + Eq + Hash> {
    /// Keys in LRU order (front = most recent, back = least recent).
    order: Vec<K>,
    capacity: usize,
}

impl<K: Clone + Eq + Hash> LruDeque<K> {
    fn new(capacity: usize) -> Self {
        Self {
            order: Vec::with_capacity(capacity),
            capacity: capacity.max(1),
        }
    }

    fn len(&self) -> usize {
        self.order.len()
    }

    fn is_full(&self) -> bool {
        self.order.len() >= self.capacity
    }

    /// Touch a key (move to front).
    fn touch(&mut self, key: &K) -> bool {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            let k = self.order.remove(pos);
            self.order.insert(0, k);
            true
        } else {
            false
        }
    }

    /// Push to front. Does NOT check capacity.
    fn push_front(&mut self, key: K) {
        self.order.insert(0, key);
    }

    /// Remove and return the LRU victim (back of deque).
    fn pop_back(&mut self) -> Option<K> {
        self.order.pop()
    }

    /// Remove a specific key.
    fn remove(&mut self, key: &K) -> bool {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
            true
        } else {
            false
        }
    }

    fn contains(&self, key: &K) -> bool {
        self.order.iter().any(|k| k == key)
    }
}

/// W-TinyLFU cache with frequency-based admission.
///
/// Provides near-optimal hit ratios by combining:
/// - **Window LRU**: captures burst/recency (1% of capacity)
/// - **Probation LRU**: new entries from window compete here (20%)
/// - **Protected LRU**: frequently accessed entries (80%)
/// - **CountMinSketch**: frequency filter for admission decisions
pub struct WTinyLfuCache<K: Clone + Eq + Hash, V> {
    /// Data storage.
    data: HashMap<K, V>,
    /// Total capacity.
    capacity: usize,
    /// Window segment (captures recency).
    window: LruDeque<K>,
    /// Main probation segment.
    probation: LruDeque<K>,
    /// Main protected segment.
    protected: LruDeque<K>,
    /// Frequency sketch for admission decisions.
    sketch: CountMinSketch,
    /// Statistics.
    stats: CacheStats,
}

impl<K: Clone + Eq + Hash, V> WTinyLfuCache<K, V> {
    /// Create a new W-TinyLFU cache.
    ///
    /// Segment sizes: window = 1%, probation = 20%, protected = 79%.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(4);
        let window_size = (capacity / 100).max(1);
        let protected_size = capacity * 79 / 100;
        let probation_size = capacity - window_size - protected_size;

        Self {
            data: HashMap::with_capacity(capacity),
            capacity,
            window: LruDeque::new(window_size),
            probation: LruDeque::new(probation_size.max(1)),
            protected: LruDeque::new(protected_size.max(1)),
            sketch: CountMinSketch::new(capacity),
            stats: CacheStats::new(),
        }
    }

    /// Get a value by key.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.stats.record_access();

        if !self.data.contains_key(key) {
            self.stats.record_miss();
            return None;
        }

        self.stats.record_hit();
        let key_hash = self.hash_key(key);
        self.sketch.increment(key_hash);

        // Promote from probation to protected on access
        if self.probation.remove(key) {
            if self.protected.is_full() {
                // Demote protected victim to probation
                if let Some(demoted) = self.protected.pop_back() {
                    self.probation.push_front(demoted);
                }
            }
            self.protected.push_front(key.clone());
        } else {
            // Touch in window or protected
            if !self.window.touch(key) {
                self.protected.touch(key);
            }
        }

        self.data.get(key)
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.stats.record_insert();
        let key_hash = self.hash_key(&key);
        self.sketch.increment(key_hash);

        // Update existing entry
        if self.data.contains_key(&key) {
            let old = self.data.insert(key.clone(), value);
            // Touch the key in whichever segment it's in
            if !self.window.touch(&key) {
                if !self.probation.remove(&key) {
                    self.protected.touch(&key);
                } else {
                    // Promote to protected on re-insert
                    if self.protected.is_full() {
                        if let Some(demoted) = self.protected.pop_back() {
                            self.probation.push_front(demoted);
                        }
                    }
                    self.protected.push_front(key);
                }
            }
            return old;
        }

        // New entry: add to window
        if self.window.is_full() {
            // Window victim competes with probation victim
            if let Some(window_victim) = self.window.pop_back() {
                self.admit_or_evict(window_victim);
            }
        }

        self.window.push_front(key.clone());
        self.data.insert(key, value);
        None
    }

    /// Remove an entry.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.window.remove(key);
        self.probation.remove(key);
        self.protected.remove(key);
        let result = self.data.remove(key);
        if result.is_some() {
            self.stats.record_removal();
        }
        result
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Cache capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.data.clear();
        self.window = LruDeque::new(self.window.capacity);
        self.probation = LruDeque::new(self.probation.capacity);
        self.protected = LruDeque::new(self.protected.capacity);
    }

    /// Admission decision: window victim vs probation victim.
    ///
    /// The candidate with higher estimated frequency wins admission
    /// to the probation segment. The loser is evicted entirely.
    fn admit_or_evict(&mut self, candidate: K) {
        if self.probation.is_full() {
            if let Some(victim) = self.probation.pop_back() {
                let candidate_freq = self.sketch.estimate(self.hash_key(&candidate));
                let victim_freq = self.sketch.estimate(self.hash_key(&victim));

                if candidate_freq > victim_freq {
                    // Candidate wins: evict victim, admit candidate
                    self.data.remove(&victim);
                    self.stats.record_eviction();
                    self.probation.push_front(candidate);
                } else {
                    // Victim wins: evict candidate, keep victim
                    self.data.remove(&candidate);
                    self.stats.record_eviction();
                    self.probation.push_front(victim);
                }
            }
        } else {
            // Probation has room: just add the candidate
            self.probation.push_front(candidate);
        }
    }

    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wtinylfu_basic() {
        let mut cache = WTinyLfuCache::new(100);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.get(&"d"), None);
    }

    #[test]
    fn test_wtinylfu_eviction() {
        let mut cache = WTinyLfuCache::new(10);

        // Fill cache
        for i in 0..20 {
            cache.insert(i, i * 10);
        }

        // Some entries should have been evicted
        assert!(cache.len() <= 10);
        let stats = cache.stats();
        assert!(stats.evictions() > 0);
    }

    #[test]
    fn test_wtinylfu_frequency_wins() {
        let mut cache: WTinyLfuCache<i32, i32> = WTinyLfuCache::new(10);

        // Insert and access key 999 many times to build frequency
        cache.insert(999, 1);
        for _ in 0..10 {
            cache.get(&999);
        }

        // Fill with cold entries
        for i in 0..20 {
            cache.insert(i, i);
        }

        // 999 should still be in cache due to high frequency
        assert!(cache.get(&999).is_some(), "hot key should survive eviction");
    }

    #[test]
    fn test_wtinylfu_update() {
        let mut cache = WTinyLfuCache::new(100);

        cache.insert("key", 1);
        assert_eq!(cache.get(&"key"), Some(&1));

        cache.insert("key", 2);
        assert_eq!(cache.get(&"key"), Some(&2));
    }

    #[test]
    fn test_wtinylfu_remove() {
        let mut cache = WTinyLfuCache::new(100);

        cache.insert("key", 1);
        assert_eq!(cache.remove(&"key"), Some(1));
        assert_eq!(cache.get(&"key"), None);
    }

    #[test]
    fn test_count_min_sketch() {
        let mut sketch = CountMinSketch::new(1000);

        sketch.increment(42);
        sketch.increment(42);
        sketch.increment(42);
        sketch.increment(99);

        assert!(sketch.estimate(42) >= 3);
        assert!(sketch.estimate(99) >= 1);
        assert_eq!(sketch.estimate(0), 0);
    }

    #[test]
    fn test_count_min_sketch_halving() {
        let mut sketch = CountMinSketch::new(10);
        sketch.reset_threshold = 5;

        for _ in 0..6 {
            sketch.increment(42);
        }

        // After halving, estimate should be lower
        assert!(sketch.estimate(42) < 6);
    }
}
