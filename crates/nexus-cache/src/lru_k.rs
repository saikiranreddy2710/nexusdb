//! LRU-K Cache implementation.
//!
//! LRU-K is an advanced page replacement algorithm that considers the
//! K-th most recent access time rather than just the most recent access.
//! This helps distinguish between frequently accessed pages and pages
//! that were accessed multiple times in a short burst.
//!
//! For K=2 (LRU-2), pages with a long time since their second-to-last
//! access are evicted first, even if they were accessed recently.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

use parking_lot::Mutex;

use crate::stats::CacheStats;

/// Default K value for LRU-K.
const DEFAULT_K: usize = 2;

/// Entry in the LRU-K cache.
struct Entry<V> {
    /// The cached value.
    value: V,
    /// History of last K access times.
    access_history: Vec<Instant>,
    /// Maximum history size (K).
    k: usize,
}

impl<V> Entry<V> {
    fn new(value: V, k: usize) -> Self {
        Self {
            value,
            access_history: vec![Instant::now()],
            k,
        }
    }

    /// Records an access to this entry.
    fn record_access(&mut self) {
        let now = Instant::now();
        self.access_history.push(now);
        // Keep only the last K accesses
        if self.access_history.len() > self.k {
            self.access_history.remove(0);
        }
    }

    /// Returns the K-th access time (or the oldest if fewer than K accesses).
    fn backward_k_distance(&self) -> Instant {
        // If we have K accesses, return the K-th most recent (oldest in history)
        // If we have fewer, return the oldest we have
        self.access_history
            .first()
            .copied()
            .unwrap_or_else(Instant::now)
    }

    /// Returns whether this entry has been accessed K times.
    fn has_k_accesses(&self) -> bool {
        self.access_history.len() >= self.k
    }
}

/// LRU-K cache that evicts based on K-th recent access time.
///
/// This algorithm provides better performance than LRU for workloads
/// with occasional scans, as it distinguishes between frequently
/// accessed items and items accessed in a short burst.
///
/// # Example
///
/// ```
/// use nexus_cache::lru_k::LruKCache;
///
/// let mut cache = LruKCache::new(100, 2); // LRU-2
/// cache.insert("key1", "value1");
/// cache.get(&"key1"); // Second access
/// // Now "key1" has full history and will be kept over items with fewer accesses
/// ```
pub struct LruKCache<K, V> {
    /// Maximum capacity.
    capacity: usize,
    /// K value for the algorithm.
    k: usize,
    /// Map from key to entry.
    entries: HashMap<K, Entry<V>>,
    /// Statistics.
    stats: CacheStats,
}

impl<K: Hash + Eq + Clone, V> LruKCache<K, V> {
    /// Creates a new LRU-K cache.
    pub fn new(capacity: usize, k: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            k: k.max(1),
            entries: HashMap::with_capacity(capacity),
            stats: CacheStats::new(),
        }
    }

    /// Creates a new LRU-2 cache (most common variant).
    pub fn lru2(capacity: usize) -> Self {
        Self::new(capacity, DEFAULT_K)
    }

    /// Returns the current number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the maximum capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Gets a reference to the value for the given key.
    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.stats.record_access();

        if let Some(entry) = self.entries.get_mut(key) {
            self.stats.record_hit();
            entry.record_access();
            Some(&entry.value)
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Gets a mutable reference to the value for the given key.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.stats.record_access();

        if let Some(entry) = self.entries.get_mut(key) {
            self.stats.record_hit();
            entry.record_access();
            Some(&mut entry.value)
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Checks if the cache contains the given key.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.entries.contains_key(key)
    }

    /// Inserts a key-value pair into the cache.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.stats.record_insert();

        // Check if key exists
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.record_access();
            return Some(std::mem::replace(&mut entry.value, value));
        }

        // Need to insert new entry
        if self.entries.len() >= self.capacity {
            self.evict();
        }

        self.entries.insert(key, Entry::new(value, self.k));
        None
    }

    /// Removes an entry from the cache.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.entries.remove(key).map(|e| e.value)
    }

    /// Clears all entries from the cache.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Evicts the entry with the oldest K-th access time.
    fn evict(&mut self) {
        if self.entries.is_empty() {
            return;
        }

        self.stats.record_eviction();

        // Find the entry with the oldest backward K-distance
        // Prefer evicting entries that haven't been accessed K times
        let victim_key = self
            .entries
            .iter()
            .min_by(|(_, a), (_, b)| {
                // Entries with fewer than K accesses are evicted first
                match (a.has_k_accesses(), b.has_k_accesses()) {
                    (false, true) => std::cmp::Ordering::Less,
                    (true, false) => std::cmp::Ordering::Greater,
                    _ => a.backward_k_distance().cmp(&b.backward_k_distance()),
                }
            })
            .map(|(k, _)| k.clone());

        if let Some(key) = victim_key {
            self.entries.remove(&key);
        }
    }
}

/// A thread-safe wrapper around LruKCache.
pub struct SyncLruKCache<K, V> {
    inner: Mutex<LruKCache<K, V>>,
}

impl<K: Hash + Eq + Clone, V: Clone> SyncLruKCache<K, V> {
    /// Creates a new synchronized LRU-K cache.
    pub fn new(capacity: usize, k: usize) -> Self {
        Self {
            inner: Mutex::new(LruKCache::new(capacity, k)),
        }
    }

    /// Creates a new synchronized LRU-2 cache.
    pub fn lru2(capacity: usize) -> Self {
        Self::new(capacity, 2)
    }

    /// Gets a value from the cache.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.lock().get(key).cloned()
    }

    /// Inserts a value into the cache.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.inner.lock().insert(key, value)
    }

    /// Removes a value from the cache.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.lock().remove(key)
    }

    /// Returns the current size.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Clears the cache.
    pub fn clear(&self) {
        self.inner.lock().clear();
    }

    /// Gets cache statistics.
    pub fn stats(&self) -> CacheStats {
        self.inner.lock().stats().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_basic_operations() {
        let mut cache = LruKCache::lru2(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_eviction_prefers_single_access() {
        let mut cache = LruKCache::lru2(2);

        cache.insert("a", 1);
        cache.get(&"a"); // Second access
        cache.insert("b", 2);
        // "a" has 2 accesses, "b" has 1 access

        cache.insert("c", 3); // Should evict "b"

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_eviction_by_backward_k_distance() {
        let mut cache = LruKCache::lru2(2);

        cache.insert("a", 1);
        cache.get(&"a"); // Second access for "a"

        sleep(Duration::from_millis(10));

        cache.insert("b", 2);
        cache.get(&"b"); // Second access for "b"

        // Both have 2 accesses, but "a" has older backward-k-distance
        cache.insert("c", 3); // Should evict "a"

        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_remove() {
        let mut cache = LruKCache::lru2(3);

        cache.insert("a", 1);
        cache.insert("b", 2);

        assert_eq!(cache.remove(&"a"), Some(1));
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_clear() {
        let mut cache = LruKCache::lru2(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.clear();

        assert!(cache.is_empty());
    }
}
