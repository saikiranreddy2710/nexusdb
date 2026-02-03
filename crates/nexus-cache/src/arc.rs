//! ARC (Adaptive Replacement Cache) implementation.
//!
//! ARC is a self-tuning cache that dynamically balances between recency
//! and frequency. It maintains two lists:
//! - T1: Recently accessed items (recency)
//! - T2: Frequently accessed items (frequency)
//!
//! And two ghost lists for tracking evicted items:
//! - B1: Ghost entries from T1
//! - B2: Ghost entries from T2
//!
//! The algorithm adapts the size of T1 and T2 based on workload patterns.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use parking_lot::Mutex;

use crate::stats::CacheStats;

/// ARC cache with adaptive balancing between recency and frequency.
///
/// # Example
///
/// ```
/// use nexus_cache::arc::ArcCache;
///
/// let mut cache = ArcCache::new(100);
/// cache.insert("key1", "value1");
/// cache.get(&"key1"); // Moves to T2 (frequently accessed)
/// ```
pub struct ArcCache<K, V> {
    /// Maximum capacity (c).
    capacity: usize,
    /// Target size for T1 (p).
    target_t1_size: usize,
    /// T1: Recently accessed items (LRU order).
    t1: VecDeque<K>,
    /// T2: Frequently accessed items (LRU order).
    t2: VecDeque<K>,
    /// B1: Ghost entries evicted from T1.
    b1: VecDeque<K>,
    /// B2: Ghost entries evicted from T2.
    b2: VecDeque<K>,
    /// Map from key to value and which list it's in.
    data: HashMap<K, (V, List)>,
    /// Statistics.
    stats: CacheStats,
}

/// Which list an entry is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum List {
    T1,
    T2,
}

impl<K: Hash + Eq + Clone, V> ArcCache<K, V> {
    /// Creates a new ARC cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            target_t1_size: 0,
            t1: VecDeque::new(),
            t2: VecDeque::new(),
            b1: VecDeque::new(),
            b2: VecDeque::new(),
            data: HashMap::with_capacity(capacity),
            stats: CacheStats::new(),
        }
    }

    /// Returns the current number of cached entries.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the maximum capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Gets a reference to the value for the given key.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.stats.record_access();

        if !self.data.contains_key(key) {
            self.stats.record_miss();
            return None;
        }

        self.stats.record_hit();
        let current_list = self.data.get(key).map(|(_, l)| *l)?;

        // Move to T2 if in T1 (promotes to frequently accessed)
        if current_list == List::T1 {
            self.move_to_t2(key);
        } else {
            // Already in T2, move to front (MRU position)
            self.move_to_front_t2(key);
        }

        self.data.get(key).map(|(v, _)| v)
    }

    /// Gets a mutable reference to the value for the given key.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.stats.record_access();

        if !self.data.contains_key(key) {
            self.stats.record_miss();
            return None;
        }

        self.stats.record_hit();
        let current_list = self.data.get(key).map(|(_, l)| *l)?;

        if current_list == List::T1 {
            self.move_to_t2(key);
        } else {
            self.move_to_front_t2(key);
        }

        self.data.get_mut(key).map(|(v, _)| v)
    }

    /// Checks if the cache contains the given key.
    pub fn contains(&self, key: &K) -> bool {
        self.data.contains_key(key)
    }

    /// Inserts a key-value pair into the cache.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.stats.record_insert();

        // Case 1: Key already in cache
        if self.data.contains_key(&key) {
            let current_list = self.data.get(&key).map(|(_, l)| *l).unwrap();
            let old = std::mem::replace(&mut self.data.get_mut(&key).unwrap().0, value);

            // Move to T2 if in T1
            if current_list == List::T1 {
                self.move_to_t2(&key);
            } else {
                self.move_to_front_t2(&key);
            }

            return Some(old);
        }

        // Case 2: Key in ghost list B1 (was recently evicted from T1)
        if self.b1.contains(&key) {
            // Increase target_t1_size
            let delta = self.adaptation_delta_b1();
            self.target_t1_size = (self.target_t1_size + delta).min(self.capacity);

            // Replace and add to T2
            self.replace(&key, false);
            self.remove_from_b1(&key);
            self.t2.push_front(key.clone());
            self.data.insert(key, (value, List::T2));
            return None;
        }

        // Case 3: Key in ghost list B2 (was recently evicted from T2)
        if self.b2.contains(&key) {
            // Decrease target_t1_size
            let delta = self.adaptation_delta_b2();
            self.target_t1_size = self.target_t1_size.saturating_sub(delta);

            // Replace and add to T2
            self.replace(&key, true);
            self.remove_from_b2(&key);
            self.t2.push_front(key.clone());
            self.data.insert(key, (value, List::T2));
            return None;
        }

        // Case 4: Key is completely new
        let l1_len = self.t1.len() + self.b1.len();
        if l1_len >= self.capacity {
            // L1 is full
            if self.t1.len() < self.capacity {
                // B1 is not empty, evict from B1
                self.b1.pop_back();
                self.replace(&key, false);
            } else {
                // T1 is full, evict from T1
                if let Some(evicted) = self.t1.pop_back() {
                    self.data.remove(&evicted);
                    self.stats.record_eviction();
                }
            }
        } else {
            let total = self.t1.len() + self.b1.len() + self.t2.len() + self.b2.len();
            if total >= self.capacity {
                if total >= 2 * self.capacity {
                    // B2 is not empty, evict from B2
                    self.b2.pop_back();
                }
                self.replace(&key, false);
            }
        }

        // Add to T1
        self.t1.push_front(key.clone());
        self.data.insert(key, (value, List::T1));
        None
    }

    /// Removes an entry from the cache.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some((value, list)) = self.data.remove(key) {
            match list {
                List::T1 => self.remove_from_t1(key),
                List::T2 => self.remove_from_t2(key),
            }
            Some(value)
        } else {
            None
        }
    }

    /// Clears all entries from the cache.
    pub fn clear(&mut self) {
        self.t1.clear();
        self.t2.clear();
        self.b1.clear();
        self.b2.clear();
        self.data.clear();
        self.target_t1_size = 0;
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Returns the current T1 size.
    pub fn t1_size(&self) -> usize {
        self.t1.len()
    }

    /// Returns the current T2 size.
    pub fn t2_size(&self) -> usize {
        self.t2.len()
    }

    /// Returns the target T1 size (adaptation parameter p).
    pub fn target_t1_size(&self) -> usize {
        self.target_t1_size
    }

    // Helper methods

    fn adaptation_delta_b1(&self) -> usize {
        let b1_len = self.b1.len().max(1);
        let b2_len = self.b2.len().max(1);
        if b1_len >= b2_len {
            1
        } else {
            (b2_len / b1_len).max(1)
        }
    }

    fn adaptation_delta_b2(&self) -> usize {
        let b1_len = self.b1.len().max(1);
        let b2_len = self.b2.len().max(1);
        if b2_len >= b1_len {
            1
        } else {
            (b1_len / b2_len).max(1)
        }
    }

    fn replace(&mut self, _key: &K, in_b2: bool) {
        let t1_len = self.t1.len();

        if t1_len > 0 && (t1_len > self.target_t1_size || (in_b2 && t1_len == self.target_t1_size))
        {
            // Evict from T1 to B1
            if let Some(evicted) = self.t1.pop_back() {
                self.data.remove(&evicted);
                self.b1.push_front(evicted);
                self.stats.record_eviction();
                // Trim B1 if needed
                while self.b1.len() > self.capacity {
                    self.b1.pop_back();
                }
            }
        } else {
            // Evict from T2 to B2
            if let Some(evicted) = self.t2.pop_back() {
                self.data.remove(&evicted);
                self.b2.push_front(evicted);
                self.stats.record_eviction();
                // Trim B2 if needed
                while self.b2.len() > self.capacity {
                    self.b2.pop_back();
                }
            }
        }
    }

    fn move_to_t2(&mut self, key: &K) {
        self.remove_from_t1(key);
        self.t2.push_front(key.clone());
        if let Some((_, list)) = self.data.get_mut(key) {
            *list = List::T2;
        }
    }

    fn move_to_front_t2(&mut self, key: &K) {
        self.remove_from_t2(key);
        self.t2.push_front(key.clone());
    }

    fn remove_from_t1(&mut self, key: &K) {
        self.t1.retain(|k| k != key);
    }

    fn remove_from_t2(&mut self, key: &K) {
        self.t2.retain(|k| k != key);
    }

    fn remove_from_b1(&mut self, key: &K) {
        self.b1.retain(|k| k != key);
    }

    fn remove_from_b2(&mut self, key: &K) {
        self.b2.retain(|k| k != key);
    }
}

/// A thread-safe wrapper around ArcCache.
pub struct SyncArcCache<K, V> {
    inner: Mutex<ArcCache<K, V>>,
}

impl<K: Hash + Eq + Clone, V: Clone> SyncArcCache<K, V> {
    /// Creates a new synchronized ARC cache.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(ArcCache::new(capacity)),
        }
    }

    /// Gets a value from the cache.
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.lock().get(key).cloned()
    }

    /// Inserts a value into the cache.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.inner.lock().insert(key, value)
    }

    /// Removes a value from the cache.
    pub fn remove(&self, key: &K) -> Option<V> {
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

    #[test]
    fn test_basic_operations() {
        let mut cache = ArcCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_eviction() {
        let mut cache = ArcCache::new(2);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3); // Should trigger eviction

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_promotion_to_t2() {
        let mut cache = ArcCache::new(3);

        cache.insert("a", 1); // Goes to T1
        assert_eq!(cache.t1_size(), 1);
        assert_eq!(cache.t2_size(), 0);

        cache.get(&"a"); // Promotes to T2
        assert_eq!(cache.t1_size(), 0);
        assert_eq!(cache.t2_size(), 1);
    }

    #[test]
    fn test_remove() {
        let mut cache = ArcCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);

        assert_eq!(cache.remove(&"a"), Some(1));
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_update_existing() {
        let mut cache = ArcCache::new(2);

        cache.insert("a", 1);
        let old = cache.insert("a", 10);

        assert_eq!(old, Some(1));
        assert_eq!(cache.get(&"a"), Some(&10));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_clear() {
        let mut cache = ArcCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.t1_size(), 0);
        assert_eq!(cache.t2_size(), 0);
    }
}
