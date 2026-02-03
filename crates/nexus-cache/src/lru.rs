//! LRU (Least Recently Used) Cache implementation.
//!
//! This is a classic LRU cache with O(1) get, insert, and eviction operations.
//! It uses a HashMap for fast lookups and a doubly-linked list for ordering.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ptr::NonNull;

use parking_lot::Mutex;

use crate::stats::CacheStats;

/// A node in the LRU linked list.
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<NonNull<Node<K, V>>>,
    next: Option<NonNull<Node<K, V>>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

/// A thread-safe LRU cache with O(1) operations.
///
/// # Example
///
/// ```
/// use nexus_cache::lru::LruCache;
///
/// let mut cache = LruCache::new(2);
/// cache.insert("a", 1);
/// cache.insert("b", 2);
/// assert_eq!(cache.get(&"a"), Some(&1));
///
/// // Adding a third item evicts "b" (least recently used)
/// cache.insert("c", 3);
/// assert_eq!(cache.get(&"b"), None);
/// ```
pub struct LruCache<K, V> {
    /// Maximum capacity.
    capacity: usize,
    /// Map from key to node pointer.
    map: HashMap<K, NonNull<Node<K, V>>>,
    /// Head of the list (most recently used).
    head: Option<NonNull<Node<K, V>>>,
    /// Tail of the list (least recently used).
    tail: Option<NonNull<Node<K, V>>>,
    /// Statistics.
    stats: CacheStats,
}

// Safety: LruCache manages its own memory and can be sent between threads
unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for LruCache<K, V> {}

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
    /// Creates a new LRU cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::with_capacity(capacity),
            head: None,
            tail: None,
            stats: CacheStats::new(),
        }
    }

    /// Returns the current number of entries.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the maximum capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Gets a reference to the value for the given key.
    ///
    /// This marks the entry as recently used.
    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.stats.record_access();

        if let Some(&node_ptr) = self.map.get(key) {
            self.stats.record_hit();
            // Move to front
            self.move_to_front(node_ptr);
            // Safety: We just verified the pointer exists in the map
            Some(unsafe { &(*node_ptr.as_ptr()).value })
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Gets a mutable reference to the value for the given key.
    ///
    /// This marks the entry as recently used.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.stats.record_access();

        if let Some(&node_ptr) = self.map.get(key) {
            self.stats.record_hit();
            self.move_to_front(node_ptr);
            // Safety: We just verified the pointer exists in the map
            Some(unsafe { &mut (*node_ptr.as_ptr()).value })
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Checks if the cache contains the given key without updating recency.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains_key(key)
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, updates the value and returns the old value.
    /// If the cache is at capacity, evicts the least recently used entry.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.stats.record_insert();

        // Check if key exists
        if let Some(&node_ptr) = self.map.get(&key) {
            // Update existing entry
            self.move_to_front(node_ptr);
            // Safety: Pointer is valid from map
            let old_value = unsafe { std::mem::replace(&mut (*node_ptr.as_ptr()).value, value) };
            return Some(old_value);
        }

        // Need to insert new entry
        // First, check if we need to evict
        if self.map.len() >= self.capacity {
            self.evict_lru();
        }

        // Create new node
        let node = Box::new(Node::new(key.clone(), value));
        let node_ptr = NonNull::new(Box::into_raw(node)).unwrap();

        // Add to front
        self.push_front(node_ptr);
        self.map.insert(key, node_ptr);

        None
    }

    /// Removes an entry from the cache.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(node_ptr) = self.map.remove(key) {
            self.unlink(node_ptr);
            // Safety: We just removed it from the map, so we own it
            let node = unsafe { Box::from_raw(node_ptr.as_ptr()) };
            Some(node.value)
        } else {
            None
        }
    }

    /// Clears all entries from the cache.
    pub fn clear(&mut self) {
        // Drop all nodes
        let mut current = self.head;
        while let Some(node_ptr) = current {
            unsafe {
                current = (*node_ptr.as_ptr()).next;
                drop(Box::from_raw(node_ptr.as_ptr()));
            }
        }
        self.map.clear();
        self.head = None;
        self.tail = None;
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Resets statistics.
    pub fn reset_stats(&mut self) {
        self.stats.reset();
    }

    /// Moves a node to the front of the list.
    fn move_to_front(&mut self, node_ptr: NonNull<Node<K, V>>) {
        // If already at front, nothing to do
        if Some(node_ptr) == self.head {
            return;
        }

        self.unlink(node_ptr);
        self.push_front(node_ptr);
    }

    /// Pushes a node to the front of the list.
    fn push_front(&mut self, node_ptr: NonNull<Node<K, V>>) {
        unsafe {
            (*node_ptr.as_ptr()).prev = None;
            (*node_ptr.as_ptr()).next = self.head;

            if let Some(head) = self.head {
                (*head.as_ptr()).prev = Some(node_ptr);
            }

            self.head = Some(node_ptr);

            if self.tail.is_none() {
                self.tail = Some(node_ptr);
            }
        }
    }

    /// Unlinks a node from the list.
    fn unlink(&mut self, node_ptr: NonNull<Node<K, V>>) {
        unsafe {
            let prev = (*node_ptr.as_ptr()).prev;
            let next = (*node_ptr.as_ptr()).next;

            if let Some(prev) = prev {
                (*prev.as_ptr()).next = next;
            } else {
                self.head = next;
            }

            if let Some(next) = next {
                (*next.as_ptr()).prev = prev;
            } else {
                self.tail = prev;
            }
        }
    }

    /// Evicts the least recently used entry.
    fn evict_lru(&mut self) {
        if let Some(tail) = self.tail {
            self.stats.record_eviction();
            // Safety: tail is valid
            let key = unsafe { (*tail.as_ptr()).key.clone() };
            self.remove(&key);
        }
    }
}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        let mut current = self.head;
        while let Some(node_ptr) = current {
            unsafe {
                current = (*node_ptr.as_ptr()).next;
                drop(Box::from_raw(node_ptr.as_ptr()));
            }
        }
    }
}

/// A thread-safe wrapper around LruCache.
pub struct SyncLruCache<K, V> {
    inner: Mutex<LruCache<K, V>>,
}

impl<K: Hash + Eq + Clone, V: Clone> SyncLruCache<K, V> {
    /// Creates a new synchronized LRU cache.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
        }
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

    #[test]
    fn test_basic_operations() {
        let mut cache = LruCache::new(3);

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
        let mut cache = LruCache::new(2);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3); // This should evict "a"

        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_access_updates_recency() {
        let mut cache = LruCache::new(2);

        cache.insert("a", 1);
        cache.insert("b", 2);

        // Access "a" to make it more recent than "b"
        cache.get(&"a");

        // Insert "c", should evict "b" (least recently used)
        cache.insert("c", 3);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    #[test]
    fn test_update_existing() {
        let mut cache = LruCache::new(2);

        cache.insert("a", 1);
        let old = cache.insert("a", 10);

        assert_eq!(old, Some(1));
        assert_eq!(cache.get(&"a"), Some(&10));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_remove() {
        let mut cache = LruCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);

        assert_eq!(cache.remove(&"a"), Some(1));
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_clear() {
        let mut cache = LruCache::new(3);

        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.get(&"a"), None);
    }

    #[test]
    fn test_statistics() {
        let mut cache = LruCache::new(2);

        cache.insert("a", 1);
        cache.get(&"a"); // hit
        cache.get(&"b"); // miss
        cache.insert("b", 2);
        cache.insert("c", 3); // eviction

        let stats = cache.stats();
        assert_eq!(stats.hits(), 1);
        assert_eq!(stats.misses(), 1);
        assert_eq!(stats.evictions(), 1);
    }

    #[test]
    fn test_sync_cache() {
        let cache = SyncLruCache::new(2);

        cache.insert("a", 1);
        cache.insert("b", 2);

        assert_eq!(cache.get(&"a"), Some(1));
        assert_eq!(cache.get(&"c"), None);
    }
}
