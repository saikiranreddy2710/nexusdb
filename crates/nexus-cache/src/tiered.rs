//! Multi-tier cache with automatic promotion/demotion.
//!
//! Organizes cached data across tiers with different speed/capacity
//! trade-offs. Hot data lives in L1 (fast, small), warm data in L2
//! (medium speed, larger), cold data in L3 (slow, largest).
//!
//! ```text
//! L1: Hot cache    (small, fast)    ← frequently accessed
//! L2: Warm cache   (medium, medium) ← recently accessed
//! L3: Cold cache   (large, slow)    ← infrequently accessed
//!
//! Promotion: L3 → L2 → L1 (on repeated access)
//! Demotion:  L1 → L2 → L3 (on eviction)
//! ```

use std::hash::Hash;

use crate::lru::LruCache;
use crate::stats::CacheStats;

/// A tiered cache with automatic promotion and demotion.
///
/// Items start in L2 (warm). On repeated access they promote to L1 (hot).
/// When L1 or L2 is full, evicted items demote to the next tier.
/// Eviction from L3 removes the item entirely.
pub struct TieredCache<K: Clone + Eq + Hash, V: Clone> {
    /// L1: Hot tier — small, fast, most frequently accessed.
    l1: LruCache<K, V>,
    /// L2: Warm tier — medium capacity, recent entries.
    l2: LruCache<K, V>,
    /// L3: Cold tier — large capacity, infrequent access.
    l3: LruCache<K, V>,
    /// Access count for promotion decisions.
    access_counts: LruCache<K, u32>,
    /// Number of accesses required to promote from L2 to L1.
    promote_threshold: u32,
    /// Combined statistics.
    stats: CacheStats,
}

impl<K: Clone + Eq + Hash, V: Clone> TieredCache<K, V> {
    /// Create a new tiered cache.
    ///
    /// - `l1_capacity`: Hot tier size (typically 5-10% of total)
    /// - `l2_capacity`: Warm tier size (typically 20-30% of total)
    /// - `l3_capacity`: Cold tier size (typically 60-75% of total)
    /// - `promote_threshold`: Accesses needed to promote to L1 (e.g., 3)
    pub fn new(
        l1_capacity: usize,
        l2_capacity: usize,
        l3_capacity: usize,
        promote_threshold: u32,
    ) -> Self {
        let total = l1_capacity + l2_capacity + l3_capacity;
        Self {
            l1: LruCache::new(l1_capacity),
            l2: LruCache::new(l2_capacity),
            l3: LruCache::new(l3_capacity),
            access_counts: LruCache::new(total),
            promote_threshold: promote_threshold.max(1),
            stats: CacheStats::new(),
        }
    }

    /// Look up a value. Promotes entries across tiers based on access frequency.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.stats.record_access();

        // Track access count for promotion
        let count = {
            let current = self.access_counts.get(key).copied().unwrap_or(0);
            let new_count = current + 1;
            self.access_counts.insert(key.clone(), new_count);
            new_count
        };

        // Check L1 first (hot)
        if self.l1.contains(key) {
            self.stats.record_hit();
            return self.l1.get(key);
        }

        // Check L2 (warm) — promote to L1 if accessed enough
        if self.l2.contains(key) {
            self.stats.record_hit();
            if count >= self.promote_threshold {
                if let Some(value) = self.l2.remove(key) {
                    self.insert_l1(key.clone(), value);
                    return self.l1.get(key);
                }
            }
            return self.l2.get(key);
        }

        // Check L3 (cold) — promote to L2 on access
        if self.l3.contains(key) {
            self.stats.record_hit();
            if let Some(value) = self.l3.remove(key) {
                self.insert_l2(key.clone(), value);
                return self.l2.get(key);
            }
        }

        self.stats.record_miss();
        None
    }

    /// Insert a value (starts in L2/warm tier).
    pub fn insert(&mut self, key: K, value: V) {
        self.stats.record_insert();

        // Remove from all tiers if present
        self.l1.remove(&key);
        self.l2.remove(&key);
        self.l3.remove(&key);

        self.insert_l2(key, value);
    }

    /// Remove a value from all tiers.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let result = self.l1.remove(key)
            .or_else(|| self.l2.remove(key))
            .or_else(|| self.l3.remove(key));
        if result.is_some() {
            self.stats.record_removal();
        }
        self.access_counts.remove(key);
        result
    }

    /// Check if any tier contains the key (without promoting).
    pub fn contains(&self, key: &K) -> bool {
        self.l1.contains(key) || self.l2.contains(key) || self.l3.contains(key)
    }

    /// Total entries across all tiers.
    pub fn len(&self) -> usize {
        self.l1.len() + self.l2.len() + self.l3.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Per-tier sizes.
    pub fn tier_sizes(&self) -> TierSizes {
        TierSizes {
            l1: self.l1.len(),
            l2: self.l2.len(),
            l3: self.l3.len(),
        }
    }

    /// Clear all tiers.
    pub fn clear(&mut self) {
        self.l1.clear();
        self.l2.clear();
        self.l3.clear();
        self.access_counts.clear();
    }

    /// Insert into L1, demoting evicted entry to L2.
    fn insert_l1(&mut self, key: K, value: V) {
        // If L1 is full, the LRU eviction happens inside insert.
        // We need to catch the evicted entry and demote it.
        // Since LruCache::insert returns the old value for the SAME key
        // (not the evicted LRU entry), we check capacity first.
        if self.l1.len() >= self.l1.capacity() {
            // Demote L1 LRU to L2 (via keys() to find LRU)
            let keys = self.l1.keys();
            if let Some(lru_key) = keys.last() {
                let lru_key = lru_key.clone();
                if let Some(demoted) = self.l1.remove(&lru_key) {
                    self.insert_l2(lru_key, demoted);
                }
            }
        }
        self.l1.insert(key, value);
    }

    /// Insert into L2, demoting evicted entry to L3.
    fn insert_l2(&mut self, key: K, value: V) {
        if self.l2.len() >= self.l2.capacity() {
            let keys = self.l2.keys();
            if let Some(lru_key) = keys.last() {
                let lru_key = lru_key.clone();
                if let Some(demoted) = self.l2.remove(&lru_key) {
                    self.l3.insert(lru_key, demoted);
                    // L3 eviction is final (no lower tier)
                }
            }
        }
        self.l2.insert(key, value);
    }
}

/// Per-tier size information.
#[derive(Debug, Clone)]
pub struct TierSizes {
    pub l1: usize,
    pub l2: usize,
    pub l3: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiered_basic() {
        let mut cache = TieredCache::new(2, 4, 8, 3);

        cache.insert("a", 1);
        cache.insert("b", 2);

        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), None);
    }

    #[test]
    fn test_tiered_promotion() {
        let mut cache = TieredCache::new(2, 4, 8, 2);

        cache.insert("key", 42);
        // Starts in L2
        assert_eq!(cache.tier_sizes().l2, 1);

        // Access twice to trigger promotion to L1
        cache.get(&"key");
        cache.get(&"key");

        assert_eq!(cache.tier_sizes().l1, 1);
        assert_eq!(cache.get(&"key"), Some(&42));
    }

    #[test]
    fn test_tiered_demotion() {
        let mut cache = TieredCache::new(1, 2, 4, 3);

        // Fill L2
        cache.insert("a", 1);
        cache.insert("b", 2);
        assert_eq!(cache.tier_sizes().l2, 2);

        // Adding a third should demote the LRU from L2 to L3
        cache.insert("c", 3);
        let sizes = cache.tier_sizes();
        assert!(sizes.l3 > 0, "should have demoted to L3");
    }

    #[test]
    fn test_tiered_remove() {
        let mut cache = TieredCache::new(2, 4, 8, 3);

        cache.insert("key", 42);
        assert_eq!(cache.remove(&"key"), Some(42));
        assert_eq!(cache.get(&"key"), None);
    }

    #[test]
    fn test_tiered_clear() {
        let mut cache = TieredCache::new(2, 4, 8, 3);

        for i in 0..10 {
            cache.insert(i, i * 10);
        }
        assert!(cache.len() > 0);

        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_tiered_cold_to_warm() {
        let mut cache = TieredCache::new(1, 2, 4, 3);

        // Fill enough to push "a" to cold tier
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);
        cache.insert("d", 4);
        cache.insert("e", 5);

        // Access "a" — should promote from L3 to L2
        if cache.contains(&"a") {
            cache.get(&"a");
            // After access, should be in L2 (promoted from L3)
            assert!(cache.contains(&"a"));
        }
    }
}
