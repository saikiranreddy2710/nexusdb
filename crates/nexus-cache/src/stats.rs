//! Cache statistics for monitoring and debugging.

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for cache operations.
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Total number of cache accesses.
    accesses: AtomicU64,
    /// Number of cache hits.
    hits: AtomicU64,
    /// Number of cache misses.
    misses: AtomicU64,
    /// Number of cache insertions.
    inserts: AtomicU64,
    /// Number of cache evictions.
    evictions: AtomicU64,
    /// Number of cache removals.
    removals: AtomicU64,
}

impl CacheStats {
    /// Creates new statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an access.
    #[inline]
    pub fn record_access(&self) {
        self.accesses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cache hit.
    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cache miss.
    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an insertion.
    #[inline]
    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an eviction.
    #[inline]
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a removal.
    #[inline]
    pub fn record_removal(&self) {
        self.removals.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns total accesses.
    pub fn accesses(&self) -> u64 {
        self.accesses.load(Ordering::Relaxed)
    }

    /// Returns cache hits.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Returns cache misses.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Returns insertions.
    pub fn inserts(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
    }

    /// Returns evictions.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Returns removals.
    pub fn removals(&self) -> u64 {
        self.removals.load(Ordering::Relaxed)
    }

    /// Returns the hit ratio (0.0 to 1.0).
    pub fn hit_ratio(&self) -> f64 {
        let accesses = self.accesses();
        if accesses == 0 {
            0.0
        } else {
            self.hits() as f64 / accesses as f64
        }
    }

    /// Returns the miss ratio (0.0 to 1.0).
    pub fn miss_ratio(&self) -> f64 {
        1.0 - self.hit_ratio()
    }

    /// Resets all statistics.
    pub fn reset(&self) {
        self.accesses.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.removals.store(0, Ordering::Relaxed);
    }
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        Self {
            accesses: AtomicU64::new(self.accesses()),
            hits: AtomicU64::new(self.hits()),
            misses: AtomicU64::new(self.misses()),
            inserts: AtomicU64::new(self.inserts()),
            evictions: AtomicU64::new(self.evictions()),
            removals: AtomicU64::new(self.removals()),
        }
    }
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CacheStats {{ accesses: {}, hits: {}, misses: {}, hit_ratio: {:.2}%, inserts: {}, evictions: {} }}",
            self.accesses(),
            self.hits(),
            self.misses(),
            self.hit_ratio() * 100.0,
            self.inserts(),
            self.evictions()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_stats() {
        let stats = CacheStats::new();

        stats.record_access();
        stats.record_hit();
        stats.record_access();
        stats.record_miss();

        assert_eq!(stats.accesses(), 2);
        assert_eq!(stats.hits(), 1);
        assert_eq!(stats.misses(), 1);
        assert!((stats.hit_ratio() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_reset() {
        let stats = CacheStats::new();

        stats.record_access();
        stats.record_hit();
        stats.reset();

        assert_eq!(stats.accesses(), 0);
        assert_eq!(stats.hits(), 0);
    }

    #[test]
    fn test_clone() {
        let stats = CacheStats::new();
        stats.record_access();
        stats.record_hit();

        let cloned = stats.clone();
        assert_eq!(cloned.accesses(), 1);
        assert_eq!(cloned.hits(), 1);
    }
}
