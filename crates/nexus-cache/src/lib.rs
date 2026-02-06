//! High-performance caching utilities for NexusDB.
//!
//! This crate provides various caching implementations optimized for
//! database workloads:
//!
//! - **LRU Cache**: Classic Least Recently Used cache with O(1) operations
//! - **LRU-K Cache**: Advanced eviction based on K-th recent access
//! - **ARC Cache**: Adaptive Replacement Cache that balances recency and frequency
//! - **Query Plan Cache**: Cache for parsed and optimized query plans
//! - **Bloom Filter**: Probabilistic data structure for fast negative lookups
//!
//! # Example
//!
//! ```rust
//! use nexus_cache::lru::LruCache;
//!
//! let mut cache = LruCache::new(100);
//! cache.insert("key1", "value1");
//! assert_eq!(cache.get(&"key1"), Some(&"value1"));
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod arc;
pub mod bloom;
pub mod lru;
pub mod lru_k;
pub mod plan_cache;
pub mod result_cache;
pub mod stats;

pub use arc::ArcCache;
pub use bloom::BloomFilter;
pub use lru::LruCache;
pub use lru_k::LruKCache;
pub use plan_cache::PlanCache;
pub use result_cache::ResultCache;
pub use stats::CacheStats;

/// Default capacity for caches when not specified.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Configuration for cache behavior.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries.
    pub capacity: usize,
    /// Time-to-live in seconds (0 = no TTL).
    pub ttl_secs: u64,
    /// Whether to collect statistics.
    pub enable_stats: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CAPACITY,
            ttl_secs: 0,
            enable_stats: true,
        }
    }
}

impl CacheConfig {
    /// Creates a new config with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }

    /// Sets the TTL in seconds.
    pub fn with_ttl(mut self, ttl_secs: u64) -> Self {
        self.ttl_secs = ttl_secs;
        self
    }

    /// Enables or disables statistics collection.
    pub fn with_stats(mut self, enable: bool) -> Self {
        self.enable_stats = enable;
        self
    }
}
