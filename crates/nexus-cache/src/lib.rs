//! High-performance caching utilities for NexusDB.
//!
//! This crate provides caching implementations from basic to research-grade:
//!
//! - **LRU Cache**: Classic Least Recently Used with O(1) operations
//! - **LRU-K Cache**: Eviction based on K-th recent access (scan-resistant)
//! - **ARC Cache**: Adaptive Replacement Cache (recency + frequency)
//! - **W-TinyLFU Cache**: Best-in-class eviction (Caffeine-style)
//! - **Tiered Cache**: Multi-tier hot/warm/cold with automatic promotion
//! - **Query Plan Cache**: Compiled query plan reuse
//! - **Result Cache**: Query result caching with table invalidation
//! - **Semantic Cache**: AI agent query normalization + fingerprinting
//! - **Predictive Prefetch**: Markov chain access pattern prediction
//! - **LLM KV Cache**: Tensor-aware cache for RAG/LLM inference
//! - **Bloom Filter**: Probabilistic fast negative lookups
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
pub mod kv_cache;
pub mod lru;
pub mod lru_k;
pub mod plan_cache;
pub mod prefetch;
pub mod result_cache;
pub mod semantic;
pub mod stats;
pub mod tiered;
pub mod w_tinylfu;

pub use arc::ArcCache;
pub use bloom::BloomFilter;
pub use kv_cache::LlmKvCache;
pub use lru::LruCache;
pub use lru_k::LruKCache;
pub use plan_cache::PlanCache;
pub use prefetch::MarkovPrefetcher;
pub use result_cache::ResultCache;
pub use semantic::{normalize_sql, structural_fingerprint, SemanticKey};
pub use stats::CacheStats;
pub use tiered::TieredCache;
pub use w_tinylfu::WTinyLfuCache;

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
