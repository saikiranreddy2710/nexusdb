//! LLM KV Cache management for RAG/inference workloads.
//!
//! Manages LLM key-value cache tensors as a database problem, inspired
//! by [AlayaDB](https://arxiv.org/abs/2504.10326). Decouples the KV cache
//! from LLM inference, enabling cache sharing across requests, persistence
//! to disk, and reduced GPU memory pressure.
//!
//! ## Storage Model
//!
//! Each cache entry represents one transformer layer's KV states for a
//! specific conversation/session:
//!
//! ```text
//! (session_id, layer_id) → (key_states: Vec<f32>, value_states: Vec<f32>)
//! ```
//!
//! ## Usage
//!
//! ```text
//! // Store KV cache after inference
//! kv_cache.store("session_1", 0, key_tensor, value_tensor);
//!
//! // Retrieve for continued inference
//! let (keys, values) = kv_cache.load("session_1", 0)?;
//! ```

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::RwLock;

use crate::lru::LruCache;
use crate::stats::CacheStats;

/// Key for a KV cache entry: (session_id, layer_id).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KvCacheKey {
    /// Session/conversation identifier.
    pub session_id: String,
    /// Transformer layer index (0..num_layers).
    pub layer_id: u32,
}

impl KvCacheKey {
    pub fn new(session_id: impl Into<String>, layer_id: u32) -> Self {
        Self {
            session_id: session_id.into(),
            layer_id,
        }
    }
}

/// A cached KV state for one transformer layer.
#[derive(Debug, Clone)]
pub struct KvCacheEntry {
    /// Key states tensor (flattened: [num_heads * head_dim * seq_len]).
    pub key_states: Vec<f32>,
    /// Value states tensor (flattened: same shape as key_states).
    pub value_states: Vec<f32>,
    /// Sequence length this cache covers.
    pub seq_len: usize,
    /// When this entry was last updated.
    pub updated_at: Instant,
}

impl KvCacheEntry {
    pub fn new(key_states: Vec<f32>, value_states: Vec<f32>, seq_len: usize) -> Self {
        Self {
            key_states,
            value_states,
            seq_len,
            updated_at: Instant::now(),
        }
    }

    /// Memory footprint in bytes.
    pub fn size_bytes(&self) -> usize {
        (self.key_states.len() + self.value_states.len()) * std::mem::size_of::<f32>()
    }
}

/// Configuration for the LLM KV cache.
#[derive(Debug, Clone)]
pub struct LlmKvCacheConfig {
    /// Maximum number of cached entries (session_id * layer pairs).
    pub max_entries: usize,
    /// Maximum total memory in bytes for cached tensors.
    pub max_memory_bytes: usize,
    /// Maximum number of sessions to track.
    pub max_sessions: usize,
}

impl Default for LlmKvCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            max_memory_bytes: 1024 * 1024 * 1024, // 1 GB
            max_sessions: 1_000,
        }
    }
}

/// LLM KV Cache manager.
///
/// Thread-safe cache for transformer KV states, organized by session
/// and layer. Supports storing, loading, extending (for incremental
/// decoding), and per-session invalidation.
pub struct LlmKvCache {
    /// Cached entries with LRU eviction.
    cache: RwLock<LruCache<KvCacheKey, KvCacheEntry>>,
    /// Session metadata: session_id -> list of layer_ids cached.
    sessions: RwLock<HashMap<String, Vec<u32>>>,
    /// Configuration.
    config: LlmKvCacheConfig,
    /// Current total memory usage in bytes.
    memory_usage: std::sync::atomic::AtomicUsize,
    /// Statistics.
    stats: CacheStats,
}

impl LlmKvCache {
    /// Create a new LLM KV cache.
    pub fn new(config: LlmKvCacheConfig) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(config.max_entries)),
            sessions: RwLock::new(HashMap::new()),
            config,
            memory_usage: std::sync::atomic::AtomicUsize::new(0),
            stats: CacheStats::new(),
        }
    }

    /// Store KV states for a session and layer.
    pub fn store(
        &self,
        session_id: &str,
        layer_id: u32,
        key_states: Vec<f32>,
        value_states: Vec<f32>,
        seq_len: usize,
    ) {
        let entry = KvCacheEntry::new(key_states, value_states, seq_len);
        let size = entry.size_bytes();
        let key = KvCacheKey::new(session_id, layer_id);

        self.stats.record_insert();

        // Track session layers
        {
            let mut sessions = self.sessions.write();
            let layers = sessions.entry(session_id.to_string()).or_default();
            if !layers.contains(&layer_id) {
                layers.push(layer_id);
            }
        }

        self.cache.write().insert(key, entry);
        self.memory_usage
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Load KV states for a session and layer.
    pub fn load(&self, session_id: &str, layer_id: u32) -> Option<KvCacheEntry> {
        self.stats.record_access();
        let key = KvCacheKey::new(session_id, layer_id);
        let result = self.cache.write().get(&key).cloned();
        if result.is_some() {
            self.stats.record_hit();
        } else {
            self.stats.record_miss();
        }
        result
    }

    /// Load all layers for a session.
    pub fn load_session(&self, session_id: &str) -> Vec<(u32, KvCacheEntry)> {
        let layers = {
            let sessions = self.sessions.read();
            sessions.get(session_id).cloned().unwrap_or_default()
        };

        let mut result = Vec::new();
        let mut cache = self.cache.write();
        for layer_id in layers {
            let key = KvCacheKey::new(session_id, layer_id);
            if let Some(entry) = cache.get(&key) {
                result.push((layer_id, entry.clone()));
            }
        }

        result.sort_by_key(|(layer_id, _)| *layer_id);
        result
    }

    /// Invalidate all cache entries for a session.
    pub fn invalidate_session(&self, session_id: &str) {
        let layers = {
            let mut sessions = self.sessions.write();
            sessions.remove(session_id).unwrap_or_default()
        };

        let mut cache = self.cache.write();
        for layer_id in layers {
            let key = KvCacheKey::new(session_id, layer_id);
            if let Some(entry) = cache.remove(&key) {
                self.memory_usage.fetch_sub(
                    entry.size_bytes(),
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Current memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of active sessions.
    pub fn active_sessions(&self) -> usize {
        self.sessions.read().len()
    }

    /// Statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        self.cache.write().clear();
        self.sessions.write().clear();
        self.memory_usage
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_cache_store_load() {
        let cache = LlmKvCache::new(LlmKvCacheConfig::default());

        let keys = vec![1.0f32; 128];
        let values = vec![2.0f32; 128];

        cache.store("session_1", 0, keys.clone(), values.clone(), 10);

        let entry = cache.load("session_1", 0).unwrap();
        assert_eq!(entry.key_states, keys);
        assert_eq!(entry.value_states, values);
        assert_eq!(entry.seq_len, 10);
    }

    #[test]
    fn test_kv_cache_session_load() {
        let cache = LlmKvCache::new(LlmKvCacheConfig::default());

        // Store 3 layers for a session
        for layer in 0..3 {
            cache.store(
                "sess_1",
                layer,
                vec![layer as f32; 64],
                vec![layer as f32; 64],
                5,
            );
        }

        let layers = cache.load_session("sess_1");
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0].0, 0); // Sorted by layer
        assert_eq!(layers[2].0, 2);
    }

    #[test]
    fn test_kv_cache_invalidate_session() {
        let cache = LlmKvCache::new(LlmKvCacheConfig::default());

        cache.store("sess_1", 0, vec![1.0; 64], vec![2.0; 64], 5);
        cache.store("sess_1", 1, vec![3.0; 64], vec![4.0; 64], 5);
        cache.store("sess_2", 0, vec![5.0; 64], vec![6.0; 64], 5);

        assert_eq!(cache.len(), 3);

        cache.invalidate_session("sess_1");
        assert_eq!(cache.len(), 1);
        assert!(cache.load("sess_1", 0).is_none());
        assert!(cache.load("sess_2", 0).is_some());
    }

    #[test]
    fn test_kv_cache_memory_tracking() {
        let cache = LlmKvCache::new(LlmKvCacheConfig::default());

        cache.store("s1", 0, vec![0.0; 256], vec![0.0; 256], 10);
        assert!(cache.memory_usage() > 0);

        cache.invalidate_session("s1");
        assert_eq!(cache.memory_usage(), 0);
    }

    #[test]
    fn test_kv_cache_stats() {
        let cache = LlmKvCache::new(LlmKvCacheConfig::default());

        cache.store("s1", 0, vec![0.0; 64], vec![0.0; 64], 5);
        cache.load("s1", 0); // hit
        cache.load("s1", 1); // miss

        assert_eq!(cache.stats().hits(), 1);
        assert_eq!(cache.stats().misses(), 1);
    }
}
