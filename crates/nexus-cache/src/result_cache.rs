//! Query Result Cache for read-heavy workloads.
//!
//! This module provides caching for query results, which is useful for
//! workloads where the same queries are executed repeatedly and the
//! underlying data changes infrequently.
//!
//! The cache supports TTL-based expiration and manual invalidation when
//! tables are modified.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::lru::LruCache;
use crate::stats::CacheStats;

/// A cached query result entry.
#[derive(Debug, Clone)]
pub struct CachedResult<R> {
    /// The cached result.
    pub result: Arc<R>,
    /// When this result was cached.
    pub created_at: Instant,
    /// Tables that this result depends on.
    pub tables: HashSet<String>,
    /// Number of times this result has been used.
    pub use_count: u64,
}

impl<R> CachedResult<R> {
    /// Creates a new cached result.
    pub fn new(result: R, tables: HashSet<String>) -> Self {
        Self {
            result: Arc::new(result),
            created_at: Instant::now(),
            tables,
            use_count: 1,
        }
    }

    /// Returns the age of this cached result.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Records a use of this result.
    pub fn record_use(&mut self) {
        self.use_count += 1;
    }
}

/// Configuration for the result cache.
#[derive(Debug, Clone)]
pub struct ResultCacheConfig {
    /// Maximum number of results to cache.
    pub max_entries: usize,
    /// Default TTL for cached results in seconds.
    pub default_ttl_secs: u64,
    /// Maximum result size to cache (in bytes, 0 = no limit).
    pub max_result_size: usize,
    /// Whether to cache SELECT queries only.
    pub select_only: bool,
}

impl Default for ResultCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 500,
            default_ttl_secs: 60,         // 1 minute
            max_result_size: 1024 * 1024, // 1 MB
            select_only: true,
        }
    }
}

impl ResultCacheConfig {
    /// Creates a new config with the given capacity and TTL.
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        Self {
            max_entries,
            default_ttl_secs: ttl_secs,
            ..Default::default()
        }
    }
}

/// Cache key for query results.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ResultCacheKey {
    /// Hash of the normalized SQL.
    sql_hash: u64,
    /// Hash of the parameter values.
    params_hash: u64,
}

impl ResultCacheKey {
    /// Creates a new cache key.
    pub fn new(sql: &str, params: &[impl Hash]) -> Self {
        use std::collections::hash_map::DefaultHasher;

        // Hash SQL
        let normalized = crate::plan_cache::normalize_sql(sql);
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        let sql_hash = hasher.finish();

        // Hash params
        let mut hasher = DefaultHasher::new();
        for param in params {
            param.hash(&mut hasher);
        }
        let params_hash = hasher.finish();

        Self {
            sql_hash,
            params_hash,
        }
    }

    /// Creates a key from just SQL (no parameters).
    pub fn from_sql(sql: &str) -> Self {
        Self::new(sql, &[] as &[u8])
    }
}

/// A cache for query results.
///
/// Stores query results keyed by SQL + parameters. Results are automatically
/// expired based on TTL and can be manually invalidated when tables are
/// modified.
///
/// # Example
///
/// ```ignore
/// use nexus_cache::result_cache::{ResultCache, ResultCacheConfig, ResultCacheKey};
///
/// let cache: ResultCache<Vec<Row>> = ResultCache::new(ResultCacheConfig::default());
///
/// let key = ResultCacheKey::from_sql("SELECT * FROM users WHERE status = 'active'");
///
/// if let Some(result) = cache.get(&key) {
///     // Use cached result
/// } else {
///     let result = execute_query(sql);
///     cache.insert(key, result, vec!["users".to_string()]);
/// }
///
/// // When users table is modified:
/// cache.invalidate_table("users");
/// ```
pub struct ResultCache<R> {
    /// Configuration.
    config: ResultCacheConfig,
    /// The underlying cache.
    cache: RwLock<LruCache<ResultCacheKey, CachedResult<R>>>,
    /// Map from table name to cache keys that depend on it.
    table_index: RwLock<std::collections::HashMap<String, HashSet<ResultCacheKey>>>,
}

impl<R: Clone> ResultCache<R> {
    /// Creates a new result cache.
    pub fn new(config: ResultCacheConfig) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(config.max_entries)),
            table_index: RwLock::new(std::collections::HashMap::new()),
            config,
        }
    }

    /// Gets a cached result.
    pub fn get(&self, key: &ResultCacheKey) -> Option<Arc<R>> {
        let mut cache = self.cache.write();

        if let Some(entry) = cache.get_mut(key) {
            // Check TTL
            if self.config.default_ttl_secs > 0
                && entry.age().as_secs() > self.config.default_ttl_secs
            {
                // Expired, remove it
                let tables = entry.tables.clone();
                cache.remove(key);
                self.remove_from_table_index(key, &tables);
                return None;
            }

            entry.record_use();
            Some(Arc::clone(&entry.result))
        } else {
            None
        }
    }

    /// Inserts a result into the cache.
    pub fn insert(&self, key: ResultCacheKey, result: R, tables: Vec<String>) {
        let table_set: HashSet<String> = tables.into_iter().collect();
        let entry = CachedResult::new(result, table_set.clone());

        // Add to table index
        {
            let mut index = self.table_index.write();
            for table in &table_set {
                index
                    .entry(table.to_lowercase())
                    .or_default()
                    .insert(key.clone());
            }
        }

        self.cache.write().insert(key, entry);
    }

    /// Removes a result from the cache.
    pub fn remove(&self, key: &ResultCacheKey) {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.remove(key) {
            let tables = entry.tables;
            drop(cache);
            self.remove_from_table_index(key, &tables);
        }
    }

    /// Invalidates all cached results that depend on the given table.
    ///
    /// Call this when a table is modified (INSERT, UPDATE, DELETE, DDL).
    pub fn invalidate_table(&self, table_name: &str) {
        let table = table_name.to_lowercase();

        // Get all keys that depend on this table
        let keys: Vec<ResultCacheKey> = {
            let index = self.table_index.read();
            index
                .get(&table)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

        // Remove all affected entries
        let mut cache = self.cache.write();
        for key in &keys {
            if let Some(entry) = cache.remove(key) {
                drop(entry);
            }
        }
        drop(cache);

        // Remove from table index
        self.table_index.write().remove(&table);
    }

    /// Invalidates all cached results that depend on any of the given tables.
    pub fn invalidate_tables(&self, tables: &[&str]) {
        for table in tables {
            self.invalidate_table(table);
        }
    }

    /// Clears all cached results.
    pub fn clear(&self) {
        self.cache.write().clear();
        self.table_index.write().clear();
    }

    /// Returns the number of cached results.
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> CacheStats {
        self.cache.read().stats().clone()
    }

    /// Removes stale entries (expired based on TTL).
    ///
    /// Note: Due to LruCache API limitations, this is currently a no-op.
    /// Expired entries are checked and removed lazily on get().
    /// A production implementation would add iteration support to LruCache
    /// for periodic background eviction.
    pub fn evict_expired(&self) {
        // For now, this is a no-op since we check TTL on get()
        // Expired entries are lazily evicted when accessed
        // A future improvement would be to add iteration support to LruCache
        // for periodic background scanning and eviction
        //
        // Skip if TTL is disabled (default_ttl_secs == 0)
        let _ = self.config.default_ttl_secs;
    }

    /// Removes a key from the table index.
    fn remove_from_table_index(&self, key: &ResultCacheKey, tables: &HashSet<String>) {
        let mut index = self.table_index.write();
        for table in tables {
            if let Some(keys) = index.get_mut(&table.to_lowercase()) {
                keys.remove(key);
                if keys.is_empty() {
                    index.remove(&table.to_lowercase());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let cache: ResultCache<Vec<i32>> = ResultCache::new(ResultCacheConfig::default());

        let key = ResultCacheKey::from_sql("SELECT * FROM users");
        cache.insert(key.clone(), vec![1, 2, 3], vec!["users".to_string()]);

        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_key_with_params() {
        let key1 = ResultCacheKey::new("SELECT * FROM users WHERE id = $1", &[1i32]);
        let key2 = ResultCacheKey::new("SELECT * FROM users WHERE id = $1", &[2i32]);
        let key3 = ResultCacheKey::new("SELECT * FROM users WHERE id = $1", &[1i32]);

        assert_ne!(key1, key2); // Different param values
        assert_eq!(key1, key3); // Same SQL and params
    }

    #[test]
    fn test_invalidate_table() {
        let cache: ResultCache<String> = ResultCache::new(ResultCacheConfig::default());

        let key1 = ResultCacheKey::from_sql("SELECT * FROM users");
        let key2 = ResultCacheKey::from_sql("SELECT * FROM orders");

        cache.insert(
            key1.clone(),
            "result1".to_string(),
            vec!["users".to_string()],
        );
        cache.insert(
            key2.clone(),
            "result2".to_string(),
            vec!["orders".to_string()],
        );

        // Invalidate users table
        cache.invalidate_table("users");

        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_some());
    }

    #[test]
    fn test_multi_table_dependency() {
        let cache: ResultCache<String> = ResultCache::new(ResultCacheConfig::default());

        let key = ResultCacheKey::from_sql("SELECT * FROM users JOIN orders ON ...");
        cache.insert(
            key.clone(),
            "join result".to_string(),
            vec!["users".to_string(), "orders".to_string()],
        );

        // Invalidating either table should remove the result
        assert!(cache.get(&key).is_some());
        cache.invalidate_table("orders");
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_clear() {
        let cache: ResultCache<String> = ResultCache::new(ResultCacheConfig::default());

        let key = ResultCacheKey::from_sql("SELECT * FROM users");
        cache.insert(key.clone(), "result".to_string(), vec!["users".to_string()]);

        cache.clear();
        assert!(cache.is_empty());
    }
}
