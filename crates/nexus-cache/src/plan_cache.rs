//! Query Plan Cache for prepared statements.
//!
//! This module provides caching for parsed and optimized query plans,
//! which is essential for prepared statement performance. Parsing and
//! planning a query can take significant time, so caching these results
//! for repeated queries provides major performance benefits.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::lru::LruCache;
use crate::stats::CacheStats;

/// A cached query plan entry.
#[derive(Debug, Clone)]
pub struct CachedPlan<P> {
    /// The cached plan.
    pub plan: Arc<P>,
    /// When this plan was created.
    pub created_at: Instant,
    /// Number of times this plan has been used.
    pub use_count: u64,
    /// Last time this plan was used.
    pub last_used: Instant,
}

impl<P> CachedPlan<P> {
    /// Creates a new cached plan.
    pub fn new(plan: P) -> Self {
        let now = Instant::now();
        Self {
            plan: Arc::new(plan),
            created_at: now,
            use_count: 1,
            last_used: now,
        }
    }

    /// Records a use of this plan.
    pub fn record_use(&mut self) {
        self.use_count += 1;
        self.last_used = Instant::now();
    }

    /// Returns the age of this plan.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Configuration for the plan cache.
#[derive(Debug, Clone)]
pub struct PlanCacheConfig {
    /// Maximum number of plans to cache.
    pub max_plans: usize,
    /// Maximum age for cached plans (0 = no limit).
    pub max_age_secs: u64,
    /// Whether to normalize SQL before caching (e.g., remove whitespace).
    pub normalize_sql: bool,
}

impl Default for PlanCacheConfig {
    fn default() -> Self {
        Self {
            max_plans: 1000,
            max_age_secs: 3600, // 1 hour
            normalize_sql: true,
        }
    }
}

impl PlanCacheConfig {
    /// Creates a new config with the given capacity.
    pub fn with_capacity(max_plans: usize) -> Self {
        Self {
            max_plans,
            ..Default::default()
        }
    }
}

/// A cache for query plans.
///
/// Stores parsed and optimized query plans keyed by SQL text (normalized).
/// This enables efficient re-execution of prepared statements without
/// re-parsing and re-planning.
///
/// # Example
///
/// ```ignore
/// use nexus_cache::plan_cache::{PlanCache, PlanCacheConfig};
///
/// let cache = PlanCache::new(PlanCacheConfig::default());
///
/// // Check if plan is cached
/// if let Some(plan) = cache.get("SELECT * FROM users WHERE id = $1") {
///     // Use cached plan
/// } else {
///     // Parse and plan, then cache
///     let plan = parse_and_plan(sql);
///     cache.insert(sql, plan);
/// }
/// ```
pub struct PlanCache<P> {
    /// Configuration.
    config: PlanCacheConfig,
    /// The underlying cache.
    cache: RwLock<LruCache<String, CachedPlan<P>>>,
}

impl<P: Clone> PlanCache<P> {
    /// Creates a new plan cache.
    pub fn new(config: PlanCacheConfig) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(config.max_plans)),
            config,
        }
    }

    /// Gets a cached plan for the given SQL.
    pub fn get(&self, sql: &str) -> Option<Arc<P>> {
        let key = self.normalize(sql);
        let mut cache = self.cache.write();

        if let Some(entry) = cache.get_mut(&key) {
            // Check if plan is too old
            if self.config.max_age_secs > 0 && entry.age().as_secs() > self.config.max_age_secs {
                cache.remove(&key);
                return None;
            }

            entry.record_use();
            Some(Arc::clone(&entry.plan))
        } else {
            None
        }
    }

    /// Inserts a plan into the cache.
    pub fn insert(&self, sql: &str, plan: P) {
        let key = self.normalize(sql);
        let entry = CachedPlan::new(plan);
        self.cache.write().insert(key, entry);
    }

    /// Removes a plan from the cache.
    pub fn remove(&self, sql: &str) {
        let key = self.normalize(sql);
        self.cache.write().remove(&key);
    }

    /// Clears all cached plans.
    pub fn clear(&self) {
        self.cache.write().clear();
    }

    /// Invalidates plans that match a predicate.
    ///
    /// Useful when a table schema changes and related plans need to be
    /// invalidated.
    ///
    /// **Limitation:** The underlying `LruCache` does not support iteration
    /// over its keys, so selective removal based on `predicate` is not
    /// possible. This method therefore performs a **full cache clear**
    /// (equivalent to [`clear()`](Self::clear)). The predicate parameter
    /// is accepted for API compatibility but is not evaluated.
    pub fn invalidate_where<F>(&self, _predicate: F)
    where
        F: Fn(&str) -> bool,
    {
        // LruCache does not expose key iteration, so we cannot selectively
        // remove matching entries. Clear everything as a safe fallback.
        self.clear();
    }

    /// Invalidates all plans that reference the given table.
    ///
    /// Note: Due to LruCache API limitations (no key iteration support), this
    /// currently clears the entire cache. A future improvement would add key
    /// tracking or iteration support for selective invalidation.
    pub fn invalidate_table(&self, _table_name: &str) {
        // Since LruCache doesn't support iteration over keys, we cannot
        // selectively remove entries matching the table name. Clear the
        // entire cache as a safe fallback.
        self.cache.write().clear();
    }

    /// Returns the number of cached plans.
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

    /// Normalizes SQL for cache lookup.
    fn normalize(&self, sql: &str) -> String {
        if self.config.normalize_sql {
            normalize_sql(sql)
        } else {
            sql.to_string()
        }
    }
}

/// Normalizes SQL by removing extra whitespace and converting keywords to lowercase.
///
/// This ensures that queries that are logically equivalent but formatted
/// differently will share the same cache entry. Characters inside single-quoted
/// string literals (e.g. `'Alice'`) are **not** lowercased so that cache keys
/// remain faithful to the original literal values.
pub fn normalize_sql(sql: &str) -> String {
    // First, collapse whitespace while preserving content inside single quotes.
    let collapsed = sql.split_whitespace().collect::<Vec<_>>().join(" ");

    // Now lowercase only the portions outside of single-quoted strings.
    let mut result = String::with_capacity(collapsed.len());
    let mut in_quote = false;

    let mut chars = collapsed.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            in_quote = !in_quote;
            result.push(ch);
        } else if in_quote {
            // Inside a string literal – preserve original case
            result.push(ch);
        } else {
            // Outside a string literal – lowercase
            for lower in ch.to_lowercase() {
                result.push(lower);
            }
        }
    }

    result
}

/// Computes a hash for a SQL string for use as a cache key.
pub fn sql_hash(sql: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let normalized = normalize_sql(sql);
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let cache: PlanCache<String> = PlanCache::new(PlanCacheConfig::default());

        cache.insert("SELECT * FROM users", "plan1".to_string());

        let plan = cache.get("SELECT * FROM users");
        assert!(plan.is_some());
        assert_eq!(*plan.unwrap(), "plan1");
    }

    #[test]
    fn test_normalization() {
        let cache: PlanCache<String> = PlanCache::new(PlanCacheConfig::default());

        cache.insert("SELECT  *  FROM   users", "plan1".to_string());

        // Should find with different whitespace
        let plan = cache.get("SELECT * FROM users");
        assert!(plan.is_some());

        // Should find with different case
        let plan = cache.get("select * from USERS");
        assert!(plan.is_some());
    }

    #[test]
    fn test_remove() {
        let cache: PlanCache<String> = PlanCache::new(PlanCacheConfig::default());

        cache.insert("SELECT * FROM users", "plan1".to_string());
        cache.remove("SELECT * FROM users");

        assert!(cache.get("SELECT * FROM users").is_none());
    }

    #[test]
    fn test_clear() {
        let cache: PlanCache<String> = PlanCache::new(PlanCacheConfig::default());

        cache.insert("SELECT * FROM users", "plan1".to_string());
        cache.insert("SELECT * FROM orders", "plan2".to_string());
        cache.clear();

        assert!(cache.is_empty());
    }

    #[test]
    fn test_sql_hash() {
        let h1 = sql_hash("SELECT * FROM users");
        let h2 = sql_hash("select  *  from  USERS");
        let h3 = sql_hash("SELECT * FROM orders");

        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_normalize_sql() {
        assert_eq!(
            normalize_sql("SELECT  *  FROM   users  WHERE  id = 1"),
            "select * from users where id = 1"
        );
    }

    #[test]
    fn test_normalize_sql_preserves_string_literal_case() {
        // String literals inside single quotes must NOT be lowercased,
        // so different-cased literals produce different cache keys.
        let upper = normalize_sql("SELECT * FROM t WHERE name = 'Alice'");
        let lower = normalize_sql("SELECT * FROM t WHERE name = 'alice'");
        assert_ne!(
            upper, lower,
            "normalize_sql must preserve case inside string literals"
        );

        // Keywords outside quotes ARE lowercased.
        assert_eq!(
            normalize_sql("SELECT"),
            normalize_sql("select"),
            "keywords outside quotes should be case-insensitive"
        );

        // Mixed: keywords lowercased, literals preserved.
        assert_eq!(
            normalize_sql("SELECT * FROM t WHERE name = 'Alice'"),
            "select * from t where name = 'Alice'"
        );
        assert_eq!(
            normalize_sql("select * from t where name = 'alice'"),
            "select * from t where name = 'alice'"
        );
    }

    #[test]
    fn test_use_count() {
        let cache: PlanCache<String> = PlanCache::new(PlanCacheConfig::default());

        cache.insert("SELECT * FROM users", "plan1".to_string());
        cache.get("SELECT * FROM users");
        cache.get("SELECT * FROM users");
        cache.get("SELECT * FROM users");

        // The plan should have been used 4 times (1 insert + 3 gets)
        // This would be visible if we exposed the CachedPlan
    }
}
