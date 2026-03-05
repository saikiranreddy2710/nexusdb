//! Vector index management for NexusDB.
//!
//! Manages per-table HNSW indexes for VECTOR columns, bridging the
//! `nexus-hnsw` crate into the database engine layer.

use std::collections::HashMap;
use std::sync::Arc;

use nexus_hnsw::{DistanceMetric, HnswConfig, HnswIndex, HnswResult, SearchResult, VectorId};
use parking_lot::RwLock;

/// Key for identifying a vector index: (database, table, index_name).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct VectorIndexKey {
    pub database: String,
    pub table: String,
    pub index_name: String,
}

impl VectorIndexKey {
    pub fn new(
        database: impl Into<String>,
        table: impl Into<String>,
        index_name: impl Into<String>,
    ) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
            index_name: index_name.into(),
        }
    }
}

/// Manages all vector indexes across all databases.
pub struct VectorIndexManager {
    indexes: RwLock<HashMap<VectorIndexKey, Arc<HnswIndex>>>,
}

impl std::fmt::Debug for VectorIndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indexes = self.indexes.read();
        f.debug_struct("VectorIndexManager")
            .field("num_indexes", &indexes.len())
            .finish()
    }
}

impl VectorIndexManager {
    /// Creates a new empty manager.
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Creates a new HNSW index and registers it.
    pub fn create_index(
        &self,
        key: VectorIndexKey,
        dimensions: u32,
        metric: DistanceMetric,
        m: usize,
        ef_construction: usize,
    ) -> HnswResult<()> {
        let config = HnswConfig::new(dimensions, metric)
            .with_m(m)
            .with_ef_construction(ef_construction);
        let index = HnswIndex::new(config)?;
        self.indexes.write().insert(key, Arc::new(index));
        Ok(())
    }

    /// Drops a vector index.
    pub fn drop_index(&self, key: &VectorIndexKey) -> bool {
        self.indexes.write().remove(key).is_some()
    }

    /// Drops all indexes for a given table.
    pub fn drop_table_indexes(&self, database: &str, table: &str) {
        self.indexes
            .write()
            .retain(|k, _| !(k.database == database && k.table == table));
    }

    /// Drops all indexes for a given database.
    pub fn drop_database_indexes(&self, database: &str) {
        self.indexes.write().retain(|k, _| k.database != database);
    }

    /// Returns a reference to an index.
    pub fn get_index(&self, key: &VectorIndexKey) -> Option<Arc<HnswIndex>> {
        self.indexes.read().get(key).cloned()
    }

    /// Inserts a vector into the named index.
    pub fn insert(
        &self,
        key: &VectorIndexKey,
        vector_id: VectorId,
        vector: &[f32],
    ) -> HnswResult<()> {
        let indexes = self.indexes.read();
        let index = indexes
            .get(key)
            .ok_or(nexus_hnsw::HnswError::InvalidConfig(format!(
                "vector index not found: {}.{}.{}",
                key.database, key.table, key.index_name
            )))?;
        index.insert(vector_id, vector)
    }

    /// Searches the named index for k nearest neighbors.
    pub fn search(
        &self,
        key: &VectorIndexKey,
        query: &[f32],
        k: usize,
    ) -> HnswResult<Vec<SearchResult>> {
        let indexes = self.indexes.read();
        let index = indexes
            .get(key)
            .ok_or(nexus_hnsw::HnswError::InvalidConfig(format!(
                "vector index not found: {}.{}.{}",
                key.database, key.table, key.index_name
            )))?;
        index.search(query, k)
    }

    /// Deletes a vector from the named index.
    pub fn delete(&self, key: &VectorIndexKey, vector_id: VectorId) -> HnswResult<()> {
        let indexes = self.indexes.read();
        let index = indexes
            .get(key)
            .ok_or(nexus_hnsw::HnswError::InvalidConfig(format!(
                "vector index not found: {}.{}.{}",
                key.database, key.table, key.index_name
            )))?;
        index.delete(vector_id)
    }

    /// Returns the number of registered indexes.
    pub fn num_indexes(&self) -> usize {
        self.indexes.read().len()
    }

    /// Lists all index keys for a given database.
    pub fn list_indexes(&self, database: &str) -> Vec<VectorIndexKey> {
        self.indexes
            .read()
            .keys()
            .filter(|k| k.database == database)
            .cloned()
            .collect()
    }

    /// Lists all index keys for a given table.
    pub fn list_table_indexes(&self, database: &str, table: &str) -> Vec<VectorIndexKey> {
        self.indexes
            .read()
            .keys()
            .filter(|k| k.database == database && k.table == table)
            .cloned()
            .collect()
    }
}

impl Default for VectorIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses a distance metric name from a SQL string.
pub fn parse_metric(s: &str) -> Option<DistanceMetric> {
    match s.to_lowercase().as_str() {
        "l2" | "euclidean" => Some(DistanceMetric::L2),
        "cosine" => Some(DistanceMetric::Cosine),
        "inner_product" | "ip" => Some(DistanceMetric::InnerProduct),
        "manhattan" | "l1" => Some(DistanceMetric::Manhattan),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> VectorIndexKey {
        VectorIndexKey::new("testdb", "embeddings", "idx_embedding")
    }

    #[test]
    fn test_create_and_search() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(test_key(), 3, DistanceMetric::L2, 8, 32)
            .unwrap();

        assert_eq!(mgr.num_indexes(), 1);

        // Insert vectors
        mgr.insert(&test_key(), 1, &[1.0, 0.0, 0.0]).unwrap();
        mgr.insert(&test_key(), 2, &[0.0, 1.0, 0.0]).unwrap();
        mgr.insert(&test_key(), 3, &[0.0, 0.0, 1.0]).unwrap();

        // Search
        let results = mgr.search(&test_key(), &[0.9, 0.1, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 1); // closest to [1,0,0]
    }

    #[test]
    fn test_drop_index() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(test_key(), 3, DistanceMetric::L2, 8, 32)
            .unwrap();
        assert!(mgr.drop_index(&test_key()));
        assert_eq!(mgr.num_indexes(), 0);
        assert!(!mgr.drop_index(&test_key())); // already dropped
    }

    #[test]
    fn test_drop_table_indexes() {
        let mgr = VectorIndexManager::new();
        let k1 = VectorIndexKey::new("db", "t1", "idx1");
        let k2 = VectorIndexKey::new("db", "t1", "idx2");
        let k3 = VectorIndexKey::new("db", "t2", "idx3");
        mgr.create_index(k1, 3, DistanceMetric::L2, 8, 32).unwrap();
        mgr.create_index(k2, 3, DistanceMetric::L2, 8, 32).unwrap();
        mgr.create_index(k3, 3, DistanceMetric::L2, 8, 32).unwrap();

        mgr.drop_table_indexes("db", "t1");
        assert_eq!(mgr.num_indexes(), 1);
    }

    #[test]
    fn test_list_indexes() {
        let mgr = VectorIndexManager::new();
        let k1 = VectorIndexKey::new("db1", "t1", "idx1");
        let k2 = VectorIndexKey::new("db2", "t1", "idx2");
        mgr.create_index(k1, 3, DistanceMetric::L2, 8, 32).unwrap();
        mgr.create_index(k2, 3, DistanceMetric::L2, 8, 32).unwrap();

        assert_eq!(mgr.list_indexes("db1").len(), 1);
        assert_eq!(mgr.list_indexes("db2").len(), 1);
        assert_eq!(mgr.list_indexes("db3").len(), 0);
    }

    #[test]
    fn test_delete_vector() {
        let mgr = VectorIndexManager::new();
        mgr.create_index(test_key(), 2, DistanceMetric::L2, 8, 32)
            .unwrap();
        mgr.insert(&test_key(), 1, &[1.0, 0.0]).unwrap();
        mgr.insert(&test_key(), 2, &[0.0, 1.0]).unwrap();

        mgr.delete(&test_key(), 1).unwrap();

        let results = mgr.search(&test_key(), &[1.0, 0.0], 5).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 2);
    }

    #[test]
    fn test_parse_metric() {
        assert_eq!(parse_metric("l2"), Some(DistanceMetric::L2));
        assert_eq!(parse_metric("COSINE"), Some(DistanceMetric::Cosine));
        assert_eq!(parse_metric("ip"), Some(DistanceMetric::InnerProduct));
        assert_eq!(parse_metric("manhattan"), Some(DistanceMetric::Manhattan));
        assert_eq!(parse_metric("unknown"), None);
    }
}
