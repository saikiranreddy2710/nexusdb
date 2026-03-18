//! Table catalog for metadata management.
//!
//! The catalog stores information about tables, their schemas, and indexes.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::logical::Schema;

use super::error::{StorageError, StorageResult};

/// Catalog file name for JSON persistence.
pub const CATALOG_FILE: &str = "catalog.json";

/// A stored default value expression for a column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefault {
    /// Column index.
    pub col_idx: usize,
    /// Default value (pre-evaluated to a Value for efficiency).
    pub value: crate::executor::Value,
}

/// A stored CHECK constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    /// The check expression (parsed AST).
    /// Not serialized — reconstructed from `sql` on load.
    #[serde(skip, default = "default_check_expr")]
    pub expr: crate::parser::Expr,
    /// Original SQL text of the CHECK expression (for persistence).
    #[serde(default)]
    pub sql: String,
}

/// Default placeholder expression used when deserializing CheckConstraints.
/// The real expression should be re-parsed from `sql` after loading.
fn default_check_expr() -> crate::parser::Expr {
    crate::parser::Expr::Literal(crate::parser::Literal::Boolean(true))
}

/// Information about a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Table schema.
    #[serde(
        serialize_with = "serialize_arc_schema",
        deserialize_with = "deserialize_arc_schema"
    )]
    pub schema: Arc<Schema>,
    /// Primary key column indices.
    pub primary_key: Vec<usize>,
    /// Index definitions.
    pub indexes: Vec<IndexInfo>,
    /// Row count estimate.
    pub row_count: u64,
    /// Table ID (for internal use).
    pub table_id: u64,
    /// Column indices that have a UNIQUE constraint (excluding PK).
    pub unique_columns: Vec<usize>,
    /// Default values for columns.
    pub defaults: Vec<ColumnDefault>,
    /// CHECK constraints.
    pub check_constraints: Vec<CheckConstraint>,
}

impl TableInfo {
    /// Creates a new table info.
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema: Arc::new(schema),
            primary_key: Vec::new(),
            indexes: Vec::new(),
            row_count: 0,
            table_id: 0,
            unique_columns: Vec::new(),
            defaults: Vec::new(),
            check_constraints: Vec::new(),
        }
    }

    /// Sets the primary key columns.
    pub fn with_primary_key(mut self, columns: Vec<usize>) -> Self {
        self.primary_key = columns;
        self
    }

    /// Adds an index.
    pub fn with_index(mut self, index: IndexInfo) -> Self {
        self.indexes.push(index);
        self
    }

    /// Sets the table ID.
    pub fn with_table_id(mut self, id: u64) -> Self {
        self.table_id = id;
        self
    }

    /// Returns the primary key column names.
    pub fn primary_key_columns(&self) -> Vec<&str> {
        self.primary_key
            .iter()
            .filter_map(|&i| self.schema.field(i).map(|f| f.name()))
            .collect()
    }

    /// Checks if the table has a primary key.
    pub fn has_primary_key(&self) -> bool {
        !self.primary_key.is_empty()
    }
}

/// The type of index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    /// Standard B-tree index.
    BTree,
    /// HNSW vector index.
    Hnsw {
        /// Distance metric name (l2, cosine, inner_product, manhattan).
        metric: String,
        /// Max connections per layer.
        m: usize,
        /// Construction candidate list size.
        ef_construction: usize,
    },
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::BTree
    }
}

/// Index information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Column indices in the index.
    pub columns: Vec<usize>,
    /// Whether the index is unique.
    pub unique: bool,
    /// Type of index (BTree or HNSW).
    pub index_type: IndexType,
}

impl IndexInfo {
    /// Creates a new BTree index.
    pub fn new(name: impl Into<String>, columns: Vec<usize>, unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            unique,
            index_type: IndexType::BTree,
        }
    }

    /// Creates a new HNSW vector index.
    pub fn hnsw(
        name: impl Into<String>,
        column: usize,
        metric: impl Into<String>,
        m: usize,
        ef_construction: usize,
    ) -> Self {
        Self {
            name: name.into(),
            columns: vec![column],
            unique: false,
            index_type: IndexType::Hnsw {
                metric: metric.into(),
                m,
                ef_construction,
            },
        }
    }
}

// =========================================================================
// Serde helpers
// =========================================================================

fn serialize_arc_schema<S>(schema: &Arc<Schema>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    schema.as_ref().serialize(serializer)
}

fn deserialize_arc_schema<'de, D>(deserializer: D) -> Result<Arc<Schema>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Schema::deserialize(deserializer).map(Arc::new)
}

/// Serializable snapshot of the catalog (for persistence).
#[derive(Debug, Serialize, Deserialize)]
struct CatalogSnapshot {
    tables: Vec<TableInfo>,
    next_table_id: u64,
}

/// Table catalog for managing table metadata.
#[derive(Debug)]
pub struct Catalog {
    /// Tables by name.
    tables: RwLock<HashMap<String, TableInfo>>,
    /// Next table ID.
    next_table_id: RwLock<u64>,
}

impl Catalog {
    /// Creates a new empty catalog.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(1),
        }
    }

    /// Serializes the catalog to JSON for persistence.
    pub fn to_json(&self) -> StorageResult<String> {
        let tables = self.tables.read().unwrap();
        let next_id = self.next_table_id.read().unwrap();
        let snapshot = CatalogSnapshot {
            tables: tables.values().cloned().collect(),
            next_table_id: *next_id,
        };
        serde_json::to_string_pretty(&snapshot).map_err(|e| {
            StorageError::InvalidOperation(format!("catalog serialization failed: {}", e))
        })
    }

    /// Restores a catalog from a JSON string.
    pub fn from_json(json: &str) -> StorageResult<Self> {
        let snapshot: CatalogSnapshot = serde_json::from_str(json).map_err(|e| {
            StorageError::InvalidOperation(format!("catalog deserialization failed: {}", e))
        })?;
        let mut table_map = HashMap::new();
        for info in snapshot.tables {
            table_map.insert(info.name.clone(), info);
        }
        Ok(Self {
            tables: RwLock::new(table_map),
            next_table_id: RwLock::new(snapshot.next_table_id),
        })
    }

    /// Returns all table infos (for recovery/enumeration).
    pub fn all_tables(&self) -> Vec<TableInfo> {
        let tables = self.tables.read().unwrap();
        tables.values().cloned().collect()
    }

    /// Creates a table.
    pub fn create_table(&self, mut info: TableInfo) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(&info.name) {
            return Err(StorageError::TableExists(info.name));
        }

        // Assign table ID
        let mut next_id = self.next_table_id.write().unwrap();
        info.table_id = *next_id;
        *next_id += 1;

        tables.insert(info.name.clone(), info);
        Ok(())
    }

    /// Drops a table.
    pub fn drop_table(&self, name: &str) -> StorageResult<TableInfo> {
        let mut tables = self.tables.write().unwrap();

        tables
            .remove(name)
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))
    }

    /// Gets table information.
    pub fn get_table(&self, name: &str) -> Option<TableInfo> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }

    /// Checks if a table exists.
    pub fn table_exists(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(name)
    }

    /// Lists all table names.
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    /// Returns the number of tables.
    pub fn table_count(&self) -> usize {
        let tables = self.tables.read().unwrap();
        tables.len()
    }

    /// Replaces the entire `TableInfo` for an existing table.
    ///
    /// Used by ALTER TABLE to update the schema while preserving table_id
    /// and other metadata.
    pub fn update_table(&self, info: TableInfo) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();
        if !tables.contains_key(&info.name) {
            return Err(StorageError::TableNotFound(info.name));
        }
        tables.insert(info.name.clone(), info);
        Ok(())
    }

    /// Updates the row count for a table.
    pub fn update_row_count(&self, name: &str, count: u64) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();

        if let Some(info) = tables.get_mut(name) {
            info.row_count = count;
            Ok(())
        } else {
            Err(StorageError::TableNotFound(name.to_string()))
        }
    }

    /// Increments the row count for a table.
    pub fn increment_row_count(&self, name: &str, delta: i64) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();

        if let Some(info) = tables.get_mut(name) {
            if delta >= 0 {
                info.row_count += delta as u64;
            } else {
                info.row_count = info.row_count.saturating_sub((-delta) as u64);
            }
            Ok(())
        } else {
            Err(StorageError::TableNotFound(name.to_string()))
        }
    }

    /// Adds an index to a table.
    pub fn add_index(&self, table_name: &str, index: IndexInfo) -> StorageResult<()> {
        let mut tables = self.tables.write().unwrap();

        let info = tables
            .get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        // Check for duplicate index name
        if info.indexes.iter().any(|i| i.name == index.name) {
            return Err(StorageError::InvalidOperation(format!(
                "index \"{}\" already exists on table \"{}\"",
                index.name, table_name
            )));
        }

        info.indexes.push(index);
        Ok(())
    }

    /// Drops an index by name (searches all tables). Returns true if found.
    pub fn drop_index(&self, index_name: &str) -> bool {
        let mut tables = self.tables.write().unwrap();

        for info in tables.values_mut() {
            let before = info.indexes.len();
            info.indexes.retain(|i| i.name != index_name);
            if info.indexes.len() < before {
                return true;
            }
        }
        false
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;
    use crate::parser::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
        ])
    }

    #[test]
    fn test_catalog_create_table() {
        let catalog = Catalog::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);

        catalog.create_table(info).unwrap();

        assert!(catalog.table_exists("users"));
        assert_eq!(catalog.table_count(), 1);

        let retrieved = catalog.get_table("users").unwrap();
        assert_eq!(retrieved.name, "users");
        assert_eq!(retrieved.primary_key, vec![0]);
    }

    #[test]
    fn test_catalog_duplicate_table() {
        let catalog = Catalog::new();

        let info = TableInfo::new("users", test_schema());
        catalog.create_table(info.clone()).unwrap();

        let result = catalog.create_table(info);
        assert!(matches!(result, Err(StorageError::TableExists(_))));
    }

    #[test]
    fn test_catalog_drop_table() {
        let catalog = Catalog::new();

        let info = TableInfo::new("users", test_schema());
        catalog.create_table(info).unwrap();

        assert!(catalog.table_exists("users"));

        catalog.drop_table("users").unwrap();

        assert!(!catalog.table_exists("users"));
    }

    #[test]
    fn test_catalog_list_tables() {
        let catalog = Catalog::new();

        catalog
            .create_table(TableInfo::new("users", test_schema()))
            .unwrap();
        catalog
            .create_table(TableInfo::new("orders", test_schema()))
            .unwrap();

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_catalog_row_count() {
        let catalog = Catalog::new();

        catalog
            .create_table(TableInfo::new("users", test_schema()))
            .unwrap();

        catalog.update_row_count("users", 100).unwrap();
        assert_eq!(catalog.get_table("users").unwrap().row_count, 100);

        catalog.increment_row_count("users", 10).unwrap();
        assert_eq!(catalog.get_table("users").unwrap().row_count, 110);

        catalog.increment_row_count("users", -20).unwrap();
        assert_eq!(catalog.get_table("users").unwrap().row_count, 90);
    }
}
