//! Table catalog for metadata management.
//!
//! The catalog stores information about tables, their schemas, and indexes.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::logical::Schema;

use super::error::{StorageError, StorageResult};

/// Information about a table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: Arc<Schema>,
    /// Primary key column indices.
    pub primary_key: Vec<usize>,
    /// Index definitions.
    pub indexes: Vec<IndexInfo>,
    /// Row count estimate.
    pub row_count: u64,
    /// Table ID (for internal use).
    pub table_id: u64,
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

/// Index information.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Column indices in the index.
    pub columns: Vec<usize>,
    /// Whether the index is unique.
    pub unique: bool,
}

impl IndexInfo {
    /// Creates a new index.
    pub fn new(name: impl Into<String>, columns: Vec<usize>, unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            unique,
        }
    }
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
