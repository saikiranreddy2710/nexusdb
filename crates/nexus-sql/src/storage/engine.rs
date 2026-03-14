//! Storage engine for managing all tables.
//!
//! This module provides `StorageEngine`, the main entry point for storage operations.
//! It manages the table catalog and provides query execution methods.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::executor::{RecordBatch, Row, Value};
use crate::logical::Schema;

use super::catalog::{Catalog, TableInfo};
use super::encoder::EncodingFormat;
use super::error::{StorageError, StorageResult};
use super::table::TableStore;

/// Storage engine that manages all tables.
///
/// The `StorageEngine` is the main entry point for all storage operations.
/// It maintains the catalog and provides methods for DDL (CREATE/DROP TABLE)
/// and DML (INSERT/SELECT/UPDATE/DELETE) operations.
///
/// When `data_dir` is set, tables are backed by [`FilePager`] on disk and
/// the catalog is persisted as JSON. When `data_dir` is `None`, everything
/// is in-memory and lost on restart.
#[derive(Debug)]
pub struct StorageEngine {
    /// Table catalog.
    catalog: Catalog,
    /// Table stores by name.
    tables: RwLock<HashMap<String, Arc<TableStore>>>,
    /// Default encoding format.
    encoding_format: EncodingFormat,
    /// Data directory for persistence (None = in-memory only).
    data_dir: Option<PathBuf>,
}

/// Name of the catalog file inside the data directory.
const CATALOG_FILE: &str = "catalog.json";

impl StorageEngine {
    /// Creates a new in-memory storage engine.
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            tables: RwLock::new(HashMap::new()),
            encoding_format: EncodingFormat::Binary,
            data_dir: None,
        }
    }

    /// Creates a disk-backed storage engine rooted at `data_dir`.
    ///
    /// If the directory contains a `catalog.json` file from a previous run,
    /// it is loaded and every table's `FilePager` is opened, recovering data
    /// that was flushed before the last shutdown.
    pub fn with_data_dir(data_dir: impl Into<PathBuf>) -> StorageResult<Self> {
        let data_dir = data_dir.into();

        // Ensure the tables subdirectory exists
        let tables_dir = data_dir.join("tables");
        std::fs::create_dir_all(&tables_dir).map_err(|e| {
            StorageError::Internal(format!(
                "failed to create tables directory {:?}: {}",
                tables_dir, e
            ))
        })?;

        let mut engine = Self {
            catalog: Catalog::new(),
            tables: RwLock::new(HashMap::new()),
            encoding_format: EncodingFormat::Binary,
            data_dir: Some(data_dir.clone()),
        };

        // Recover catalog from disk if it exists
        let catalog_path = data_dir.join(CATALOG_FILE);
        if catalog_path.exists() {
            engine.recover_catalog(&catalog_path)?;
        }

        Ok(engine)
    }

    /// Returns the data directory, if any.
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// Recovers catalog and table stores from a catalog JSON file.
    fn recover_catalog(&mut self, catalog_path: &Path) -> StorageResult<()> {
        use nexus_storage::sagetree::{FilePager, SageTree, SageTreeConfig};

        let data = std::fs::read_to_string(catalog_path).map_err(|e| {
            StorageError::Internal(format!("failed to read catalog file: {}", e))
        })?;

        let table_infos: Vec<TableInfo> = serde_json::from_str(&data).map_err(|e| {
            StorageError::Internal(format!("failed to parse catalog file: {}", e))
        })?;

        let tables_dir = self.data_dir.as_ref().unwrap().join("tables");

        for info in table_infos {
            let table_file = tables_dir.join(format!("{}.db", info.name));

            let tree = if table_file.exists() {
                let pager = FilePager::open(&table_file).map_err(|e| {
                    StorageError::Internal(format!(
                        "failed to open table file {:?}: {}",
                        table_file, e
                    ))
                })?;
                SageTree::with_pager(SageTreeConfig::default(), Box::new(pager))
            } else {
                // Catalog references a table but the file doesn't exist —
                // create an empty file-backed table.
                let pager = FilePager::open(&table_file).map_err(|e| {
                    StorageError::Internal(format!(
                        "failed to create table file {:?}: {}",
                        table_file, e
                    ))
                })?;
                SageTree::with_pager(SageTreeConfig::default(), Box::new(pager))
            };

            let store = Arc::new(TableStore::with_tree(info.clone(), tree));

            // Register in catalog (skip ID assignment, preserve original IDs)
            self.catalog.create_table(info.clone())?;

            let mut tables = self.tables.write().unwrap();
            tables.insert(info.name.clone(), store);
        }

        Ok(())
    }

    /// Persists the catalog to disk (no-op for in-memory engines).
    pub fn persist_catalog(&self) -> StorageResult<()> {
        let data_dir = match &self.data_dir {
            Some(d) => d,
            None => return Ok(()),
        };

        let catalog_path = data_dir.join(CATALOG_FILE);

        let table_names = self.catalog.list_tables();
        let mut table_infos = Vec::with_capacity(table_names.len());
        for name in &table_names {
            if let Some(info) = self.catalog.get_table(name) {
                table_infos.push(info);
            }
        }

        let json = serde_json::to_string_pretty(&table_infos).map_err(|e| {
            StorageError::Internal(format!("failed to serialize catalog: {}", e))
        })?;

        std::fs::write(&catalog_path, json).map_err(|e| {
            StorageError::Internal(format!("failed to write catalog file: {}", e))
        })?;

        Ok(())
    }

    /// Flushes all table stores to disk and persists the catalog.
    pub fn flush_all(&self) -> StorageResult<()> {
        let tables = self.tables.read().unwrap();
        for store in tables.values() {
            store.flush()?;
        }
        drop(tables);
        self.persist_catalog()
    }

    /// Sets the default encoding format for new tables.
    pub fn with_encoding_format(mut self, format: EncodingFormat) -> Self {
        self.encoding_format = format;
        self
    }

    /// Returns a reference to the catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    // =========================================================================
    // DDL Operations
    // =========================================================================

    /// Creates a new table.
    ///
    /// If `data_dir` is set, the table is backed by a [`FilePager`] on disk.
    pub fn create_table(&self, info: TableInfo) -> StorageResult<()> {
        // Register in catalog
        self.catalog.create_table(info.clone())?;

        // Create table store — file-backed if data_dir is set
        let store = if let Some(ref data_dir) = self.data_dir {
            use nexus_storage::sagetree::{FilePager, SageTree, SageTreeConfig};

            let tables_dir = data_dir.join("tables");
            let table_file = tables_dir.join(format!("{}.db", info.name));
            let pager = FilePager::open(&table_file).map_err(|e| {
                StorageError::Internal(format!(
                    "failed to create table file {:?}: {}",
                    table_file, e
                ))
            })?;
            let tree = SageTree::with_pager(SageTreeConfig::default(), Box::new(pager));
            Arc::new(TableStore::with_tree(info.clone(), tree))
        } else {
            Arc::new(TableStore::new(info.clone()))
        };

        // Add to tables map
        let mut tables = self.tables.write().unwrap();
        tables.insert(info.name.clone(), store);

        // Persist catalog to disk
        self.persist_catalog()?;

        Ok(())
    }

    /// Creates a table from schema with optional primary key.
    pub fn create_table_from_schema(
        &self,
        name: impl Into<String>,
        schema: Schema,
        primary_key: Option<Vec<usize>>,
    ) -> StorageResult<()> {
        let mut info = TableInfo::new(name, schema);
        if let Some(pk) = primary_key {
            info = info.with_primary_key(pk);
        }
        self.create_table(info)
    }

    /// Drops a table.
    ///
    /// If the table is file-backed, the `.db` file is also removed.
    pub fn drop_table(&self, name: &str) -> StorageResult<()> {
        // Remove from catalog
        self.catalog.drop_table(name)?;

        // Remove table store
        let mut tables = self.tables.write().unwrap();
        tables.remove(name);
        drop(tables);

        // Remove the data file if we are disk-backed
        if let Some(ref data_dir) = self.data_dir {
            let table_file = data_dir.join("tables").join(format!("{}.db", name));
            if table_file.exists() {
                let _ = std::fs::remove_file(&table_file);
            }
        }

        // Persist catalog to disk
        self.persist_catalog()?;

        Ok(())
    }

    /// Alters a table by replacing its schema and migrating existing rows.
    ///
    /// `row_mapper` transforms each old row into the new schema.  For ADD
    /// COLUMN it appends NULLs, for DROP COLUMN it removes a column, for
    /// RENAME COLUMN the data is unchanged (only the schema differs).
    ///
    /// The operation is atomic: the old `TableStore` is replaced by a new
    /// one in a single lock scope.
    pub fn alter_table(
        &self,
        name: &str,
        new_info: TableInfo,
        row_mapper: impl Fn(&Row) -> Row,
    ) -> StorageResult<()> {
        // 1. Read all rows from the old store
        let old_store = self.get_table_store(name)?;
        let old_rows = old_store.scan_all()?;

        // 2. Update catalog with new schema
        self.catalog.update_table(new_info.clone())?;

        // 3. Create new table store with the new schema (file-backed if data_dir is set)
        let new_store = if let Some(ref data_dir) = self.data_dir {
            use nexus_storage::sagetree::{FilePager, SageTree, SageTreeConfig};

            // Write to a temporary file, then swap
            let tables_dir = data_dir.join("tables");
            let table_file = tables_dir.join(format!("{}.db", name));
            // Remove old file so we start fresh
            let _ = std::fs::remove_file(&table_file);
            let pager = FilePager::open(&table_file).map_err(|e| {
                StorageError::Internal(format!(
                    "failed to create table file {:?}: {}",
                    table_file, e
                ))
            })?;
            let tree = SageTree::with_pager(SageTreeConfig::default(), Box::new(pager));
            Arc::new(TableStore::with_tree(new_info, tree))
        } else {
            Arc::new(TableStore::new(new_info))
        };

        // 4. Re-insert rows mapped to the new schema
        for old_row in &old_rows {
            let new_row = row_mapper(old_row);
            new_store.insert(new_row)?;
        }

        // 5. Atomically swap the store
        let mut tables = self.tables.write().unwrap();
        tables.insert(name.to_string(), new_store);
        drop(tables);

        // Persist catalog to disk
        self.persist_catalog()?;

        Ok(())
    }

    /// Checks if a table exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.catalog.table_exists(name)
    }

    /// Gets table information.
    pub fn get_table_info(&self, name: &str) -> Option<TableInfo> {
        self.catalog.get_table(name)
    }

    /// Lists all table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    /// Gets a table store.
    pub fn get_table(&self, name: &str) -> Option<Arc<TableStore>> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }

    // =========================================================================
    // DML Operations - INSERT
    // =========================================================================

    /// Inserts rows into a table.
    ///
    /// Returns the number of rows inserted.
    pub fn execute_insert(&self, table: &str, rows: Vec<Row>) -> StorageResult<u64> {
        let store = self.get_table_store(table)?;
        let mut count = 0u64;

        for row in rows {
            store.insert(row)?;
            count += 1;
        }

        // Update row count in catalog
        self.catalog.increment_row_count(table, count as i64)?;

        Ok(count)
    }

    /// Inserts a single row into a table.
    pub fn insert_row(&self, table: &str, row: Row) -> StorageResult<()> {
        let store = self.get_table_store(table)?;
        store.insert(row)?;
        self.catalog.increment_row_count(table, 1)?;
        Ok(())
    }

    // =========================================================================
    // DML Operations - SELECT
    // =========================================================================

    /// Scans all rows from a table.
    pub fn execute_scan(&self, table: &str) -> StorageResult<Vec<RecordBatch>> {
        let store = self.get_table_store(table)?;
        let info = self
            .catalog
            .get_table(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        let rows = store.scan_all()?;

        if rows.is_empty() {
            return Ok(vec![RecordBatch::empty(info.schema.clone())]);
        }

        let batch = rows_to_record_batch(rows, info.schema)?;
        Ok(vec![batch])
    }

    /// Scans rows from a table with a limit.
    pub fn execute_scan_with_limit(
        &self,
        table: &str,
        limit: usize,
    ) -> StorageResult<Vec<RecordBatch>> {
        let store = self.get_table_store(table)?;
        let info = self
            .catalog
            .get_table(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        let rows = store.scan_with_limit(limit)?;

        if rows.is_empty() {
            return Ok(vec![RecordBatch::empty(info.schema.clone())]);
        }

        let batch = rows_to_record_batch(rows, info.schema)?;
        Ok(vec![batch])
    }

    /// Gets a row by primary key.
    pub fn get_by_key(&self, table: &str, key_values: &[Value]) -> StorageResult<Option<Row>> {
        let store = self.get_table_store(table)?;
        store.get(key_values)
    }

    // =========================================================================
    // DML Operations - UPDATE
    // =========================================================================

    /// Updates a row by primary key.
    pub fn execute_update(&self, table: &str, row: Row) -> StorageResult<bool> {
        let store = self.get_table_store(table)?;
        store.update(row)
    }

    /// Upserts a row (insert or update).
    pub fn execute_upsert(&self, table: &str, row: Row) -> StorageResult<()> {
        let store = self.get_table_store(table)?;
        store.upsert(row)
    }

    // =========================================================================
    // DML Operations - DELETE
    // =========================================================================

    /// Deletes a row by primary key.
    pub fn execute_delete(&self, table: &str, key_values: &[Value]) -> StorageResult<bool> {
        let store = self.get_table_store(table)?;
        let deleted = store.delete(key_values)?;

        if deleted {
            self.catalog.increment_row_count(table, -1)?;
        }

        Ok(deleted)
    }

    /// Deletes all rows from a table (TRUNCATE).
    pub fn truncate_table(&self, table: &str) -> StorageResult<u64> {
        let store = self.get_table_store(table)?;
        let count = store.row_count() as u64;
        store.clear();
        self.catalog.update_row_count(table, 0)?;
        Ok(count)
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    /// Gets a table store, returning an error if the table doesn't exist.
    fn get_table_store(&self, name: &str) -> StorageResult<Arc<TableStore>> {
        self.get_table(name)
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))
    }

    // =========================================================================
    // Maintenance
    // =========================================================================

    /// Consolidates all delta chains in all tables.
    pub fn consolidate_all(&self) {
        let tables = self.tables.read().unwrap();
        for store in tables.values() {
            store.consolidate();
        }
    }

    /// Returns statistics about the storage engine.
    pub fn stats(&self) -> StorageStats {
        let tables = self.tables.read().unwrap();
        StorageStats {
            table_count: tables.len(),
            total_rows: tables.values().map(|t| t.row_count()).sum(),
        }
    }
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage engine statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Number of tables.
    pub table_count: usize,
    /// Total number of rows across all tables.
    pub total_rows: usize,
}

/// Query session for executing statements.
///
/// A `QuerySession` wraps a reference to a `StorageEngine` and provides
/// a convenient interface for executing queries.
pub struct QuerySession {
    /// Storage engine reference.
    engine: Arc<StorageEngine>,
}

impl QuerySession {
    /// Creates a new query session.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Returns the storage engine.
    pub fn engine(&self) -> &StorageEngine {
        &self.engine
    }

    /// Creates a table.
    pub fn create_table(&self, info: TableInfo) -> StorageResult<()> {
        self.engine.create_table(info)
    }

    /// Drops a table.
    pub fn drop_table(&self, name: &str) -> StorageResult<()> {
        self.engine.drop_table(name)
    }

    /// Inserts rows into a table.
    pub fn insert(&self, table: &str, rows: Vec<Row>) -> StorageResult<u64> {
        self.engine.execute_insert(table, rows)
    }

    /// Scans a table.
    pub fn scan(&self, table: &str) -> StorageResult<Vec<RecordBatch>> {
        self.engine.execute_scan(table)
    }

    /// Gets a row by primary key.
    pub fn get(&self, table: &str, key: &[Value]) -> StorageResult<Option<Row>> {
        self.engine.get_by_key(table, key)
    }

    /// Updates a row.
    pub fn update(&self, table: &str, row: Row) -> StorageResult<bool> {
        self.engine.execute_update(table, row)
    }

    /// Deletes a row by primary key.
    pub fn delete(&self, table: &str, key: &[Value]) -> StorageResult<bool> {
        self.engine.execute_delete(table, key)
    }
}

/// Converts a vector of rows to a RecordBatch.
fn rows_to_record_batch(rows: Vec<Row>, schema: Arc<Schema>) -> StorageResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::empty(schema));
    }

    RecordBatch::from_rows(schema, &rows).map_err(|e| StorageError::Internal(e))
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
            Field::nullable("age", DataType::Int),
        ])
    }

    #[test]
    fn test_engine_create_table() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);

        engine.create_table(info).unwrap();

        assert!(engine.table_exists("users"));
        assert_eq!(engine.list_tables().len(), 1);
    }

    #[test]
    fn test_engine_drop_table() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        assert!(engine.table_exists("users"));

        engine.drop_table("users").unwrap();

        assert!(!engine.table_exists("users"));
    }

    #[test]
    fn test_engine_insert_and_scan() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::String("Alice".to_string()),
                Value::Int(30),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::String("Bob".to_string()),
                Value::Int(25),
            ]),
        ];

        let inserted = engine.execute_insert("users", rows).unwrap();
        assert_eq!(inserted, 2);

        let batches = engine.execute_scan("users").unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_engine_get_by_key() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        let row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);
        engine.insert_row("users", row).unwrap();

        let result = engine.get_by_key("users", &[Value::Int(1)]).unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.get(1), Some(&Value::String("Alice".to_string())));
    }

    #[test]
    fn test_engine_update() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        let row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);
        engine.insert_row("users", row).unwrap();

        let updated_row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice Smith".to_string()),
            Value::Int(31),
        ]);
        let updated = engine.execute_update("users", updated_row).unwrap();
        assert!(updated);

        let result = engine
            .get_by_key("users", &[Value::Int(1)])
            .unwrap()
            .unwrap();
        assert_eq!(
            result.get(1),
            Some(&Value::String("Alice Smith".to_string()))
        );
    }

    #[test]
    fn test_engine_delete() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        let row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);
        engine.insert_row("users", row).unwrap();

        let deleted = engine.execute_delete("users", &[Value::Int(1)]).unwrap();
        assert!(deleted);

        let result = engine.get_by_key("users", &[Value::Int(1)]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_engine_truncate() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        for i in 0..10 {
            let row = Row::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Int(20 + i),
            ]);
            engine.insert_row("users", row).unwrap();
        }

        let deleted = engine.truncate_table("users").unwrap();
        assert_eq!(deleted, 10);

        let batches = engine.execute_scan("users").unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_query_session() {
        let engine = Arc::new(StorageEngine::new());
        let session = QuerySession::new(engine);

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        session.create_table(info).unwrap();

        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ])];
        session.insert("users", rows).unwrap();

        let batches = session.scan("users").unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_engine_stats() {
        let engine = StorageEngine::new();

        let info = TableInfo::new("users", test_schema()).with_primary_key(vec![0]);
        engine.create_table(info).unwrap();

        for i in 0..5 {
            let row = Row::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Int(20 + i),
            ]);
            engine.insert_row("users", row).unwrap();
        }

        let stats = engine.stats();
        assert_eq!(stats.table_count, 1);
        assert_eq!(stats.total_rows, 5);
    }

    #[test]
    fn test_engine_table_not_found() {
        let engine = StorageEngine::new();

        let result = engine.execute_scan("nonexistent");
        assert!(matches!(result, Err(StorageError::TableNotFound(_))));
    }
}
