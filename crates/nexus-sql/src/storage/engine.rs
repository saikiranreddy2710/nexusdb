//! Storage engine for managing all tables.
//!
//! This module provides `StorageEngine`, the main entry point for storage operations.
//! It manages the table catalog and provides query execution methods.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::executor::{RecordBatch, Row, Value};
use crate::logical::Schema;

use super::catalog::{Catalog, TableInfo};
use super::encoder::EncodingFormat;
use super::error::{StorageError, StorageResult};
use super::table::TableStore;

/// Serializable catalog entry for disk persistence.
#[derive(Debug, Serialize, Deserialize)]
struct CatalogEntry {
    name: String,
    schema: Schema,
    primary_key: Vec<usize>,
    table_id: u64,
}

/// Storage engine that manages all tables.
///
/// The `StorageEngine` is the main entry point for all storage operations.
/// It maintains the catalog and provides methods for DDL (CREATE/DROP TABLE)
/// and DML (INSERT/SELECT/UPDATE/DELETE) operations.
#[derive(Debug)]
pub struct StorageEngine {
    /// Table catalog.
    catalog: Catalog,
    /// Table stores by name.
    tables: RwLock<HashMap<String, Arc<TableStore>>>,
    /// Default encoding format.
    encoding_format: EncodingFormat,
    /// Data directory for persistent storage. None = in-memory only.
    data_dir: Option<PathBuf>,
}

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

    /// Creates a new storage engine backed by the given data directory.
    ///
    /// Tables will be persisted as files under `data_dir/tables/<table_name>.db`.
    pub fn with_data_dir(data_dir: impl Into<PathBuf>) -> StorageResult<Self> {
        let data_dir = data_dir.into();
        let tables_dir = data_dir.join("tables");
        std::fs::create_dir_all(&tables_dir).map_err(|e| {
            StorageError::EngineError(format!(
                "failed to create tables directory {:?}: {}",
                tables_dir, e
            ))
        })?;

        let mut engine = Self {
            catalog: Catalog::new(),
            tables: RwLock::new(HashMap::new()),
            encoding_format: EncodingFormat::Binary,
            data_dir: Some(data_dir),
        };

        // Recover catalog from disk if available
        engine.recover_catalog()?;

        Ok(engine)
    }

    /// Returns the data directory, if configured.
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
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
    // Catalog Persistence
    // =========================================================================

    /// Path to the catalog metadata file.
    fn catalog_path(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|d| d.join("catalog.json"))
    }

    /// Persists catalog metadata to disk.
    fn persist_catalog(&self) -> StorageResult<()> {
        let path = match self.catalog_path() {
            Some(p) => p,
            None => return Ok(()), // No-op for in-memory mode
        };

        let entries: Vec<CatalogEntry> = self
            .catalog
            .list_tables()
            .iter()
            .filter_map(|name| {
                self.catalog.get_table(name).map(|info| CatalogEntry {
                    name: info.name.clone(),
                    schema: (*info.schema).clone(),
                    primary_key: info.primary_key.clone(),
                    table_id: info.table_id,
                })
            })
            .collect();

        let json = serde_json::to_string_pretty(&entries).map_err(|e| {
            StorageError::EngineError(format!("failed to serialize catalog: {}", e))
        })?;

        // Write atomically: write to temp file then rename
        let tmp_path = path.with_extension("json.tmp");
        std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| {
            StorageError::EngineError(format!("failed to write catalog: {}", e))
        })?;
        std::fs::rename(&tmp_path, &path).map_err(|e| {
            StorageError::EngineError(format!("failed to rename catalog file: {}", e))
        })?;

        Ok(())
    }

    /// Recovers catalog and table stores from disk.
    fn recover_catalog(&mut self) -> StorageResult<()> {
        let path = match self.catalog_path() {
            Some(p) => p,
            None => return Ok(()),
        };

        if !path.exists() {
            return Ok(());
        }

        let json = std::fs::read_to_string(&path).map_err(|e| {
            StorageError::EngineError(format!("failed to read catalog: {}", e))
        })?;

        let entries: Vec<CatalogEntry> = serde_json::from_str(&json).map_err(|e| {
            StorageError::EngineError(format!("failed to deserialize catalog: {}", e))
        })?;

        let data_dir = self.data_dir.as_ref().unwrap();
        let mut tables = self.tables.write().unwrap();

        for entry in entries {
            let mut info = TableInfo::new(entry.name.clone(), entry.schema);
            if !entry.primary_key.is_empty() {
                info = info.with_primary_key(entry.primary_key);
            }
            info = info.with_table_id(entry.table_id);

            // Register in catalog
            let _ = self.catalog.create_table(info.clone());

            // Open existing table file
            let table_path = data_dir.join("tables").join(format!("{}.db", entry.name));
            let store = TableStore::with_data_path(info, &table_path)?;
            tables.insert(entry.name, Arc::new(store));
        }

        Ok(())
    }

    // =========================================================================
    // DDL Operations
    // =========================================================================

    /// Creates a new table.
    pub fn create_table(&self, info: TableInfo) -> StorageResult<()> {
        // Register in catalog
        self.catalog.create_table(info.clone())?;

        // Create table store (disk-backed if data_dir is set)
        let store = if let Some(ref data_dir) = self.data_dir {
            let table_path = data_dir.join("tables").join(format!("{}.db", info.name));
            Arc::new(TableStore::with_data_path(info.clone(), &table_path)?)
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
    pub fn drop_table(&self, name: &str) -> StorageResult<()> {
        // Remove from catalog
        self.catalog.drop_table(name)?;

        // Remove table store
        let mut tables = self.tables.write().unwrap();
        tables.remove(name);

        // Persist catalog to disk
        drop(tables); // release lock before IO
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

    /// Flushes all tables to disk (no-op for in-memory engine).
    pub fn flush_all(&self) -> StorageResult<()> {
        let tables = self.tables.read().unwrap();
        for store in tables.values() {
            store.flush()?;
        }
        Ok(())
    }

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
