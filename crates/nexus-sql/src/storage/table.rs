//! Per-table storage wrapper around SageTree.
//!
//! This module provides `TableStore`, which wraps a SageTree instance
//! and provides table-level operations for INSERT, GET, DELETE, and SCAN.

use nexus_common::types::Key;
use nexus_storage::sagetree::{CursorEntry, KeyRange, SageTree};

use crate::executor::{Row, Value};

use super::catalog::TableInfo;
use super::encoder::{RowDecoder, RowEncoder};
use super::error::{StorageError, StorageResult};

/// Per-table storage wrapper around SageTree.
///
/// `TableStore` provides a high-level interface for table operations,
/// handling the encoding/decoding between SQL rows and SageTree key-value pairs.
#[derive(Debug)]
pub struct TableStore {
    /// Table metadata.
    info: TableInfo,
    /// The underlying SageTree.
    tree: SageTree,
    /// Row encoder.
    encoder: RowEncoder,
    /// Row decoder.
    decoder: RowDecoder,
}

impl TableStore {
    /// Creates a new table store.
    pub fn new(info: TableInfo) -> Self {
        let encoder = RowEncoder::new(info.table_id, info.primary_key.clone(), info.schema.clone());
        let decoder = RowDecoder::new(info.schema.clone());

        Self {
            info,
            tree: SageTree::new(),
            encoder,
            decoder,
        }
    }

    /// Returns the table info.
    pub fn info(&self) -> &TableInfo {
        &self.info
    }

    /// Returns the table name.
    pub fn name(&self) -> &str {
        &self.info.name
    }

    /// Returns the number of rows in the table (approximate).
    pub fn row_count(&self) -> usize {
        self.tree.len()
    }

    /// Returns true if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    // =========================================================================
    // Row Operations
    // =========================================================================

    /// Inserts a row into the table.
    ///
    /// Returns an error if a row with the same primary key already exists.
    pub fn insert(&self, row: Row) -> StorageResult<()> {
        // Validate row column count
        if row.num_columns() != self.info.schema.len() {
            return Err(StorageError::SchemaMismatch(format!(
                "Expected {} columns, got {}",
                self.info.schema.len(),
                row.num_columns()
            )));
        }

        // Encode key and value
        let key = self.encoder.encode_key(&row)?;
        let value = self.encoder.encode_value(&row)?;

        // Insert into tree
        self.tree.insert(key, value).map_err(|e| {
            if matches!(e, nexus_storage::sagetree::SageTreeError::DuplicateKey) {
                StorageError::PrimaryKeyViolation(format!(
                    "Duplicate primary key in table '{}'",
                    self.info.name
                ))
            } else {
                StorageError::from(e)
            }
        })
    }

    /// Gets a row by primary key values.
    ///
    /// The `key_values` slice should contain values for each primary key column
    /// in order.
    pub fn get(&self, key_values: &[Value]) -> StorageResult<Option<Row>> {
        // Build a temporary row with just the primary key values
        // to encode the key
        let mut temp_row_values = vec![Value::Null; self.info.schema.len()];
        for (i, &pk_idx) in self.info.primary_key.iter().enumerate() {
            if i < key_values.len() {
                temp_row_values[pk_idx] = key_values[i].clone();
            }
        }
        let temp_row = Row::new(temp_row_values);
        let key = self.encoder.encode_key(&temp_row)?;

        // Get from tree
        match self.tree.get(&key)? {
            Some(value) => {
                let row = self.decoder.decode(&value)?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Deletes a row by primary key values.
    pub fn delete(&self, key_values: &[Value]) -> StorageResult<bool> {
        // Build key from primary key values
        let mut temp_row_values = vec![Value::Null; self.info.schema.len()];
        for (i, &pk_idx) in self.info.primary_key.iter().enumerate() {
            if i < key_values.len() {
                temp_row_values[pk_idx] = key_values[i].clone();
            }
        }
        let temp_row = Row::new(temp_row_values);
        let key = self.encoder.encode_key(&temp_row)?;

        // Delete from tree
        match self.tree.delete(&key) {
            Ok(()) => Ok(true),
            Err(nexus_storage::sagetree::SageTreeError::KeyNotFound) => Ok(false),
            Err(e) => Err(StorageError::from(e)),
        }
    }

    /// Updates a row by primary key.
    ///
    /// The row must contain valid primary key values.
    pub fn update(&self, row: Row) -> StorageResult<bool> {
        // Validate row column count
        if row.num_columns() != self.info.schema.len() {
            return Err(StorageError::SchemaMismatch(format!(
                "Expected {} columns, got {}",
                self.info.schema.len(),
                row.num_columns()
            )));
        }

        // Encode key and value
        let key = self.encoder.encode_key(&row)?;
        let value = self.encoder.encode_value(&row)?;

        // Update in tree
        match self.tree.update(&key, value) {
            Ok(()) => Ok(true),
            Err(nexus_storage::sagetree::SageTreeError::KeyNotFound) => Ok(false),
            Err(e) => Err(StorageError::from(e)),
        }
    }

    /// Upserts a row (insert or update).
    pub fn upsert(&self, row: Row) -> StorageResult<()> {
        // Validate row column count
        if row.num_columns() != self.info.schema.len() {
            return Err(StorageError::SchemaMismatch(format!(
                "Expected {} columns, got {}",
                self.info.schema.len(),
                row.num_columns()
            )));
        }

        // Encode key and value
        let key = self.encoder.encode_key(&row)?;
        let value = self.encoder.encode_value(&row)?;

        // Upsert in tree
        self.tree.upsert(key, value)?;
        Ok(())
    }

    // =========================================================================
    // Scan Operations
    // =========================================================================

    /// Scans all rows in the table.
    pub fn scan(&self) -> TableScanIterator {
        TableScanIterator::new(self, ScanRange::All)
    }

    /// Scans rows within a primary key range.
    pub fn scan_range(&self, start: &[Value], end: &[Value]) -> TableScanIterator {
        TableScanIterator::new(
            self,
            ScanRange::Range {
                start: start.to_vec(),
                end: end.to_vec(),
            },
        )
    }

    /// Scans rows with a primary key prefix.
    pub fn scan_prefix(&self, prefix: &[Value]) -> TableScanIterator {
        TableScanIterator::new(self, ScanRange::Prefix(prefix.to_vec()))
    }

    /// Scans all rows and returns them as a vector.
    pub fn scan_all(&self) -> StorageResult<Vec<Row>> {
        let entries = self.tree.scan(KeyRange::all())?;
        self.decode_entries(entries)
    }

    /// Scans rows with a limit.
    pub fn scan_with_limit(&self, limit: usize) -> StorageResult<Vec<Row>> {
        let entries = self.tree.scan_with_limit(KeyRange::all(), limit)?;
        self.decode_entries(entries)
    }

    /// Decodes a vector of cursor entries into rows.
    fn decode_entries(&self, entries: Vec<CursorEntry>) -> StorageResult<Vec<Row>> {
        entries
            .into_iter()
            .map(|entry| self.decoder.decode(&entry.value))
            .collect()
    }

    /// Builds a storage key from primary key values.
    fn build_key(&self, key_values: &[Value]) -> StorageResult<Key> {
        let mut temp_row_values = vec![Value::Null; self.info.schema.len()];
        for (i, &pk_idx) in self.info.primary_key.iter().enumerate() {
            if i < key_values.len() {
                temp_row_values[pk_idx] = key_values[i].clone();
            }
        }
        let temp_row = Row::new(temp_row_values);
        self.encoder.encode_key(&temp_row)
    }

    // =========================================================================
    // Maintenance
    // =========================================================================

    /// Forces consolidation of delta chains.
    pub fn consolidate(&self) {
        self.tree.consolidate_all();
    }

    /// Clears all data from the table.
    pub fn clear(&self) {
        self.tree.clear();
    }
}

/// Range specification for table scans.
#[derive(Debug, Clone)]
enum ScanRange {
    /// Scan all rows.
    All,
    /// Scan rows in a key range.
    Range { start: Vec<Value>, end: Vec<Value> },
    /// Scan rows with a key prefix.
    Prefix(Vec<Value>),
}

/// Iterator over table rows.
///
/// This iterator fetches rows from the underlying SageTree and decodes them.
pub struct TableScanIterator {
    /// Decoded rows buffer.
    rows: Vec<Row>,
    /// Current position in the buffer.
    position: usize,
    /// Any error encountered during scanning.
    error: Option<StorageError>,
}

impl TableScanIterator {
    /// Creates a new table scan iterator.
    fn new(store: &TableStore, range: ScanRange) -> Self {
        // Fetch all matching entries
        let result = match range {
            ScanRange::All => store.tree.scan(KeyRange::all()),
            ScanRange::Range { start, end } => {
                // Build keys from values
                match (store.build_key(&start), store.build_key(&end)) {
                    (Ok(start_key), Ok(end_key)) => {
                        store.tree.scan(KeyRange::new(start_key, end_key))
                    }
                    (Err(e), _) | (_, Err(e)) => Err(
                        nexus_storage::sagetree::SageTreeError::structure_error(&e.to_string()),
                    ),
                }
            }
            ScanRange::Prefix(prefix) => match store.build_key(&prefix) {
                Ok(prefix_key) => store.tree.scan_prefix(&prefix_key),
                Err(e) => Err(nexus_storage::sagetree::SageTreeError::structure_error(
                    &e.to_string(),
                )),
            },
        };

        match result {
            Ok(entries) => {
                // Decode all entries
                let mut rows = Vec::with_capacity(entries.len());
                let mut error = None;

                for entry in entries {
                    match store.decoder.decode(&entry.value) {
                        Ok(row) => rows.push(row),
                        Err(e) => {
                            error = Some(e);
                            break;
                        }
                    }
                }

                Self {
                    rows,
                    position: 0,
                    error,
                }
            }
            Err(e) => Self {
                rows: Vec::new(),
                position: 0,
                error: Some(StorageError::from(e)),
            },
        }
    }

    /// Returns any error that occurred during scanning.
    pub fn error(&self) -> Option<&StorageError> {
        self.error.as_ref()
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Collects all rows into a vector.
    pub fn collect_rows(self) -> StorageResult<Vec<Row>> {
        if let Some(e) = self.error {
            Err(e)
        } else {
            Ok(self.rows)
        }
    }
}

impl Iterator for TableScanIterator {
    type Item = StorageResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return error if any
        if let Some(e) = self.error.take() {
            return Some(Err(e));
        }

        // Return next row
        if self.position < self.rows.len() {
            let row = self.rows[self.position].clone();
            self.position += 1;
            Some(Ok(row))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{Field, Schema};
    use crate::parser::DataType;

    fn test_table_info() -> TableInfo {
        let schema = Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
            Field::nullable("age", DataType::Int),
        ]);
        TableInfo::new("users", schema)
            .with_primary_key(vec![0])
            .with_table_id(1)
    }

    #[test]
    fn test_table_store_insert_and_get() {
        let info = test_table_info();
        let store = TableStore::new(info);

        let row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);

        store.insert(row).unwrap();

        let result = store.get(&[Value::Int(1)]).unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.get(0), Some(&Value::Int(1)));
        assert_eq!(retrieved.get(1), Some(&Value::String("Alice".to_string())));
        assert_eq!(retrieved.get(2), Some(&Value::Int(30)));
    }

    #[test]
    fn test_table_store_insert_duplicate() {
        let info = test_table_info();
        let store = TableStore::new(info);

        let row1 = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);
        let row2 = Row::new(vec![
            Value::Int(1),
            Value::String("Bob".to_string()),
            Value::Int(25),
        ]);

        store.insert(row1).unwrap();
        let result = store.insert(row2);

        assert!(matches!(result, Err(StorageError::PrimaryKeyViolation(_))));
    }

    #[test]
    fn test_table_store_delete() {
        let info = test_table_info();
        let store = TableStore::new(info);

        let row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);

        store.insert(row).unwrap();
        assert!(store.get(&[Value::Int(1)]).unwrap().is_some());

        let deleted = store.delete(&[Value::Int(1)]).unwrap();
        assert!(deleted);

        assert!(store.get(&[Value::Int(1)]).unwrap().is_none());
    }

    #[test]
    fn test_table_store_update() {
        let info = test_table_info();
        let store = TableStore::new(info);

        let row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);

        store.insert(row).unwrap();

        let updated_row = Row::new(vec![
            Value::Int(1),
            Value::String("Alice Smith".to_string()),
            Value::Int(31),
        ]);

        let updated = store.update(updated_row).unwrap();
        assert!(updated);

        let result = store.get(&[Value::Int(1)]).unwrap().unwrap();
        assert_eq!(
            result.get(1),
            Some(&Value::String("Alice Smith".to_string()))
        );
        assert_eq!(result.get(2), Some(&Value::Int(31)));
    }

    #[test]
    fn test_table_store_upsert() {
        let info = test_table_info();
        let store = TableStore::new(info);

        // Insert new
        let row1 = Row::new(vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);
        store.upsert(row1).unwrap();

        assert_eq!(store.row_count(), 1);

        // Update existing
        let row2 = Row::new(vec![
            Value::Int(1),
            Value::String("Alice Updated".to_string()),
            Value::Int(31),
        ]);
        store.upsert(row2).unwrap();

        assert_eq!(store.row_count(), 1);

        let result = store.get(&[Value::Int(1)]).unwrap().unwrap();
        assert_eq!(
            result.get(1),
            Some(&Value::String("Alice Updated".to_string()))
        );
    }

    #[test]
    fn test_table_store_scan_all() {
        let info = test_table_info();
        let store = TableStore::new(info);

        for i in 0..10 {
            let row = Row::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Int(20 + i),
            ]);
            store.insert(row).unwrap();
        }

        let rows = store.scan_all().unwrap();
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_table_store_scan_with_limit() {
        let info = test_table_info();
        let store = TableStore::new(info);

        for i in 0..100 {
            let row = Row::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Int(20 + i),
            ]);
            store.insert(row).unwrap();
        }

        let rows = store.scan_with_limit(10).unwrap();
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_table_store_schema_mismatch() {
        let info = test_table_info();
        let store = TableStore::new(info);

        // Row with wrong number of columns
        let row = Row::new(vec![Value::Int(1), Value::String("Alice".to_string())]);

        let result = store.insert(row);
        assert!(matches!(result, Err(StorageError::SchemaMismatch(_))));
    }

    #[test]
    fn test_table_scan_iterator() {
        let info = test_table_info();
        let store = TableStore::new(info);

        for i in 0..5 {
            let row = Row::new(vec![
                Value::Int(i),
                Value::String(format!("User{}", i)),
                Value::Int(20 + i),
            ]);
            store.insert(row).unwrap();
        }

        let mut count = 0;
        for result in store.scan() {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 5);
    }
}
