//! Record batch for vectorized execution.
//!
//! This module defines the `RecordBatch` type which represents a batch of rows
//! for efficient vectorized processing.

use std::fmt;
use std::sync::Arc;

use super::{Row, Value};
use crate::logical::{Field, Schema};
use crate::parser::DataType;

/// A batch of rows for vectorized execution.
///
/// RecordBatch stores data in a columnar format for efficient processing.
/// Each column is represented as a `Column` which is a vector of values.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// The schema of this batch.
    schema: Arc<Schema>,
    /// The columns in this batch.
    columns: Vec<Column>,
    /// Number of rows in this batch.
    num_rows: usize,
}

impl RecordBatch {
    /// Creates a new record batch with the given schema and columns.
    pub fn new(schema: Arc<Schema>, columns: Vec<Column>) -> Result<Self, String> {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);

        // Verify all columns have the same length
        for (i, col) in columns.iter().enumerate() {
            if col.len() != num_rows {
                return Err(format!(
                    "Column {} has {} rows, expected {}",
                    i,
                    col.len(),
                    num_rows
                ));
            }
        }

        // Verify column count matches schema
        if columns.len() != schema.fields().len() {
            return Err(format!(
                "Column count {} doesn't match schema field count {}",
                columns.len(),
                schema.fields().len()
            ));
        }

        Ok(Self {
            schema,
            columns,
            num_rows,
        })
    }

    /// Creates an empty record batch with the given schema.
    pub fn empty(schema: Arc<Schema>) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| Column::empty(f.data_type.clone()))
            .collect();
        Self {
            schema,
            columns,
            num_rows: 0,
        }
    }

    /// Creates a record batch from rows.
    pub fn from_rows(schema: Arc<Schema>, rows: &[Row]) -> Result<Self, String> {
        if rows.is_empty() {
            return Ok(Self::empty(schema));
        }

        let num_cols = schema.fields().len();
        let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(rows.len()); num_cols];

        for row in rows {
            if row.num_columns() != num_cols {
                return Err(format!(
                    "Row has {} columns, expected {}",
                    row.num_columns(),
                    num_cols
                ));
            }
            for (i, value) in row.iter().enumerate() {
                columns[i].push(value.clone());
            }
        }

        let columns: Vec<Column> = columns
            .into_iter()
            .zip(schema.fields().iter())
            .map(|(values, field)| Column::new(field.data_type.clone(), values))
            .collect();

        Self::new(schema, columns)
    }

    /// Returns the schema of this batch.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Returns the number of rows in this batch.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the number of columns in this batch.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns true if this batch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Returns the column at the given index.
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Returns the columns.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the row at the given index.
    pub fn row(&self, index: usize) -> Option<Row> {
        if index >= self.num_rows {
            return None;
        }
        let values: Vec<Value> = self.columns.iter().map(|c| c.get(index).clone()).collect();
        Some(Row::new(values))
    }

    /// Returns an iterator over the rows.
    pub fn rows(&self) -> impl Iterator<Item = Row> + '_ {
        (0..self.num_rows).map(move |i| self.row(i).unwrap())
    }

    /// Projects this batch to include only the specified columns.
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch, String> {
        let fields: Vec<Field> = indices
            .iter()
            .map(|&i| {
                self.schema
                    .fields()
                    .get(i)
                    .cloned()
                    .ok_or_else(|| format!("Invalid column index: {}", i))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let columns: Vec<Column> = indices
            .iter()
            .map(|&i| {
                self.columns
                    .get(i)
                    .cloned()
                    .ok_or_else(|| format!("Invalid column index: {}", i))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::new(schema, columns)
    }

    /// Filters rows based on a boolean column.
    pub fn filter(&self, mask: &[bool]) -> Result<RecordBatch, String> {
        if mask.len() != self.num_rows {
            return Err(format!(
                "Filter mask length {} doesn't match row count {}",
                mask.len(),
                self.num_rows
            ));
        }

        let columns: Vec<Column> = self
            .columns
            .iter()
            .map(|col| {
                let values: Vec<Value> = col
                    .values()
                    .iter()
                    .zip(mask.iter())
                    .filter(|(_, &keep)| keep)
                    .map(|(v, _)| v.clone())
                    .collect();
                Column::new(col.data_type().clone(), values)
            })
            .collect();

        RecordBatch::new(self.schema.clone(), columns)
    }

    /// Concatenates multiple batches.
    pub fn concat(batches: &[RecordBatch]) -> Result<RecordBatch, String> {
        if batches.is_empty() {
            return Err("Cannot concat empty batch list".to_string());
        }

        let schema = batches[0].schema.clone();

        // Verify all batches have the same schema
        for batch in batches.iter().skip(1) {
            if batch.schema.fields().len() != schema.fields().len() {
                return Err("Schema mismatch in concat".to_string());
            }
        }

        let num_cols = schema.fields().len();
        let mut columns: Vec<Vec<Value>> = vec![Vec::new(); num_cols];

        for batch in batches {
            for (i, col) in batch.columns.iter().enumerate() {
                columns[i].extend(col.values().iter().cloned());
            }
        }

        let columns: Vec<Column> = columns
            .into_iter()
            .zip(schema.fields().iter())
            .map(|(values, field)| Column::new(field.data_type.clone(), values))
            .collect();

        RecordBatch::new(schema, columns)
    }

    /// Slices this batch to a range of rows.
    pub fn slice(&self, offset: usize, length: usize) -> Result<RecordBatch, String> {
        if offset > self.num_rows {
            return Err(format!(
                "Slice offset {} exceeds row count {}",
                offset, self.num_rows
            ));
        }

        let end = (offset + length).min(self.num_rows);
        let columns: Vec<Column> = self
            .columns
            .iter()
            .map(|col| {
                let values: Vec<Value> = col.values()[offset..end].to_vec();
                Column::new(col.data_type().clone(), values)
            })
            .collect();

        RecordBatch::new(self.schema.clone(), columns)
    }
}

impl fmt::Display for RecordBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "RecordBatch ({} rows x {} cols)",
            self.num_rows,
            self.num_columns()
        )?;

        // Header
        for (i, field) in self.schema.fields().iter().enumerate() {
            if i > 0 {
                write!(f, " | ")?;
            }
            write!(f, "{}", field.name())?;
        }
        writeln!(f)?;

        // Separator
        writeln!(
            f,
            "{}",
            "-".repeat(
                self.schema
                    .fields()
                    .iter()
                    .map(|f| f.name().len() + 3)
                    .sum::<usize>()
            )
        )?;

        // Data (limit to 10 rows for display)
        let display_rows = self.num_rows.min(10);
        for i in 0..display_rows {
            for (j, col) in self.columns.iter().enumerate() {
                if j > 0 {
                    write!(f, " | ")?;
                }
                write!(f, "{}", col.get(i))?;
            }
            writeln!(f)?;
        }

        if self.num_rows > 10 {
            writeln!(f, "... ({} more rows)", self.num_rows - 10)?;
        }

        Ok(())
    }
}

/// A column of values.
#[derive(Debug, Clone)]
pub struct Column {
    /// The data type of this column.
    data_type: DataType,
    /// The values in this column.
    values: Vec<Value>,
}

impl Column {
    /// Creates a new column with the given data type and values.
    pub fn new(data_type: DataType, values: Vec<Value>) -> Self {
        Self { data_type, values }
    }

    /// Creates an empty column with the given data type.
    pub fn empty(data_type: DataType) -> Self {
        Self {
            data_type,
            values: Vec::new(),
        }
    }

    /// Returns the data type of this column.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the number of values in this column.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if this column is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the value at the given index.
    pub fn get(&self, index: usize) -> &Value {
        self.values.get(index).unwrap_or(&Value::Null)
    }

    /// Returns the values as a slice.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Returns an iterator over the values.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Appends a value to this column.
    pub fn push(&mut self, value: Value) {
        self.values.push(value);
    }

    /// Creates a column of integers.
    pub fn int_column(values: Vec<i32>) -> Self {
        Self {
            data_type: DataType::Int,
            values: values.into_iter().map(Value::Int).collect(),
        }
    }

    /// Creates a column of bigints.
    pub fn bigint_column(values: Vec<i64>) -> Self {
        Self {
            data_type: DataType::BigInt,
            values: values.into_iter().map(Value::BigInt).collect(),
        }
    }

    /// Creates a column of strings.
    pub fn string_column(values: Vec<String>) -> Self {
        Self {
            data_type: DataType::Text,
            values: values.into_iter().map(Value::String).collect(),
        }
    }

    /// Creates a column of booleans.
    pub fn bool_column(values: Vec<bool>) -> Self {
        Self {
            data_type: DataType::Boolean,
            values: values.into_iter().map(Value::Boolean).collect(),
        }
    }
}

/// Builder for creating record batches incrementally.
#[derive(Debug)]
pub struct RecordBatchBuilder {
    /// The schema.
    schema: Arc<Schema>,
    /// The columns being built.
    columns: Vec<Vec<Value>>,
}

impl RecordBatchBuilder {
    /// Creates a new builder with the given schema.
    pub fn new(schema: Arc<Schema>) -> Self {
        let columns = vec![Vec::new(); schema.fields().len()];
        Self { schema, columns }
    }

    /// Appends a row to the batch.
    pub fn append_row(&mut self, row: &Row) -> Result<(), String> {
        if row.num_columns() != self.columns.len() {
            return Err(format!(
                "Row has {} columns, expected {}",
                row.num_columns(),
                self.columns.len()
            ));
        }
        for (i, value) in row.iter().enumerate() {
            self.columns[i].push(value.clone());
        }
        Ok(())
    }

    /// Appends a value to a specific column.
    pub fn append_value(&mut self, column: usize, value: Value) -> Result<(), String> {
        self.columns
            .get_mut(column)
            .ok_or_else(|| format!("Invalid column index: {}", column))?
            .push(value);
        Ok(())
    }

    /// Returns the current number of rows.
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|c| c.len()).unwrap_or(0)
    }

    /// Builds the record batch.
    pub fn build(self) -> Result<RecordBatch, String> {
        let columns: Vec<Column> = self
            .columns
            .into_iter()
            .zip(self.schema.fields().iter())
            .map(|(values, field)| Column::new(field.data_type.clone(), values))
            .collect();
        RecordBatch::new(self.schema, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
        ]))
    }

    #[test]
    fn test_record_batch_new() {
        let schema = test_schema();
        let columns = vec![
            Column::int_column(vec![1, 2, 3]),
            Column::string_column(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let batch = RecordBatch::new(schema, columns).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_record_batch_from_rows() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![Value::int(1), Value::string("a")]),
            Row::new(vec![Value::int(2), Value::string("b")]),
        ];
        let batch = RecordBatch::from_rows(schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_record_batch_row() {
        let schema = test_schema();
        let columns = vec![
            Column::int_column(vec![1, 2]),
            Column::string_column(vec!["a".to_string(), "b".to_string()]),
        ];
        let batch = RecordBatch::new(schema, columns).unwrap();

        let row0 = batch.row(0).unwrap();
        assert_eq!(row0.get(0), Some(&Value::Int(1)));
        assert_eq!(row0.get(1), Some(&Value::String("a".to_string())));
    }

    #[test]
    fn test_record_batch_project() {
        let schema = test_schema();
        let columns = vec![
            Column::int_column(vec![1, 2]),
            Column::string_column(vec!["a".to_string(), "b".to_string()]),
        ];
        let batch = RecordBatch::new(schema, columns).unwrap();

        let projected = batch.project(&[1]).unwrap();
        assert_eq!(projected.num_columns(), 1);
        assert_eq!(
            projected.column(0).unwrap().get(0),
            &Value::String("a".to_string())
        );
    }

    #[test]
    fn test_record_batch_filter() {
        let schema = test_schema();
        let columns = vec![
            Column::int_column(vec![1, 2, 3]),
            Column::string_column(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let batch = RecordBatch::new(schema, columns).unwrap();

        let filtered = batch.filter(&[true, false, true]).unwrap();
        assert_eq!(filtered.num_rows(), 2);
        assert_eq!(filtered.column(0).unwrap().get(0), &Value::Int(1));
        assert_eq!(filtered.column(0).unwrap().get(1), &Value::Int(3));
    }

    #[test]
    fn test_record_batch_builder() {
        let schema = test_schema();
        let mut builder = RecordBatchBuilder::new(schema);

        builder
            .append_row(&Row::new(vec![Value::int(1), Value::string("a")]))
            .unwrap();
        builder
            .append_row(&Row::new(vec![Value::int(2), Value::string("b")]))
            .unwrap();

        let batch = builder.build().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_column_constructors() {
        let col = Column::int_column(vec![1, 2, 3]);
        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), &Value::Int(1));

        let col = Column::string_column(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(col.len(), 2);
    }
}
