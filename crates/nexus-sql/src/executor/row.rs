//! Row representation for query execution.
//!
//! This module defines the `Row` type which represents a single row of data
//! during query execution.

use std::fmt;
use std::sync::Arc;

use super::Value;
use crate::logical::Schema;

/// A single row of values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Row {
    /// The values in this row.
    values: Vec<Value>,
}

impl Row {
    /// Creates a new row with the given values.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Creates an empty row.
    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Creates a row with all NULL values.
    pub fn nulls(num_columns: usize) -> Self {
        Self {
            values: vec![Value::Null; num_columns],
        }
    }

    /// Returns the number of columns in this row.
    pub fn num_columns(&self) -> usize {
        self.values.len()
    }

    /// Returns true if this row is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the value at the given index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Returns a mutable reference to the value at the given index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Sets the value at the given index.
    pub fn set(&mut self, index: usize, value: Value) {
        if index < self.values.len() {
            self.values[index] = value;
        }
    }

    /// Appends a value to this row.
    pub fn push(&mut self, value: Value) {
        self.values.push(value);
    }

    /// Extends this row with values from an iterator.
    pub fn extend<I: IntoIterator<Item = Value>>(&mut self, iter: I) {
        self.values.extend(iter);
    }

    /// Returns an iterator over the values.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    /// Returns the values as a slice.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes the row and returns the values.
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Projects this row to include only the specified columns.
    pub fn project(&self, indices: &[usize]) -> Row {
        let values = indices
            .iter()
            .map(|&i| self.values.get(i).cloned().unwrap_or(Value::Null))
            .collect();
        Row { values }
    }

    /// Concatenates this row with another row.
    pub fn concat(&self, other: &Row) -> Row {
        let mut values = self.values.clone();
        values.extend(other.values.iter().cloned());
        Row { values }
    }

    /// Creates a row from a slice of values.
    pub fn from_slice(values: &[Value]) -> Self {
        Self {
            values: values.to_vec(),
        }
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, value) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", value)?;
        }
        write!(f, ")")
    }
}

/// A row with an associated schema.
#[derive(Debug, Clone)]
pub struct TypedRow {
    /// The row data.
    pub row: Row,
    /// The schema.
    pub schema: Arc<Schema>,
}

impl TypedRow {
    /// Creates a new typed row.
    pub fn new(row: Row, schema: Arc<Schema>) -> Self {
        Self { row, schema }
    }

    /// Returns the value for a column by name.
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.schema
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .and_then(|i| self.row.get(i))
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.row.num_columns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_new() {
        let row = Row::new(vec![Value::int(1), Value::string("hello")]);
        assert_eq!(row.num_columns(), 2);
    }

    #[test]
    fn test_row_get() {
        let row = Row::new(vec![Value::int(1), Value::int(2)]);
        assert_eq!(row.get(0), Some(&Value::int(1)));
        assert_eq!(row.get(1), Some(&Value::int(2)));
        assert_eq!(row.get(2), None);
    }

    #[test]
    fn test_row_project() {
        let row = Row::new(vec![Value::int(1), Value::int(2), Value::int(3)]);
        let projected = row.project(&[2, 0]);
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.get(0), Some(&Value::int(3)));
        assert_eq!(projected.get(1), Some(&Value::int(1)));
    }

    #[test]
    fn test_row_concat() {
        let row1 = Row::new(vec![Value::int(1)]);
        let row2 = Row::new(vec![Value::int(2), Value::int(3)]);
        let concat = row1.concat(&row2);
        assert_eq!(concat.num_columns(), 3);
    }

    #[test]
    fn test_row_nulls() {
        let row = Row::nulls(3);
        assert_eq!(row.num_columns(), 3);
        assert!(row.get(0).unwrap().is_null());
    }

    #[test]
    fn test_row_display() {
        let row = Row::new(vec![Value::int(1), Value::string("hello")]);
        assert_eq!(row.to_string(), "(1, hello)");
    }
}
