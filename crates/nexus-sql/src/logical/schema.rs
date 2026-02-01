//! Schema representation for logical plans.
//!
//! Schemas describe the output columns of each operator in a logical plan.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::parser::DataType;

/// A column in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Column {
    /// Optional table/relation qualifier.
    pub qualifier: Option<String>,
    /// Column name.
    pub name: String,
}

impl Column {
    /// Creates a new unqualified column.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            qualifier: None,
            name: name.into(),
        }
    }

    /// Creates a new qualified column.
    pub fn qualified(qualifier: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            qualifier: Some(qualifier.into()),
            name: name.into(),
        }
    }

    /// Returns the fully qualified name.
    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{}.{}", q, self.name),
            None => self.name.clone(),
        }
    }

    /// Returns true if this column matches the given name.
    pub fn matches(&self, qualifier: Option<&str>, name: &str) -> bool {
        if self.name != name {
            return false;
        }
        match (qualifier, &self.qualifier) {
            (Some(q), Some(sq)) => q == sq,
            (None, _) => true,
            (Some(_), None) => false,
        }
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.qualified_name())
    }
}

/// A field in a schema (column + type + nullability).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    /// Column identifier.
    pub column: Column,
    /// Data type.
    pub data_type: DataType,
    /// Whether NULL is allowed.
    pub nullable: bool,
}

impl Field {
    /// Creates a new field.
    pub fn new(column: Column, data_type: DataType, nullable: bool) -> Self {
        Self {
            column,
            data_type,
            nullable,
        }
    }

    /// Creates a new non-nullable field.
    pub fn not_null(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            column: Column::new(name),
            data_type,
            nullable: false,
        }
    }

    /// Creates a new nullable field.
    pub fn nullable(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            column: Column::new(name),
            data_type,
            nullable: true,
        }
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.column.name
    }

    /// Returns the qualified column name.
    pub fn qualified_name(&self) -> String {
        self.column.qualified_name()
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}{}",
            self.column,
            self.data_type,
            if self.nullable { "" } else { " NOT NULL" }
        )
    }
}

/// Schema describes the columns output by a plan node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// Fields in the schema.
    fields: Vec<Field>,
    /// Index by column name for fast lookup.
    #[serde(skip)]
    index: HashMap<String, usize>,
}

impl Schema {
    /// Creates an empty schema.
    pub fn empty() -> Self {
        Self {
            fields: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// Creates a schema from a list of fields.
    pub fn new(fields: Vec<Field>) -> Self {
        let mut schema = Self {
            fields: Vec::with_capacity(fields.len()),
            index: HashMap::new(),
        };
        for field in fields {
            schema.add_field(field);
        }
        schema
    }

    /// Adds a field to the schema.
    pub fn add_field(&mut self, field: Field) {
        let name = field.qualified_name();
        self.index.insert(name, self.fields.len());
        // Also index by unqualified name if not already present
        if !self.index.contains_key(&field.column.name) {
            self.index
                .insert(field.column.name.clone(), self.fields.len());
        }
        self.fields.push(field);
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns true if the schema has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns the fields.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Returns the field at the given index.
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    /// Finds a field by name.
    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.index.get(name).and_then(|&i| self.fields.get(i))
    }

    /// Finds the index of a field by name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Finds the index of a column.
    pub fn index_of_column(&self, column: &Column) -> Option<usize> {
        // Try qualified name first
        if let Some(idx) = self.index.get(&column.qualified_name()) {
            return Some(*idx);
        }
        // Fall back to unqualified
        self.index.get(&column.name).copied()
    }

    /// Merges two schemas (for joins).
    pub fn merge(&self, other: &Schema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend(other.fields.iter().cloned());
        Schema::new(fields)
    }

    /// Projects the schema to the specified column indices.
    pub fn project(&self, indices: &[usize]) -> Self {
        let fields: Vec<_> = indices
            .iter()
            .filter_map(|&i| self.fields.get(i).cloned())
            .collect();
        Schema::new(fields)
    }

    /// Returns the data types of all fields.
    pub fn data_types(&self) -> Vec<&DataType> {
        self.fields.iter().map(|f| &f.data_type).collect()
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, "]")
    }
}

/// A reference-counted schema for sharing.
pub type SchemaRef = Arc<Schema>;

/// Table metadata for planning.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: SchemaRef,
    /// Estimated row count.
    pub row_count: Option<u64>,
    /// Available indexes.
    pub indexes: Vec<IndexMeta>,
}

impl TableMeta {
    /// Creates new table metadata.
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema: Arc::new(schema),
            row_count: None,
            indexes: Vec::new(),
        }
    }

    /// Sets the estimated row count.
    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = Some(count);
        self
    }

    /// Adds an index.
    pub fn with_index(mut self, index: IndexMeta) -> Self {
        self.indexes.push(index);
        self
    }
}

/// Index metadata.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    /// Index name.
    pub name: String,
    /// Indexed columns.
    pub columns: Vec<String>,
    /// Whether the index is unique.
    pub unique: bool,
    /// Estimated index size in pages.
    pub size_pages: Option<u64>,
}

impl IndexMeta {
    /// Creates new index metadata.
    pub fn new(name: impl Into<String>, columns: Vec<String>, unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            unique,
            size_pages: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column() {
        let col = Column::new("id");
        assert_eq!(col.name, "id");
        assert!(col.qualifier.is_none());
        assert_eq!(col.qualified_name(), "id");

        let qcol = Column::qualified("users", "id");
        assert_eq!(qcol.qualified_name(), "users.id");
    }

    #[test]
    fn test_column_matches() {
        let col = Column::qualified("users", "id");
        assert!(col.matches(Some("users"), "id"));
        assert!(col.matches(None, "id"));
        assert!(!col.matches(Some("orders"), "id"));
        assert!(!col.matches(None, "name"));
    }

    #[test]
    fn test_schema() {
        let schema = Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
        ]);

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.field(0).unwrap().name(), "id");
        assert!(schema.field_by_name("name").is_some());
        assert_eq!(schema.index_of("id"), Some(0));
    }

    #[test]
    fn test_schema_merge() {
        let s1 = Schema::new(vec![Field::not_null("a", DataType::Int)]);
        let s2 = Schema::new(vec![Field::not_null("b", DataType::Int)]);
        let merged = s1.merge(&s2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_schema_project() {
        let schema = Schema::new(vec![
            Field::not_null("a", DataType::Int),
            Field::not_null("b", DataType::Int),
            Field::not_null("c", DataType::Int),
        ]);
        let projected = schema.project(&[0, 2]);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.field(0).unwrap().name(), "a");
        assert_eq!(projected.field(1).unwrap().name(), "c");
    }
}
