//! Query execution results.

use std::sync::Arc;
use std::time::Duration;

use nexus_sql::executor::{RecordBatch, Row, Value};
use nexus_sql::logical::Schema;

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum StatementResult {
    /// DDL statement (CREATE, DROP, ALTER).
    Ddl { command: String },
    /// SELECT query result.
    Query(ExecuteResult),
    /// INSERT result.
    Insert { rows_affected: u64 },
    /// UPDATE result.
    Update { rows_affected: u64 },
    /// DELETE result.
    Delete { rows_affected: u64 },
    /// Transaction control (BEGIN, COMMIT, ROLLBACK).
    Transaction { command: String },
    /// Empty result (e.g., for comments).
    Empty,
}

impl StatementResult {
    /// Creates a DDL result.
    pub fn ddl(command: impl Into<String>) -> Self {
        StatementResult::Ddl {
            command: command.into(),
        }
    }

    /// Creates a transaction result.
    pub fn transaction(command: impl Into<String>) -> Self {
        StatementResult::Transaction {
            command: command.into(),
        }
    }

    /// Returns the number of rows affected, if applicable.
    pub fn rows_affected(&self) -> Option<u64> {
        match self {
            StatementResult::Insert { rows_affected } => Some(*rows_affected),
            StatementResult::Update { rows_affected } => Some(*rows_affected),
            StatementResult::Delete { rows_affected } => Some(*rows_affected),
            StatementResult::Query(result) => Some(result.total_rows as u64),
            _ => None,
        }
    }

    /// Returns the query result if this is a SELECT.
    pub fn as_query(&self) -> Option<&ExecuteResult> {
        match self {
            StatementResult::Query(result) => Some(result),
            _ => None,
        }
    }

    /// Returns true if this is an error (currently always false).
    pub fn is_ok(&self) -> bool {
        true
    }

    /// Display as a string.
    pub fn display(&self) -> String {
        match self {
            StatementResult::Ddl { command } => format!("{}", command),
            StatementResult::Query(result) => result.display(),
            StatementResult::Insert { rows_affected } => {
                format!("INSERT {}", rows_affected)
            }
            StatementResult::Update { rows_affected } => {
                format!("UPDATE {}", rows_affected)
            }
            StatementResult::Delete { rows_affected } => {
                format!("DELETE {}", rows_affected)
            }
            StatementResult::Transaction { command } => format!("{}", command),
            StatementResult::Empty => String::new(),
        }
    }
}

/// Result of executing a query.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    /// Output schema.
    pub schema: Arc<Schema>,
    /// Result batches.
    pub batches: Vec<RecordBatch>,
    /// Total number of rows.
    pub total_rows: usize,
    /// Execution time.
    pub execution_time: Duration,
}

impl ExecuteResult {
    /// Creates an empty result with the given schema.
    pub fn empty(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            total_rows: 0,
            execution_time: Duration::ZERO,
        }
    }

    /// Creates a result from record batches.
    pub fn from_batches(schema: Arc<Schema>, batches: Vec<RecordBatch>, elapsed: Duration) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            schema,
            batches,
            total_rows,
            execution_time: elapsed,
        }
    }

    /// Returns all rows.
    pub fn rows(&self) -> Vec<Row> {
        self.batches.iter().flat_map(|b| b.rows()).collect()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }

    /// Returns the first row if exists.
    pub fn first_row(&self) -> Option<Row> {
        self.batches.first().and_then(|b| b.rows().next())
    }

    /// Returns a single value from the first row.
    pub fn scalar(&self) -> Option<Value> {
        self.first_row().and_then(|r| r.get(0).cloned())
    }

    /// Pretty prints the result.
    pub fn display(&self) -> String {
        let mut output = String::new();

        // Header
        let fields = self.schema.fields();
        let mut col_widths: Vec<usize> = fields.iter().map(|f| f.name().len()).collect();

        // Calculate column widths
        for batch in &self.batches {
            for row in batch.rows() {
                for (i, val) in row.iter().enumerate() {
                    let width = val.to_string().len();
                    if i < col_widths.len() {
                        col_widths[i] = col_widths[i].max(width);
                    }
                }
            }
        }

        // Print header
        for (i, field) in fields.iter().enumerate() {
            if i > 0 {
                output.push_str(" | ");
            }
            output.push_str(&format!("{:width$}", field.name(), width = col_widths[i]));
        }
        output.push('\n');

        // Print separator
        for (i, width) in col_widths.iter().enumerate() {
            if i > 0 {
                output.push_str("-+-");
            }
            output.push_str(&"-".repeat(*width));
        }
        output.push('\n');

        // Print data
        for batch in &self.batches {
            for row in batch.rows() {
                for (i, val) in row.iter().enumerate() {
                    if i > 0 {
                        output.push_str(" | ");
                    }
                    output.push_str(&format!("{:width$}", val, width = col_widths[i]));
                }
                output.push('\n');
            }
        }

        // Print footer
        output.push_str(&format!(
            "({} rows, {:.2}ms)\n",
            self.total_rows,
            self.execution_time.as_secs_f64() * 1000.0
        ));

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_sql::logical::Field;
    use nexus_sql::parser::DataType;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
        ]))
    }

    #[test]
    fn test_statement_result_display() {
        let ddl = StatementResult::ddl("CREATE TABLE");
        assert_eq!(ddl.display(), "CREATE TABLE");

        let insert = StatementResult::Insert { rows_affected: 5 };
        assert_eq!(insert.display(), "INSERT 5");
        assert_eq!(insert.rows_affected(), Some(5));
    }

    #[test]
    fn test_execute_result_empty() {
        let result = ExecuteResult::empty(test_schema());
        assert!(result.is_empty());
        assert_eq!(result.total_rows, 0);
    }

    #[test]
    fn test_execute_result_display() {
        let schema = test_schema();
        let rows = vec![Row::new(vec![Value::Int(1), Value::String("Alice".into())])];
        let batch = RecordBatch::from_rows(schema.clone(), &rows).unwrap();

        let result = ExecuteResult::from_batches(schema, vec![batch], Duration::from_millis(10));

        let display = result.display();
        assert!(display.contains("id"));
        assert!(display.contains("name"));
        assert!(display.contains("Alice"));
        assert!(display.contains("(1 rows"));
    }
}
