//! SQL Parser for NexusDB.
//!
//! This module provides SQL parsing capabilities using the `sqlparser` crate
//! and transforms the AST into NexusDB's internal representation.
//!
//! # Supported SQL
//!
//! - SELECT queries with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
//! - INSERT, UPDATE, DELETE statements
//! - CREATE TABLE, DROP TABLE, ALTER TABLE
//! - CREATE INDEX, DROP INDEX
//! - Transaction control (BEGIN, COMMIT, ROLLBACK)
//! - Common Table Expressions (WITH)
//!
//! # Usage
//!
//! ```
//! use nexus_sql::parser::{Parser, Statement};
//!
//! let sql = "SELECT id, name FROM users WHERE age > 18";
//! let statements = Parser::parse(sql).unwrap();
//! ```

use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser as SqlParser;
use thiserror::Error;

mod expr;
mod statement;
mod types;

pub use expr::*;
pub use statement::*;
pub use types::*;

/// Errors that can occur during SQL parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    /// Error from the underlying sqlparser crate.
    #[error("SQL syntax error: {0}")]
    Syntax(String),

    /// Unsupported SQL feature.
    #[error("Unsupported SQL feature: {0}")]
    Unsupported(String),

    /// Invalid identifier.
    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),

    /// Invalid literal value.
    #[error("Invalid literal: {0}")]
    InvalidLiteral(String),

    /// Type mismatch.
    #[error("Type error: {0}")]
    TypeError(String),

    /// Empty query.
    #[error("Empty query")]
    EmptyQuery,
}

impl From<sqlparser::parser::ParserError> for ParseError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        ParseError::Syntax(err.to_string())
    }
}

/// Result type for parsing operations.
pub type ParseResult<T> = Result<T, ParseError>;

/// SQL Parser for NexusDB.
///
/// Uses PostgreSQL dialect by default for compatibility with common tools.
pub struct Parser;

impl Parser {
    /// Parses a SQL string into a list of statements.
    pub fn parse(sql: &str) -> ParseResult<Vec<Statement>> {
        if sql.trim().is_empty() {
            return Err(ParseError::EmptyQuery);
        }

        let dialect = PostgreSqlDialect {};
        let ast = SqlParser::parse_sql(&dialect, sql)?;

        if ast.is_empty() {
            return Err(ParseError::EmptyQuery);
        }

        ast.into_iter().map(Statement::from_sql_ast).collect()
    }

    /// Parses a single SQL statement.
    pub fn parse_one(sql: &str) -> ParseResult<Statement> {
        let mut statements = Self::parse(sql)?;
        if statements.len() != 1 {
            return Err(ParseError::Syntax(format!(
                "Expected 1 statement, got {}",
                statements.len()
            )));
        }
        Ok(statements.remove(0))
    }

    /// Parses a SQL expression.
    pub fn parse_expr(sql: &str) -> ParseResult<Expr> {
        let dialect = PostgreSqlDialect {};
        let mut parser = SqlParser::new(&dialect).try_with_sql(sql)?;
        let expr = parser.parse_expr()?;
        Expr::from_sql_ast(expr)
    }
}

/// A column reference (table.column or just column).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Optional table or alias name.
    pub table: Option<String>,
    /// Column name.
    pub column: String,
}

impl ColumnRef {
    /// Creates a new column reference.
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }

    /// Creates a column reference with a table qualifier.
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
        }
    }

    /// Returns true if this column has a table qualifier.
    pub fn is_qualified(&self) -> bool {
        self.table.is_some()
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref table) = self.table {
            write!(f, "{}.{}", table, self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

/// A table reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    /// Optional schema name.
    pub schema: Option<String>,
    /// Table name.
    pub table: String,
    /// Optional alias.
    pub alias: Option<String>,
}

impl TableRef {
    /// Creates a new table reference.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            alias: None,
        }
    }

    /// Creates a table reference with a schema.
    pub fn with_schema(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            table: table.into(),
            alias: None,
        }
    }

    /// Adds an alias to the table reference.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Returns the effective name (alias or table name).
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref schema) = self.schema {
            write!(f, "{}.{}", schema, self.table)?;
        } else {
            write!(f, "{}", self.table)?;
        }
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT id, name FROM users";
        let statements = Parser::parse(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert_eq!(select.from.len(), 1);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT * FROM users WHERE age > 18";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table.table, "users");
                assert_eq!(insert.columns.len(), 2);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name.table, "users");
                assert_eq!(ct.columns.len(), 2);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_empty_query() {
        let result = Parser::parse("");
        assert!(matches!(result, Err(ParseError::EmptyQuery)));
    }

    #[test]
    fn test_column_ref() {
        let col = ColumnRef::new("id");
        assert_eq!(col.to_string(), "id");
        assert!(!col.is_qualified());

        let col = ColumnRef::qualified("users", "id");
        assert_eq!(col.to_string(), "users.id");
        assert!(col.is_qualified());
    }

    #[test]
    fn test_table_ref() {
        let table = TableRef::new("users");
        assert_eq!(table.to_string(), "users");

        let table = TableRef::with_schema("public", "users");
        assert_eq!(table.to_string(), "public.users");

        let table = TableRef::new("users").with_alias("u");
        assert_eq!(table.to_string(), "users AS u");
        assert_eq!(table.effective_name(), "u");
    }
}
