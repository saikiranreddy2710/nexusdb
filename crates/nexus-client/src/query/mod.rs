//! Query builder for fluent SQL construction.
//!
//! Provides a type-safe way to build SQL queries with parameter binding.

use std::fmt::Write;

use super::client::{Client, QueryResult, Value};
use super::error::{ClientError, ClientResult};

/// A fluent query builder.
pub struct QueryBuilder<'a> {
    /// Reference to the client.
    client: &'a Client,
    /// SQL query parts.
    parts: Vec<QueryPart>,
    /// Parameter counter.
    param_count: usize,
}

/// A part of the query being built.
#[derive(Debug, Clone)]
enum QueryPart {
    /// Raw SQL text.
    Sql(String),
    /// A parameter placeholder.
    Param(Value),
}

impl<'a> QueryBuilder<'a> {
    /// Creates a new query builder.
    pub fn new(client: &'a Client) -> Self {
        Self {
            client,
            parts: Vec::new(),
            param_count: 0,
        }
    }

    /// Appends raw SQL to the query.
    pub fn sql(mut self, sql: impl AsRef<str>) -> Self {
        self.parts.push(QueryPart::Sql(sql.as_ref().to_string()));
        self
    }

    /// Appends a parameter to the query.
    pub fn bind<T: Into<Value>>(mut self, value: T) -> Self {
        self.param_count += 1;
        self.parts.push(QueryPart::Param(value.into()));
        self
    }

    /// Appends SELECT clause.
    pub fn select(self, columns: &[&str]) -> Self {
        self.sql("SELECT ").sql(columns.join(", "))
    }

    /// Appends SELECT * clause.
    pub fn select_all(self) -> Self {
        self.sql("SELECT *")
    }

    /// Appends FROM clause.
    pub fn from(self, table: &str) -> Self {
        self.sql(" FROM ").sql(table)
    }

    /// Appends WHERE clause.
    pub fn where_clause(self, condition: &str) -> Self {
        self.sql(" WHERE ").sql(condition)
    }

    /// Appends WHERE with equality condition.
    pub fn where_eq<T: Into<Value>>(self, column: &str, value: T) -> Self {
        self.sql(" WHERE ")
            .sql(column)
            .sql(" = ")
            .bind(value)
    }

    /// Appends AND condition.
    pub fn and(self, condition: &str) -> Self {
        self.sql(" AND ").sql(condition)
    }

    /// Appends AND with equality condition.
    pub fn and_eq<T: Into<Value>>(self, column: &str, value: T) -> Self {
        self.sql(" AND ").sql(column).sql(" = ").bind(value)
    }

    /// Appends OR condition.
    pub fn or(self, condition: &str) -> Self {
        self.sql(" OR ").sql(condition)
    }

    /// Appends OR with equality condition.
    pub fn or_eq<T: Into<Value>>(self, column: &str, value: T) -> Self {
        self.sql(" OR ").sql(column).sql(" = ").bind(value)
    }

    /// Appends ORDER BY clause.
    pub fn order_by(self, column: &str) -> Self {
        self.sql(" ORDER BY ").sql(column)
    }

    /// Appends ORDER BY DESC clause.
    pub fn order_by_desc(self, column: &str) -> Self {
        self.sql(" ORDER BY ").sql(column).sql(" DESC")
    }

    /// Appends LIMIT clause.
    pub fn limit(self, limit: u64) -> Self {
        self.sql(" LIMIT ").sql(limit.to_string())
    }

    /// Appends OFFSET clause.
    pub fn offset(self, offset: u64) -> Self {
        self.sql(" OFFSET ").sql(offset.to_string())
    }

    /// Appends INSERT INTO clause.
    pub fn insert_into(self, table: &str) -> Self {
        self.sql("INSERT INTO ").sql(table)
    }

    /// Appends column list for INSERT.
    pub fn columns(self, columns: &[&str]) -> Self {
        self.sql(" (").sql(columns.join(", ")).sql(")")
    }

    /// Appends VALUES clause.
    pub fn values(mut self, values: &[Value]) -> Self {
        self = self.sql(" VALUES (");
        for (i, value) in values.iter().enumerate() {
            if i > 0 {
                self = self.sql(", ");
            }
            self = self.bind(value.clone());
        }
        self.sql(")")
    }

    /// Appends UPDATE clause.
    pub fn update(self, table: &str) -> Self {
        self.sql("UPDATE ").sql(table)
    }

    /// Appends SET clause.
    pub fn set<T: Into<Value>>(self, column: &str, value: T) -> Self {
        self.sql(" SET ")
            .sql(column)
            .sql(" = ")
            .bind(value)
    }

    /// Appends additional SET assignment.
    pub fn set_more<T: Into<Value>>(self, column: &str, value: T) -> Self {
        self.sql(", ").sql(column).sql(" = ").bind(value)
    }

    /// Appends DELETE FROM clause.
    pub fn delete_from(self, table: &str) -> Self {
        self.sql("DELETE FROM ").sql(table)
    }

    /// Appends JOIN clause.
    pub fn join(self, table: &str, condition: &str) -> Self {
        self.sql(" JOIN ")
            .sql(table)
            .sql(" ON ")
            .sql(condition)
    }

    /// Appends LEFT JOIN clause.
    pub fn left_join(self, table: &str, condition: &str) -> Self {
        self.sql(" LEFT JOIN ")
            .sql(table)
            .sql(" ON ")
            .sql(condition)
    }

    /// Appends RIGHT JOIN clause.
    pub fn right_join(self, table: &str, condition: &str) -> Self {
        self.sql(" RIGHT JOIN ")
            .sql(table)
            .sql(" ON ")
            .sql(condition)
    }

    /// Appends GROUP BY clause.
    pub fn group_by(self, columns: &[&str]) -> Self {
        self.sql(" GROUP BY ").sql(columns.join(", "))
    }

    /// Appends HAVING clause.
    pub fn having(self, condition: &str) -> Self {
        self.sql(" HAVING ").sql(condition)
    }

    /// Builds the final SQL string.
    pub fn build(&self) -> String {
        let mut sql = String::new();
        let mut param_index = 1;

        for part in &self.parts {
            match part {
                QueryPart::Sql(s) => sql.push_str(s),
                QueryPart::Param(v) => {
                    // Format the value inline for now
                    // In a real implementation, we'd use placeholders
                    let _ = write!(sql, "{}", format_value(v));
                    param_index += 1;
                }
            }
        }

        // Silence unused variable warning
        let _ = param_index;
        sql
    }

    /// Builds and returns SQL with parameters separated.
    pub fn build_with_params(&self) -> (String, Vec<Value>) {
        let mut sql = String::new();
        let mut params = Vec::new();
        let mut param_index = 1;

        for part in &self.parts {
            match part {
                QueryPart::Sql(s) => sql.push_str(s),
                QueryPart::Param(v) => {
                    let _ = write!(sql, "${}", param_index);
                    params.push(v.clone());
                    param_index += 1;
                }
            }
        }

        (sql, params)
    }

    /// Executes the query.
    pub async fn execute(self) -> ClientResult<QueryResult> {
        let sql = self.build();
        self.client.execute(&sql).await
    }

    /// Executes the query and returns a single value.
    pub async fn fetch_one<T: super::client::FromValue>(self) -> ClientResult<Option<T>> {
        let sql = self.build();
        self.client.query_one(&sql).await
    }

    /// Executes the query and returns all rows.
    pub async fn fetch_all(self) -> ClientResult<Vec<Vec<Value>>> {
        let sql = self.build();
        self.client.query_all(&sql).await
    }
}

/// Formats a value for SQL.
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => format!("'\\x{}'", hex_encode(b)),
    }
}

/// Hex encodes bytes.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// Implement Into<Value> for common types
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}

/// A prepared statement that can be executed multiple times.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The SQL template with placeholders.
    sql: String,
    /// Number of parameters expected.
    param_count: usize,
}

impl PreparedStatement {
    /// Creates a new prepared statement.
    pub fn new(sql: impl Into<String>) -> Self {
        let sql = sql.into();
        let param_count = sql.matches('$').count();
        Self { sql, param_count }
    }

    /// Returns the SQL template.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns the expected parameter count.
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// Binds parameters and returns the final SQL.
    pub fn bind(&self, params: &[Value]) -> ClientResult<String> {
        if params.len() != self.param_count {
            return Err(ClientError::QueryFailed(format!(
                "expected {} parameters, got {}",
                self.param_count,
                params.len()
            )));
        }

        let mut sql = self.sql.clone();
        for (i, param) in params.iter().enumerate().rev() {
            let placeholder = format!("${}", i + 1);
            sql = sql.replace(&placeholder, &format_value(param));
        }

        Ok(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock client for testing
    fn mock_client() -> Client {
        Client::connect_default().unwrap()
    }

    #[test]
    fn test_query_builder_select() {
        let client = mock_client();
        let sql = client
            .query()
            .select(&["id", "name"])
            .from("users")
            .where_clause("active = TRUE")
            .order_by("name")
            .limit(10)
            .build();

        assert_eq!(
            sql,
            "SELECT id, name FROM users WHERE active = TRUE ORDER BY name LIMIT 10"
        );
    }

    #[test]
    fn test_query_builder_select_all() {
        let client = mock_client();
        let sql = client.query().select_all().from("users").build();

        assert_eq!(sql, "SELECT * FROM users");
    }

    #[test]
    fn test_query_builder_with_params() {
        let client = mock_client();
        let sql = client
            .query()
            .select_all()
            .from("users")
            .where_eq("id", 42)
            .build();

        assert_eq!(sql, "SELECT * FROM users WHERE id = 42");
    }

    #[test]
    fn test_query_builder_insert() {
        let client = mock_client();
        let sql = client
            .query()
            .insert_into("users")
            .columns(&["name", "email"])
            .values(&[Value::String("Alice".to_string()), Value::String("alice@example.com".to_string())])
            .build();

        assert_eq!(
            sql,
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
        );
    }

    #[test]
    fn test_query_builder_update() {
        let client = mock_client();
        let sql = client
            .query()
            .update("users")
            .set("name", "Bob")
            .where_eq("id", 1)
            .build();

        assert_eq!(sql, "UPDATE users SET name = 'Bob' WHERE id = 1");
    }

    #[test]
    fn test_query_builder_delete() {
        let client = mock_client();
        let sql = client
            .query()
            .delete_from("users")
            .where_eq("id", 1)
            .build();

        assert_eq!(sql, "DELETE FROM users WHERE id = 1");
    }

    #[test]
    fn test_query_builder_join() {
        let client = mock_client();
        let sql = client
            .query()
            .select(&["u.name", "o.total"])
            .from("users u")
            .left_join("orders o", "u.id = o.user_id")
            .where_eq("u.id", 1)
            .build();

        assert!(sql.contains("LEFT JOIN orders o ON u.id = o.user_id"));
    }

    #[test]
    fn test_query_builder_build_with_params() {
        let client = mock_client();
        let (sql, params) = client
            .query()
            .select_all()
            .from("users")
            .where_eq("id", 42)
            .and_eq("active", true)
            .build_with_params();

        assert_eq!(sql, "SELECT * FROM users WHERE id = $1 AND active = $2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_prepared_statement() {
        let stmt = PreparedStatement::new("SELECT * FROM users WHERE id = $1");
        assert_eq!(stmt.param_count(), 1);

        let sql = stmt.bind(&[Value::Integer(42)]).unwrap();
        assert_eq!(sql, "SELECT * FROM users WHERE id = 42");
    }

    #[test]
    fn test_prepared_statement_wrong_params() {
        let stmt = PreparedStatement::new("SELECT * FROM users WHERE id = $1 AND name = $2");
        let result = stmt.bind(&[Value::Integer(42)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_formatting() {
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&Value::Boolean(true)), "TRUE");
        assert_eq!(format_value(&Value::Integer(42)), "42");
        assert_eq!(format_value(&Value::Float(3.14)), "3.14");
        assert_eq!(format_value(&Value::String("hello".to_string())), "'hello'");
        assert_eq!(
            format_value(&Value::String("it's".to_string())),
            "'it''s'"
        );
    }

    #[test]
    fn test_value_from_types() {
        let _: Value = 42i32.into();
        let _: Value = 42i64.into();
        let _: Value = 3.14f64.into();
        let _: Value = true.into();
        let _: Value = "hello".into();
        let _: Value = String::from("world").into();
        let _: Value = Some(42).into();
        let _: Value = None::<i32>.into();
    }
}
