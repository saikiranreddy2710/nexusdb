//! Session management for database connections.
//!
//! A Session represents a single client connection to the database.
//! It maintains connection-level state such as the current transaction,
//! session variables, and prepared statements.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus_common::types::TxnId;
use nexus_mvcc::IsolationLevel;
use nexus_sql::executor::{Row, Value};
use nexus_sql::logical::{build_plan, Field, MemoryCatalog, Schema};
use nexus_sql::optimizer::{Optimizer, OptimizerConfig};
use nexus_sql::parser::{
    BinaryOperator, ColumnConstraint, Expr, InsertSource, Literal, Parser, Statement, UnaryOperator,
};
use nexus_sql::physical::{ExecutionContext, PhysicalPlanner};
use nexus_sql::storage::{StorageEngine, TableInfo};

use super::error::{DatabaseError, DatabaseResult};
use super::result::{ExecuteResult, StatementResult};

/// Unique session identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    /// Creates a new session ID.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the numeric ID.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "session_{}", self.0)
    }
}

/// Session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Session is idle (no active transaction).
    Idle,
    /// Session has an active transaction.
    InTransaction,
    /// Session is in a failed transaction (must rollback).
    Failed,
    /// Session is closed.
    Closed,
}

/// Session configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Default isolation level.
    pub default_isolation: IsolationLevel,
    /// Autocommit mode.
    pub autocommit: bool,
    /// Query timeout.
    pub query_timeout: Duration,
    /// Maximum rows to return.
    pub max_rows: Option<usize>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            default_isolation: IsolationLevel::SnapshotIsolation,
            autocommit: true,
            query_timeout: Duration::from_secs(300),
            max_rows: None,
        }
    }
}

/// A database session representing a client connection.
pub struct Session {
    /// Session ID.
    id: SessionId,
    /// Session state.
    state: SessionState,
    /// Configuration.
    config: SessionConfig,
    /// Current transaction ID (if in transaction).
    current_txn: Option<TxnId>,
    /// Storage engine.
    storage: Arc<StorageEngine>,
    /// Session variables.
    variables: HashMap<String, String>,
    /// When the session was created.
    created_at: Instant,
    /// Statement counter.
    statement_count: u64,
}

impl Session {
    /// Creates a new session.
    pub fn new(id: SessionId, storage: Arc<StorageEngine>, config: SessionConfig) -> Self {
        Self {
            id,
            state: SessionState::Idle,
            config,
            current_txn: None,
            storage,
            variables: HashMap::new(),
            created_at: Instant::now(),
            statement_count: 0,
        }
    }

    /// Returns the session ID.
    pub fn id(&self) -> SessionId {
        self.id
    }

    /// Returns the current state.
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Returns true if in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.current_txn.is_some()
    }

    /// Returns the current transaction ID if any.
    pub fn transaction_id(&self) -> Option<TxnId> {
        self.current_txn
    }

    /// Returns the storage engine.
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Returns session uptime.
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Returns the statement count.
    pub fn statement_count(&self) -> u64 {
        self.statement_count
    }

    // =========================================================================
    // Transaction Control
    // =========================================================================

    /// Begins a new transaction.
    pub fn begin(&mut self) -> DatabaseResult<()> {
        if self.current_txn.is_some() {
            return Err(DatabaseError::TransactionError(
                "transaction already in progress".to_string(),
            ));
        }

        // For now, we use a simple transaction ID
        // In the future, this will integrate with TransactionManager
        let txn_id = TxnId::new(self.id.as_u64() * 1_000_000 + self.statement_count);
        self.current_txn = Some(txn_id);
        self.state = SessionState::InTransaction;

        Ok(())
    }

    /// Commits the current transaction.
    pub fn commit(&mut self) -> DatabaseResult<()> {
        if self.current_txn.is_none() {
            return Err(DatabaseError::TransactionError(
                "no transaction in progress".to_string(),
            ));
        }

        // For now, just clear the transaction state
        // In the future, this will call TransactionManager::commit()
        self.current_txn = None;
        self.state = SessionState::Idle;

        Ok(())
    }

    /// Rolls back the current transaction.
    pub fn rollback(&mut self) -> DatabaseResult<()> {
        if self.current_txn.is_none() {
            // Rollback on idle is a no-op
            return Ok(());
        }

        // For now, just clear the transaction state
        // In the future, this will call TransactionManager::abort()
        self.current_txn = None;
        self.state = SessionState::Idle;

        Ok(())
    }

    // =========================================================================
    // SQL Execution
    // =========================================================================

    /// Executes a SQL statement.
    pub fn execute(&mut self, sql: &str) -> DatabaseResult<StatementResult> {
        self.statement_count += 1;
        let start = Instant::now();

        // Parse the SQL
        let statement = Parser::parse_one(sql)?;

        // Execute based on statement type
        let result = self.execute_statement(&statement, start)?;

        Ok(result)
    }

    /// Executes multiple SQL statements.
    pub fn execute_batch(&mut self, sql: &str) -> DatabaseResult<Vec<StatementResult>> {
        let statements = Parser::parse(sql)?;

        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let start = Instant::now();
            results.push(self.execute_statement(&statement, start)?);
        }

        Ok(results)
    }

    /// Executes a parsed statement.
    fn execute_statement(
        &mut self,
        statement: &Statement,
        start: Instant,
    ) -> DatabaseResult<StatementResult> {
        match statement {
            // Transaction control
            Statement::Begin => {
                self.begin()?;
                Ok(StatementResult::transaction("BEGIN"))
            }
            Statement::Commit => {
                self.commit()?;
                Ok(StatementResult::transaction("COMMIT"))
            }
            Statement::Rollback => {
                self.rollback()?;
                Ok(StatementResult::transaction("ROLLBACK"))
            }

            // DDL
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::DropTable(drop) => self.execute_drop_table(drop),

            // DML
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),

            // Query
            Statement::Select(_) => self.execute_query(statement, start),

            _ => Err(DatabaseError::NotImplemented(format!(
                "statement type not yet supported"
            ))),
        }
    }

    /// Executes CREATE TABLE.
    fn execute_create_table(
        &self,
        create: &nexus_sql::parser::CreateTableStatement,
    ) -> DatabaseResult<StatementResult> {
        let name = &create.name.table;

        // Check if table exists
        if self.storage.table_exists(name) {
            if create.if_not_exists {
                return Ok(StatementResult::ddl("CREATE TABLE"));
            }
            return Err(DatabaseError::StorageError(
                nexus_sql::storage::StorageError::TableExists(name.clone()),
            ));
        }

        // Build schema from columns
        let fields: Vec<Field> = create
            .columns
            .iter()
            .map(|col| {
                let nullable = col.nullable;
                if nullable {
                    Field::nullable(&col.name, col.data_type.clone())
                } else {
                    Field::not_null(&col.name, col.data_type.clone())
                }
            })
            .collect();

        let schema = Schema::new(fields);

        // Find primary key columns
        let primary_key: Vec<usize> = create
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| {
                col.constraints
                    .iter()
                    .any(|c| matches!(c, ColumnConstraint::PrimaryKey))
            })
            .map(|(i, _)| i)
            .collect();

        // Create table info
        let mut info = TableInfo::new(name.clone(), schema);
        if !primary_key.is_empty() {
            info = info.with_primary_key(primary_key);
        }

        self.storage.create_table(info)?;

        Ok(StatementResult::ddl("CREATE TABLE"))
    }

    /// Executes DROP TABLE.
    fn execute_drop_table(
        &self,
        drop: &nexus_sql::parser::DropTableStatement,
    ) -> DatabaseResult<StatementResult> {
        for table_ref in &drop.names {
            let name = &table_ref.table;
            if !self.storage.table_exists(name) {
                if drop.if_exists {
                    continue;
                }
                return Err(DatabaseError::StorageError(
                    nexus_sql::storage::StorageError::TableNotFound(name.clone()),
                ));
            }
            self.storage.drop_table(name)?;
        }
        Ok(StatementResult::ddl("DROP TABLE"))
    }

    /// Executes INSERT.
    fn execute_insert(
        &self,
        insert: &nexus_sql::parser::InsertStatement,
    ) -> DatabaseResult<StatementResult> {
        let table_name = &insert.table.table;
        let table_info = self.storage.get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        let schema = &table_info.schema;

        // Determine column order
        let column_order: Vec<usize> = if insert.columns.is_empty() {
            // All columns in schema order
            (0..schema.fields().len()).collect()
        } else {
            // Map column names to indices
            insert
                .columns
                .iter()
                .map(|name| {
                    schema.index_of(name).ok_or_else(|| {
                        DatabaseError::ExecutionError(format!("unknown column: {}", name))
                    })
                })
                .collect::<DatabaseResult<Vec<_>>>()?
        };

        // Convert values to rows
        let value_rows = match &insert.values {
            InsertSource::Values(rows) => rows,
            InsertSource::Query(_) => {
                return Err(DatabaseError::NotImplemented(
                    "INSERT from SELECT query".to_string(),
                ))
            }
            InsertSource::DefaultValues => {
                return Err(DatabaseError::NotImplemented(
                    "INSERT DEFAULT VALUES".to_string(),
                ))
            }
        };

        let mut rows = Vec::with_capacity(value_rows.len());
        for value_row in value_rows {
            let mut row_values = vec![Value::Null; schema.fields().len()];

            for (i, expr) in value_row.iter().enumerate() {
                if i < column_order.len() {
                    let col_idx = column_order[i];
                    row_values[col_idx] = self.eval_literal(expr)?;
                }
            }

            rows.push(Row::new(row_values));
        }

        let count = self.storage.execute_insert(table_name, rows)?;
        Ok(StatementResult::Insert {
            rows_affected: count,
        })
    }

    /// Executes UPDATE.
    fn execute_update(
        &self,
        update: &nexus_sql::parser::UpdateStatement,
    ) -> DatabaseResult<StatementResult> {
        let table_name = &update.table.table;
        let table_info = self.storage.get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // Get all rows (we'll filter later)
        let batches = self.storage.execute_scan(table_name)?;
        let all_rows: Vec<Row> = batches.iter().flat_map(|b| b.rows()).collect();

        let mut updated_count = 0u64;

        for row in all_rows {
            // Check WHERE clause
            if let Some(where_expr) = &update.where_clause {
                if !self.eval_where(&row, where_expr, &table_info.schema)? {
                    continue;
                }
            }

            // Apply updates
            let mut new_row = row.clone();
            for assignment in &update.assignments {
                let col_name = &assignment.column.column;
                if let Some(col_idx) = table_info.schema.index_of(col_name) {
                    new_row.set(col_idx, self.eval_literal(&assignment.value)?);
                }
            }

            // Update the row
            if self.storage.execute_update(table_name, new_row)? {
                updated_count += 1;
            }
        }

        Ok(StatementResult::Update {
            rows_affected: updated_count,
        })
    }

    /// Executes DELETE.
    fn execute_delete(
        &self,
        delete: &nexus_sql::parser::DeleteStatement,
    ) -> DatabaseResult<StatementResult> {
        let table_name = &delete.table.table;
        let table_info = self.storage.get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // Get all rows
        let batches = self.storage.execute_scan(table_name)?;
        let all_rows: Vec<Row> = batches.iter().flat_map(|b| b.rows()).collect();

        let pk_col_names = table_info.primary_key_columns();
        let pk_col_indices: Vec<usize> = pk_col_names
            .iter()
            .filter_map(|name| table_info.schema.index_of(name))
            .collect();
        let mut deleted_count = 0u64;

        for row in all_rows {
            // Check WHERE clause
            if let Some(where_expr) = &delete.where_clause {
                if !self.eval_where(&row, where_expr, &table_info.schema)? {
                    continue;
                }
            }

            // Get primary key values
            let key_values: Vec<Value> = pk_col_indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();

            if self.storage.execute_delete(table_name, &key_values)? {
                deleted_count += 1;
            }
        }

        Ok(StatementResult::Delete {
            rows_affected: deleted_count,
        })
    }

    /// Executes a SELECT query.
    fn execute_query(
        &self,
        statement: &Statement,
        start: Instant,
    ) -> DatabaseResult<StatementResult> {
        // Build a catalog from current storage
        let mut catalog = MemoryCatalog::new();
        for table_name in self.storage.list_tables() {
            if let Some(table_info) = self.storage.get_table_info(&table_name) {
                catalog.add_table(nexus_sql::logical::TableMeta::new(
                    table_name.clone(),
                    (*table_info.schema).clone(),
                ));
            }
        }

        // Build logical plan
        let logical_plan =
            build_plan(statement, &catalog).map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Optimize
        let optimizer = Optimizer::new(OptimizerConfig::default());
        let optimized = optimizer
            .optimize(logical_plan)
            .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Create physical plan
        let ctx = ExecutionContext::default();
        let physical_planner = PhysicalPlanner::new(&ctx);
        let physical_plan = physical_planner
            .create_physical_plan(&optimized.root)
            .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Execute
        let mut executor = nexus_sql::executor::QueryExecutor::new(ctx);

        // Register tables with data
        for table_name in self.storage.list_tables() {
            if let Ok(batches) = self.storage.execute_scan(&table_name) {
                executor.register_table(table_name, batches);
            }
        }

        let result = executor.execute(&physical_plan)?;

        let elapsed = start.elapsed();
        let execute_result = ExecuteResult::from_batches(result.schema, result.batches, elapsed);

        Ok(StatementResult::Query(execute_result))
    }

    // =========================================================================
    // Expression Evaluation Helpers
    // =========================================================================

    /// Evaluates a literal expression to a Value.
    fn eval_literal(&self, expr: &Expr) -> DatabaseResult<Value> {
        match expr {
            Expr::Literal(lit) => Ok(Value::from_literal(lit)),
            Expr::UnaryOp { op, expr } => {
                // Handle negative numbers
                if let UnaryOperator::Minus = op {
                    if let Expr::Literal(Literal::Integer(n)) = expr.as_ref() {
                        return Ok(Value::Int((-n) as i32));
                    }
                    if let Expr::Literal(Literal::Float(f)) = expr.as_ref() {
                        return Ok(Value::Double(-f));
                    }
                }
                Err(DatabaseError::ExecutionError(
                    "complex expression not supported in VALUES".to_string(),
                ))
            }
            _ => Err(DatabaseError::ExecutionError(
                "complex expression not supported in VALUES".to_string(),
            )),
        }
    }

    /// Evaluates a WHERE clause against a row.
    fn eval_where(&self, row: &Row, expr: &Expr, schema: &Schema) -> DatabaseResult<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Handle AND/OR specially
                match op {
                    BinaryOperator::And => {
                        return Ok(self.eval_where(row, left, schema)?
                            && self.eval_where(row, right, schema)?);
                    }
                    BinaryOperator::Or => {
                        return Ok(self.eval_where(row, left, schema)?
                            || self.eval_where(row, right, schema)?);
                    }
                    _ => {}
                }

                let left_val = self.eval_expr(row, left, schema)?;
                let right_val = self.eval_expr(row, right, schema)?;

                Ok(match op {
                    BinaryOperator::Eq => left_val == right_val,
                    BinaryOperator::NotEq => left_val != right_val,
                    BinaryOperator::Lt => left_val < right_val,
                    BinaryOperator::LtEq => left_val <= right_val,
                    BinaryOperator::Gt => left_val > right_val,
                    BinaryOperator::GtEq => left_val >= right_val,
                    _ => {
                        return Err(DatabaseError::ExecutionError(format!(
                            "unsupported operator in WHERE: {:?}",
                            op
                        )))
                    }
                })
            }
            Expr::IsNull(e) => {
                let val = self.eval_expr(row, e, schema)?;
                Ok(val == Value::Null)
            }
            Expr::IsNotNull(e) => {
                let val = self.eval_expr(row, e, schema)?;
                Ok(val != Value::Null)
            }
            _ => Err(DatabaseError::ExecutionError(
                "unsupported WHERE expression".to_string(),
            )),
        }
    }

    /// Evaluates an expression against a row.
    fn eval_expr(&self, row: &Row, expr: &Expr, schema: &Schema) -> DatabaseResult<Value> {
        match expr {
            Expr::Literal(lit) => Ok(Value::from_literal(lit)),
            Expr::Column(col_ref) => {
                if let Some(idx) = schema.index_of(&col_ref.column) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Err(DatabaseError::ExecutionError(format!(
                        "unknown column: {}",
                        col_ref.column
                    )))
                }
            }
            _ => Err(DatabaseError::ExecutionError(
                "complex expression in WHERE not fully supported".to_string(),
            )),
        }
    }

    // =========================================================================
    // Session Variables
    // =========================================================================

    /// Sets a session variable.
    pub fn set_variable(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.variables.insert(name.into(), value.into());
    }

    /// Gets a session variable.
    pub fn get_variable(&self, name: &str) -> Option<&str> {
        self.variables.get(name).map(|s| s.as_str())
    }

    /// Closes the session.
    pub fn close(&mut self) {
        if self.current_txn.is_some() {
            let _ = self.rollback();
        }
        self.state = SessionState::Closed;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_session() -> Session {
        let storage = Arc::new(StorageEngine::new());
        Session::new(SessionId::new(1), storage, SessionConfig::default())
    }

    #[test]
    fn test_session_create_table() {
        let mut session = create_session();

        let result = session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        assert!(matches!(result, StatementResult::Ddl { .. }));

        assert!(session.storage.table_exists("users"));
    }

    #[test]
    fn test_session_insert() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        let result = session
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 1);
        } else {
            panic!("expected Insert result");
        }
    }

    #[test]
    fn test_session_select() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (2, 'Bob')")
            .unwrap();

        let result = session.execute("SELECT * FROM users").unwrap();

        if let StatementResult::Query(query_result) = result {
            assert_eq!(query_result.total_rows, 2);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_transaction() {
        let mut session = create_session();

        session.execute("BEGIN").unwrap();
        assert!(session.in_transaction());

        session.execute("COMMIT").unwrap();
        assert!(!session.in_transaction());
    }

    #[test]
    fn test_session_rollback() {
        let mut session = create_session();

        session.execute("BEGIN").unwrap();
        session.execute("ROLLBACK").unwrap();
        assert!(!session.in_transaction());
    }

    #[test]
    fn test_session_update() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();

        let result = session
            .execute("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();

        if let StatementResult::Update { rows_affected } = result {
            assert_eq!(rows_affected, 1);
        } else {
            panic!("expected Update result");
        }

        // Verify update
        let query_result = session.execute("SELECT * FROM users").unwrap();
        if let StatementResult::Query(result) = query_result {
            let rows = result.rows();
            assert_eq!(rows[0].get(1), Some(&Value::String("Bob".to_string())));
        }
    }

    #[test]
    fn test_session_delete() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (2, 'Bob')")
            .unwrap();

        let result = session.execute("DELETE FROM users WHERE id = 1").unwrap();

        if let StatementResult::Delete { rows_affected } = result {
            assert_eq!(rows_affected, 1);
        } else {
            panic!("expected Delete result");
        }

        // Verify delete
        let query_result = session.execute("SELECT * FROM users").unwrap();
        if let StatementResult::Query(result) = query_result {
            assert_eq!(result.total_rows, 1);
        }
    }

    #[test]
    fn test_session_select_with_where() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (2, 'Bob')")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (3, 'Charlie')")
            .unwrap();

        // Test SELECT with WHERE clause
        let result = session.execute("SELECT * FROM users WHERE id = 1").unwrap();

        if let StatementResult::Query(query_result) = result {
            assert_eq!(
                query_result.total_rows, 1,
                "Expected 1 row for id = 1, got {}",
                query_result.total_rows
            );
            let rows = query_result.rows();
            assert_eq!(rows.len(), 1);
            // First column should be id = 1
            assert_eq!(rows[0].get(0), Some(&Value::Int(1)));
        } else {
            panic!("expected Query result");
        }

        // Test SELECT with WHERE clause and partial projection
        let result = session
            .execute("SELECT name FROM users WHERE id = 2")
            .unwrap();

        if let StatementResult::Query(query_result) = result {
            assert_eq!(
                query_result.total_rows, 1,
                "Expected 1 row for id = 2, got {}",
                query_result.total_rows
            );
            let rows = query_result.rows();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&Value::String("Bob".to_string())));
        } else {
            panic!("expected Query result");
        }
    }
}
