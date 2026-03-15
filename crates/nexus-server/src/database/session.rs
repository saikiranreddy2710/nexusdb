//! Session management for database connections.
//!
//! A Session represents a single client connection to the database.
//! It maintains connection-level state such as the current transaction,
//! session variables, and prepared statements.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus_cache::plan_cache::{PlanCache, PlanCacheConfig};
use nexus_cache::result_cache::{ResultCache, ResultCacheConfig, ResultCacheKey};
use nexus_common::types::TxnId;
use nexus_mvcc::IsolationLevel;
use nexus_security::audit::{AuditAction, AuditLog};
use nexus_security::authn::Identity;
use nexus_security::authz::{Authorizer, Privilege};
use nexus_sql::executor::{Row, Value};
use nexus_sql::logical::{build_plan, Field, MemoryCatalog, Schema};
use nexus_sql::optimizer::{Optimizer, OptimizerConfig};
use nexus_sql::parser::{
    BinaryOperator, ColumnConstraint, Expr, InsertSource, Literal, Parser, Statement, UnaryOperator,
};
use nexus_sql::physical::{ExecutionContext, PhysicalPlan, PhysicalPlanner};
use nexus_sql::storage::{StorageEngine, TableInfo};

use super::engine::Database;
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
    #[allow(dead_code)]
    config: SessionConfig,
    /// Current transaction ID (if in transaction).
    current_txn: Option<TxnId>,
    /// Reference to the database engine (for multi-database).
    database: Arc<Database>,
    /// Current database name for this session.
    current_database: String,
    /// Session variables.
    variables: HashMap<String, String>,
    /// When the session was created.
    created_at: Instant,
    /// Statement counter.
    statement_count: u64,
    /// Query plan cache for prepared statements and repeated queries.
    plan_cache: PlanCache<PhysicalPlan>,
    /// Query result cache for SELECT queries.
    result_cache: Arc<ResultCache<ExecuteResult>>,
    /// Authenticated identity for this session.
    identity: Identity,
    /// Reference to authorizer for permission checks.
    authorizer: Arc<Authorizer>,
    /// Reference to audit log for recording events.
    audit_log: Arc<AuditLog>,
}

impl Session {
    /// Creates a new session attached to the given database.
    pub fn new(
        id: SessionId,
        database: Arc<Database>,
        current_database: String,
        config: SessionConfig,
    ) -> Self {
        let authorizer = Arc::clone(database.authorizer());
        let audit_log = Arc::clone(database.audit_log());
        Self {
            id,
            state: SessionState::Idle,
            config,
            current_txn: None,
            database,
            current_database,
            variables: HashMap::new(),
            created_at: Instant::now(),
            statement_count: 0,
            plan_cache: PlanCache::new(PlanCacheConfig::with_capacity(100)),
            result_cache: Arc::new(ResultCache::new(ResultCacheConfig::new(500, 60))),
            identity: Identity::system(), // default system identity; overridden for authenticated sessions
            authorizer,
            audit_log,
        }
    }

    /// Creates a new session with a specific authenticated identity.
    pub fn new_with_identity(
        id: SessionId,
        database: Arc<Database>,
        current_database: String,
        config: SessionConfig,
        identity: Identity,
    ) -> Self {
        let authorizer = Arc::clone(database.authorizer());
        let audit_log = Arc::clone(database.audit_log());
        Self {
            id,
            state: SessionState::Idle,
            config,
            current_txn: None,
            database,
            current_database,
            variables: HashMap::new(),
            created_at: Instant::now(),
            statement_count: 0,
            plan_cache: PlanCache::new(PlanCacheConfig::with_capacity(100)),
            result_cache: Arc::new(ResultCache::new(ResultCacheConfig::new(500, 60))),
            identity,
            authorizer,
            audit_log,
        }
    }

    /// Returns the authenticated identity for this session.
    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Sets the identity for this session (used after gRPC authentication).
    pub fn set_identity(&mut self, identity: Identity) {
        self.identity = identity;
    }

    /// Returns the storage engine for the current database.
    fn storage(&self) -> Arc<StorageEngine> {
        self.database.get_or_create_storage(&self.current_database)
    }

    /// Switches the session to the given database.
    pub fn set_current_database(&mut self, name: &str) {
        self.current_database = name.to_string();
    }

    /// Returns the current database name.
    pub fn current_database(&self) -> &str {
        &self.current_database
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

    /// Returns the storage engine for the current database (for tests that need direct access).
    pub fn storage_ref(&self) -> Arc<StorageEngine> {
        self.storage()
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

        // Authorize the statement before execution
        self.authorize_statement(&statement)?;

        // Execute based on statement type
        let result = self.execute_statement(&statement, sql, start);

        // Audit the query
        let success = result.is_ok();
        let action = Self::statement_audit_action(&statement);
        self.audit_log.record(
            &self.identity.username,
            action,
            Self::statement_target(&statement),
            Some(sql.to_string()),
            None,
            success,
        );

        result
    }

    /// Checks whether the current identity is authorized to execute the given statement.
    ///
    /// Maps each SQL statement type to the required privilege and calls the authorizer.
    /// If the authorizer is in permissive mode (enforce=false), all checks pass.
    fn authorize_statement(&self, statement: &Statement) -> DatabaseResult<()> {
        let db = &self.current_database;

        let decision = match statement {
            // SELECT / SET OPERATION requires Select privilege on all referenced tables
            Statement::Select(select) => {
                let tables = Self::extract_table_names_from_statement(statement);
                // Check all referenced tables; deny if any fails
                for table in &tables {
                    let d =
                        self.authorizer
                            .check_table(&self.identity, db, table, Privilege::Select);
                    if !d.is_allowed() {
                        return Err(DatabaseError::AuthorizationError(format!(
                            "user '{}' lacks SELECT on {}.{}",
                            self.identity.username, db, table
                        )));
                    }
                }
                // If no FROM tables (e.g. SELECT 1), allow
                if tables.is_empty() && !select.from.is_empty() {
                    // Table names were somehow not extracted; fallback to db-level check
                    self.authorizer
                        .check_database(&self.identity, db, Privilege::Select)
                } else {
                    nexus_security::AccessDecision::Allow
                }
            }
            Statement::SetOperation { left, right, .. } => {
                // Authorize both sides recursively
                self.authorize_statement(left)?;
                self.authorize_statement(right)?;
                return Ok(());
            }

            // INSERT requires Insert privilege
            Statement::Insert(insert) => self.authorizer.check_table(
                &self.identity,
                db,
                &insert.table.table,
                Privilege::Insert,
            ),

            // UPDATE requires Update privilege
            Statement::Update(update) => self.authorizer.check_table(
                &self.identity,
                db,
                &update.table.table,
                Privilege::Update,
            ),

            // DELETE requires Delete privilege
            Statement::Delete(delete) => self.authorizer.check_table(
                &self.identity,
                db,
                &delete.table.table,
                Privilege::Delete,
            ),

            // CREATE TABLE requires Create privilege at database level
            Statement::CreateTable(_) => {
                self.authorizer
                    .check_database(&self.identity, db, Privilege::Create)
            }

            // DROP TABLE requires Drop privilege
            Statement::DropTable(drop) => {
                for table_ref in &drop.names {
                    let d = self.authorizer.check_table(
                        &self.identity,
                        db,
                        &table_ref.table,
                        Privilege::Drop,
                    );
                    if !d.is_allowed() {
                        return Err(DatabaseError::AuthorizationError(format!(
                            "user '{}' lacks DROP on {}.{}",
                            self.identity.username, db, table_ref.table
                        )));
                    }
                }
                nexus_security::AccessDecision::Allow
            }

            // CREATE/DROP DATABASE requires global-level privilege
            // (not database-level, since the database may not exist yet)
            Statement::CreateDatabase { .. } => self
                .authorizer
                .check_global(&self.identity, Privilege::Create),
            Statement::DropDatabase { .. } => self
                .authorizer
                .check_global(&self.identity, Privilege::Drop),

            // CREATE/DROP INDEX requires table-level privilege
            Statement::CreateIndex(ci) => {
                self.authorizer
                    .check_table(&self.identity, db, &ci.table.table, Privilege::Create)
            }
            Statement::DropIndex(_) => {
                self.authorizer
                    .check_database(&self.identity, db, Privilege::Drop)
            }

            // ALTER TABLE requires Alter privilege on the table
            Statement::AlterTable(alter) => self.authorizer.check_table(
                &self.identity,
                db,
                &alter.table.table,
                Privilege::Alter,
            ),

            // EXPLAIN / EXPLAIN ANALYZE: authorize the inner statement
            Statement::Explain {
                statement: inner, ..
            }
            | Statement::ExplainAnalyze {
                statement: inner, ..
            } => {
                return self.authorize_statement(inner);
            }

            // Transaction control, SHOW, DESCRIBE, USE - always allowed
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::ShowTables
            | Statement::ShowDatabases
            | Statement::DescribeTable(_)
            | Statement::UseDatabase(_) => nexus_security::AccessDecision::Allow,

            // Deny any future unrecognized statement types by default.
            // Currently unreachable (all variants handled above), but kept as
            // a safety net for when new statement types are added to the parser.
            #[allow(unreachable_patterns)]
            _ => nexus_security::AccessDecision::Deny("statement type not authorized".to_string()),
        };

        match decision {
            nexus_security::AccessDecision::Allow => Ok(()),
            nexus_security::AccessDecision::Deny(reason) => {
                Err(DatabaseError::AuthorizationError(reason))
            }
        }
    }

    /// Extract table names from any statement for authorization checks.
    fn extract_table_names_from_statement(statement: &Statement) -> Vec<String> {
        let mut tables = Vec::new();
        match statement {
            Statement::Select(select) => {
                Self::collect_from_items(&select.from, &mut tables);
            }
            Statement::SetOperation { left, right, .. } => {
                tables.extend(Self::extract_table_names_from_statement(left));
                tables.extend(Self::extract_table_names_from_statement(right));
            }
            Statement::Insert(insert) => tables.push(insert.table.table.clone()),
            Statement::Update(update) => tables.push(update.table.table.clone()),
            Statement::Delete(delete) => tables.push(delete.table.table.clone()),
            _ => {}
        }
        tables
    }

    /// Map a statement to its audit action category.
    fn statement_audit_action(statement: &Statement) -> AuditAction {
        match statement {
            Statement::Select(_)
            | Statement::SetOperation { .. }
            | Statement::ShowTables
            | Statement::ShowDatabases
            | Statement::DescribeTable(_) => AuditAction::Query,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                AuditAction::DataModification
            }
            Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::AlterTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. } => AuditAction::SchemaChange,
            _ => AuditAction::Query,
        }
    }

    /// Extract the target object name for audit logging.
    fn statement_target(statement: &Statement) -> Option<String> {
        match statement {
            Statement::Select(_) => None,
            Statement::Insert(insert) => Some(insert.table.table.clone()),
            Statement::Update(update) => Some(update.table.table.clone()),
            Statement::Delete(delete) => Some(delete.table.table.clone()),
            Statement::CreateTable(create) => Some(create.name.table.clone()),
            Statement::DropTable(drop) => Some(
                drop.names
                    .iter()
                    .map(|t| t.table.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            Statement::AlterTable(alter) => Some(alter.table.table.clone()),
            Statement::DescribeTable(name) => Some(name.to_string()),
            Statement::CreateIndex(ci) => Some(format!("{} ON {}", ci.name, ci.table.table)),
            Statement::DropIndex(di) => Some(di.names.join(", ")),
            Statement::CreateDatabase { name, .. } | Statement::DropDatabase { name, .. } => {
                Some(name.clone())
            }
            _ => None,
        }
    }

    /// Executes multiple SQL statements.
    ///
    /// Each statement is individually authorized and audited, same as `execute()`.
    pub fn execute_batch(&mut self, sql: &str) -> DatabaseResult<Vec<StatementResult>> {
        let statements = Parser::parse(sql)?;

        let mut results = Vec::with_capacity(statements.len());
        for statement in &statements {
            self.statement_count += 1;
            let start = Instant::now();

            // Authorize before execution
            self.authorize_statement(statement)?;

            let result = self.execute_statement(statement, sql, start);

            // Audit each statement
            let success = result.is_ok();
            let action = Self::statement_audit_action(statement);
            self.audit_log.record(
                &self.identity.username,
                action,
                Self::statement_target(statement),
                Some(format!("{:?}", statement)),
                None,
                success,
            );

            results.push(result?);
        }

        Ok(results)
    }

    /// Executes a parsed statement.
    fn execute_statement(
        &mut self,
        statement: &Statement,
        sql: &str,
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

            // DDL - invalidate plan cache and result cache when schema changes
            Statement::CreateTable(create) => {
                self.plan_cache.clear();
                self.result_cache.clear();
                self.execute_create_table(create)
            }
            Statement::DropTable(drop) => {
                self.plan_cache.clear();
                for table_ref in &drop.names {
                    self.result_cache.invalidate_table(&table_ref.table);
                }
                self.execute_drop_table(drop)
            }
            Statement::CreateIndex(create_index) => {
                self.plan_cache.clear();
                self.result_cache
                    .invalidate_table(&create_index.table.table);
                self.execute_create_index(create_index)
            }
            Statement::DropIndex(drop_index) => {
                self.plan_cache.clear();
                self.result_cache.clear();
                self.execute_drop_index(drop_index)
            }
            Statement::AlterTable(alter) => {
                self.plan_cache.clear();
                self.result_cache.invalidate_table(&alter.table.table);
                self.execute_alter_table(alter)
            }

            // DML - invalidate result cache for affected tables
            Statement::Insert(insert) => {
                self.result_cache.invalidate_table(&insert.table.table);
                self.execute_insert(insert)
            }
            Statement::Update(update) => {
                self.result_cache.invalidate_table(&update.table.table);
                self.execute_update(update)
            }
            Statement::Delete(delete) => {
                self.result_cache.invalidate_table(&delete.table.table);
                self.execute_delete(delete)
            }

            // Query - use plan cache
            Statement::Select(_) | Statement::SetOperation { .. } => {
                self.execute_query(statement, sql, start)
            }

            // SHOW/DESCRIBE statements
            Statement::ShowTables => self.execute_show_tables(),
            Statement::ShowDatabases => self.execute_show_databases(),
            Statement::DescribeTable(table_name) => self.execute_describe_table(table_name),

            // EXPLAIN / EXPLAIN ANALYZE
            Statement::Explain {
                statement: inner,
                format,
            } => self.execute_explain(inner, format, false, start),
            Statement::ExplainAnalyze {
                statement: inner,
                format,
            } => self.execute_explain(inner, format, true, start),

            // Multi-database
            Statement::CreateDatabase {
                name,
                if_not_exists,
            } => {
                self.database.create_database(name, *if_not_exists)?;
                Ok(StatementResult::ddl("CREATE DATABASE"))
            }
            Statement::DropDatabase { name, if_exists } => {
                self.database.drop_database(name, *if_exists)?;
                Ok(StatementResult::ddl("DROP DATABASE"))
            }
            Statement::UseDatabase(name) => {
                self.set_current_database(name);
                self.plan_cache.clear();
                self.result_cache.clear();
                Ok(StatementResult::transaction("USE"))
            }

            _ => Err(DatabaseError::NotImplemented(
                "statement type not yet supported".to_string(),
            )),
        }
    }

    /// Executes CREATE TABLE.
    fn execute_create_table(
        &self,
        create: &nexus_sql::parser::CreateTableStatement,
    ) -> DatabaseResult<StatementResult> {
        let name = &create.name.table;

        // Check if table exists
        if self.storage().table_exists(name) {
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

        // Collect UNIQUE column indices (from column-level constraints)
        let unique_columns: Vec<usize> = create
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| {
                col.constraints
                    .iter()
                    .any(|c| matches!(c, ColumnConstraint::Unique))
            })
            .map(|(i, _)| i)
            .collect();

        // Collect DEFAULT values (evaluate literal defaults at CREATE time)
        let defaults: Vec<nexus_sql::storage::ColumnDefault> = create
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                col.default.as_ref().and_then(|expr| {
                    self.eval_literal(expr)
                        .ok()
                        .map(|val| nexus_sql::storage::ColumnDefault {
                            col_idx: i,
                            value: val,
                        })
                })
            })
            .collect();

        // Collect CHECK constraints
        let check_constraints: Vec<nexus_sql::storage::CheckConstraint> = create
            .columns
            .iter()
            .filter_map(|col| {
                col.constraints.iter().find_map(|c| {
                    if let ColumnConstraint::Check(expr) = c {
                        Some(nexus_sql::storage::CheckConstraint { expr: expr.clone() })
                    } else {
                        None
                    }
                })
            })
            .collect();

        // Create table info
        let mut info = TableInfo::new(name.clone(), schema);
        if !primary_key.is_empty() {
            info = info.with_primary_key(primary_key);
        }
        info.unique_columns = unique_columns;
        info.defaults = defaults;
        info.check_constraints = check_constraints;

        self.storage().create_table(info)?;

        Ok(StatementResult::ddl("CREATE TABLE"))
    }

    /// Executes DROP TABLE.
    fn execute_drop_table(
        &self,
        drop: &nexus_sql::parser::DropTableStatement,
    ) -> DatabaseResult<StatementResult> {
        for table_ref in &drop.names {
            let name = &table_ref.table;
            if !self.storage().table_exists(name) {
                if drop.if_exists {
                    continue;
                }
                return Err(DatabaseError::StorageError(
                    nexus_sql::storage::StorageError::TableNotFound(name.clone()),
                ));
            }
            // Also clean up any vector indexes for this table
            self.database
                .vector_index_manager()
                .drop_table_indexes(&self.current_database, name);
            self.storage().drop_table(name)?;
        }
        Ok(StatementResult::ddl("DROP TABLE"))
    }

    /// Executes ALTER TABLE.
    fn execute_alter_table(
        &self,
        alter: &nexus_sql::parser::AlterTableStatement,
    ) -> DatabaseResult<StatementResult> {
        use nexus_sql::parser::AlterOperation;

        let table_name = &alter.table.table;

        // Check table exists
        if !self.storage().table_exists(table_name) {
            if alter.if_exists {
                return Ok(StatementResult::ddl("ALTER TABLE"));
            }
            return Err(DatabaseError::StorageError(
                nexus_sql::storage::StorageError::TableNotFound(table_name.clone()),
            ));
        }

        // Apply each operation sequentially
        for op in &alter.operations {
            let table_info = self.storage().get_table_info(table_name).ok_or_else(|| {
                DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                    table_name.clone(),
                ))
            })?;

            match op {
                AlterOperation::AddColumn(col_def) => {
                    // Check column doesn't already exist
                    if table_info.schema.index_of(&col_def.name).is_some() {
                        return Err(DatabaseError::ExecutionError(format!(
                            "column \"{}\" already exists in table \"{}\"",
                            col_def.name, table_name
                        )));
                    }

                    // Build new schema with the added column
                    let new_field = if col_def.nullable {
                        Field::nullable(&col_def.name, col_def.data_type.clone())
                    } else {
                        Field::not_null(&col_def.name, col_def.data_type.clone())
                    };

                    let mut new_fields: Vec<Field> = table_info.schema.fields().to_vec();
                    new_fields.push(new_field);
                    let new_schema = Schema::new(new_fields);

                    let mut new_info = TableInfo::new(table_name.clone(), new_schema);
                    new_info.primary_key = table_info.primary_key.clone();
                    new_info.indexes = table_info.indexes.clone();
                    new_info.table_id = table_info.table_id;
                    new_info.row_count = table_info.row_count;
                    new_info.unique_columns = table_info.unique_columns.clone();
                    new_info.defaults = table_info.defaults.clone();
                    new_info.check_constraints = table_info.check_constraints.clone();

                    // Rows gain a NULL column at the end
                    let old_col_count = table_info.schema.len();
                    self.storage().alter_table(table_name, new_info, |row| {
                        let mut values: Vec<Value> = row.iter().cloned().collect();
                        // Pad with NULLs if the row has fewer columns
                        while values.len() <= old_col_count {
                            values.push(Value::Null);
                        }
                        Row::new(values)
                    })?;
                }

                AlterOperation::DropColumn { name, if_exists } => {
                    let col_idx = match table_info.schema.index_of(name) {
                        Some(idx) => idx,
                        None => {
                            if *if_exists {
                                continue;
                            }
                            return Err(DatabaseError::ExecutionError(format!(
                                "column \"{}\" does not exist in table \"{}\"",
                                name, table_name
                            )));
                        }
                    };

                    // Prevent dropping last column
                    if table_info.schema.len() <= 1 {
                        return Err(DatabaseError::ExecutionError(
                            "cannot drop the only column of a table".to_string(),
                        ));
                    }

                    // Prevent dropping PK column
                    if table_info.primary_key.contains(&col_idx) {
                        return Err(DatabaseError::ExecutionError(format!(
                            "cannot drop primary key column \"{}\"",
                            name
                        )));
                    }

                    // Build new schema without the dropped column
                    let new_fields: Vec<Field> = table_info
                        .schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != col_idx)
                        .map(|(_, f)| f.clone())
                        .collect();
                    let new_schema = Schema::new(new_fields);

                    // Adjust PK indices: indices after the dropped column shift left
                    let new_pk: Vec<usize> = table_info
                        .primary_key
                        .iter()
                        .filter(|&&i| i != col_idx)
                        .map(|&i| if i > col_idx { i - 1 } else { i })
                        .collect();

                    // Adjust index column indices
                    let new_indexes: Vec<_> = table_info
                        .indexes
                        .iter()
                        .filter(|idx| !idx.columns.contains(&col_idx))
                        .map(|idx| {
                            let mut new_idx = idx.clone();
                            new_idx.columns = idx
                                .columns
                                .iter()
                                .map(|&c| if c > col_idx { c - 1 } else { c })
                                .collect();
                            new_idx
                        })
                        .collect();

                    let mut new_info = TableInfo::new(table_name.clone(), new_schema);
                    new_info.primary_key = new_pk;
                    new_info.indexes = new_indexes;
                    new_info.table_id = table_info.table_id;
                    new_info.row_count = table_info.row_count;
                    // Remove dropped column from unique/default lists and adjust indices
                    new_info.unique_columns = table_info
                        .unique_columns
                        .iter()
                        .filter(|&&i| i != col_idx)
                        .map(|&i| if i > col_idx { i - 1 } else { i })
                        .collect();
                    new_info.defaults = table_info
                        .defaults
                        .iter()
                        .filter(|d| d.col_idx != col_idx)
                        .map(|d| nexus_sql::storage::ColumnDefault {
                            col_idx: if d.col_idx > col_idx {
                                d.col_idx - 1
                            } else {
                                d.col_idx
                            },
                            value: d.value.clone(),
                        })
                        .collect();
                    new_info.check_constraints = table_info.check_constraints.clone();

                    // Rows lose the column at col_idx
                    self.storage().alter_table(table_name, new_info, |row| {
                        let values: Vec<Value> = row
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != col_idx)
                            .map(|(_, v)| v.clone())
                            .collect();
                        Row::new(values)
                    })?;
                }

                AlterOperation::RenameColumn { old_name, new_name } => {
                    let col_idx = table_info.schema.index_of(old_name).ok_or_else(|| {
                        DatabaseError::ExecutionError(format!(
                            "column \"{}\" does not exist in table \"{}\"",
                            old_name, table_name
                        ))
                    })?;

                    // Check new name doesn't conflict
                    if table_info.schema.index_of(new_name).is_some() {
                        return Err(DatabaseError::ExecutionError(format!(
                            "column \"{}\" already exists in table \"{}\"",
                            new_name, table_name
                        )));
                    }

                    // Build new schema with the renamed column
                    let new_fields: Vec<Field> = table_info
                        .schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(i, f)| {
                            if i == col_idx {
                                if f.nullable {
                                    Field::nullable(new_name.as_str(), f.data_type.clone())
                                } else {
                                    Field::not_null(new_name.as_str(), f.data_type.clone())
                                }
                            } else {
                                f.clone()
                            }
                        })
                        .collect();
                    let new_schema = Schema::new(new_fields);

                    let mut new_info = TableInfo::new(table_name.clone(), new_schema);
                    new_info.primary_key = table_info.primary_key.clone();
                    new_info.indexes = table_info.indexes.clone();
                    new_info.table_id = table_info.table_id;
                    new_info.row_count = table_info.row_count;
                    new_info.unique_columns = table_info.unique_columns.clone();
                    new_info.defaults = table_info.defaults.clone();
                    new_info.check_constraints = table_info.check_constraints.clone();

                    // Data doesn't change — identity mapper
                    self.storage()
                        .alter_table(table_name, new_info, |row| row.clone())?;
                }

                AlterOperation::RenameTable(new_name) => {
                    // Read all data, drop old table, create new table with new name
                    let old_rows = {
                        let store = self.storage().get_table(table_name).ok_or_else(|| {
                            DatabaseError::StorageError(
                                nexus_sql::storage::StorageError::TableNotFound(table_name.clone()),
                            )
                        })?;
                        store.scan_all().map_err(DatabaseError::StorageError)?
                    };

                    self.storage()
                        .drop_table(table_name)
                        .map_err(DatabaseError::StorageError)?;

                    let mut new_info =
                        TableInfo::new(new_name.clone(), (*table_info.schema).clone());
                    new_info.primary_key = table_info.primary_key.clone();
                    new_info.indexes = table_info.indexes.clone();
                    new_info.table_id = table_info.table_id;

                    self.storage()
                        .create_table(new_info)
                        .map_err(DatabaseError::StorageError)?;
                    self.storage()
                        .execute_insert(new_name, old_rows)
                        .map_err(DatabaseError::StorageError)?;
                }

                AlterOperation::AlterColumn { .. } => {
                    return Err(DatabaseError::NotImplemented(
                        "ALTER COLUMN (change type/nullability)".to_string(),
                    ));
                }
            }
        }

        Ok(StatementResult::ddl("ALTER TABLE"))
    }

    /// Executes CREATE INDEX.
    fn execute_create_index(
        &self,
        create: &nexus_sql::parser::CreateIndexStatement,
    ) -> DatabaseResult<StatementResult> {
        use nexus_sql::parser::DataType;

        let table_name = &create.table.table;
        let table_info = self.storage().get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // Resolve column indices
        let mut col_indices = Vec::new();
        for col_expr in &create.columns {
            let col_name = col_expr.expr.to_string();
            let idx = table_info
                .schema
                .fields()
                .iter()
                .position(|f| f.name() == col_name)
                .ok_or_else(|| {
                    DatabaseError::ExecutionError(format!(
                        "column \"{}\" not found in table \"{}\"",
                        col_name, table_name
                    ))
                })?;
            col_indices.push(idx);
        }

        if col_indices.is_empty() {
            return Err(DatabaseError::ExecutionError(
                "CREATE INDEX requires at least one column".to_string(),
            ));
        }

        // Detect if this is a vector index:
        // a single VECTOR column gets an HNSW index automatically
        let is_vector_index = col_indices.len() == 1 && {
            let field = &table_info.schema.fields()[col_indices[0]];
            matches!(field.data_type, DataType::Vector(_))
        };

        if is_vector_index {
            let col_idx = col_indices[0];
            let field = &table_info.schema.fields()[col_idx];
            let dim = match &field.data_type {
                DataType::Vector(d) => *d,
                _ => unreachable!(),
            };

            let metric =
                super::vector_index::parse_metric("l2").unwrap_or(nexus_hnsw::DistanceMetric::L2);
            let m = 16;
            let ef_construction = 200;

            let key = super::vector_index::VectorIndexKey::new(
                &self.current_database,
                table_name,
                &create.name,
            );
            self.database
                .vector_index_manager()
                .create_index(key, dim, metric, m, ef_construction)
                .map_err(|e| {
                    DatabaseError::ExecutionError(format!("failed to create vector index: {}", e))
                })?;

            // Register in catalog
            let index_info = nexus_sql::storage::IndexInfo::hnsw(
                &create.name,
                col_idx,
                "l2",
                m,
                ef_construction,
            );
            self.storage().catalog().add_index(table_name, index_info)?;

            // Backfill: insert existing rows into the new vector index
            let backfill_key = super::vector_index::VectorIndexKey::new(
                &self.current_database,
                table_name,
                &create.name,
            );
            if let Ok(batches) = self.storage().execute_scan(table_name) {
                for batch in &batches {
                    for row in batch.rows() {
                        if let Some(Value::Vector(ref vec_data)) = row.get(col_idx) {
                            let vector_id: u64 = if !table_info.primary_key.is_empty() {
                                let pk_val = row
                                    .get(table_info.primary_key[0])
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                match pk_val {
                                    Value::Int(v) => v as u64,
                                    Value::BigInt(v) => v as u64,
                                    Value::SmallInt(v) => v as u64,
                                    Value::TinyInt(v) => v as u64,
                                    _ => {
                                        use std::hash::{Hash, Hasher};
                                        let mut hasher =
                                            std::collections::hash_map::DefaultHasher::new();
                                        format!("{:?}", pk_val).hash(&mut hasher);
                                        hasher.finish()
                                    }
                                }
                            } else {
                                0u64 // fallback; no PK
                            };
                            let _ = self.database.vector_index_manager().insert(
                                &backfill_key,
                                vector_id,
                                vec_data,
                            );
                        }
                    }
                }
            }

            tracing::info!(
                "Created HNSW vector index \"{}\" on {}.{} ({}D, L2)",
                create.name,
                table_name,
                field.name(),
                dim
            );
        } else {
            // Standard BTree index (metadata only for now)
            let index_info =
                nexus_sql::storage::IndexInfo::new(&create.name, col_indices, create.unique);
            self.storage().catalog().add_index(table_name, index_info)?;

            tracing::info!("Created index \"{}\" on {}", create.name, table_name);
        }

        Ok(StatementResult::ddl("CREATE INDEX"))
    }

    /// Executes DROP INDEX.
    fn execute_drop_index(
        &self,
        drop: &nexus_sql::parser::DropIndexStatement,
    ) -> DatabaseResult<StatementResult> {
        let storage = self.storage();
        for name in &drop.names {
            // Try to find which table this index belongs to
            let dropped = storage.catalog().drop_index(name);

            if !dropped && !drop.if_exists {
                return Err(DatabaseError::ExecutionError(format!(
                    "index \"{}\" does not exist",
                    name
                )));
            }

            // Also drop from vector index manager (if it was a vector index)
            // We check all tables since we don't know which table the index belongs to
            let vim = self.database.vector_index_manager();
            let keys = vim.list_indexes(&self.current_database);
            for key in keys {
                if key.index_name == *name {
                    vim.drop_index(&key);
                    break;
                }
            }
        }
        Ok(StatementResult::ddl("DROP INDEX"))
    }

    /// Executes INSERT.
    fn execute_insert(
        &self,
        insert: &nexus_sql::parser::InsertStatement,
    ) -> DatabaseResult<StatementResult> {
        let table_name = &insert.table.table;
        let table_info = self.storage().get_table_info(table_name).ok_or_else(|| {
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

        // Build rows from the INSERT source
        let mut rows = match &insert.values {
            InsertSource::Values(value_rows) => {
                let mut rows = Vec::with_capacity(value_rows.len());
                for value_row in value_rows {
                    let mut row_values = vec![Value::Null; schema.fields().len()];

                    for (i, expr) in value_row.iter().enumerate() {
                        if i < column_order.len() {
                            let col_idx = column_order[i];
                            let val = self.eval_literal(expr)?;

                            // Auto-coerce string values to vectors for VECTOR columns
                            let val = if matches!(
                                &schema.fields()[col_idx].data_type,
                                nexus_sql::parser::DataType::Vector(_)
                            ) {
                                val.cast(&schema.fields()[col_idx].data_type)
                                    .map_err(|e| DatabaseError::ExecutionError(e))?
                            } else {
                                val
                            };

                            // Validate vector dimension against the column's declared dimension
                            if let Value::Vector(ref vec_data) = val {
                                if let nexus_sql::parser::DataType::Vector(expected_dim) =
                                    &schema.fields()[col_idx].data_type
                                {
                                    if vec_data.len() != *expected_dim as usize {
                                        return Err(DatabaseError::ExecutionError(format!(
                                            "vector dimension mismatch for column '{}': expected {}, got {}",
                                            schema.fields()[col_idx].name(),
                                            expected_dim,
                                            vec_data.len()
                                        )));
                                    }
                                }
                            }

                            row_values[col_idx] = val;
                        }
                    }

                    rows.push(Row::new(row_values));
                }
                rows
            }
            InsertSource::Query(select_stmt) => {
                // Execute the SELECT query to get source rows
                let select_statement = Statement::Select((**select_stmt).clone());
                let source_rows = self.execute_select_for_insert(&select_statement)?;

                // Validate column count
                let expected_cols = column_order.len();
                if !source_rows.is_empty() {
                    let actual_cols = source_rows[0].num_columns();
                    if actual_cols != expected_cols {
                        return Err(DatabaseError::ExecutionError(format!(
                            "INSERT...SELECT column count mismatch: target has {} columns, SELECT produces {}",
                            expected_cols, actual_cols
                        )));
                    }
                }

                // Map SELECT output columns to target table columns
                let mut rows = Vec::with_capacity(source_rows.len());
                for source_row in &source_rows {
                    let mut row_values = vec![Value::Null; schema.fields().len()];

                    for (i, val) in source_row.iter().enumerate() {
                        if i < column_order.len() {
                            let col_idx = column_order[i];

                            // Auto-coerce for VECTOR columns
                            let val = if matches!(
                                &schema.fields()[col_idx].data_type,
                                nexus_sql::parser::DataType::Vector(_)
                            ) {
                                val.clone()
                                    .cast(&schema.fields()[col_idx].data_type)
                                    .map_err(|e| DatabaseError::ExecutionError(e))?
                            } else {
                                val.clone()
                            };

                            // Validate vector dimension
                            if let Value::Vector(ref vec_data) = val {
                                if let nexus_sql::parser::DataType::Vector(expected_dim) =
                                    &schema.fields()[col_idx].data_type
                                {
                                    if vec_data.len() != *expected_dim as usize {
                                        return Err(DatabaseError::ExecutionError(format!(
                                            "vector dimension mismatch for column '{}': expected {}, got {}",
                                            schema.fields()[col_idx].name(),
                                            expected_dim,
                                            vec_data.len()
                                        )));
                                    }
                                }
                            }

                            row_values[col_idx] = val;
                        }
                    }

                    rows.push(Row::new(row_values));
                }
                rows
            }
            InsertSource::DefaultValues => {
                // Single row — defaults will be applied below
                let row_values = vec![Value::Null; schema.fields().len()];
                vec![Row::new(row_values)]
            }
        };

        // Apply defaults and validate constraints for each row
        for row in &mut rows {
            Self::apply_defaults(row, &table_info);
            self.validate_row(row, &table_info)?;
            self.validate_unique(row, table_name, &table_info, None)?;
        }

        let count = self.storage().execute_insert(table_name, rows.clone())?;

        // Populate any HNSW vector indexes for this table
        let vim = self.database.vector_index_manager();
        let vector_keys = vim.list_table_indexes(&self.current_database, table_name);
        if !vector_keys.is_empty() {
            for vk in &vector_keys {
                if let Some(idx_info) = table_info.indexes.iter().find(|i| i.name == vk.index_name)
                {
                    if let Some(&col_idx) = idx_info.columns.first() {
                        for row in rows.iter() {
                            if let Some(Value::Vector(ref vec_data)) = row.get(col_idx) {
                                let vector_id = Self::derive_vector_id(row, &table_info);
                                let _ = vim.insert(vk, vector_id, vec_data);
                            }
                        }
                    }
                }
            }
        }

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
        let table_info = self.storage().get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // Get all rows (we'll filter later)
        let batches = self.storage().execute_scan(table_name)?;
        let all_rows: Vec<Row> = batches.iter().flat_map(|b| b.rows()).collect();

        let mut updated_count = 0u64;

        for row in all_rows {
            // Check WHERE clause
            if let Some(where_expr) = &update.where_clause {
                if !self.eval_where(&row, where_expr, &table_info.schema)? {
                    continue;
                }
            }

            // Apply updates — evaluate expressions against the ORIGINAL row
            // so that `SET x = x + 1` reads the old value of x.
            let mut new_row = row.clone();
            for assignment in &update.assignments {
                let col_name = &assignment.column.column;
                if let Some(col_idx) = table_info.schema.index_of(col_name) {
                    let val = self.eval_expr(&row, &assignment.value, &table_info.schema)?;

                    // Auto-coerce for VECTOR columns (string -> Vector)
                    let val = if matches!(
                        &table_info.schema.fields()[col_idx].data_type,
                        nexus_sql::parser::DataType::Vector(_)
                    ) {
                        val.cast(&table_info.schema.fields()[col_idx].data_type)
                            .map_err(|e| DatabaseError::ExecutionError(e))?
                    } else {
                        val
                    };

                    new_row.set(col_idx, val);
                }
            }

            // Validate constraints on the new row
            self.validate_row(&new_row, &table_info)?;

            // For UNIQUE checks, exclude the current row (same PK)
            let pk_vals: Vec<Value> = table_info
                .primary_key
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            self.validate_unique(&new_row, table_name, &table_info, Some(&pk_vals))?;

            // Update the row
            if self.storage().execute_update(table_name, new_row.clone())? {
                // Sync HNSW vector indexes if any vector columns changed
                self.sync_hnsw_update(table_name, &table_info, &row, &new_row);
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
        let table_info = self.storage().get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // Get all rows
        let batches = self.storage().execute_scan(table_name)?;
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

            if self.storage().execute_delete(table_name, &key_values)? {
                // Sync HNSW vector indexes — remove deleted vector entries
                self.sync_hnsw_delete(table_name, &table_info, &row);
                deleted_count += 1;
            }
        }

        Ok(StatementResult::Delete {
            rows_affected: deleted_count,
        })
    }

    /// Executes a SELECT statement and returns raw rows for INSERT...SELECT.
    ///
    /// This bypasses result caching and plan caching since the result will be
    /// consumed by the INSERT, not returned to the user.
    fn execute_select_for_insert(&self, statement: &Statement) -> DatabaseResult<Vec<Row>> {
        // Build catalog from current storage
        let mut catalog = MemoryCatalog::new();
        for tbl in self.storage().list_tables() {
            if let Some(info) = self.storage().get_table_info(&tbl) {
                catalog.add_table(nexus_sql::logical::TableMeta::new(
                    tbl.clone(),
                    (*info.schema).clone(),
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
        let plan = physical_planner
            .create_physical_plan(&optimized.root)
            .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Execute
        let exec_ctx = ExecutionContext::default();
        let mut executor = nexus_sql::executor::QueryExecutor::new(exec_ctx);

        for tbl in self.storage().list_tables() {
            if let Ok(batches) = self.storage().execute_scan(&tbl) {
                executor.register_table(tbl, batches);
            }
        }

        let result = executor.execute(&plan)?;

        // Flatten batches into rows
        let rows: Vec<Row> = result.batches.iter().flat_map(|b| b.rows()).collect();

        Ok(rows)
    }

    /// Executes a SELECT query.
    ///
    /// Uses plan caching to avoid repeated parsing, planning, and optimization
    /// for the same SQL queries.
    fn execute_query(
        &self,
        statement: &Statement,
        sql: &str,
        start: Instant,
    ) -> DatabaseResult<StatementResult> {
        // ── Result cache lookup (only outside transactions) ─────
        let cache_key = if !self.in_transaction() {
            let key = ResultCacheKey::new(sql, &[&self.current_database]);
            if let Some(cached) = self.result_cache.get(&key) {
                // Cache hit - return the cached result directly
                return Ok(StatementResult::Query((*cached).clone()));
            }
            Some(key)
        } else {
            None
        };

        // ── Plan cache lookup / build ───────────────────────────
        let physical_plan = if let Some(cached_plan) = self.plan_cache.get(sql) {
            (*cached_plan).clone()
        } else {
            // Build a catalog from current storage
            let mut catalog = MemoryCatalog::new();
            for table_name in self.storage().list_tables() {
                if let Some(table_info) = self.storage().get_table_info(&table_name) {
                    catalog.add_table(nexus_sql::logical::TableMeta::new(
                        table_name.clone(),
                        (*table_info.schema).clone(),
                    ));
                }
            }

            // Build logical plan
            let logical_plan = build_plan(statement, &catalog)
                .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

            // Optimize
            let optimizer = Optimizer::new(OptimizerConfig::default());
            let optimized = optimizer
                .optimize(logical_plan)
                .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

            // Create physical plan
            let ctx = ExecutionContext::default();
            let physical_planner = PhysicalPlanner::new(&ctx);
            let plan = physical_planner
                .create_physical_plan(&optimized.root)
                .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

            // Cache the plan for future use
            self.plan_cache.insert(sql, plan.clone());

            plan
        };

        // ── Execute ─────────────────────────────────────────────
        let ctx = ExecutionContext::default();
        let mut executor = nexus_sql::executor::QueryExecutor::new(ctx);

        for table_name in self.storage().list_tables() {
            if let Ok(batches) = self.storage().execute_scan(&table_name) {
                executor.register_table(table_name, batches);
            }
        }

        let result = executor.execute(&physical_plan)?;

        let elapsed = start.elapsed();
        let execute_result = ExecuteResult::from_batches(result.schema, result.batches, elapsed);

        // ── Store in result cache ───────────────────────────────
        if let Some(key) = cache_key {
            let tables = Self::extract_table_names(statement);
            self.result_cache
                .insert(key, execute_result.clone(), tables);
        }

        Ok(StatementResult::Query(execute_result))
    }

    /// Executes EXPLAIN or EXPLAIN ANALYZE for a statement.
    ///
    /// Builds the logical and physical plans for the inner statement and
    /// formats the plan tree. For EXPLAIN ANALYZE, the query is actually
    /// executed and runtime metrics are collected.
    fn execute_explain(
        &self,
        inner: &Statement,
        format: &nexus_sql::parser::ExplainFormat,
        analyze: bool,
        start: Instant,
    ) -> DatabaseResult<StatementResult> {
        use nexus_sql::parser::ExplainFormat;

        // Build a catalog from current storage
        let mut catalog = MemoryCatalog::new();
        let storage = self.storage();
        for table_name in storage.list_tables() {
            if let Some(table_info) = storage.get_table_info(&table_name) {
                let meta = nexus_sql::logical::TableMeta::new(
                    table_name.clone(),
                    (*table_info.schema).clone(),
                );
                catalog.add_table(meta);
            }
        }

        // Build logical plan from inner statement
        let logical_plan =
            build_plan(inner, &catalog).map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Optimize with stats collection
        let optimizer = Optimizer::new(OptimizerConfig::default());
        let (optimized, opt_stats) = optimizer
            .optimize_with_stats(logical_plan)
            .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Create physical plan
        let ctx = ExecutionContext::default();
        let physical_planner = PhysicalPlanner::new(&ctx);
        let physical_plan = physical_planner
            .create_physical_plan(&optimized.root)
            .map_err(|e| DatabaseError::PlanError(e.to_string()))?;

        // Attach cost estimates from the logical plan
        let stats = nexus_sql::optimizer::Statistics::default();
        let physical_plan = physical_plan.with_cost_estimates(&optimized.root, &stats);
        let planning_time_us = start.elapsed().as_micros() as u64;
        let physical_plan = physical_plan.with_planning_time(planning_time_us);

        // For EXPLAIN ANALYZE, actually execute and collect per-operator metrics
        let analyze_metrics = if analyze {
            let ctx = ExecutionContext::default();
            let mut executor = nexus_sql::executor::QueryExecutor::new(ctx);
            for table_name in storage.list_tables() {
                if let Ok(batches) = storage.execute_scan(&table_name) {
                    executor.register_table(table_name, batches);
                }
            }
            let (_result, metrics) = executor.execute_with_metrics(&physical_plan)?;
            Some(metrics)
        } else {
            None
        };

        // Format output
        let explain_text = match format {
            ExplainFormat::Text => {
                physical_plan.explain_rich(analyze_metrics.as_ref(), Some(&opt_stats))
            }
            ExplainFormat::Verbose => {
                let mut text = String::from("=== Logical Plan ===\n");
                text.push_str(&optimized.explain());
                text.push_str("\n=== Physical Plan ===\n");
                text.push_str(
                    &physical_plan.explain_rich(analyze_metrics.as_ref(), Some(&opt_stats)),
                );
                text
            }
            ExplainFormat::Json => {
                // Build enhanced JSON with warnings and suggestions
                let (warnings, suggestions) = self.analyze_plan_issues(&physical_plan);
                physical_plan.explain_json_llm(
                    analyze_metrics.as_ref(),
                    Some(&opt_stats),
                    &warnings,
                    &suggestions,
                )
            }
        };

        // Build result: one row per line for TEXT/VERBOSE, one row for JSON
        let schema = Arc::new(Schema::new(vec![Field::not_null(
            "plan",
            nexus_sql::parser::DataType::Text,
        )]));

        let rows: Vec<Row> = if matches!(format, ExplainFormat::Json) {
            vec![Row::new(vec![Value::String(explain_text)])]
        } else {
            explain_text
                .lines()
                .map(|line| Row::new(vec![Value::String(line.to_string())]))
                .collect()
        };

        let total_rows = rows.len();
        let batch = nexus_sql::executor::RecordBatch::from_rows(schema.clone(), &rows)
            .map_err(|e| DatabaseError::ExecutionError(e))?;

        let elapsed = start.elapsed();
        let result = ExecuteResult {
            schema,
            batches: vec![batch],
            total_rows,
            execution_time: elapsed,
        };

        Ok(StatementResult::Query(result))
    }

    /// Analyzes the physical plan for potential issues and optimization suggestions.
    ///
    /// Returns `(warnings, suggestions)` for inclusion in JSON explain output.
    fn analyze_plan_issues(&self, plan: &PhysicalPlan) -> (Vec<String>, Vec<String>) {
        let mut warnings = Vec::new();
        let mut suggestions = Vec::new();

        // Check for sequential scans with filters (could benefit from indexes)
        let seq_scans = plan
            .find_operators(|op| matches!(op, nexus_sql::physical::PhysicalOperator::SeqScan(_)));
        for op in &seq_scans {
            if let nexus_sql::physical::PhysicalOperator::SeqScan(scan) = op {
                if !scan.filters.is_empty() {
                    warnings.push(format!(
                        "Sequential scan with filter on table '{}' — consider an index",
                        scan.table_name
                    ));
                    for filter in &scan.filters {
                        suggestions.push(format!(
                            "Consider CREATE INDEX on '{}' for filter: {}",
                            scan.table_name, filter
                        ));
                    }
                }
            }
        }

        // Check for nested loop joins (potentially expensive)
        let nl_joins = plan.find_operators(|op| {
            matches!(op, nexus_sql::physical::PhysicalOperator::NestedLoopJoin(_))
        });
        for op in &nl_joins {
            if let nexus_sql::physical::PhysicalOperator::NestedLoopJoin(join) = op {
                if join.condition.is_some() {
                    warnings.push(
                        "Nested loop join detected — may be expensive for large tables".to_string(),
                    );
                    suggestions.push(
                        "Consider rewriting the query to use equi-join conditions for hash join"
                            .to_string(),
                    );
                }
            }
        }

        (warnings, suggestions)
    }

    /// Extract table names referenced in a statement (for cache dependency tracking).
    fn extract_table_names(statement: &Statement) -> Vec<String> {
        let mut tables = Vec::new();
        if let Statement::Select(select) = statement {
            Self::collect_from_items(&select.from, &mut tables);
        }
        tables
    }

    /// Recursively collect table names from FROM items.
    fn collect_from_items(from: &[nexus_sql::parser::FromItem], tables: &mut Vec<String>) {
        for item in from {
            match item {
                nexus_sql::parser::FromItem::Table(t) => {
                    tables.push(t.table.clone());
                }
                nexus_sql::parser::FromItem::Join { left, right, .. } => {
                    Self::collect_from_items(&[*left.clone()], tables);
                    Self::collect_from_items(&[*right.clone()], tables);
                }
                nexus_sql::parser::FromItem::Subquery { query, .. } => {
                    Self::collect_from_items(&query.from, tables);
                }
            }
        }
    }

    // =========================================================================
    // SHOW/DESCRIBE Commands
    // =========================================================================

    /// Executes SHOW TABLES.
    fn execute_show_tables(&self) -> DatabaseResult<StatementResult> {
        use nexus_sql::logical::{Field, Schema};
        use nexus_sql::parser::DataType;

        let table_names = self.storage().list_tables();
        let mut sorted_names = table_names;
        sorted_names.sort();

        // Build schema for result
        let schema = Arc::new(Schema::new(vec![Field::not_null(
            "table_name",
            DataType::Text,
        )]));

        // Build rows
        let rows: Vec<Row> = sorted_names
            .into_iter()
            .map(|name| Row::new(vec![Value::String(name)]))
            .collect();

        let total_rows = rows.len();
        let batch = nexus_sql::executor::RecordBatch::from_rows(schema.clone(), &rows)
            .map_err(|e| DatabaseError::ExecutionError(e))?;

        let result = ExecuteResult {
            schema,
            batches: vec![batch],
            total_rows,
            execution_time: std::time::Duration::from_millis(0),
        };

        Ok(StatementResult::Query(result))
    }

    /// Executes SHOW DATABASES.
    fn execute_show_databases(&self) -> DatabaseResult<StatementResult> {
        use nexus_sql::logical::{Field, Schema};
        use nexus_sql::parser::DataType;

        let names = self.database.list_databases();
        let schema = Arc::new(Schema::new(vec![Field::not_null(
            "database_name",
            DataType::Text,
        )]));

        let rows: Vec<Row> = names
            .into_iter()
            .map(|name| Row::new(vec![Value::String(name)]))
            .collect();
        let total_rows = rows.len();

        let batch = nexus_sql::executor::RecordBatch::from_rows(schema.clone(), &rows)
            .map_err(|e| DatabaseError::ExecutionError(e))?;

        let result = ExecuteResult {
            schema,
            batches: vec![batch],
            total_rows,
            execution_time: std::time::Duration::from_millis(0),
        };

        Ok(StatementResult::Query(result))
    }

    /// Executes DESCRIBE TABLE.
    fn execute_describe_table(&self, table_name: &str) -> DatabaseResult<StatementResult> {
        use nexus_sql::logical::{Field, Schema};
        use nexus_sql::parser::DataType;

        let table_info = self.storage().get_table_info(table_name).ok_or_else(|| {
            DatabaseError::StorageError(nexus_sql::storage::StorageError::TableNotFound(
                table_name.to_string(),
            ))
        })?;

        // Build schema for result
        let schema = Arc::new(Schema::new(vec![
            Field::not_null("column_name", DataType::Text),
            Field::not_null("data_type", DataType::Text),
            Field::not_null("nullable", DataType::Boolean),
            Field::not_null("primary_key", DataType::Boolean),
        ]));

        let pk_columns: std::collections::HashSet<usize> =
            table_info.primary_key.iter().copied().collect();

        // Build rows for each column
        let rows: Vec<Row> = table_info
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Row::new(vec![
                    Value::String(field.name().to_string()),
                    Value::String(format!("{:?}", field.data_type)),
                    Value::Boolean(field.nullable),
                    Value::Boolean(pk_columns.contains(&i)),
                ])
            })
            .collect();

        let total_rows = rows.len();
        let batch = nexus_sql::executor::RecordBatch::from_rows(schema.clone(), &rows)
            .map_err(|e| DatabaseError::ExecutionError(e))?;

        let result = ExecuteResult {
            schema,
            batches: vec![batch],
            total_rows,
            execution_time: std::time::Duration::from_millis(0),
        };

        Ok(StatementResult::Query(result))
    }

    // =========================================================================
    // Pattern Matching
    // =========================================================================

    /// SQL LIKE pattern matching.
    ///
    /// Supports `%` (any sequence of characters) and `_` (any single character).
    /// If `case_insensitive` is true, comparison ignores case (ILIKE).
    fn sql_like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
        let text: Vec<char> = if case_insensitive {
            text.to_lowercase().chars().collect()
        } else {
            text.chars().collect()
        };
        let pattern: Vec<char> = if case_insensitive {
            pattern.to_lowercase().chars().collect()
        } else {
            pattern.chars().collect()
        };

        Self::like_match_impl(&text, &pattern)
    }

    /// Recursive LIKE matching with `%` and `_` wildcards.
    fn like_match_impl(text: &[char], pattern: &[char]) -> bool {
        if pattern.is_empty() {
            return text.is_empty();
        }

        match pattern[0] {
            '%' => {
                // Skip consecutive %
                let mut p = 0;
                while p < pattern.len() && pattern[p] == '%' {
                    p += 1;
                }
                if p == pattern.len() {
                    return true; // trailing % matches everything
                }

                // Try matching the rest of the pattern from every position in text
                for t in 0..=text.len() {
                    if Self::like_match_impl(&text[t..], &pattern[p..]) {
                        return true;
                    }
                }
                false
            }
            '_' => {
                // _ matches exactly one character
                if text.is_empty() {
                    false
                } else {
                    Self::like_match_impl(&text[1..], &pattern[1..])
                }
            }
            c => {
                if text.is_empty() || text[0] != c {
                    false
                } else {
                    Self::like_match_impl(&text[1..], &pattern[1..])
                }
            }
        }
    }

    // =========================================================================
    // Constraint Enforcement
    // =========================================================================

    /// Applies DEFAULT values to a row for columns that are NULL and have defaults.
    fn apply_defaults(row: &mut Row, table_info: &nexus_sql::storage::TableInfo) {
        for def in &table_info.defaults {
            if def.col_idx < row.num_columns() {
                if let Some(val) = row.get(def.col_idx) {
                    if val.is_null() {
                        row.set(def.col_idx, def.value.clone());
                    }
                }
            }
        }
    }

    /// Validates a row against table constraints (NOT NULL, CHECK).
    ///
    /// Should be called after defaults have been applied.
    fn validate_row(
        &self,
        row: &Row,
        table_info: &nexus_sql::storage::TableInfo,
    ) -> DatabaseResult<()> {
        // NOT NULL enforcement
        for (i, field) in table_info.schema.fields().iter().enumerate() {
            if !field.nullable {
                if let Some(val) = row.get(i) {
                    if val.is_null() {
                        return Err(DatabaseError::ExecutionError(format!(
                            "NOT NULL constraint violated: column '{}' cannot be NULL",
                            field.name()
                        )));
                    }
                }
            }
        }

        // CHECK constraint enforcement
        for check in &table_info.check_constraints {
            let result = self.eval_expr(row, &check.expr, &table_info.schema)?;
            match result {
                Value::Boolean(true) | Value::Null => {} // NULL = unknown, pass per SQL standard
                Value::Boolean(false) => {
                    return Err(DatabaseError::ExecutionError(format!(
                        "CHECK constraint violated: {}",
                        check.expr
                    )));
                }
                _ => {
                    return Err(DatabaseError::ExecutionError(
                        "CHECK constraint must evaluate to boolean".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validates UNIQUE constraint for a row against existing table data.
    ///
    /// Checks each unique column to ensure no other row has the same value.
    /// `exclude_pk` allows skipping the row being updated (identified by PK).
    fn validate_unique(
        &self,
        row: &Row,
        table_name: &str,
        table_info: &nexus_sql::storage::TableInfo,
        exclude_pk: Option<&[Value]>,
    ) -> DatabaseResult<()> {
        if table_info.unique_columns.is_empty() {
            return Ok(());
        }

        let batches = self.storage().execute_scan(table_name)?;
        let existing_rows: Vec<Row> = batches.iter().flat_map(|b| b.rows()).collect();

        for &col_idx in &table_info.unique_columns {
            let new_val = row.get(col_idx).cloned().unwrap_or(Value::Null);
            if new_val.is_null() {
                continue; // NULL is allowed in UNIQUE columns (per SQL standard)
            }

            for existing in &existing_rows {
                // Skip the row we're updating (same PK)
                if let Some(pk_vals) = exclude_pk {
                    let existing_pk: Vec<Value> = table_info
                        .primary_key
                        .iter()
                        .map(|&i| existing.get(i).cloned().unwrap_or(Value::Null))
                        .collect();
                    if existing_pk == pk_vals {
                        continue;
                    }
                }

                let existing_val = existing.get(col_idx).cloned().unwrap_or(Value::Null);
                if new_val == existing_val {
                    let col_name = table_info
                        .schema
                        .fields()
                        .get(col_idx)
                        .map(|f| f.name())
                        .unwrap_or("?");
                    return Err(DatabaseError::ExecutionError(format!(
                        "UNIQUE constraint violated: duplicate value '{}' in column '{}'",
                        new_val, col_name
                    )));
                }
            }
        }

        Ok(())
    }

    // =========================================================================
    // HNSW Vector Index Helpers
    // =========================================================================

    /// Derives a `u64` vector ID from a row's primary key value.
    ///
    /// Integer PK types are cast directly; other types are hashed.
    /// Returns 0 if the table has no primary key.
    fn derive_vector_id(row: &Row, table_info: &nexus_sql::storage::TableInfo) -> u64 {
        if table_info.primary_key.is_empty() {
            return 0;
        }
        let pk_val = row
            .get(table_info.primary_key[0])
            .cloned()
            .unwrap_or(Value::Null);
        match pk_val {
            Value::Int(v) => v as u64,
            Value::BigInt(v) => v as u64,
            Value::SmallInt(v) => v as u64,
            Value::TinyInt(v) => v as u64,
            _ => {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                format!("{:?}", pk_val).hash(&mut hasher);
                hasher.finish()
            }
        }
    }

    /// Syncs HNSW vector indexes after a row is deleted.
    ///
    /// Removes the vector entry from every HNSW index on the table.
    fn sync_hnsw_delete(
        &self,
        table_name: &str,
        table_info: &nexus_sql::storage::TableInfo,
        row: &Row,
    ) {
        let vim = self.database.vector_index_manager();
        let vector_keys = vim.list_table_indexes(&self.current_database, table_name);
        for vk in &vector_keys {
            let vector_id = Self::derive_vector_id(row, table_info);
            let _ = vim.delete(vk, vector_id); // best-effort
        }
    }

    /// Syncs HNSW vector indexes after a row is updated.
    ///
    /// If the vector column changed, removes the old entry and inserts the new one.
    /// If only non-vector columns changed, no HNSW action is needed (vector_id
    /// is PK-based, so the mapping is still valid).
    fn sync_hnsw_update(
        &self,
        table_name: &str,
        table_info: &nexus_sql::storage::TableInfo,
        old_row: &Row,
        new_row: &Row,
    ) {
        let vim = self.database.vector_index_manager();
        let vector_keys = vim.list_table_indexes(&self.current_database, table_name);

        for vk in &vector_keys {
            if let Some(idx_info) = table_info.indexes.iter().find(|i| i.name == vk.index_name) {
                if let Some(&col_idx) = idx_info.columns.first() {
                    let old_vec = old_row.get(col_idx);
                    let new_vec = new_row.get(col_idx);

                    // Only sync if the vector column actually changed
                    if old_vec != new_vec {
                        let vector_id = Self::derive_vector_id(old_row, table_info);

                        // Remove old entry
                        let _ = vim.delete(vk, vector_id);

                        // Insert new entry if the new value is a vector
                        if let Some(Value::Vector(ref vec_data)) = new_vec {
                            let new_vector_id = Self::derive_vector_id(new_row, table_info);
                            let _ = vim.insert(vk, new_vector_id, vec_data);
                        }
                    }
                }
            }
        }
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
                        return Ok(Value::BigInt(-n));
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

    /// Evaluates a WHERE clause against a row, returning a boolean result.
    ///
    /// Delegates to `eval_expr` for the full expression evaluation and coerces
    /// the result to a boolean. NULL is treated as false (row excluded).
    fn eval_where(&self, row: &Row, expr: &Expr, schema: &Schema) -> DatabaseResult<bool> {
        let val = self.eval_expr(row, expr, schema)?;
        match val {
            Value::Boolean(b) => Ok(b),
            Value::Null => Ok(false), // NULL in WHERE context = exclude row
            _ => Err(DatabaseError::ExecutionError(format!(
                "WHERE clause must evaluate to boolean, got {:?}",
                val
            ))),
        }
    }

    /// Evaluates an expression against a row.
    /// Evaluates an arbitrary expression against a row.
    ///
    /// Supports literals, column references, arithmetic (`+`, `-`, `*`, `/`, `%`),
    /// string concatenation (`||`), unary operators (`-`, `NOT`), scalar functions
    /// (UPPER, LOWER, ABS, etc.), CASE expressions, CAST, IS NULL, IS NOT NULL,
    /// and parenthesized sub-expressions.
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

            Expr::BinaryOp { left, op, right } => {
                let lv = self.eval_expr(row, left, schema)?;
                let rv = self.eval_expr(row, right, schema)?;

                // NULL propagation: any arithmetic with NULL yields NULL
                if lv.is_null() || rv.is_null() {
                    // Logical operators handle NULL differently
                    match op {
                        BinaryOperator::And => {
                            // FALSE AND NULL = FALSE; NULL AND TRUE = NULL
                            if matches!(&lv, Value::Boolean(false))
                                || matches!(&rv, Value::Boolean(false))
                            {
                                return Ok(Value::Boolean(false));
                            }
                            return Ok(Value::Null);
                        }
                        BinaryOperator::Or => {
                            // TRUE OR NULL = TRUE; NULL OR FALSE = NULL
                            if matches!(&lv, Value::Boolean(true))
                                || matches!(&rv, Value::Boolean(true))
                            {
                                return Ok(Value::Boolean(true));
                            }
                            return Ok(Value::Null);
                        }
                        BinaryOperator::Eq => return Ok(Value::Null),
                        BinaryOperator::NotEq => return Ok(Value::Null),
                        _ => return Ok(Value::Null),
                    }
                }

                match op {
                    // Arithmetic
                    BinaryOperator::Plus => match (&lv, &rv) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                        (Value::BigInt(a), Value::BigInt(b)) => Ok(Value::BigInt(a + b)),
                        _ => {
                            if let (Some(a), Some(b)) = (lv.to_f64(), rv.to_f64()) {
                                Ok(Value::Double(a + b))
                            } else {
                                Err(DatabaseError::ExecutionError(format!(
                                    "cannot add {:?} and {:?}",
                                    lv, rv
                                )))
                            }
                        }
                    },
                    BinaryOperator::Minus => match (&lv, &rv) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                        (Value::BigInt(a), Value::BigInt(b)) => Ok(Value::BigInt(a - b)),
                        _ => {
                            if let (Some(a), Some(b)) = (lv.to_f64(), rv.to_f64()) {
                                Ok(Value::Double(a - b))
                            } else {
                                Err(DatabaseError::ExecutionError(format!(
                                    "cannot subtract {:?} and {:?}",
                                    lv, rv
                                )))
                            }
                        }
                    },
                    BinaryOperator::Multiply => match (&lv, &rv) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                        (Value::BigInt(a), Value::BigInt(b)) => Ok(Value::BigInt(a * b)),
                        _ => {
                            if let (Some(a), Some(b)) = (lv.to_f64(), rv.to_f64()) {
                                Ok(Value::Double(a * b))
                            } else {
                                Err(DatabaseError::ExecutionError(format!(
                                    "cannot multiply {:?} and {:?}",
                                    lv, rv
                                )))
                            }
                        }
                    },
                    BinaryOperator::Divide => {
                        if let (Some(a), Some(b)) = (lv.to_f64(), rv.to_f64()) {
                            if b == 0.0 {
                                Err(DatabaseError::ExecutionError(
                                    "division by zero".to_string(),
                                ))
                            } else {
                                Ok(Value::Double(a / b))
                            }
                        } else {
                            Err(DatabaseError::ExecutionError(format!(
                                "cannot divide {:?} by {:?}",
                                lv, rv
                            )))
                        }
                    }
                    BinaryOperator::Modulo => match (&lv, &rv) {
                        (Value::Int(a), Value::Int(b)) => {
                            if *b == 0 {
                                Err(DatabaseError::ExecutionError(
                                    "division by zero".to_string(),
                                ))
                            } else {
                                Ok(Value::Int(a % b))
                            }
                        }
                        (Value::BigInt(a), Value::BigInt(b)) => {
                            if *b == 0 {
                                Err(DatabaseError::ExecutionError(
                                    "division by zero".to_string(),
                                ))
                            } else {
                                Ok(Value::BigInt(a % b))
                            }
                        }
                        _ => {
                            if let (Some(a), Some(b)) = (lv.to_f64(), rv.to_f64()) {
                                if b == 0.0 {
                                    Err(DatabaseError::ExecutionError(
                                        "division by zero".to_string(),
                                    ))
                                } else {
                                    Ok(Value::Double(a % b))
                                }
                            } else {
                                Err(DatabaseError::ExecutionError(format!(
                                    "cannot modulo {:?} by {:?}",
                                    lv, rv
                                )))
                            }
                        }
                    },

                    // String concatenation
                    BinaryOperator::Concat => {
                        let ls = lv.to_string_value().unwrap_or_default();
                        let rs = rv.to_string_value().unwrap_or_default();
                        Ok(Value::String(format!("{}{}", ls, rs)))
                    }

                    // Comparison operators (return Boolean)
                    BinaryOperator::Eq => Ok(Value::Boolean(lv == rv)),
                    BinaryOperator::NotEq => Ok(Value::Boolean(lv != rv)),
                    BinaryOperator::Lt => Ok(Value::Boolean(lv < rv)),
                    BinaryOperator::LtEq => Ok(Value::Boolean(lv <= rv)),
                    BinaryOperator::Gt => Ok(Value::Boolean(lv > rv)),
                    BinaryOperator::GtEq => Ok(Value::Boolean(lv >= rv)),

                    // Logical operators
                    BinaryOperator::And => {
                        let lb = lv.to_bool().unwrap_or(false);
                        let rb = rv.to_bool().unwrap_or(false);
                        Ok(Value::Boolean(lb && rb))
                    }
                    BinaryOperator::Or => {
                        let lb = lv.to_bool().unwrap_or(false);
                        let rb = rv.to_bool().unwrap_or(false);
                        Ok(Value::Boolean(lb || rb))
                    }

                    // LIKE / NOT LIKE / ILIKE pattern matching
                    BinaryOperator::Like | BinaryOperator::NotLike | BinaryOperator::ILike => {
                        let text = lv.to_string_value().unwrap_or_default();
                        let pattern = rv.to_string_value().unwrap_or_default();

                        let case_insensitive = matches!(op, BinaryOperator::ILike);
                        let matched = Self::sql_like_match(&text, &pattern, case_insensitive);

                        let result = matches!(op, BinaryOperator::NotLike) != matched;
                        Ok(Value::Boolean(result))
                    }

                    _ => Err(DatabaseError::ExecutionError(format!(
                        "unsupported binary operator: {:?}",
                        op
                    ))),
                }
            }

            Expr::UnaryOp { op, expr: inner } => {
                let val = self.eval_expr(row, inner, schema)?;
                match op {
                    UnaryOperator::Minus => match &val {
                        Value::Int(n) => Ok(Value::Int(-n)),
                        Value::BigInt(n) => Ok(Value::BigInt(-n)),
                        Value::Float(n) => Ok(Value::Float(-n)),
                        Value::Double(n) => Ok(Value::Double(-n)),
                        Value::Null => Ok(Value::Null),
                        _ => Err(DatabaseError::ExecutionError(format!(
                            "cannot negate {:?}",
                            val
                        ))),
                    },
                    UnaryOperator::Plus => Ok(val), // no-op
                    UnaryOperator::Not => match &val {
                        Value::Boolean(b) => Ok(Value::Boolean(!b)),
                        Value::Null => Ok(Value::Null),
                        _ => Err(DatabaseError::ExecutionError(format!(
                            "NOT requires boolean, got {:?}",
                            val
                        ))),
                    },
                    _ => Err(DatabaseError::ExecutionError(format!(
                        "unsupported unary operator: {:?}",
                        op
                    ))),
                }
            }

            Expr::Function(func) => {
                // Evaluate function arguments
                let args: DatabaseResult<Vec<Value>> = func
                    .args
                    .iter()
                    .map(|a| self.eval_expr(row, a, schema))
                    .collect();
                let args = args?;

                nexus_sql::executor::evaluate_scalar_function(&func.name, &args)
                    .map_err(|e| DatabaseError::ExecutionError(format!("function error: {}", e)))
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    // Simple CASE: CASE expr WHEN val THEN result ...
                    let op_val = self.eval_expr(row, op, schema)?;
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.eval_expr(row, when_expr, schema)?;
                        if op_val == when_val {
                            return self.eval_expr(row, then_expr, schema);
                        }
                    }
                } else {
                    // Searched CASE: CASE WHEN condition THEN result ...
                    for (when_expr, then_expr) in when_clauses {
                        let cond = self.eval_expr(row, when_expr, schema)?;
                        if matches!(cond, Value::Boolean(true)) {
                            return self.eval_expr(row, then_expr, schema);
                        }
                    }
                }
                // ELSE clause
                if let Some(else_expr) = else_clause {
                    self.eval_expr(row, else_expr, schema)
                } else {
                    Ok(Value::Null)
                }
            }

            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                let val = self.eval_expr(row, inner, schema)?;
                val.cast(data_type)
                    .map_err(|e| DatabaseError::ExecutionError(format!("CAST error: {}", e)))
            }

            Expr::IsNull(inner) => {
                let val = self.eval_expr(row, inner, schema)?;
                Ok(Value::Boolean(val.is_null()))
            }

            Expr::IsNotNull(inner) => {
                let val = self.eval_expr(row, inner, schema)?;
                Ok(Value::Boolean(!val.is_null()))
            }

            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let val = self.eval_expr(row, inner, schema)?;
                let low_val = self.eval_expr(row, low, schema)?;
                let high_val = self.eval_expr(row, high, schema)?;

                if val.is_null() || low_val.is_null() || high_val.is_null() {
                    return Ok(Value::Null);
                }

                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
            }

            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let val = self.eval_expr(row, inner, schema)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }

                let mut found = false;
                for item in list {
                    let item_val = self.eval_expr(row, item, schema)?;
                    if val == item_val {
                        found = true;
                        break;
                    }
                }

                Ok(Value::Boolean(if *negated { !found } else { found }))
            }

            Expr::Nested(inner) => self.eval_expr(row, inner, schema),

            _ => Err(DatabaseError::ExecutionError(format!(
                "unsupported expression in SET/WHERE: {:?}",
                std::mem::discriminant(expr)
            ))),
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
    use crate::database::engine::Database;

    fn create_session() -> Session {
        let db = Arc::new(Database::open_memory().expect("open"));
        Session::new(
            SessionId::new(1),
            db,
            "nexusdb".to_string(),
            SessionConfig::default(),
        )
    }

    #[test]
    fn test_session_create_table() {
        let mut session = create_session();

        let result = session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        assert!(matches!(result, StatementResult::Ddl { .. }));

        assert!(session.storage_ref().table_exists("users"));
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

    #[test]
    fn test_session_show_tables() {
        let mut session = create_session();

        // No tables initially
        let result = session.execute("SHOW TABLES").unwrap();
        if let StatementResult::Query(query_result) = result {
            assert_eq!(query_result.total_rows, 0);
        } else {
            panic!("expected Query result");
        }

        // Create some tables
        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
            .unwrap();

        // Should show 2 tables
        let result = session.execute("SHOW TABLES").unwrap();
        if let StatementResult::Query(query_result) = result {
            assert_eq!(query_result.total_rows, 2);
            let rows = query_result.rows();
            // Should be sorted alphabetically
            assert_eq!(rows[0].get(0), Some(&Value::String("orders".to_string())));
            assert_eq!(rows[1].get(0), Some(&Value::String("users".to_string())));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_describe_table() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)")
            .unwrap();

        let result = session.execute("DESCRIBE users").unwrap();
        if let StatementResult::Query(query_result) = result {
            assert_eq!(query_result.total_rows, 3);
            let rows = query_result.rows();

            // Check column names
            assert_eq!(rows[0].get(0), Some(&Value::String("id".to_string())));
            assert_eq!(rows[1].get(0), Some(&Value::String("name".to_string())));
            assert_eq!(rows[2].get(0), Some(&Value::String("email".to_string())));

            // Check primary key column
            assert_eq!(rows[0].get(3), Some(&Value::Boolean(true))); // id is PK
            assert_eq!(rows[1].get(3), Some(&Value::Boolean(false))); // name is not PK
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_show_databases() {
        let mut session = create_session();

        let result = session.execute("SHOW databases").unwrap();
        if let StatementResult::Query(query_result) = result {
            assert_eq!(query_result.total_rows, 1);
            let rows = query_result.rows();
            assert_eq!(rows[0].get(0), Some(&Value::String("nexusdb".to_string())));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_having_clause() {
        let mut session = create_session();

        // Create table
        session
            .execute("CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, amount INT)")
            .unwrap();

        // Insert test data
        session
            .execute("INSERT INTO sales VALUES (1, 'Widget', 100)")
            .unwrap();
        session
            .execute("INSERT INTO sales VALUES (2, 'Widget', 200)")
            .unwrap();
        session
            .execute("INSERT INTO sales VALUES (3, 'Gadget', 150)")
            .unwrap();
        session
            .execute("INSERT INTO sales VALUES (4, 'Widget', 50)")
            .unwrap();
        session
            .execute("INSERT INTO sales VALUES (5, 'Gadget', 50)")
            .unwrap();

        // First test: simple GROUP BY without HAVING
        let result = session.execute("SELECT product, SUM(amount) FROM sales GROUP BY product");
        assert!(result.is_ok(), "GROUP BY failed: {:?}", result);

        // Second test: GROUP BY with HAVING
        let result = session.execute(
            "SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 200",
        );
        assert!(result.is_ok(), "HAVING clause failed: {:?}", result);

        if let StatementResult::Query(query_result) = result.unwrap() {
            // Widget: 350, Gadget: 200 -> only Widget should be returned
            assert_eq!(
                query_result.total_rows, 1,
                "HAVING should filter to 1 group"
            );
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_explain() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();

        // EXPLAIN should return a plan description as a query result
        let result = session.execute("EXPLAIN SELECT * FROM users").unwrap();

        if let StatementResult::Query(query_result) = result {
            assert!(query_result.total_rows > 0, "EXPLAIN should produce output");
            let rows = query_result.rows();
            // The plan column should be "plan"
            assert_eq!(query_result.schema.fields()[0].name(), "plan");
            // Should contain SeqScan reference
            let plan_text: String = rows
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan_text.contains("SeqScan"),
                "EXPLAIN output should mention SeqScan, got: {}",
                plan_text
            );
        } else {
            panic!("expected Query result for EXPLAIN");
        }
    }

    #[test]
    fn test_session_explain_analyze() {
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

        // EXPLAIN ANALYZE should execute the query and include actual metrics
        let result = session
            .execute("EXPLAIN ANALYZE SELECT * FROM users")
            .unwrap();

        if let StatementResult::Query(query_result) = result {
            assert!(
                query_result.total_rows > 0,
                "EXPLAIN ANALYZE should produce output"
            );
            let plan_text: String = query_result
                .rows()
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            // Should contain actual runtime info
            assert!(
                plan_text.contains("actual"),
                "EXPLAIN ANALYZE output should contain actual metrics, got: {}",
                plan_text
            );
        } else {
            panic!("expected Query result for EXPLAIN ANALYZE");
        }
    }

    #[test]
    fn test_session_explain_verbose() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        let result = session
            .execute("EXPLAIN VERBOSE SELECT * FROM users")
            .unwrap();

        if let StatementResult::Query(query_result) = result {
            let plan_text: String = query_result
                .rows()
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            // Verbose includes both logical and physical plan
            assert!(
                plan_text.contains("Logical Plan"),
                "VERBOSE should include logical plan, got: {}",
                plan_text
            );
            assert!(
                plan_text.contains("Physical Plan"),
                "VERBOSE should include physical plan, got: {}",
                plan_text
            );
        } else {
            panic!("expected Query result for EXPLAIN VERBOSE");
        }
    }

    #[test]
    fn test_session_explain_json() {
        // Test the JSON explain format by directly calling execute_explain
        // with ExplainFormat::Json, since the EXPLAIN (FORMAT JSON) syntax
        // may not be supported by the underlying sqlparser version.
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        // Parse the inner statement, then call execute_explain directly
        let inner = nexus_sql::parser::Parser::parse_one("SELECT * FROM users").unwrap();
        let format = nexus_sql::parser::ExplainFormat::Json;
        let start = std::time::Instant::now();
        let result = session
            .execute_explain(&inner, &format, false, start)
            .unwrap();

        if let StatementResult::Query(query_result) = result {
            assert_eq!(query_result.total_rows, 1, "JSON should be a single row");
            let json_str = match query_result.rows()[0].get(0) {
                Some(Value::String(s)) => s.clone(),
                _ => panic!("expected string value"),
            };
            // Should look like valid JSON with plan key
            assert!(
                json_str.contains("\"plan\""),
                "JSON output should contain 'plan' key, got: {}",
                json_str
            );
            assert!(
                json_str.starts_with('{'),
                "JSON output should start with '{{', got: {}",
                json_str
            );
            assert!(
                json_str.contains("\"operator\""),
                "JSON output should contain operator info, got: {}",
                json_str
            );
        } else {
            panic!("expected Query result for EXPLAIN JSON");
        }
    }

    #[test]
    fn test_session_explain_with_filter() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();
        session
            .execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();

        let result = session
            .execute("EXPLAIN SELECT * FROM users WHERE age > 25")
            .unwrap();

        if let StatementResult::Query(query_result) = result {
            let plan_text: String = query_result
                .rows()
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan_text.contains("SeqScan"),
                "Plan should include SeqScan, got: {}",
                plan_text
            );
        } else {
            panic!("expected Query result");
        }
    }

    // =========================================================================
    // DISTINCT aggregate end-to-end tests
    // =========================================================================

    /// Helper: extract the first column value from a single-row query result.
    fn extract_single_value(result: StatementResult) -> Value {
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1, "expected 1 row, got {}", qr.total_rows);
            qr.rows()[0].get(0).cloned().expect("row has no columns")
        } else {
            panic!("expected Query result, got {:?}", result);
        }
    }

    #[test]
    fn test_session_count_distinct_basic() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE items (id INT PRIMARY KEY, category TEXT, price INT)")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (1, 'A', 10)")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (2, 'B', 20)")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (3, 'A', 30)")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (4, 'C', 10)")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (5, 'A', 20)")
            .unwrap();

        // COUNT(DISTINCT category) should be 3 (A, B, C)
        let result = session
            .execute("SELECT COUNT(DISTINCT category) FROM items")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::BigInt(3), "expected 3 distinct categories");

        // COUNT(category) without DISTINCT should be 5
        let result = session
            .execute("SELECT COUNT(category) FROM items")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::BigInt(5), "expected 5 total categories");
    }

    #[test]
    fn test_session_count_distinct_with_nulls() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        session.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        session.execute("INSERT INTO t VALUES (3, 'y')").unwrap();
        session.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
        session.execute("INSERT INTO t VALUES (5, 'x')").unwrap();

        // COUNT(DISTINCT val) should be 2 (x, y) — NULLs excluded
        let result = session
            .execute("SELECT COUNT(DISTINCT val) FROM t")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::BigInt(2));
    }

    #[test]
    fn test_session_sum_distinct() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO nums VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO nums VALUES (2, 20)").unwrap();
        session.execute("INSERT INTO nums VALUES (3, 10)").unwrap();
        session.execute("INSERT INTO nums VALUES (4, 30)").unwrap();
        session.execute("INSERT INTO nums VALUES (5, 20)").unwrap();

        // SUM(DISTINCT val) = 10 + 20 + 30 = 60
        let result = session
            .execute("SELECT SUM(DISTINCT val) FROM nums")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Double(60.0));

        // SUM(val) without DISTINCT = 10 + 20 + 10 + 30 + 20 = 90
        let result = session.execute("SELECT SUM(val) FROM nums").unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Double(90.0));
    }

    #[test]
    fn test_session_avg_distinct() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO nums VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO nums VALUES (2, 20)").unwrap();
        session.execute("INSERT INTO nums VALUES (3, 10)").unwrap();

        // AVG(DISTINCT val) = (10 + 20) / 2 = 15
        let result = session
            .execute("SELECT AVG(DISTINCT val) FROM nums")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Double(15.0));

        // AVG(val) = (10 + 20 + 10) / 3 = 13.333...
        let result = session.execute("SELECT AVG(val) FROM nums").unwrap();
        if let Value::Double(d) = extract_single_value(result) {
            assert!((d - 13.333333).abs() < 0.001);
        } else {
            panic!("expected Double");
        }
    }

    #[test]
    fn test_session_count_distinct_with_group_by() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE orders (id INT PRIMARY KEY, dept TEXT, product TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO orders VALUES (1, 'Sales', 'Widget')")
            .unwrap();
        session
            .execute("INSERT INTO orders VALUES (2, 'Sales', 'Gadget')")
            .unwrap();
        session
            .execute("INSERT INTO orders VALUES (3, 'Sales', 'Widget')")
            .unwrap();
        session
            .execute("INSERT INTO orders VALUES (4, 'Eng', 'Widget')")
            .unwrap();
        session
            .execute("INSERT INTO orders VALUES (5, 'Eng', 'Widget')")
            .unwrap();

        // GROUP BY dept: Sales has 2 distinct products, Eng has 1
        let result = session
            .execute("SELECT dept, COUNT(DISTINCT product) FROM orders GROUP BY dept")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2);
            // Collect into a map for order-independent assertion
            let mut counts = std::collections::HashMap::new();
            for row in qr.rows() {
                let dept = match row.get(0) {
                    Some(Value::String(s)) => s.clone(),
                    other => panic!("unexpected dept value: {:?}", other),
                };
                let count = match row.get(1) {
                    Some(Value::BigInt(n)) => *n,
                    other => panic!("unexpected count value: {:?}", other),
                };
                counts.insert(dept, count);
            }
            assert_eq!(counts["Sales"], 2, "Sales should have 2 distinct products");
            assert_eq!(counts["Eng"], 1, "Eng should have 1 distinct product");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_mixed_distinct_and_non_distinct() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO t VALUES (2, 10)").unwrap();
        session.execute("INSERT INTO t VALUES (3, 20)").unwrap();

        // Mix COUNT(DISTINCT val) with COUNT(val) in same query
        let result = session
            .execute("SELECT COUNT(DISTINCT val), COUNT(val) FROM t")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1);
            let row = &qr.rows()[0];
            let distinct_count = row.get(0).cloned().unwrap();
            let total_count = row.get(1).cloned().unwrap();
            assert_eq!(
                distinct_count,
                Value::BigInt(2),
                "COUNT(DISTINCT val) should be 2"
            );
            assert_eq!(total_count, Value::BigInt(3), "COUNT(val) should be 3");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_count_distinct_empty_table() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE empty (id INT PRIMARY KEY, val TEXT)")
            .unwrap();

        let result = session
            .execute("SELECT COUNT(DISTINCT val) FROM empty")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::BigInt(0), "COUNT(DISTINCT) on empty table = 0");
    }

    #[test]
    fn test_session_count_distinct_single_value() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 42)").unwrap();
        session.execute("INSERT INTO t VALUES (2, 42)").unwrap();
        session.execute("INSERT INTO t VALUES (3, 42)").unwrap();

        // All same value -> COUNT(DISTINCT) = 1
        let result = session
            .execute("SELECT COUNT(DISTINCT val) FROM t")
            .unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::BigInt(1));
    }

    // =========================================================================
    // UNION / INTERSECT / EXCEPT end-to-end tests
    // =========================================================================

    /// Helper: collect all values from the first column, sorted for order-independent comparison.
    fn collect_first_column_sorted(result: StatementResult) -> Vec<Value> {
        if let StatementResult::Query(qr) = result {
            let mut vals: Vec<Value> = qr
                .rows()
                .iter()
                .map(|r| r.get(0).cloned().expect("row has no columns"))
                .collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            vals
        } else {
            panic!("expected Query result, got {:?}", result);
        }
    }

    /// Helper: create two tables for set operation tests.
    fn setup_set_op_tables(session: &mut Session) {
        session
            .execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        // t1: {1/Alice, 2/Bob, 3/Charlie}
        session
            .execute("INSERT INTO t1 VALUES (1, 'Alice')")
            .unwrap();
        session.execute("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
        session
            .execute("INSERT INTO t1 VALUES (3, 'Charlie')")
            .unwrap();

        // t2: {4/Bob, 5/Charlie, 6/Diana}
        session.execute("INSERT INTO t2 VALUES (4, 'Bob')").unwrap();
        session
            .execute("INSERT INTO t2 VALUES (5, 'Charlie')")
            .unwrap();
        session
            .execute("INSERT INTO t2 VALUES (6, 'Diana')")
            .unwrap();
    }

    #[test]
    fn test_session_union_all() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // UNION ALL: all rows from both sides (6 total)
        let result = session
            .execute("SELECT name FROM t1 UNION ALL SELECT name FROM t2")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 6, "UNION ALL should return all 6 rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_union_dedup() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // UNION (without ALL): deduplicated. Bob+Charlie appear in both.
        // Unique names: Alice, Bob, Charlie, Diana = 4
        let result = session
            .execute("SELECT name FROM t1 UNION SELECT name FROM t2")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(
                qr.total_rows, 4,
                "UNION should deduplicate to 4 unique names"
            );
            let names: Vec<String> = qr
                .rows()
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Bob".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
            assert!(names.contains(&"Diana".to_string()));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_intersect() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // INTERSECT: names in both t1 AND t2 = {Bob, Charlie}
        let result = session
            .execute("SELECT name FROM t1 INTERSECT SELECT name FROM t2")
            .unwrap();

        let vals = collect_first_column_sorted(result);
        assert_eq!(vals.len(), 2, "INTERSECT should return 2 rows");
        assert_eq!(vals[0], Value::string("Bob"));
        assert_eq!(vals[1], Value::string("Charlie"));
    }

    #[test]
    fn test_session_except() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // EXCEPT: names in t1 but NOT in t2 = {Alice}
        let result = session
            .execute("SELECT name FROM t1 EXCEPT SELECT name FROM t2")
            .unwrap();

        let vals = collect_first_column_sorted(result);
        assert_eq!(vals.len(), 1, "EXCEPT should return 1 row");
        assert_eq!(vals[0], Value::string("Alice"));
    }

    #[test]
    fn test_session_except_reverse() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // EXCEPT reversed: names in t2 but NOT in t1 = {Diana}
        let result = session
            .execute("SELECT name FROM t2 EXCEPT SELECT name FROM t1")
            .unwrap();

        let vals = collect_first_column_sorted(result);
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0], Value::string("Diana"));
    }

    #[test]
    fn test_session_union_all_with_order_by_limit() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // UNION ALL with ORDER BY and LIMIT
        let result = session
            .execute("SELECT name FROM t1 UNION ALL SELECT name FROM t2 ORDER BY name LIMIT 3")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 3, "LIMIT 3 should return 3 rows");
            // Should be alphabetically first 3: Alice, Bob, Bob
            let names: Vec<String> = qr
                .rows()
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(names[0], "Alice");
            assert_eq!(names[1], "Bob");
            assert_eq!(names[2], "Bob");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_intersect_all() {
        let mut session = create_session();

        // Setup tables with duplicates for INTERSECT ALL testing
        session
            .execute("CREATE TABLE a (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session
            .execute("CREATE TABLE b (id INT PRIMARY KEY, val INT)")
            .unwrap();

        // a: val = {1, 1, 2, 3}  (via 4 rows)
        session.execute("INSERT INTO a VALUES (1, 1)").unwrap();
        session.execute("INSERT INTO a VALUES (2, 1)").unwrap();
        session.execute("INSERT INTO a VALUES (3, 2)").unwrap();
        session.execute("INSERT INTO a VALUES (4, 3)").unwrap();

        // b: val = {1, 2, 2}  (via 3 rows)
        session.execute("INSERT INTO b VALUES (1, 1)").unwrap();
        session.execute("INSERT INTO b VALUES (2, 2)").unwrap();
        session.execute("INSERT INTO b VALUES (3, 2)").unwrap();

        // INTERSECT ALL: min(count_a, count_b) for each value
        // val=1: min(2,1)=1, val=2: min(1,2)=1, val=3: min(1,0)=0
        // Result: {1, 2} = 2 rows
        let result = session
            .execute("SELECT val FROM a INTERSECT ALL SELECT val FROM b")
            .unwrap();

        let vals = collect_first_column_sorted(result);
        assert_eq!(vals.len(), 2, "INTERSECT ALL should return 2 rows");
    }

    #[test]
    fn test_session_except_all() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE a (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session
            .execute("CREATE TABLE b (id INT PRIMARY KEY, val INT)")
            .unwrap();

        // a: val = {1, 1, 2, 3}
        session.execute("INSERT INTO a VALUES (1, 1)").unwrap();
        session.execute("INSERT INTO a VALUES (2, 1)").unwrap();
        session.execute("INSERT INTO a VALUES (3, 2)").unwrap();
        session.execute("INSERT INTO a VALUES (4, 3)").unwrap();

        // b: val = {1, 2}
        session.execute("INSERT INTO b VALUES (1, 1)").unwrap();
        session.execute("INSERT INTO b VALUES (2, 2)").unwrap();

        // EXCEPT ALL: left_count - right_count for each value
        // val=1: 2-1=1, val=2: 1-1=0, val=3: 1-0=1
        // Result: {1, 3} = 2 rows
        let result = session
            .execute("SELECT val FROM a EXCEPT ALL SELECT val FROM b")
            .unwrap();

        let vals = collect_first_column_sorted(result);
        assert_eq!(vals.len(), 2, "EXCEPT ALL should return 2 rows");
    }

    #[test]
    fn test_session_union_empty_result() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE t2 (id INT PRIMARY KEY, val TEXT)")
            .unwrap();

        // Both empty -> UNION returns 0 rows
        let result = session
            .execute("SELECT val FROM t1 UNION ALL SELECT val FROM t2")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 0);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_intersect_disjoint() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // INTERSECT with disjoint id columns -> empty
        let result = session
            .execute("SELECT id FROM t1 INTERSECT SELECT id FROM t2")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(
                qr.total_rows, 0,
                "INTERSECT of disjoint sets should be empty"
            );
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_except_identical() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO t VALUES (2, 20)").unwrap();

        // EXCEPT of identical queries -> empty
        let result = session
            .execute("SELECT val FROM t EXCEPT SELECT val FROM t")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 0, "EXCEPT of same set should be empty");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_union_multi_column() {
        let mut session = create_session();
        setup_set_op_tables(&mut session);

        // UNION with multiple columns (id, name)
        let result = session
            .execute("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2")
            .unwrap();

        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 6, "UNION ALL of 3+3 rows = 6");
        } else {
            panic!("expected Query result");
        }
    }

    // =========================================================================
    // INSERT...SELECT end-to-end tests
    // =========================================================================

    #[test]
    fn test_session_insert_select_basic() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE src (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE dst (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        session
            .execute("INSERT INTO src VALUES (1, 'Alice')")
            .unwrap();
        session
            .execute("INSERT INTO src VALUES (2, 'Bob')")
            .unwrap();
        session
            .execute("INSERT INTO src VALUES (3, 'Charlie')")
            .unwrap();

        // INSERT...SELECT: copy all rows from src to dst
        let result = session
            .execute("INSERT INTO dst SELECT * FROM src")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 3, "should insert 3 rows");
        } else {
            panic!("expected Insert result, got {:?}", result);
        }

        // Verify dst has the rows
        let result = session.execute("SELECT * FROM dst").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 3);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_insert_select_with_where() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE src (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();
        session
            .execute("CREATE TABLE dst (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();

        session
            .execute("INSERT INTO src VALUES (1, 'Alice', 30)")
            .unwrap();
        session
            .execute("INSERT INTO src VALUES (2, 'Bob', 20)")
            .unwrap();
        session
            .execute("INSERT INTO src VALUES (3, 'Charlie', 35)")
            .unwrap();

        // INSERT with filtered SELECT
        let result = session
            .execute("INSERT INTO dst SELECT * FROM src WHERE age > 25")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 2, "should insert 2 rows (age > 25)");
        } else {
            panic!("expected Insert result");
        }
    }

    #[test]
    fn test_session_insert_select_with_columns() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE src (id INT PRIMARY KEY, name TEXT, val INT)")
            .unwrap();
        session
            .execute("CREATE TABLE dst (id INT PRIMARY KEY, val INT, name TEXT)")
            .unwrap();

        session
            .execute("INSERT INTO src VALUES (1, 'X', 100)")
            .unwrap();

        // INSERT with explicit column mapping (reordered)
        let result = session
            .execute("INSERT INTO dst (id, name, val) SELECT id, name, val FROM src")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 1);
        } else {
            panic!("expected Insert result");
        }

        // Verify the data is correct
        let result = session.execute("SELECT name, val FROM dst").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1);
            let row = &qr.rows()[0];
            assert_eq!(row.get(0).cloned(), Some(Value::string("X")));
            assert_eq!(row.get(1).cloned(), Some(Value::Int(100)));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_insert_select_empty_result() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session
            .execute("CREATE TABLE dst (id INT PRIMARY KEY, val INT)")
            .unwrap();

        // SELECT with impossible WHERE -> 0 rows inserted
        let result = session
            .execute("INSERT INTO dst SELECT * FROM src WHERE val > 999")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 0, "empty SELECT should insert 0 rows");
        } else {
            panic!("expected Insert result");
        }
    }

    #[test]
    fn test_session_insert_select_with_aggregates() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE sales (id INT PRIMARY KEY, dept TEXT, amount INT)")
            .unwrap();
        session
            .execute("CREATE TABLE summary (dept TEXT PRIMARY KEY, total INT)")
            .unwrap();

        session
            .execute("INSERT INTO sales VALUES (1, 'A', 100)")
            .unwrap();
        session
            .execute("INSERT INTO sales VALUES (2, 'A', 200)")
            .unwrap();
        session
            .execute("INSERT INTO sales VALUES (3, 'B', 150)")
            .unwrap();

        // INSERT...SELECT with GROUP BY aggregate
        let result = session
            .execute("INSERT INTO summary SELECT dept, SUM(amount) FROM sales GROUP BY dept")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 2, "should insert 2 department summaries");
        } else {
            panic!("expected Insert result");
        }
    }

    #[test]
    fn test_session_insert_select_self_table() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO t VALUES (2, 20)").unwrap();

        // INSERT from same table (snapshot semantics — reads before writes)
        // This inserts the existing rows with new IDs
        let result = session
            .execute("INSERT INTO t SELECT id + 100, val FROM t")
            .unwrap();

        if let StatementResult::Insert { rows_affected } = result {
            assert_eq!(rows_affected, 2, "should insert 2 copies");
        } else {
            panic!("expected Insert result");
        }

        // Verify total rows
        let result = session.execute("SELECT * FROM t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 4, "should have 4 total rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_insert_select_column_count_mismatch() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE src (id INT PRIMARY KEY, name TEXT, extra INT)")
            .unwrap();
        session
            .execute("CREATE TABLE dst (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO src VALUES (1, 'Alice', 99)")
            .unwrap();

        // SELECT produces 3 columns, but dst has only 2 -> error
        let result = session.execute("INSERT INTO dst SELECT * FROM src");
        assert!(result.is_err(), "column count mismatch should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("column count mismatch"),
            "error should mention column mismatch: {}",
            err
        );
    }

    // =========================================================================
    // ALTER TABLE end-to-end tests
    // =========================================================================

    #[test]
    fn test_session_alter_table_add_column() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'Alice')")
            .unwrap();
        session.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

        // ADD COLUMN
        session.execute("ALTER TABLE t ADD COLUMN age INT").unwrap();

        // Verify schema changed via DESCRIBE
        let result = session.execute("DESCRIBE t").unwrap();
        if let StatementResult::Query(qr) = result {
            // Should now have 3 columns
            assert_eq!(qr.total_rows, 3, "should have 3 columns after ADD COLUMN");
        } else {
            panic!("expected Query result");
        }

        // Verify existing rows get NULL for new column
        let result = session.execute("SELECT id, name, age FROM t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2);
            for row in qr.rows() {
                assert_eq!(
                    row.get(2).cloned(),
                    Some(Value::Null),
                    "new column should be NULL"
                );
            }
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_alter_table_drop_column() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'Alice', 30)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (2, 'Bob', 25)")
            .unwrap();

        // DROP COLUMN
        session.execute("ALTER TABLE t DROP COLUMN age").unwrap();

        // Verify schema
        let result = session.execute("DESCRIBE t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2, "should have 2 columns after DROP");
        } else {
            panic!("expected Query result");
        }

        // Verify data - should only have id and name
        let result = session.execute("SELECT * FROM t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_alter_table_rename_column() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'Alice')")
            .unwrap();

        // RENAME COLUMN
        session
            .execute("ALTER TABLE t RENAME COLUMN name TO full_name")
            .unwrap();

        // Verify we can query with new name
        let result = session.execute("SELECT full_name FROM t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1);
            assert_eq!(qr.rows()[0].get(0).cloned(), Some(Value::string("Alice")));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_alter_table_add_column_duplicate_error() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        let result = session.execute("ALTER TABLE t ADD COLUMN name TEXT");
        assert!(result.is_err(), "duplicate column should fail");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn test_session_alter_table_drop_column_not_found() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        let result = session.execute("ALTER TABLE t DROP COLUMN nonexistent");
        assert!(result.is_err(), "dropping nonexistent column should fail");
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_session_alter_table_drop_column_if_exists() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        // DROP COLUMN IF EXISTS on nonexistent column should succeed
        let result = session
            .execute("ALTER TABLE t DROP COLUMN IF EXISTS nonexistent")
            .unwrap();
        assert!(matches!(result, StatementResult::Ddl { .. }));
    }

    #[test]
    fn test_session_alter_table_drop_pk_column_error() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();

        let result = session.execute("ALTER TABLE t DROP COLUMN id");
        assert!(result.is_err(), "dropping PK column should fail");
        assert!(result.unwrap_err().to_string().contains("primary key"));
    }

    #[test]
    fn test_session_alter_table_drop_last_column_error() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .unwrap();

        let result = session.execute("ALTER TABLE t DROP COLUMN id");
        // Could fail with either "only column" or "primary key" - both are valid
        assert!(result.is_err(), "dropping last/PK column should fail");
    }

    #[test]
    fn test_session_alter_table_rename_column_conflict() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();

        let result = session.execute("ALTER TABLE t RENAME COLUMN name TO age");
        assert!(result.is_err(), "rename to existing name should fail");
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_session_alter_table_if_exists_nonexistent() {
        let mut session = create_session();

        // IF EXISTS on nonexistent table should succeed silently
        let result = session
            .execute("ALTER TABLE IF EXISTS nonexistent ADD COLUMN x INT")
            .unwrap();
        assert!(matches!(result, StatementResult::Ddl { .. }));
    }

    #[test]
    fn test_session_alter_table_add_then_insert() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1)").unwrap();

        // Add a column then insert a row using it
        session
            .execute("ALTER TABLE t ADD COLUMN val TEXT")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (2, 'hello')")
            .unwrap();

        let result = session.execute("SELECT * FROM t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_session_alter_table_rename_table() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE old_name (id INT PRIMARY KEY, val TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO old_name VALUES (1, 'data')")
            .unwrap();

        // RENAME TABLE
        session
            .execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();

        // Verify via SHOW TABLES that old_name is gone and new_name exists
        let result = session.execute("SHOW TABLES").unwrap();
        if let StatementResult::Query(qr) = &result {
            let tables: Vec<String> = qr
                .rows()
                .iter()
                .filter_map(|r| match r.get(0) {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert!(
                !tables.contains(&"old_name".to_string()),
                "old_name should not be in table list: {:?}",
                tables
            );
            assert!(
                tables.contains(&"new_name".to_string()),
                "new_name should be in table list: {:?}",
                tables
            );
        }

        // New name should have the data
        let result = session.execute("SELECT * FROM new_name").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1);
        } else {
            panic!("expected Query result");
        }

        // New name should have the data
        let result = session.execute("SELECT * FROM new_name").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1);
        } else {
            panic!("expected Query result");
        }
    }

    // =========================================================================
    // UPDATE SET expression tests
    // =========================================================================

    #[test]
    fn test_session_update_set_column_plus_literal() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO t VALUES (2, 20)").unwrap();

        // SET val = val + 5
        let result = session.execute("UPDATE t SET val = val + 5").unwrap();
        if let StatementResult::Update { rows_affected } = result {
            assert_eq!(rows_affected, 2);
        } else {
            panic!("expected Update result");
        }

        // Verify: 10+5=15, 20+5=25
        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Int(15));

        let result = session.execute("SELECT val FROM t WHERE id = 2").unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Int(25));
    }

    #[test]
    fn test_session_update_set_multiply() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, price INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 100)").unwrap();

        // SET price = price * 2
        session.execute("UPDATE t SET price = price * 2").unwrap();

        let result = session.execute("SELECT price FROM t WHERE id = 1").unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Int(200));
    }

    #[test]
    fn test_session_update_set_subtract_and_divide() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 100)").unwrap();

        // SET val = val - 10
        session.execute("UPDATE t SET val = val - 10").unwrap();
        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        let val = extract_single_value(result);
        assert_eq!(val, Value::Int(90));
    }

    #[test]
    fn test_session_update_set_with_where_expression() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        session.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        session.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        // Only update rows where val > 15
        session
            .execute("UPDATE t SET val = val + 100 WHERE val > 15")
            .unwrap();

        // id=1: val=10 (unchanged), id=2: val=120, id=3: val=130
        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::Int(10));

        let result = session.execute("SELECT val FROM t WHERE id = 2").unwrap();
        assert_eq!(extract_single_value(result), Value::Int(120));

        let result = session.execute("SELECT val FROM t WHERE id = 3").unwrap();
        assert_eq!(extract_single_value(result), Value::Int(130));
    }

    #[test]
    fn test_session_update_set_column_to_column() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();

        // SET a = b (copy column value)
        session.execute("UPDATE t SET a = b WHERE id = 1").unwrap();

        let result = session.execute("SELECT a FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::Int(20));
    }

    #[test]
    fn test_session_update_set_cross_column_arithmetic() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10, 3)").unwrap();

        // SET a = a + b
        session
            .execute("UPDATE t SET a = a + b WHERE id = 1")
            .unwrap();

        let result = session.execute("SELECT a FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::Int(13));
    }

    #[test]
    fn test_session_update_set_string_concat() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'hello')")
            .unwrap();

        // SET name = name || ' world'
        session
            .execute("UPDATE t SET name = name || ' world' WHERE id = 1")
            .unwrap();

        let result = session.execute("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::string("hello world"));
    }

    #[test]
    fn test_session_update_set_function_call() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'hello')")
            .unwrap();

        // SET name = UPPER(name)
        session
            .execute("UPDATE t SET name = UPPER(name) WHERE id = 1")
            .unwrap();

        let result = session.execute("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::string("HELLO"));
    }

    #[test]
    fn test_session_update_set_negation() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 42)").unwrap();

        // SET val = -val
        session
            .execute("UPDATE t SET val = -val WHERE id = 1")
            .unwrap();

        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::Int(-42));
    }

    #[test]
    fn test_session_update_set_null_propagation() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

        // SET val = val + 1 — NULL + 1 = NULL
        session
            .execute("UPDATE t SET val = val + 1 WHERE id = 1")
            .unwrap();

        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::Null);
    }

    #[test]
    fn test_session_update_set_division_by_zero() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 42)").unwrap();

        // SET val = val / 0 — should error
        let result = session.execute("UPDATE t SET val = val / 0");
        assert!(result.is_err(), "division by zero should fail");
        assert!(result.unwrap_err().to_string().contains("division by zero"));
    }

    #[test]
    fn test_session_update_multiple_set_expressions() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();

        // SET a = a + 1, b = b * 2
        session
            .execute("UPDATE t SET a = a + 1, b = b * 2 WHERE id = 1")
            .unwrap();

        let result = session.execute("SELECT a, b FROM t WHERE id = 1").unwrap();
        if let StatementResult::Query(qr) = result {
            let row = &qr.rows()[0];
            assert_eq!(row.get(0).cloned(), Some(Value::Int(11)));
            assert_eq!(row.get(1).cloned(), Some(Value::Int(40)));
        } else {
            panic!("expected Query result");
        }
    }

    // =========================================================================
    // Constraint enforcement tests
    // =========================================================================

    #[test]
    fn test_constraint_not_null_insert_violation() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap();

        // Explicit NULL into NOT NULL column should fail
        let result = session.execute("INSERT INTO t VALUES (1, NULL)");
        assert!(result.is_err(), "NOT NULL violation should fail");
        assert!(result.unwrap_err().to_string().contains("NOT NULL"));
    }

    #[test]
    fn test_constraint_not_null_insert_omitted_column() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap();

        // Omitting NOT NULL column should fail (gets NULL by default)
        let result = session.execute("INSERT INTO t (id) VALUES (1)");
        assert!(result.is_err(), "omitting NOT NULL column should fail");
        assert!(result.unwrap_err().to_string().contains("NOT NULL"));
    }

    #[test]
    fn test_constraint_default_overrides_null() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT DEFAULT 42)")
            .unwrap();

        // Omitting the column — default should apply
        session.execute("INSERT INTO t (id) VALUES (1)").unwrap();

        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(
            extract_single_value(result),
            Value::BigInt(42),
            "default should substitute for omitted column"
        );
    }

    #[test]
    fn test_constraint_not_null_update_violation() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'Alice')")
            .unwrap();

        // Setting NOT NULL column to NULL should fail
        let result = session.execute("UPDATE t SET name = NULL WHERE id = 1");
        assert!(
            result.is_err(),
            "UPDATE to NULL on NOT NULL column should fail"
        );
        assert!(result.unwrap_err().to_string().contains("NOT NULL"));
    }

    #[test]
    fn test_constraint_default_value() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, status TEXT DEFAULT 'active')")
            .unwrap();

        // Insert without specifying status — should get default
        session.execute("INSERT INTO t (id) VALUES (1)").unwrap();

        let result = session
            .execute("SELECT status FROM t WHERE id = 1")
            .unwrap();
        assert_eq!(
            extract_single_value(result),
            Value::string("active"),
            "default value should be applied"
        );
    }

    #[test]
    fn test_constraint_default_with_not_null() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT NOT NULL DEFAULT 0)")
            .unwrap();

        // Omitting val — default 0 should be applied, satisfying NOT NULL
        session.execute("INSERT INTO t (id) VALUES (1)").unwrap();

        let result = session.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(extract_single_value(result), Value::BigInt(0));
    }

    #[test]
    fn test_constraint_unique_insert_violation() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)")
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'a@b.com')")
            .unwrap();

        // Duplicate UNIQUE value should fail
        let result = session.execute("INSERT INTO t VALUES (2, 'a@b.com')");
        assert!(result.is_err(), "UNIQUE violation should fail");
        assert!(result.unwrap_err().to_string().contains("UNIQUE"));
    }

    #[test]
    fn test_constraint_unique_null_allowed() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        // Multiple NULLs are allowed in UNIQUE columns per SQL standard
        session.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

        let result = session.execute("SELECT * FROM t").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_constraint_unique_update_violation() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 'AAA')").unwrap();
        session.execute("INSERT INTO t VALUES (2, 'BBB')").unwrap();

        // Updating to existing UNIQUE value should fail
        let result = session.execute("UPDATE t SET code = 'AAA' WHERE id = 2");
        assert!(result.is_err(), "UNIQUE violation on UPDATE should fail");
        assert!(result.unwrap_err().to_string().contains("UNIQUE"));
    }

    #[test]
    fn test_constraint_unique_update_same_value_ok() {
        let mut session = create_session();

        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 'AAA')").unwrap();

        // Updating a row to its own UNIQUE value should succeed
        session
            .execute("UPDATE t SET code = 'AAA' WHERE id = 1")
            .unwrap();
    }

    // =========================================================================
    // WHERE clause: LIKE, IN, BETWEEN tests
    // =========================================================================

    /// Helper: set up a table for WHERE clause tests.
    fn setup_where_test_table(session: &mut Session) {
        session
            .execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (1, 'Apple', 150, 'fruit')")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (2, 'Banana', 80, 'fruit')")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (3, 'Carrot', 60, 'vegetable')")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (4, 'Apple Pie', 350, 'dessert')")
            .unwrap();
        session
            .execute("INSERT INTO items VALUES (5, 'Mango', 200, 'fruit')")
            .unwrap();
    }

    #[test]
    fn test_where_like_percent() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // LIKE '%apple%' (should match 'Apple' and 'Apple Pie' case-sensitively)
        let result = session
            .execute("SELECT name FROM items WHERE name LIKE 'Apple%'")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 2, "LIKE 'Apple%' should match 2 rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_like_underscore() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // LIKE '_ango' should match 'Mango'
        let result = session
            .execute("SELECT name FROM items WHERE name LIKE '_ango'")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 1);
            assert_eq!(qr.rows()[0].get(0).cloned(), Some(Value::string("Mango")));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_not_like() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // NOT LIKE 'Apple%' — should match Banana, Carrot, Mango (3 rows)
        let result = session
            .execute("SELECT name FROM items WHERE name NOT LIKE 'Apple%'")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 3);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_in_list() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // IN list
        let result = session
            .execute("SELECT name FROM items WHERE category IN ('fruit', 'dessert')")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            // fruit: Apple, Banana, Mango; dessert: Apple Pie = 4
            assert_eq!(qr.total_rows, 4, "IN should match 4 rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_not_in_list() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // NOT IN list
        let result = session
            .execute("SELECT name FROM items WHERE category NOT IN ('fruit')")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            // vegetable: Carrot, dessert: Apple Pie = 2
            assert_eq!(qr.total_rows, 2, "NOT IN should match 2 rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_in_integers() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // IN with integers
        let result = session
            .execute("SELECT name FROM items WHERE id IN (1, 3, 5)")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 3);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_between() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // BETWEEN (inclusive)
        let result = session
            .execute("SELECT name FROM items WHERE price BETWEEN 80 AND 200")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            // 80 (Banana), 150 (Apple), 200 (Mango) = 3
            assert_eq!(qr.total_rows, 3, "BETWEEN 80 AND 200 should match 3 rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_not_between() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // NOT BETWEEN
        let result = session
            .execute("SELECT name FROM items WHERE price NOT BETWEEN 100 AND 300")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            // Outside [100,300]: 80 (Banana), 60 (Carrot), 350 (Apple Pie) = 3
            assert_eq!(qr.total_rows, 3, "NOT BETWEEN should match 3 rows");
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_like_combined_with_and() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // LIKE + AND
        let result = session
            .execute("SELECT name FROM items WHERE category = 'fruit' AND price > 100")
            .unwrap();
        if let StatementResult::Query(qr) = result {
            // fruit with price > 100: Apple(150), Mango(200) = 2
            assert_eq!(qr.total_rows, 2);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_where_in_update() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // UPDATE with IN
        session
            .execute("UPDATE items SET price = price + 10 WHERE id IN (1, 3)")
            .unwrap();

        let result = session
            .execute("SELECT price FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(extract_single_value(result), Value::Int(160));

        let result = session
            .execute("SELECT price FROM items WHERE id = 3")
            .unwrap();
        assert_eq!(extract_single_value(result), Value::Int(70));
    }

    #[test]
    fn test_where_between_in_delete() {
        let mut session = create_session();
        setup_where_test_table(&mut session);

        // DELETE with BETWEEN
        session
            .execute("DELETE FROM items WHERE price BETWEEN 100 AND 200")
            .unwrap();

        // Remaining: Banana(80), Carrot(60), Apple Pie(350) = 3
        let result = session.execute("SELECT * FROM items").unwrap();
        if let StatementResult::Query(qr) = result {
            assert_eq!(qr.total_rows, 3);
        } else {
            panic!("expected Query result");
        }
    }
}
