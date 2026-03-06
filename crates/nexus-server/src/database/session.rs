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

        // Create table info
        let mut info = TableInfo::new(name.clone(), schema);
        if !primary_key.is_empty() {
            info = info.with_primary_key(primary_key);
        }

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

        let count = self.storage().execute_insert(table_name, rows.clone())?;

        // Populate any HNSW vector indexes for this table
        let vim = self.database.vector_index_manager();
        let vector_keys = vim.list_table_indexes(&self.current_database, table_name);
        if !vector_keys.is_empty() {
            // Find HNSW index info to know which column has the vector
            for vk in &vector_keys {
                if let Some(idx_info) = table_info.indexes.iter().find(|i| i.name == vk.index_name)
                {
                    if let Some(&col_idx) = idx_info.columns.first() {
                        for (row_i, row) in rows.iter().enumerate() {
                            if let Some(Value::Vector(ref vec_data)) = row.get(col_idx) {
                                // Derive a VectorId from the primary key or row counter
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
                                    row_i as u64
                                };
                                // Best-effort: ignore errors (e.g. duplicate id)
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

            // Apply updates
            let mut new_row = row.clone();
            for assignment in &update.assignments {
                let col_name = &assignment.column.column;
                if let Some(col_idx) = table_info.schema.index_of(col_name) {
                    new_row.set(col_idx, self.eval_literal(&assignment.value)?);
                }
            }

            // Update the row
            if self.storage().execute_update(table_name, new_row)? {
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
                deleted_count += 1;
            }
        }

        Ok(StatementResult::Delete {
            rows_affected: deleted_count,
        })
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
}
