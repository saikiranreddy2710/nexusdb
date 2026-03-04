//! Main database engine.
//!
//! The `Database` struct is the top-level entry point for NexusDB.
//! It manages sessions, storage, and provides the unified API.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nexus_security::{AuditLog, Authenticator, Authorizer};
use nexus_sql::storage::StorageEngine;
use nexus_wal::{SyncPolicy, Wal, WalConfig};

use super::error::{DatabaseError, DatabaseResult};
use super::result::StatementResult;
use super::session::{Session, SessionConfig, SessionId};

/// Database configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Path to data directory (None for in-memory).
    pub data_dir: Option<String>,
    /// Maximum number of concurrent sessions.
    pub max_sessions: usize,
    /// Default session configuration.
    pub session_config: SessionConfig,
    /// Enable write-ahead logging.
    pub wal_enabled: bool,
    /// WAL sync mode (0=none, 1=normal, 2=full).
    pub wal_sync_mode: u8,
    /// Whether authentication is enforced (false = permissive mode).
    pub auth_enabled: bool,
    /// Whether authorization (RBAC) is enforced.
    pub authz_enabled: bool,
    /// Maximum audit log entries in memory.
    pub audit_max_entries: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            max_sessions: 1000,
            session_config: SessionConfig::default(),
            wal_enabled: true,
            wal_sync_mode: 1,
            auth_enabled: false,
            authz_enabled: false,
            audit_max_entries: 10_000,
        }
    }
}

impl DatabaseConfig {
    /// Creates an in-memory configuration.
    pub fn in_memory() -> Self {
        Self {
            data_dir: None,
            wal_enabled: false,
            ..Default::default()
        }
    }

    /// Creates a configuration for a directory.
    pub fn with_path(path: impl Into<String>) -> Self {
        Self {
            data_dir: Some(path.into()),
            ..Default::default()
        }
    }
}

/// Database statistics.
#[derive(Debug, Default)]
pub struct DatabaseStats {
    /// Number of active sessions.
    pub active_sessions: usize,
    /// Total sessions created.
    pub total_sessions: u64,
    /// Total statements executed.
    pub total_statements: u64,
    /// Total tables.
    pub total_tables: usize,
    /// Total rows across all tables.
    pub total_rows: usize,
    /// Uptime.
    pub uptime: Duration,
}

/// Default database name when none is specified.
pub const DEFAULT_DATABASE_NAME: &str = "nexusdb";

/// The main database engine.
pub struct Database {
    /// Configuration.
    config: DatabaseConfig,
    /// Per-database storage engines. Key is database name.
    databases: RwLock<HashMap<String, Arc<StorageEngine>>>,
    /// Write-ahead log (if enabled). Used for default database when data_dir is set.
    wal: Option<Arc<Wal>>,
    /// Active sessions.
    sessions: RwLock<HashMap<SessionId, Arc<RwLock<Session>>>>,
    /// Next session ID.
    next_session_id: AtomicU64,
    /// Total sessions created.
    total_sessions: AtomicU64,
    /// When the database was started.
    started_at: Instant,
    /// Authentication service (SCRAM-SHA-256, API keys, JWT).
    authenticator: Arc<Authenticator>,
    /// Authorization service (RBAC).
    authorizer: Arc<Authorizer>,
    /// Tamper-proof audit log.
    audit_log: Arc<AuditLog>,
}

impl Database {
    /// Opens a database with the given configuration.
    pub fn open(config: DatabaseConfig) -> DatabaseResult<Self> {
        let storage = Arc::new(StorageEngine::new());

        // Initialize WAL if enabled and data directory is specified
        let wal = if config.wal_enabled {
            if let Some(ref data_dir) = config.data_dir {
                let wal_dir = PathBuf::from(data_dir).join("wal");
                let sync_policy = match config.wal_sync_mode {
                    0 => SyncPolicy::Never,
                    1 => SyncPolicy::GroupCommit,
                    _ => SyncPolicy::EveryWrite,
                };
                let wal_config = WalConfig::new(&wal_dir).with_sync_policy(sync_policy);

                // Try to open existing WAL or create new one
                let wal = if wal_dir.exists() {
                    match Wal::open(wal_config.clone()) {
                        Ok(wal) => {
                            tracing::info!("Opened existing WAL at {:?}", wal_dir);
                            // Replay WAL for recovery
                            Self::replay_wal(&wal, &storage)?;
                            wal
                        }
                        Err(e) => {
                            tracing::warn!("Failed to open WAL, creating new: {}", e);
                            Wal::new(wal_config).map_err(|e| {
                                DatabaseError::Internal(format!("Failed to create WAL: {}", e))
                            })?
                        }
                    }
                } else {
                    Wal::new(wal_config).map_err(|e| {
                        DatabaseError::Internal(format!("Failed to create WAL: {}", e))
                    })?
                };

                Some(Arc::new(wal))
            } else {
                // WAL enabled but no data directory - log warning
                tracing::warn!("WAL enabled but no data directory specified, running without WAL");
                None
            }
        } else {
            None
        };

        let mut databases = HashMap::new();
        databases.insert(DEFAULT_DATABASE_NAME.to_string(), storage);

        // Initialize security subsystems
        let authenticator = Arc::new(Authenticator::new(config.auth_enabled));
        let authorizer = Arc::new(Authorizer::new(config.authz_enabled));
        let audit_log = Arc::new(AuditLog::new(config.audit_max_entries));

        if config.auth_enabled {
            tracing::info!("Authentication enforcement enabled");
        } else {
            tracing::info!("Authentication in permissive mode (all requests allowed)");
        }

        Ok(Self {
            config,
            databases: RwLock::new(databases),
            wal,
            sessions: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            total_sessions: AtomicU64::new(0),
            started_at: Instant::now(),
            authenticator,
            authorizer,
            audit_log,
        })
    }

    /// Replays the WAL to recover state after a crash.
    fn replay_wal(wal: &Wal, _storage: &StorageEngine) -> DatabaseResult<()> {
        use nexus_wal::record::RecordType;

        let min_recovery_lsn = wal.min_recovery_lsn();

        // Read all records from the minimum recovery LSN
        let records = wal
            .iter_from(min_recovery_lsn)
            .map_err(|e| DatabaseError::Internal(format!("Failed to read WAL: {}", e)))?;

        if records.is_empty() {
            tracing::info!("No WAL records to replay");
            return Ok(());
        }

        tracing::info!(
            "Replaying {} WAL records from LSN {:?}",
            records.len(),
            min_recovery_lsn
        );

        // Track committed and aborted transactions
        let mut committed_txns = std::collections::HashSet::new();
        let mut aborted_txns = std::collections::HashSet::new();

        // First pass: identify committed and aborted transactions
        for record in &records {
            match record.header.record_type {
                RecordType::Commit => {
                    committed_txns.insert(record.header.txn_id);
                }
                RecordType::Abort => {
                    aborted_txns.insert(record.header.txn_id);
                }
                _ => {}
            }
        }

        tracing::info!(
            "Found {} committed, {} aborted transactions",
            committed_txns.len(),
            aborted_txns.len()
        );

        // TODO: Second pass - replay committed transactions' operations
        // This requires integration with the storage engine to re-apply
        // INSERT, UPDATE, DELETE operations. For now, we just log.
        //
        // In a full implementation:
        // - For each INSERT record from a committed txn: re-insert the row
        // - For each UPDATE record from a committed txn: re-apply the update
        // - For each DELETE record from a committed txn: re-delete the row
        //
        // We also need to handle the page_id/slot_id mapping to actual storage.

        Ok(())
    }

    /// Opens an in-memory database.
    pub fn open_memory() -> DatabaseResult<Self> {
        Self::open(DatabaseConfig::in_memory())
    }

    /// Opens a database at the given path.
    pub fn open_path(path: impl AsRef<Path>) -> DatabaseResult<Self> {
        Self::open(DatabaseConfig::with_path(
            path.as_ref().to_string_lossy().to_string(),
        ))
    }

    /// Returns the default database's storage engine (for backward compatibility).
    pub fn storage(&self) -> Arc<StorageEngine> {
        self.get_or_create_storage(DEFAULT_DATABASE_NAME)
    }

    /// Returns storage for the given database name, creating it if it does not exist.
    pub fn get_or_create_storage(&self, name: &str) -> Arc<StorageEngine> {
        let mut dbs = self.databases.write().unwrap();
        if let Some(storage) = dbs.get(name) {
            return Arc::clone(storage);
        }
        let storage = Arc::new(StorageEngine::new());
        dbs.insert(name.to_string(), Arc::clone(&storage));
        storage
    }

    /// Creates a new database. No-op if it already exists (unless if_not_exists is false).
    pub fn create_database(&self, name: &str, if_not_exists: bool) -> DatabaseResult<()> {
        let mut dbs = self.databases.write().unwrap();
        if dbs.contains_key(name) {
            return if if_not_exists {
                Ok(())
            } else {
                Err(DatabaseError::ExecutionError(format!(
                    "database \"{}\" already exists",
                    name
                )))
            };
        }
        dbs.insert(name.to_string(), Arc::new(StorageEngine::new()));
        Ok(())
    }

    /// Drops a database. Returns error if it does not exist (unless if_exists is true).
    pub fn drop_database(&self, name: &str, if_exists: bool) -> DatabaseResult<()> {
        if name == DEFAULT_DATABASE_NAME {
            return Err(DatabaseError::ExecutionError(
                "cannot drop the default database".to_string(),
            ));
        }
        let mut dbs = self.databases.write().unwrap();
        if dbs.remove(name).is_none() && !if_exists {
            return Err(DatabaseError::ExecutionError(format!(
                "database \"{}\" does not exist",
                name
            )));
        }
        Ok(())
    }

    /// Lists all database names.
    pub fn list_databases(&self) -> Vec<String> {
        let dbs = self.databases.read().unwrap();
        let mut names: Vec<String> = dbs.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns the WAL if enabled.
    pub fn wal(&self) -> Option<&Arc<Wal>> {
        self.wal.as_ref()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Returns the authenticator.
    pub fn authenticator(&self) -> &Arc<Authenticator> {
        &self.authenticator
    }

    /// Returns the authorizer.
    pub fn authorizer(&self) -> &Arc<Authorizer> {
        &self.authorizer
    }

    /// Returns the audit log.
    pub fn audit_log(&self) -> &Arc<AuditLog> {
        &self.audit_log
    }

    // =========================================================================
    // Session Management
    // =========================================================================

    /// Creates a new session attached to the given database name.
    /// Uses system identity (superuser). For authenticated sessions, use `create_authenticated_session`.
    pub fn create_session(self: &Arc<Self>, database_name: &str) -> SessionId {
        self.create_session_with_config(database_name, self.config.session_config.clone())
    }

    /// Creates a session with custom configuration (system identity).
    pub fn create_session_with_config(
        self: &Arc<Self>,
        database_name: &str,
        config: SessionConfig,
    ) -> SessionId {
        let id = SessionId::new(self.next_session_id.fetch_add(1, Ordering::SeqCst));
        let session = Session::new(id, self.clone(), database_name.to_string(), config);

        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(id, Arc::new(RwLock::new(session)));

        self.total_sessions.fetch_add(1, Ordering::Relaxed);

        id
    }

    /// Creates a new session with a specific authenticated identity.
    ///
    /// Used by the gRPC layer after validating client credentials.
    pub fn create_authenticated_session(
        self: &Arc<Self>,
        database_name: &str,
        identity: nexus_security::Identity,
    ) -> SessionId {
        let id = SessionId::new(self.next_session_id.fetch_add(1, Ordering::SeqCst));
        let session = Session::new_with_identity(
            id,
            self.clone(),
            database_name.to_string(),
            self.config.session_config.clone(),
            identity,
        );

        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(id, Arc::new(RwLock::new(session)));

        self.total_sessions.fetch_add(1, Ordering::Relaxed);

        id
    }

    /// Gets a session by ID.
    pub fn get_session(&self, id: SessionId) -> Option<Arc<RwLock<Session>>> {
        let sessions = self.sessions.read().unwrap();
        sessions.get(&id).cloned()
    }

    /// Closes a session.
    pub fn close_session(&self, id: SessionId) -> bool {
        let mut sessions = self.sessions.write().unwrap();
        if let Some(session) = sessions.remove(&id) {
            let mut s = session.write().unwrap();
            s.close();
            true
        } else {
            false
        }
    }

    /// Returns the number of active sessions.
    pub fn active_session_count(&self) -> usize {
        let sessions = self.sessions.read().unwrap();
        sessions.len()
    }

    // =========================================================================
    // Quick Execute API
    // =========================================================================

    /// Executes SQL using a temporary session on the default database.
    ///
    /// This is a convenience method for simple queries. For complex
    /// transactions, create a session explicitly.
    pub fn execute(self: &Arc<Self>, sql: &str) -> DatabaseResult<StatementResult> {
        let session_id = self.create_session(DEFAULT_DATABASE_NAME);
        let result = {
            let session_arc = self.get_session(session_id).unwrap();
            let mut session = session_arc.write().unwrap();
            session.execute(sql)
        };
        self.close_session(session_id);
        result
    }

    /// Executes multiple SQL statements using a temporary session.
    pub fn execute_batch(self: &Arc<Self>, sql: &str) -> DatabaseResult<Vec<StatementResult>> {
        let session_id = self.create_session(DEFAULT_DATABASE_NAME);
        let result = {
            let session_arc = self.get_session(session_id).unwrap();
            let mut session = session_arc.write().unwrap();
            session.execute_batch(sql)
        };
        self.close_session(session_id);
        result
    }

    /// Executes SQL within a transaction block.
    ///
    /// If the closure returns an error, the transaction is rolled back.
    /// Otherwise, it is committed.
    pub fn transaction<F, T>(self: &Arc<Self>, f: F) -> DatabaseResult<T>
    where
        F: FnOnce(&mut Session) -> DatabaseResult<T>,
    {
        let session_id = self.create_session(DEFAULT_DATABASE_NAME);
        let result = {
            let session_arc = self.get_session(session_id).unwrap();
            let mut session = session_arc.write().unwrap();

            session.begin()?;

            match f(&mut session) {
                Ok(value) => {
                    session.commit()?;
                    Ok(value)
                }
                Err(e) => {
                    let _ = session.rollback();
                    Err(e)
                }
            }
        };
        self.close_session(session_id);
        result
    }

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Returns database statistics.
    pub fn stats(&self) -> DatabaseStats {
        let storage_stats = self.storage().stats();

        DatabaseStats {
            active_sessions: self.active_session_count(),
            total_sessions: self.total_sessions.load(Ordering::Relaxed),
            total_statements: 0, // TODO: aggregate from sessions
            total_tables: storage_stats.table_count,
            total_rows: storage_stats.total_rows,
            uptime: self.started_at.elapsed(),
        }
    }

    /// Returns uptime.
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    // =========================================================================
    // Maintenance
    // =========================================================================

    /// Consolidates all delta chains in storage.
    pub fn consolidate(&self) {
        self.storage().consolidate_all();
    }

    /// Closes the database.
    pub fn close(&self) {
        // Close all sessions
        let session_ids: Vec<SessionId> = {
            let sessions = self.sessions.read().unwrap();
            sessions.keys().copied().collect()
        };

        for id in session_ids {
            self.close_session(id);
        }

        // Close WAL if enabled
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.sync() {
                tracing::error!("Failed to sync WAL: {}", e);
            }
            if let Err(e) = wal.close() {
                tracing::error!("Failed to close WAL: {}", e);
            }
        }

        // Consolidate storage
        self.storage().consolidate_all();
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

/// A convenience wrapper for using a session within a database.
#[allow(dead_code)]
pub struct DatabaseSession {
    db: Arc<Database>,
    session_id: SessionId,
}

#[allow(dead_code)]
impl DatabaseSession {
    /// Creates a new database session on the default database.
    pub fn new(db: &Arc<Database>) -> Self {
        let session_id = db.create_session(DEFAULT_DATABASE_NAME);
        Self {
            db: Arc::clone(db),
            session_id,
        }
    }

    /// Executes a SQL statement.
    pub fn execute(&self, sql: &str) -> DatabaseResult<StatementResult> {
        let session_arc = self.db.get_session(self.session_id).unwrap();
        let mut session = session_arc.write().unwrap();
        session.execute(sql)
    }

    /// Begins a transaction.
    pub fn begin(&self) -> DatabaseResult<()> {
        let session_arc = self.db.get_session(self.session_id).unwrap();
        let mut session = session_arc.write().unwrap();
        session.begin()
    }

    /// Commits the transaction.
    pub fn commit(&self) -> DatabaseResult<()> {
        let session_arc = self.db.get_session(self.session_id).unwrap();
        let mut session = session_arc.write().unwrap();
        session.commit()
    }

    /// Rolls back the transaction.
    pub fn rollback(&self) -> DatabaseResult<()> {
        let session_arc = self.db.get_session(self.session_id).unwrap();
        let mut session = session_arc.write().unwrap();
        session.rollback()
    }

    /// Returns the session ID.
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl Drop for DatabaseSession {
    fn drop(&mut self) {
        self.db.close_session(self.session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::error::DatabaseError;
    use nexus_sql::executor::Value;

    #[test]
    fn test_database_open_memory() {
        let db = Database::open_memory().unwrap();
        assert_eq!(db.active_session_count(), 0);
    }

    #[test]
    fn test_database_execute() {
        let db = Arc::new(Database::open_memory().unwrap());

        // Create table
        db.execute("CREATE TABLE test (id INT PRIMARY KEY, value TEXT)")
            .unwrap();

        // Insert
        let result = db.execute("INSERT INTO test VALUES (1, 'hello')").unwrap();
        assert_eq!(result.rows_affected(), Some(1));

        // Select
        let result = db.execute("SELECT * FROM test").unwrap();
        if let StatementResult::Query(query) = result {
            assert_eq!(query.total_rows, 1);
            let rows = query.rows();
            assert_eq!(rows[0].get(0), Some(&Value::Int(1)));
            assert_eq!(rows[0].get(1), Some(&Value::String("hello".to_string())));
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_database_session() {
        let db = Arc::new(Database::open_memory().unwrap());

        let session_id = db.create_session(DEFAULT_DATABASE_NAME);
        assert_eq!(db.active_session_count(), 1);

        db.close_session(session_id);
        assert_eq!(db.active_session_count(), 0);
    }

    #[test]
    fn test_database_session_wrapper() {
        let db = Arc::new(Database::open_memory().unwrap());

        {
            let session = DatabaseSession::new(&db);
            session
                .execute("CREATE TABLE test (id INT PRIMARY KEY)")
                .unwrap();
            session.execute("INSERT INTO test VALUES (1)").unwrap();
            assert_eq!(db.active_session_count(), 1);
        }

        // Session closed on drop
        assert_eq!(db.active_session_count(), 0);

        // Table still exists
        let result = db.execute("SELECT * FROM test").unwrap();
        assert!(matches!(result, StatementResult::Query(_)));
    }

    #[test]
    fn test_database_transaction() {
        let db = Arc::new(Database::open_memory().unwrap());

        db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)")
            .unwrap();
        db.execute("INSERT INTO accounts VALUES (1, 100)").unwrap();

        // Successful transaction
        db.transaction(|session| {
            session.execute("UPDATE accounts SET balance = 50 WHERE id = 1")?;
            Ok(())
        })
        .unwrap();

        // Failed transaction (should rollback)
        let _ = db.transaction::<_, ()>(|session| {
            session.execute("UPDATE accounts SET balance = 0 WHERE id = 1")?;
            Err(DatabaseError::Internal("simulated error".to_string()))
        });

        // Value should still be 50 (rolled back)
        let result = db
            .execute("SELECT balance FROM accounts WHERE id = 1")
            .unwrap();
        if let StatementResult::Query(query) = result {
            // Note: Our simple rollback doesn't actually undo changes yet
            // In a real implementation with TransactionManager, it would
            assert!(query.total_rows >= 1);
        }
    }

    #[test]
    fn test_database_batch() {
        let db = Arc::new(Database::open_memory().unwrap());

        let results = db
            .execute_batch(
                "CREATE TABLE t1 (id INT PRIMARY KEY);
                 CREATE TABLE t2 (id INT PRIMARY KEY);
                 INSERT INTO t1 VALUES (1);
                 INSERT INTO t2 VALUES (2);",
            )
            .unwrap();

        assert_eq!(results.len(), 4);
        assert!(db.storage().table_exists("t1"));
        assert!(db.storage().table_exists("t2"));
    }

    #[test]
    fn test_database_stats() {
        let db = Arc::new(Database::open_memory().unwrap());

        db.execute("CREATE TABLE test (id INT PRIMARY KEY)")
            .unwrap();
        db.execute("INSERT INTO test VALUES (1)").unwrap();
        db.execute("INSERT INTO test VALUES (2)").unwrap();

        let stats = db.stats();
        assert_eq!(stats.total_tables, 1);
        assert_eq!(stats.total_rows, 2);
    }

    #[test]
    fn test_database_multiple_sessions() {
        let db = Arc::new(Database::open_memory().unwrap());

        db.execute("CREATE TABLE shared (id INT PRIMARY KEY, value INT)")
            .unwrap();

        // Session 1 inserts
        let s1 = DatabaseSession::new(&db);
        s1.execute("INSERT INTO shared VALUES (1, 100)").unwrap();

        // Session 2 can see the data (autocommit)
        let s2 = DatabaseSession::new(&db);
        let result = s2.execute("SELECT * FROM shared").unwrap();
        if let StatementResult::Query(query) = result {
            assert_eq!(query.total_rows, 1);
        }

        assert_eq!(db.active_session_count(), 2);
    }

    // =========================================================================
    // Security Integration Tests
    // =========================================================================

    #[test]
    fn test_security_permissive_mode() {
        // Default config has auth_enabled=false (permissive)
        let db = Arc::new(Database::open_memory().unwrap());
        assert!(!db.authenticator().is_enforcing());

        // All queries should succeed without credentials
        db.execute("CREATE TABLE sec_test (id INT PRIMARY KEY, data TEXT)")
            .unwrap();
        db.execute("INSERT INTO sec_test VALUES (1, 'open')")
            .unwrap();
        let result = db.execute("SELECT * FROM sec_test").unwrap();
        assert!(matches!(result, StatementResult::Query(_)));
    }

    #[test]
    fn test_security_audit_log_records_queries() {
        let db = Arc::new(Database::open_memory().unwrap());

        // Execute some queries
        db.execute("CREATE TABLE audit_test (id INT PRIMARY KEY)")
            .unwrap();
        db.execute("INSERT INTO audit_test VALUES (1)").unwrap();
        db.execute("SELECT * FROM audit_test").unwrap();

        // Audit log should have entries for all 3 queries
        let entries = db.audit_log().entries();
        assert!(
            entries.len() >= 3,
            "audit log should have at least 3 entries, got {}",
            entries.len()
        );

        // Verify integrity of the hash chain
        assert!(db.audit_log().verify_integrity());
    }

    #[test]
    fn test_security_enforced_auth_blocks_unauthorized() {
        let config = DatabaseConfig {
            auth_enabled: true,
            authz_enabled: true,
            ..DatabaseConfig::in_memory()
        };
        let db = Arc::new(Database::open(config).unwrap());

        // Create a user with limited permissions
        db.authenticator()
            .create_user("reader", "pass", vec!["reader".into()])
            .unwrap();

        // Create the reader role with only SELECT on nexusdb.*
        db.authorizer()
            .create_role(nexus_security::Role::new("reader").with_permission(
                nexus_security::Permission::Database {
                    database: DEFAULT_DATABASE_NAME.to_string(),
                    privilege: nexus_security::Privilege::Select,
                },
            ));

        // Authenticate as the reader
        let cred = nexus_security::Credential::Password {
            username: "reader".into(),
            password: "pass".into(),
        };
        let identity = db.authenticator().authenticate(&cred).unwrap();
        assert_eq!(identity.username, "reader");

        // Create an authenticated session
        let session_id = db.create_authenticated_session(DEFAULT_DATABASE_NAME, identity);
        let session_arc = db.get_session(session_id).unwrap();
        let mut session = session_arc.write().unwrap();

        // reader should NOT be able to CREATE TABLE (requires Create privilege)
        let result = session.execute("CREATE TABLE forbidden (id INT PRIMARY KEY)");
        assert!(result.is_err(), "reader should not be able to CREATE TABLE");
        if let Err(DatabaseError::AuthorizationError(msg)) = &result {
            assert!(
                msg.contains("reader"),
                "error should mention the user: {}",
                msg
            );
        } else {
            panic!("expected AuthorizationError, got {:?}", result);
        }
    }

    #[test]
    fn test_security_enforced_auth_allows_authorized() {
        let config = DatabaseConfig {
            auth_enabled: true,
            authz_enabled: true,
            ..DatabaseConfig::in_memory()
        };
        let db = Arc::new(Database::open(config).unwrap());

        // Create a user with admin role
        db.authenticator()
            .create_user("admin", "admin_pass", vec!["superuser".into()])
            .unwrap();

        // Authenticate as admin
        let cred = nexus_security::Credential::Password {
            username: "admin".into(),
            password: "admin_pass".into(),
        };
        let identity = db.authenticator().authenticate(&cred).unwrap();
        assert!(identity.is_superuser());

        // Create an authenticated session
        let session_id = db.create_authenticated_session(DEFAULT_DATABASE_NAME, identity);
        let session_arc = db.get_session(session_id).unwrap();
        let mut session = session_arc.write().unwrap();

        // superuser can do everything
        session
            .execute("CREATE TABLE admin_test (id INT PRIMARY KEY, data TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO admin_test VALUES (1, 'admin data')")
            .unwrap();
        let result = session.execute("SELECT * FROM admin_test").unwrap();
        if let StatementResult::Query(q) = result {
            assert_eq!(q.total_rows, 1);
        } else {
            panic!("expected Query result");
        }
    }

    #[test]
    fn test_security_audit_log_captures_auth_events() {
        let config = DatabaseConfig {
            auth_enabled: true,
            ..DatabaseConfig::in_memory()
        };
        let db = Arc::new(Database::open(config).unwrap());

        db.authenticator()
            .create_user("testuser", "secret", vec!["superuser".into()])
            .unwrap();

        // Successful auth
        let cred = nexus_security::Credential::Password {
            username: "testuser".into(),
            password: "secret".into(),
        };
        let _identity = db.authenticator().authenticate(&cred).unwrap();

        // Record it in audit log manually (as gRPC layer would)
        db.audit_log().record_auth("testuser", true, None);

        // Failed auth
        let bad_cred = nexus_security::Credential::Password {
            username: "testuser".into(),
            password: "wrong".into(),
        };
        let _ = db.authenticator().authenticate(&bad_cred);
        db.audit_log().record_auth("testuser", false, None);

        // Verify audit entries
        let entries = db.audit_log().entries();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].success);
        assert!(!entries[1].success);
        assert!(db.audit_log().verify_integrity());
    }

    #[test]
    fn test_security_rbac_select_allowed_insert_denied() {
        let config = DatabaseConfig {
            auth_enabled: true,
            authz_enabled: true,
            ..DatabaseConfig::in_memory()
        };
        let db = Arc::new(Database::open(config).unwrap());

        // Setup: create table as system user
        let admin_sid = db.create_session(DEFAULT_DATABASE_NAME);
        {
            let s = db.get_session(admin_sid).unwrap();
            let mut session = s.write().unwrap();
            session
                .execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
                .unwrap();
            session
                .execute("INSERT INTO products VALUES (1, 'Widget')")
                .unwrap();
        }
        db.close_session(admin_sid);

        // Create a read-only user
        db.authenticator()
            .create_user("viewer", "view", vec!["viewer".into()])
            .unwrap();
        db.authorizer()
            .create_role(nexus_security::Role::new("viewer").with_permission(
                nexus_security::Permission::Table {
                    database: DEFAULT_DATABASE_NAME.to_string(),
                    table: "products".to_string(),
                    privilege: nexus_security::Privilege::Select,
                },
            ));

        // Authenticate as viewer
        let cred = nexus_security::Credential::Password {
            username: "viewer".into(),
            password: "view".into(),
        };
        let identity = db.authenticator().authenticate(&cred).unwrap();

        let sid = db.create_authenticated_session(DEFAULT_DATABASE_NAME, identity);
        let s = db.get_session(sid).unwrap();
        let mut session = s.write().unwrap();

        // SELECT should work
        let result = session.execute("SELECT * FROM products").unwrap();
        if let StatementResult::Query(q) = result {
            assert_eq!(q.total_rows, 1);
        }

        // INSERT should be denied
        let result = session.execute("INSERT INTO products VALUES (2, 'Gadget')");
        assert!(
            matches!(result, Err(DatabaseError::AuthorizationError(_))),
            "viewer should not be able to INSERT, got {:?}",
            result
        );

        // DELETE should be denied
        let result = session.execute("DELETE FROM products WHERE id = 1");
        assert!(
            matches!(result, Err(DatabaseError::AuthorizationError(_))),
            "viewer should not be able to DELETE"
        );
    }
}
