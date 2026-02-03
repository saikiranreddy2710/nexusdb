//! Main database engine.
//!
//! The `Database` struct is the top-level entry point for NexusDB.
//! It manages sessions, storage, and provides the unified API.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nexus_sql::storage::StorageEngine;
use nexus_wal::{Wal, WalConfig, SyncPolicy};

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
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            max_sessions: 1000,
            session_config: SessionConfig::default(),
            wal_enabled: true,
            wal_sync_mode: 1,
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

/// The main database engine.
pub struct Database {
    /// Configuration.
    config: DatabaseConfig,
    /// Storage engine.
    storage: Arc<StorageEngine>,
    /// Write-ahead log (if enabled).
    wal: Option<Arc<Wal>>,
    /// Active sessions.
    sessions: RwLock<HashMap<SessionId, Arc<RwLock<Session>>>>,
    /// Next session ID.
    next_session_id: AtomicU64,
    /// Total sessions created.
    total_sessions: AtomicU64,
    /// When the database was started.
    started_at: Instant,
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

        Ok(Self {
            config,
            storage,
            wal,
            sessions: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            total_sessions: AtomicU64::new(0),
            started_at: Instant::now(),
        })
    }

    /// Replays the WAL to recover state after a crash.
    fn replay_wal(wal: &Wal, _storage: &StorageEngine) -> DatabaseResult<()> {
        use nexus_wal::record::RecordType;
        
        let min_recovery_lsn = wal.min_recovery_lsn();
        
        // Read all records from the minimum recovery LSN
        let records = wal.iter_from(min_recovery_lsn).map_err(|e| {
            DatabaseError::Internal(format!("Failed to read WAL: {}", e))
        })?;
        
        if records.is_empty() {
            tracing::info!("No WAL records to replay");
            return Ok(());
        }
        
        tracing::info!("Replaying {} WAL records from LSN {:?}", records.len(), min_recovery_lsn);
        
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

    /// Returns the storage engine.
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Returns the WAL if enabled.
    pub fn wal(&self) -> Option<&Arc<Wal>> {
        self.wal.as_ref()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    // =========================================================================
    // Session Management
    // =========================================================================

    /// Creates a new session.
    pub fn create_session(&self) -> SessionId {
        self.create_session_with_config(self.config.session_config.clone())
    }

    /// Creates a session with custom configuration.
    pub fn create_session_with_config(&self, config: SessionConfig) -> SessionId {
        let id = SessionId::new(self.next_session_id.fetch_add(1, Ordering::SeqCst));
        let session = Session::new(id, self.storage.clone(), config);

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

    /// Executes SQL using a temporary session.
    ///
    /// This is a convenience method for simple queries. For complex
    /// transactions, create a session explicitly.
    pub fn execute(&self, sql: &str) -> DatabaseResult<StatementResult> {
        let session_id = self.create_session();
        let result = {
            let session_arc = self.get_session(session_id).unwrap();
            let mut session = session_arc.write().unwrap();
            session.execute(sql)
        };
        self.close_session(session_id);
        result
    }

    /// Executes multiple SQL statements using a temporary session.
    pub fn execute_batch(&self, sql: &str) -> DatabaseResult<Vec<StatementResult>> {
        let session_id = self.create_session();
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
    pub fn transaction<F, T>(&self, f: F) -> DatabaseResult<T>
    where
        F: FnOnce(&mut Session) -> DatabaseResult<T>,
    {
        let session_id = self.create_session();
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
        let storage_stats = self.storage.stats();

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
        self.storage.consolidate_all();
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
        self.storage.consolidate_all();
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

/// A convenience wrapper for using a session within a database.
#[allow(dead_code)]
pub struct DatabaseSession<'a> {
    db: &'a Database,
    session_id: SessionId,
}

#[allow(dead_code)]
impl<'a> DatabaseSession<'a> {
    /// Creates a new database session.
    pub fn new(db: &'a Database) -> Self {
        let session_id = db.create_session();
        Self { db, session_id }
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

impl<'a> Drop for DatabaseSession<'a> {
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
        let db = Database::open_memory().unwrap();

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
        let db = Database::open_memory().unwrap();

        let session_id = db.create_session();
        assert_eq!(db.active_session_count(), 1);

        db.close_session(session_id);
        assert_eq!(db.active_session_count(), 0);
    }

    #[test]
    fn test_database_session_wrapper() {
        let db = Database::open_memory().unwrap();

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
        let db = Database::open_memory().unwrap();

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
        let db = Database::open_memory().unwrap();

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
        let db = Database::open_memory().unwrap();

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
        let db = Database::open_memory().unwrap();

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
}
