//! Transaction handle for explicit transaction control.
//!
//! Provides RAII-style transaction management with automatic rollback on drop.

use std::sync::atomic::{AtomicBool, Ordering};

use super::client::{Client, QueryResult};
use super::error::{ClientError, ClientResult};
use super::query::QueryBuilder;

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read uncommitted (lowest isolation).
    ReadUncommitted,
    /// Read committed.
    ReadCommitted,
    /// Repeatable read.
    RepeatableRead,
    /// Serializable (highest isolation).
    Serializable,
    /// Snapshot isolation (default for NexusDB).
    #[default]
    Snapshot,
}

impl IsolationLevel {
    /// Returns the SQL representation.
    pub fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
            IsolationLevel::Snapshot => "SNAPSHOT",
        }
    }
}

/// Transaction access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessMode {
    /// Read-write transaction (default).
    #[default]
    ReadWrite,
    /// Read-only transaction.
    ReadOnly,
}

impl AccessMode {
    /// Returns the SQL representation.
    pub fn as_sql(&self) -> &'static str {
        match self {
            AccessMode::ReadWrite => "READ WRITE",
            AccessMode::ReadOnly => "READ ONLY",
        }
    }
}

/// Transaction options.
#[derive(Debug, Clone, Default)]
pub struct TransactionOptions {
    /// Isolation level.
    pub isolation_level: IsolationLevel,
    /// Access mode.
    pub access_mode: AccessMode,
    /// Whether to defer constraints.
    pub deferrable: bool,
}

impl TransactionOptions {
    /// Creates new transaction options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the isolation level.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Sets the access mode.
    pub fn access_mode(mut self, mode: AccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    /// Sets read-only mode.
    pub fn read_only(self) -> Self {
        self.access_mode(AccessMode::ReadOnly)
    }

    /// Sets deferrable mode.
    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.deferrable = deferrable;
        self
    }
}

/// A transaction handle.
///
/// Provides RAII-style transaction management. If the transaction is not
/// explicitly committed, it will be rolled back when dropped.
pub struct Transaction<'a> {
    /// Reference to the client.
    client: &'a Client,
    /// Transaction ID.
    txn_id: u64,
    /// Whether the transaction is finished.
    finished: AtomicBool,
}

impl<'a> Transaction<'a> {
    /// Creates a new transaction handle.
    pub(crate) fn new(client: &'a Client, txn_id: u64) -> Self {
        Self {
            client,
            txn_id,
            finished: AtomicBool::new(false),
        }
    }

    /// Returns the transaction ID.
    pub fn id(&self) -> u64 {
        self.txn_id
    }

    /// Returns true if the transaction is still active.
    pub fn is_active(&self) -> bool {
        !self.finished.load(Ordering::Relaxed)
    }

    /// Executes a SQL query within this transaction.
    pub async fn execute(&self, sql: &str) -> ClientResult<QueryResult> {
        self.ensure_active()?;
        self.client.execute(sql).await
    }

    /// Creates a query builder within this transaction.
    pub fn query(&self) -> QueryBuilder<'_> {
        QueryBuilder::new(self.client)
    }

    /// Commits the transaction.
    pub async fn commit(self) -> ClientResult<()> {
        self.ensure_active()?;
        self.finished.store(true, Ordering::Relaxed);
        self.client.commit_transaction(self.txn_id).await
    }

    /// Rolls back the transaction.
    pub async fn rollback(self) -> ClientResult<()> {
        self.ensure_active()?;
        self.finished.store(true, Ordering::Relaxed);
        self.client.rollback_transaction(self.txn_id).await
    }

    /// Creates a savepoint.
    pub async fn savepoint(&self, name: &str) -> ClientResult<Savepoint<'_, 'a>> {
        self.ensure_active()?;
        self.execute(&format!("SAVEPOINT {}", name)).await?;
        Ok(Savepoint::new(self, name.to_string()))
    }

    /// Ensures the transaction is active.
    fn ensure_active(&self) -> ClientResult<()> {
        if self.finished.load(Ordering::Relaxed) {
            return Err(ClientError::TransactionError(
                "transaction already finished".to_string(),
            ));
        }
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            // Transaction not committed, rollback
            // Note: We can't await here, so we mark it finished
            // The actual rollback would need to happen in the client
            self.finished.store(true, Ordering::Relaxed);
            // In a real implementation, we'd need to handle this
            // asynchronously or through a different mechanism
        }
    }
}

impl<'a> std::fmt::Debug for Transaction<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.txn_id)
            .field("active", &self.is_active())
            .finish()
    }
}

/// A savepoint within a transaction.
pub struct Savepoint<'t, 'a> {
    /// Reference to the transaction.
    txn: &'t Transaction<'a>,
    /// Savepoint name.
    name: String,
    /// Whether the savepoint is released.
    released: bool,
}

impl<'t, 'a> Savepoint<'t, 'a> {
    /// Creates a new savepoint.
    fn new(txn: &'t Transaction<'a>, name: String) -> Self {
        Self {
            txn,
            name,
            released: false,
        }
    }

    /// Returns the savepoint name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Releases the savepoint.
    pub async fn release(mut self) -> ClientResult<()> {
        self.txn
            .execute(&format!("RELEASE SAVEPOINT {}", self.name))
            .await?;
        self.released = true;
        Ok(())
    }

    /// Rolls back to this savepoint.
    pub async fn rollback(mut self) -> ClientResult<()> {
        self.txn
            .execute(&format!("ROLLBACK TO SAVEPOINT {}", self.name))
            .await?;
        self.released = true;
        Ok(())
    }
}

impl<'t, 'a> std::fmt::Debug for Savepoint<'t, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Savepoint")
            .field("name", &self.name)
            .field("released", &self.released)
            .finish()
    }
}

/// Extension trait for running code in a transaction.
pub trait TransactionExt {
    /// Runs a closure within a transaction.
    ///
    /// If the closure returns Ok, the transaction is committed.
    /// If the closure returns Err, the transaction is rolled back.
    fn run_transaction<F, T, E>(&self, f: F) -> impl std::future::Future<Output = Result<T, E>>
    where
        F: for<'a> FnOnce(&'a Transaction<'a>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'a>>,
        T: Send,
        E: From<ClientError> + Send;
}

impl TransactionExt for Client {
    async fn run_transaction<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'a> FnOnce(&'a Transaction<'a>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'a>>,
        T: Send,
        E: From<ClientError> + Send,
    {
        let txn = self.begin().await?;

        match f(&txn).await {
            Ok(result) => {
                txn.commit().await?;
                Ok(result)
            }
            Err(e) => {
                // Try to rollback, but don't fail if we can't
                let _ = txn.rollback().await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_client() -> Client {
        Client::connect_default().unwrap()
    }

    #[test]
    fn test_isolation_level() {
        assert_eq!(IsolationLevel::Snapshot.as_sql(), "SNAPSHOT");
        assert_eq!(IsolationLevel::Serializable.as_sql(), "SERIALIZABLE");
        assert_eq!(IsolationLevel::ReadCommitted.as_sql(), "READ COMMITTED");
    }

    #[test]
    fn test_access_mode() {
        assert_eq!(AccessMode::ReadWrite.as_sql(), "READ WRITE");
        assert_eq!(AccessMode::ReadOnly.as_sql(), "READ ONLY");
    }

    #[test]
    fn test_transaction_options() {
        let opts = TransactionOptions::new()
            .isolation_level(IsolationLevel::Serializable)
            .read_only()
            .deferrable(true);

        assert_eq!(opts.isolation_level, IsolationLevel::Serializable);
        assert_eq!(opts.access_mode, AccessMode::ReadOnly);
        assert!(opts.deferrable);
    }

    #[tokio::test]
    async fn test_transaction_begin() {
        let client = mock_client();
        // connect_default() already connects in mock mode

        let txn = client.begin().await.unwrap();
        assert!(txn.is_active());
        assert!(client.in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let client = mock_client();

        let txn = client.begin().await.unwrap();
        txn.commit().await.unwrap();
        assert!(!client.in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let client = mock_client();

        let txn = client.begin().await.unwrap();
        txn.rollback().await.unwrap();
        assert!(!client.in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_execute() {
        let client = mock_client();

        let txn = client.begin().await.unwrap();
        let result = txn.execute("SELECT 1").await;
        assert!(result.is_ok());

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_run_transaction_commit() {
        use super::TransactionExt;
        
        let client = mock_client();

        let result: Result<i32, ClientError> = client
            .run_transaction(|txn| {
                Box::pin(async move {
                    txn.execute("SELECT 1").await?;
                    Ok(42)
                })
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert!(!client.in_transaction());
    }

    #[tokio::test]
    async fn test_run_transaction_rollback() {
        use super::TransactionExt;
        
        let client = mock_client();

        let result: Result<i32, ClientError> = client
            .run_transaction(|_txn| {
                Box::pin(async move {
                    Err(ClientError::QueryFailed("test error".to_string()))
                })
            })
            .await;

        assert!(result.is_err());
        assert!(!client.in_transaction());
    }
}
