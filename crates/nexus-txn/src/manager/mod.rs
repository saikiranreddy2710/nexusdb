//! Transaction manager for coordinating transaction lifecycle.
//!
//! This module provides the core transaction management functionality:
//! - Transaction lifecycle (begin, commit, abort)
//! - Integration with MVCC version store
//! - Integration with WAL for durability
//! - Lock acquisition and release
//! - Serializable Snapshot Isolation (SSI) validation
//!
//! # Transaction States
//!
//! ```text
//! ┌───────┐    begin()    ┌────────┐
//! │ Start │──────────────▶│ Active │
//! └───────┘               └────────┘
//!                              │
//!                    ┌────────┴────────┐
//!                    │                 │
//!               commit()           abort()
//!                    │                 │
//!                    ▼                 ▼
//!             ┌───────────┐     ┌──────────┐
//!             │ Committed │     │ Aborted  │
//!             └───────────┘     └──────────┘
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use nexus_common::types::TxnId;
use parking_lot::{Mutex, RwLock};

use nexus_mvcc::{
    HlcTimestamp, HybridLogicalClock, IsolationLevel, SnapshotManager, VersionId, VersionStore,
};

use crate::deadlock::DeadlockDetector;
use crate::lock::{LockManager, LockMode, LockResult, ResourceId};

/// The state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and can perform operations.
    Active,
    /// Transaction is in the process of committing.
    Committing,
    /// Transaction has been committed.
    Committed,
    /// Transaction is in the process of aborting.
    Aborting,
    /// Transaction has been aborted.
    Aborted,
}

impl TransactionState {
    /// Returns true if the transaction can perform operations.
    pub fn is_active(&self) -> bool {
        *self == TransactionState::Active
    }

    /// Returns true if the transaction has ended.
    pub fn is_ended(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed | TransactionState::Aborted
        )
    }
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Committing => write!(f, "Committing"),
            TransactionState::Committed => write!(f, "Committed"),
            TransactionState::Aborting => write!(f, "Aborting"),
            TransactionState::Aborted => write!(f, "Aborted"),
        }
    }
}

/// A write operation recorded in the transaction.
#[derive(Debug, Clone)]
pub struct WriteRecord {
    /// The key that was written.
    pub key: Bytes,
    /// The version ID created.
    pub version_id: VersionId,
    /// Whether this was an insert or update.
    pub is_insert: bool,
}

/// A read operation recorded in the transaction.
#[derive(Debug, Clone)]
pub struct ReadRecord {
    /// The key that was read.
    pub key: Bytes,
    /// The version that was read (if any).
    pub version_id: Option<VersionId>,
}

/// A transaction handle for performing operations.
pub struct Transaction {
    /// Unique transaction ID.
    id: TxnId,
    /// Current state.
    state: TransactionState,
    /// Start timestamp.
    start_ts: HlcTimestamp,
    /// Commit timestamp (set during commit).
    commit_ts: Option<HlcTimestamp>,
    /// Isolation level.
    isolation: IsolationLevel,
    /// Read operations performed.
    reads: Vec<ReadRecord>,
    /// Write operations performed.
    writes: Vec<WriteRecord>,
    /// When the transaction started.
    started_at: Instant,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(id: TxnId, start_ts: HlcTimestamp, isolation: IsolationLevel) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            start_ts,
            commit_ts: None,
            isolation,
            reads: Vec::new(),
            writes: Vec::new(),
            started_at: Instant::now(),
        }
    }

    /// Returns the transaction ID.
    pub fn id(&self) -> TxnId {
        self.id
    }

    /// Returns the current state.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Returns the start timestamp.
    pub fn start_ts(&self) -> &HlcTimestamp {
        &self.start_ts
    }

    /// Returns the commit timestamp if committed.
    pub fn commit_ts(&self) -> Option<&HlcTimestamp> {
        self.commit_ts.as_ref()
    }

    /// Returns the isolation level.
    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// Returns the read set.
    pub fn reads(&self) -> &[ReadRecord] {
        &self.reads
    }

    /// Returns the write set.
    pub fn writes(&self) -> &[WriteRecord] {
        &self.writes
    }

    /// Returns how long the transaction has been running.
    pub fn duration(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Records a read.
    fn record_read(&mut self, key: Bytes, version_id: Option<VersionId>) {
        self.reads.push(ReadRecord { key, version_id });
    }

    /// Records a write.
    fn record_write(&mut self, key: Bytes, version_id: VersionId, is_insert: bool) {
        self.writes.push(WriteRecord {
            key,
            version_id,
            is_insert,
        });
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("isolation", &self.isolation)
            .field("reads", &self.reads.len())
            .field("writes", &self.writes.len())
            .finish()
    }
}

/// Errors that can occur during transaction operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionError {
    /// Transaction not found.
    NotFound(TxnId),
    /// Transaction is not in the expected state.
    InvalidState {
        /// The transaction ID.
        txn_id: TxnId,
        /// The current state.
        current: TransactionState,
        /// The expected state(s).
        expected: &'static str,
    },
    /// Lock acquisition failed.
    LockFailed {
        /// The transaction ID.
        txn_id: TxnId,
        /// The resource that couldn't be locked.
        resource: String,
        /// The reason for failure.
        reason: LockResult,
    },
    /// Deadlock detected.
    Deadlock(TxnId),
    /// Conflict detected (for SSI).
    Conflict {
        /// This transaction.
        txn_id: TxnId,
        /// The conflicting transaction.
        other_txn: TxnId,
    },
    /// Transaction timed out.
    Timeout(TxnId),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::NotFound(txn) => {
                write!(f, "transaction not found: {:?}", txn)
            }
            TransactionError::InvalidState {
                txn_id,
                current,
                expected,
            } => {
                write!(
                    f,
                    "transaction {:?} in invalid state {}, expected {}",
                    txn_id, current, expected
                )
            }
            TransactionError::LockFailed {
                txn_id,
                resource,
                reason,
            } => {
                write!(
                    f,
                    "transaction {:?} failed to lock {}: {:?}",
                    txn_id, resource, reason
                )
            }
            TransactionError::Deadlock(txn) => {
                write!(f, "deadlock detected for transaction {:?}", txn)
            }
            TransactionError::Conflict { txn_id, other_txn } => {
                write!(
                    f,
                    "conflict between transactions {:?} and {:?}",
                    txn_id, other_txn
                )
            }
            TransactionError::Timeout(txn) => {
                write!(f, "transaction {:?} timed out", txn)
            }
            TransactionError::Internal(msg) => {
                write!(f, "internal error: {}", msg)
            }
        }
    }
}

impl std::error::Error for TransactionError {}

/// Result type for transaction operations.
pub type TxnResult<T> = Result<T, TransactionError>;

/// Configuration for the transaction manager.
#[derive(Debug, Clone)]
pub struct TransactionManagerConfig {
    /// Default isolation level for new transactions.
    pub default_isolation: IsolationLevel,
    /// Lock timeout.
    pub lock_timeout: Duration,
    /// Transaction timeout (max duration).
    pub txn_timeout: Duration,
    /// Whether to enable deadlock detection.
    pub deadlock_detection: bool,
}

impl Default for TransactionManagerConfig {
    fn default() -> Self {
        Self {
            default_isolation: IsolationLevel::SnapshotIsolation,
            lock_timeout: Duration::from_secs(30),
            txn_timeout: Duration::from_secs(300),
            deadlock_detection: true,
        }
    }
}

/// Statistics about the transaction manager.
#[derive(Debug, Default)]
pub struct TransactionStats {
    /// Total transactions started.
    pub started: AtomicU64,
    /// Total transactions committed.
    pub committed: AtomicU64,
    /// Total transactions aborted.
    pub aborted: AtomicU64,
    /// Currently active transactions.
    pub active: AtomicU64,
    /// Total conflicts detected.
    pub conflicts: AtomicU64,
    /// Total deadlocks detected.
    pub deadlocks: AtomicU64,
}

impl TransactionStats {
    /// Creates new stats.
    pub fn new() -> Self {
        Self::default()
    }
}

/// The transaction manager coordinates all transaction operations.
pub struct TransactionManager {
    /// Clock for generating timestamps.
    clock: Arc<HybridLogicalClock>,
    /// Version store for MVCC.
    version_store: Arc<VersionStore>,
    /// Snapshot manager for isolation.
    snapshot_manager: Arc<SnapshotManager>,
    /// Lock manager.
    lock_manager: Arc<LockManager>,
    /// Deadlock detector.
    deadlock_detector: Arc<DeadlockDetector>,
    /// Active transactions.
    transactions: RwLock<HashMap<TxnId, Mutex<Transaction>>>,
    /// Configuration.
    config: TransactionManagerConfig,
    /// Statistics.
    stats: TransactionStats,
    /// Next transaction ID.
    next_txn_id: AtomicU64,
}

impl TransactionManager {
    /// Creates a new transaction manager.
    pub fn new(clock: Arc<HybridLogicalClock>, version_store: Arc<VersionStore>) -> Self {
        Self::with_config(clock, version_store, TransactionManagerConfig::default())
    }

    /// Creates a transaction manager with custom configuration.
    pub fn with_config(
        clock: Arc<HybridLogicalClock>,
        version_store: Arc<VersionStore>,
        config: TransactionManagerConfig,
    ) -> Self {
        Self {
            clock,
            version_store,
            snapshot_manager: Arc::new(SnapshotManager::new()),
            lock_manager: Arc::new(LockManager::new()),
            deadlock_detector: Arc::new(DeadlockDetector::new()),
            transactions: RwLock::new(HashMap::new()),
            config,
            stats: TransactionStats::new(),
            next_txn_id: AtomicU64::new(1),
        }
    }

    /// Begins a new transaction with the default isolation level.
    pub fn begin(&self) -> TxnResult<TxnId> {
        self.begin_with_isolation(self.config.default_isolation)
    }

    /// Begins a new transaction with a specific isolation level.
    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> TxnResult<TxnId> {
        let txn_id = TxnId::new(self.next_txn_id.fetch_add(1, AtomicOrdering::SeqCst));
        let start_ts = self.clock.now();

        let txn = Transaction::new(txn_id, start_ts, isolation);

        // Register with snapshot manager
        self.snapshot_manager.begin_txn(start_ts, isolation);

        // Register with deadlock detector
        if self.config.deadlock_detection {
            self.deadlock_detector.register_txn(txn_id);
        }

        // Store the transaction
        self.transactions.write().insert(txn_id, Mutex::new(txn));

        self.stats.started.fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.active.fetch_add(1, AtomicOrdering::Relaxed);

        Ok(txn_id)
    }

    /// Gets a value by key.
    pub fn get(&self, txn_id: TxnId, table_id: u64, key: &Bytes) -> TxnResult<Option<Bytes>> {
        let txns = self.transactions.read();
        let txn_lock = txns
            .get(&txn_id)
            .ok_or(TransactionError::NotFound(txn_id))?;
        let mut txn = txn_lock.lock();

        if !txn.state.is_active() {
            return Err(TransactionError::InvalidState {
                txn_id,
                current: txn.state,
                expected: "Active",
            });
        }

        // Acquire shared lock
        let resource = ResourceId::row(table_id, key.clone());
        let lock_result = self.lock_manager.lock(
            txn_id,
            resource.clone(),
            LockMode::Shared,
            Some(self.config.lock_timeout),
        );

        if !lock_result.is_success() {
            return Err(TransactionError::LockFailed {
                txn_id,
                resource: format!("{}", resource),
                reason: lock_result,
            });
        }

        // Get visible version
        let version = self.version_store.get_visible(key, txn_id, &txn.start_ts);

        let version_id = version.as_ref().map(|v| v.id);
        txn.record_read(key.clone(), version_id);

        // Record in snapshot manager for SSI
        self.snapshot_manager.record_read(txn_id, key.clone());

        Ok(version.map(|v| v.data))
    }

    /// Puts a value by key.
    pub fn put(
        &self,
        txn_id: TxnId,
        table_id: u64,
        key: Bytes,
        value: Bytes,
    ) -> TxnResult<VersionId> {
        let txns = self.transactions.read();
        let txn_lock = txns
            .get(&txn_id)
            .ok_or(TransactionError::NotFound(txn_id))?;
        let mut txn = txn_lock.lock();

        if !txn.state.is_active() {
            return Err(TransactionError::InvalidState {
                txn_id,
                current: txn.state,
                expected: "Active",
            });
        }

        // Acquire exclusive lock
        let resource = ResourceId::row(table_id, key.clone());
        let lock_result = self.lock_manager.lock(
            txn_id,
            resource.clone(),
            LockMode::Exclusive,
            Some(self.config.lock_timeout),
        );

        match lock_result {
            LockResult::Deadlock => {
                self.stats.deadlocks.fetch_add(1, AtomicOrdering::Relaxed);
                return Err(TransactionError::Deadlock(txn_id));
            }
            result if !result.is_success() => {
                return Err(TransactionError::LockFailed {
                    txn_id,
                    resource: format!("{}", resource),
                    reason: result,
                });
            }
            _ => {}
        }

        // Check if this is an insert or update
        let existing = self.version_store.get_visible(&key, txn_id, &txn.start_ts);
        let is_insert = existing.is_none();

        // Create new version
        let version_id =
            self.version_store
                .create_version(key.clone(), txn.start_ts, txn_id, value);

        txn.record_write(key.clone(), version_id, is_insert);

        // Record in snapshot manager for SSI
        self.snapshot_manager.record_write(txn_id, key, version_id);

        Ok(version_id)
    }

    /// Deletes a key.
    pub fn delete(&self, txn_id: TxnId, table_id: u64, key: &Bytes) -> TxnResult<bool> {
        let txns = self.transactions.read();
        let txn_lock = txns
            .get(&txn_id)
            .ok_or(TransactionError::NotFound(txn_id))?;
        let txn = txn_lock.lock();

        if !txn.state.is_active() {
            return Err(TransactionError::InvalidState {
                txn_id,
                current: txn.state,
                expected: "Active",
            });
        }

        // Acquire exclusive lock
        let resource = ResourceId::row(table_id, key.clone());
        let lock_result = self.lock_manager.lock(
            txn_id,
            resource.clone(),
            LockMode::Exclusive,
            Some(self.config.lock_timeout),
        );

        if !lock_result.is_success() {
            return Err(TransactionError::LockFailed {
                txn_id,
                resource: format!("{}", resource),
                reason: lock_result,
            });
        }

        // Find and mark the version as deleted
        if let Some(chain) = self.version_store.get_chain(key) {
            if let Some(version) = chain.get_latest_committed() {
                chain.delete_version(version.id, txn.start_ts, txn_id);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Commits a transaction.
    pub fn commit(&self, txn_id: TxnId) -> TxnResult<HlcTimestamp> {
        // First, validate for SSI if needed
        if let Some(info) = self.snapshot_manager.get_txn_info(txn_id) {
            if info.isolation == IsolationLevel::Serializable {
                if let Err(e) = self.snapshot_manager.validate_for_commit(txn_id) {
                    self.stats.conflicts.fetch_add(1, AtomicOrdering::Relaxed);
                    // Abort the transaction
                    self.abort(txn_id)?;
                    return Err(TransactionError::Internal(format!(
                        "SSI validation failed: {}",
                        e
                    )));
                }
            }
        }

        let commit_ts = self.clock.now();

        {
            let txns = self.transactions.read();
            let txn_lock = txns
                .get(&txn_id)
                .ok_or(TransactionError::NotFound(txn_id))?;
            let mut txn = txn_lock.lock();

            if !txn.state.is_active() {
                return Err(TransactionError::InvalidState {
                    txn_id,
                    current: txn.state,
                    expected: "Active",
                });
            }

            txn.state = TransactionState::Committing;

            // Commit all versions
            for write in &txn.writes {
                self.version_store
                    .commit(&write.key, write.version_id, commit_ts);
            }

            txn.commit_ts = Some(commit_ts);
            txn.state = TransactionState::Committed;
        }

        // Release locks
        self.lock_manager.release_all(txn_id);

        // Unregister from snapshot manager
        self.snapshot_manager.commit_txn(txn_id);

        // Unregister from deadlock detector
        if self.config.deadlock_detection {
            self.deadlock_detector.unregister_txn(txn_id);
        }

        self.stats.committed.fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.active.fetch_sub(1, AtomicOrdering::Relaxed);

        Ok(commit_ts)
    }

    /// Aborts a transaction.
    pub fn abort(&self, txn_id: TxnId) -> TxnResult<()> {
        {
            let txns = self.transactions.read();
            let txn_lock = txns
                .get(&txn_id)
                .ok_or(TransactionError::NotFound(txn_id))?;
            let mut txn = txn_lock.lock();

            if txn.state.is_ended() {
                return Err(TransactionError::InvalidState {
                    txn_id,
                    current: txn.state,
                    expected: "Active or Committing",
                });
            }

            txn.state = TransactionState::Aborting;

            // Abort all versions
            for write in &txn.writes {
                self.version_store.abort(&write.key, write.version_id);
            }

            txn.state = TransactionState::Aborted;
        }

        // Release locks
        self.lock_manager.release_all(txn_id);

        // Unregister from snapshot manager
        self.snapshot_manager.abort_txn(txn_id);

        // Unregister from deadlock detector
        if self.config.deadlock_detection {
            self.deadlock_detector.unregister_txn(txn_id);
        }

        self.stats.aborted.fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.active.fetch_sub(1, AtomicOrdering::Relaxed);

        Ok(())
    }

    /// Returns information about a transaction.
    pub fn get_transaction(&self, txn_id: TxnId) -> Option<(TransactionState, IsolationLevel)> {
        let txns = self.transactions.read();
        txns.get(&txn_id).map(|t| {
            let txn = t.lock();
            (txn.state, txn.isolation)
        })
    }

    /// Returns the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.stats.active.load(AtomicOrdering::Relaxed) as usize
    }

    /// Returns statistics.
    pub fn stats(&self) -> &TransactionStats {
        &self.stats
    }

    /// Returns the lock manager.
    pub fn lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }

    /// Returns the version store.
    pub fn version_store(&self) -> &Arc<VersionStore> {
        &self.version_store
    }

    /// Returns the clock.
    pub fn clock(&self) -> &Arc<HybridLogicalClock> {
        &self.clock
    }
}

impl fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionManager")
            .field("active_count", &self.active_count())
            .field("lock_count", &self.lock_manager.lock_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_manager() -> TransactionManager {
        let clock = Arc::new(HybridLogicalClock::new(1));
        let version_store = Arc::new(VersionStore::new());
        TransactionManager::new(clock, version_store)
    }

    #[test]
    fn test_transaction_lifecycle() {
        let tm = create_manager();

        // Begin
        let txn_id = tm.begin().unwrap();
        assert_eq!(tm.active_count(), 1);

        // Check state
        let (state, _isolation) = tm.get_transaction(txn_id).unwrap();
        assert_eq!(state, TransactionState::Active);

        // Commit
        let commit_ts = tm.commit(txn_id).unwrap();
        assert!(!commit_ts.is_zero());
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn test_transaction_abort() {
        let tm = create_manager();

        let txn_id = tm.begin().unwrap();
        tm.abort(txn_id).unwrap();

        let (state, _) = tm.get_transaction(txn_id).unwrap();
        assert_eq!(state, TransactionState::Aborted);
    }

    #[test]
    fn test_put_get() {
        let tm = create_manager();

        let txn_id = tm.begin().unwrap();
        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        // Put
        let version_id = tm.put(txn_id, 1, key.clone(), value.clone()).unwrap();
        assert!(version_id.is_valid());

        // Get (own writes should be visible)
        let result = tm.get(txn_id, 1, &key).unwrap();
        assert_eq!(result, Some(value));

        tm.commit(txn_id).unwrap();
    }

    #[test]
    fn test_isolation() {
        let tm = create_manager();

        // Use two different keys to avoid lock conflicts
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        // Transaction 1: write to key1
        let txn1 = tm.begin().unwrap();
        tm.put(txn1, 1, key1.clone(), Bytes::from("value1"))
            .unwrap();
        tm.commit(txn1).unwrap();

        // Transaction 2: read key1, write key2
        let txn2 = tm.begin().unwrap();
        let result = tm.get(txn2, 1, &key1).unwrap();
        assert_eq!(result, Some(Bytes::from("value1")));
        tm.put(txn2, 1, key2.clone(), Bytes::from("value2"))
            .unwrap();

        // Transaction 3: started while txn2 is active
        let txn3 = tm.begin().unwrap();
        // Can see key1 (committed by txn1)
        let result = tm.get(txn3, 1, &key1).unwrap();
        assert_eq!(result, Some(Bytes::from("value1")));

        // Commit txn2
        tm.commit(txn2).unwrap();

        // Txn3 should NOT see key2 (snapshot isolation)
        // txn2 committed after txn3 started, so txn3's snapshot doesn't include it
        let result = tm.get(txn3, 1, &key2).unwrap();
        assert_eq!(result, None);

        // Transaction 4: started after txn2 committed
        let txn4 = tm.begin().unwrap();
        let result = tm.get(txn4, 1, &key2).unwrap();
        assert_eq!(result, Some(Bytes::from("value2")));

        tm.commit(txn3).unwrap();
        tm.commit(txn4).unwrap();
    }

    #[test]
    fn test_transaction_not_found() {
        let tm = create_manager();

        let result = tm.commit(TxnId::new(999));
        assert!(matches!(result, Err(TransactionError::NotFound(_))));
    }

    #[test]
    fn test_double_commit() {
        let tm = create_manager();

        let txn_id = tm.begin().unwrap();
        tm.commit(txn_id).unwrap();

        let result = tm.commit(txn_id);
        assert!(matches!(result, Err(TransactionError::InvalidState { .. })));
    }

    #[test]
    fn test_stats() {
        let tm = create_manager();

        let txn1 = tm.begin().unwrap();
        let txn2 = tm.begin().unwrap();

        assert_eq!(tm.stats().started.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(tm.stats().active.load(AtomicOrdering::Relaxed), 2);

        tm.commit(txn1).unwrap();
        assert_eq!(tm.stats().committed.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(tm.stats().active.load(AtomicOrdering::Relaxed), 1);

        tm.abort(txn2).unwrap();
        assert_eq!(tm.stats().aborted.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(tm.stats().active.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn test_transaction_state_display() {
        assert_eq!(format!("{}", TransactionState::Active), "Active");
        assert_eq!(format!("{}", TransactionState::Committed), "Committed");
        assert_eq!(format!("{}", TransactionState::Aborted), "Aborted");
    }

    #[test]
    fn test_serializable_isolation() {
        let clock = Arc::new(HybridLogicalClock::new(1));
        let version_store = Arc::new(VersionStore::new());
        let tm = TransactionManager::new(clock, version_store);

        let txn = tm
            .begin_with_isolation(IsolationLevel::Serializable)
            .unwrap();
        assert!(tm.get_transaction(txn).is_some());
        tm.commit(txn).unwrap();
    }
}
