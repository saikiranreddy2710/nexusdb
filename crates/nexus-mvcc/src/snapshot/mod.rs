//! Snapshot isolation implementation.
//!
//! This module provides snapshot-based isolation for MVCC, allowing
//! transactions to see a consistent view of the database as of a
//! specific timestamp.
//!
//! # Isolation Levels
//!
//! - **Snapshot Isolation (SI)**: Each transaction sees data as of its start time.
//!   Prevents dirty reads, non-repeatable reads, and phantom reads.
//!
//! - **Serializable Snapshot Isolation (SSI)**: SI plus detection of
//!   write-write and read-write conflicts that could lead to serialization
//!   anomalies.
//!
//! # Read Visibility Rules
//!
//! A version V is visible to transaction T if:
//! 1. V was committed before T started, AND
//! 2. V was not deleted before T started, AND
//! 3. V is the latest such version
//!
//! Additionally, T can always see its own uncommitted writes.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use bytes::Bytes;
use nexus_common::types::TxnId;
use parking_lot::RwLock;

use crate::hlc::HlcTimestamp;
use crate::version::{Version, VersionId, VersionState};

/// Isolation level for a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read committed: sees committed data at each statement.
    ReadCommitted,
    /// Snapshot isolation: sees data as of transaction start.
    SnapshotIsolation,
    /// Serializable snapshot isolation: SI with conflict detection.
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::SnapshotIsolation
    }
}

/// A snapshot representing a consistent view of the database.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The timestamp of this snapshot.
    read_ts: HlcTimestamp,
    /// Transaction ID that owns this snapshot.
    txn_id: TxnId,
    /// Isolation level.
    isolation: IsolationLevel,
    /// Set of transactions that were active when this snapshot was taken.
    /// These transactions' uncommitted writes are invisible.
    active_txns: HashSet<TxnId>,
}

impl Snapshot {
    /// Creates a new snapshot.
    pub fn new(
        read_ts: HlcTimestamp,
        txn_id: TxnId,
        isolation: IsolationLevel,
        active_txns: HashSet<TxnId>,
    ) -> Self {
        Self {
            read_ts,
            txn_id,
            isolation,
            active_txns,
        }
    }

    /// Creates a snapshot for a specific timestamp (for read-only queries).
    pub fn at_timestamp(read_ts: HlcTimestamp) -> Self {
        Self {
            read_ts,
            txn_id: TxnId::INVALID,
            isolation: IsolationLevel::SnapshotIsolation,
            active_txns: HashSet::new(),
        }
    }

    /// Returns the read timestamp.
    pub fn read_ts(&self) -> &HlcTimestamp {
        &self.read_ts
    }

    /// Returns the transaction ID.
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Returns the isolation level.
    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// Checks if a version is visible in this snapshot.
    pub fn is_visible(&self, version: &Version) -> bool {
        // Own writes are always visible (unless aborted)
        if version.created_by == self.txn_id {
            return version.state != VersionState::Aborted;
        }

        // For other transactions:
        // 1. Must be committed
        if version.state != VersionState::Committed {
            return false;
        }

        // 2. Creator must not have been active when snapshot was taken
        if self.active_txns.contains(&version.created_by) {
            return false;
        }

        // 3. Begin timestamp must be <= snapshot timestamp
        if version.begin_ts > self.read_ts {
            return false;
        }

        // 4. Must not have been deleted before snapshot (or by an invisible txn)
        if version.end_ts <= self.read_ts {
            // Was deleted before our snapshot
            return false;
        }

        // 5. If deleted by a visible transaction, it's not visible
        if version.deleted_by.is_valid()
            && !self.active_txns.contains(&version.deleted_by)
            && version.deleted_by != self.txn_id
        {
            // Check if the deleting transaction committed before our snapshot
            // This is a simplified check; in practice we'd check commit timestamps
            if version.end_ts <= self.read_ts {
                return false;
            }
        }

        true
    }

    /// Checks if a transaction's writes are visible in this snapshot.
    pub fn can_see_txn(&self, txn_id: TxnId) -> bool {
        // Can see own writes
        if txn_id == self.txn_id {
            return true;
        }
        // Can see if not in active set
        !self.active_txns.contains(&txn_id)
    }
}

/// Manages active transactions and their snapshots.
#[derive(Debug)]
pub struct SnapshotManager {
    /// Next transaction ID to assign.
    next_txn_id: AtomicU64,
    /// Currently active transactions.
    active_txns: RwLock<HashMap<TxnId, TransactionInfo>>,
    /// Minimum active transaction timestamp (for GC).
    min_active_ts: RwLock<HlcTimestamp>,
}

/// Information about an active transaction.
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// Transaction ID.
    pub txn_id: TxnId,
    /// Start timestamp.
    pub start_ts: HlcTimestamp,
    /// Isolation level.
    pub isolation: IsolationLevel,
    /// Read timestamp for this transaction's snapshot.
    pub read_ts: HlcTimestamp,
    /// Keys read by this transaction (for SSI).
    pub read_set: HashSet<Bytes>,
    /// Keys written by this transaction.
    pub write_set: HashSet<Bytes>,
    /// Versions created by this transaction.
    pub created_versions: Vec<(Bytes, VersionId)>,
}

impl TransactionInfo {
    /// Creates info for a new transaction.
    pub fn new(txn_id: TxnId, start_ts: HlcTimestamp, isolation: IsolationLevel) -> Self {
        Self {
            txn_id,
            start_ts,
            read_ts: start_ts,
            isolation,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            created_versions: Vec::new(),
        }
    }

    /// Records a read operation.
    pub fn record_read(&mut self, key: Bytes) {
        self.read_set.insert(key);
    }

    /// Records a write operation.
    pub fn record_write(&mut self, key: Bytes, version_id: VersionId) {
        self.write_set.insert(key.clone());
        self.created_versions.push((key, version_id));
    }

    /// Checks if there's a read-write conflict with another transaction.
    pub fn has_rw_conflict(&self, other: &TransactionInfo) -> bool {
        // Check if other wrote something we read
        for read_key in &self.read_set {
            if other.write_set.contains(read_key) {
                return true;
            }
        }
        false
    }

    /// Checks if there's a write-write conflict with another transaction.
    pub fn has_ww_conflict(&self, other: &TransactionInfo) -> bool {
        for write_key in &self.write_set {
            if other.write_set.contains(write_key) {
                return true;
            }
        }
        false
    }
}

impl SnapshotManager {
    /// Creates a new snapshot manager.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            min_active_ts: RwLock::new(HlcTimestamp::MAX),
        }
    }

    /// Begins a new transaction and returns its info.
    pub fn begin_txn(&self, start_ts: HlcTimestamp, isolation: IsolationLevel) -> TransactionInfo {
        let txn_id = TxnId::new(self.next_txn_id.fetch_add(1, AtomicOrdering::SeqCst));
        let info = TransactionInfo::new(txn_id, start_ts, isolation);

        {
            let mut active = self.active_txns.write();
            active.insert(txn_id, info.clone());
        }

        self.update_min_active_ts();

        info
    }

    /// Takes a snapshot for a transaction.
    pub fn take_snapshot(&self, txn_id: TxnId) -> Option<Snapshot> {
        let active = self.active_txns.read();
        let info = active.get(&txn_id)?;

        // Get IDs of all currently active transactions (except self)
        let active_txn_ids: HashSet<TxnId> =
            active.keys().filter(|&id| *id != txn_id).copied().collect();

        Some(Snapshot::new(
            info.read_ts,
            txn_id,
            info.isolation,
            active_txn_ids,
        ))
    }

    /// Records a read for a transaction.
    pub fn record_read(&self, txn_id: TxnId, key: Bytes) {
        let mut active = self.active_txns.write();
        if let Some(info) = active.get_mut(&txn_id) {
            info.record_read(key);
        }
    }

    /// Records a write for a transaction.
    pub fn record_write(&self, txn_id: TxnId, key: Bytes, version_id: VersionId) {
        let mut active = self.active_txns.write();
        if let Some(info) = active.get_mut(&txn_id) {
            info.record_write(key, version_id);
        }
    }

    /// Gets a transaction's info.
    pub fn get_txn_info(&self, txn_id: TxnId) -> Option<TransactionInfo> {
        self.active_txns.read().get(&txn_id).cloned()
    }

    /// Validates a transaction for commit (SSI conflict detection).
    pub fn validate_for_commit(&self, txn_id: TxnId) -> Result<(), ConflictError> {
        let active = self.active_txns.read();
        let info = match active.get(&txn_id) {
            Some(i) => i,
            None => return Err(ConflictError::TransactionNotFound(txn_id)),
        };

        if info.isolation != IsolationLevel::Serializable {
            return Ok(()); // Only SSI needs validation
        }

        // Check for conflicts with other active transactions
        for (other_id, other_info) in active.iter() {
            if *other_id == txn_id {
                continue;
            }

            // Check for dangerous structure (rw-antidependency cycle)
            if info.has_rw_conflict(other_info) && other_info.has_rw_conflict(info) {
                return Err(ConflictError::SerializationAnomaly {
                    txn1: txn_id,
                    txn2: *other_id,
                });
            }

            // Check for write-write conflict
            if info.has_ww_conflict(other_info) {
                return Err(ConflictError::WriteWriteConflict {
                    txn1: txn_id,
                    txn2: *other_id,
                });
            }
        }

        Ok(())
    }

    /// Commits a transaction.
    pub fn commit_txn(&self, txn_id: TxnId) -> Option<TransactionInfo> {
        let info = {
            let mut active = self.active_txns.write();
            active.remove(&txn_id)
        };

        self.update_min_active_ts();
        info
    }

    /// Aborts a transaction.
    pub fn abort_txn(&self, txn_id: TxnId) -> Option<TransactionInfo> {
        let info = {
            let mut active = self.active_txns.write();
            active.remove(&txn_id)
        };

        self.update_min_active_ts();
        info
    }

    /// Returns the minimum active transaction timestamp.
    pub fn min_active_ts(&self) -> HlcTimestamp {
        *self.min_active_ts.read()
    }

    /// Returns the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns.read().len()
    }

    /// Updates the minimum active timestamp.
    fn update_min_active_ts(&self) {
        let active = self.active_txns.read();
        let min = active
            .values()
            .map(|info| &info.start_ts)
            .min()
            .cloned()
            .unwrap_or(HlcTimestamp::MAX);

        *self.min_active_ts.write() = min;
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during conflict detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictError {
    /// Transaction not found.
    TransactionNotFound(TxnId),
    /// Write-write conflict detected.
    WriteWriteConflict {
        /// First transaction.
        txn1: TxnId,
        /// Second transaction.
        txn2: TxnId,
    },
    /// Serialization anomaly detected.
    SerializationAnomaly {
        /// First transaction in cycle.
        txn1: TxnId,
        /// Second transaction in cycle.
        txn2: TxnId,
    },
}

impl fmt::Display for ConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TransactionNotFound(txn) => {
                write!(f, "transaction not found: {:?}", txn)
            }
            Self::WriteWriteConflict { txn1, txn2 } => {
                write!(f, "write-write conflict between {:?} and {:?}", txn1, txn2)
            }
            Self::SerializationAnomaly { txn1, txn2 } => {
                write!(
                    f,
                    "serialization anomaly detected between {:?} and {:?}",
                    txn1, txn2
                )
            }
        }
    }
}

impl std::error::Error for ConflictError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts(physical: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp::new(physical, logical, 1)
    }

    #[test]
    fn test_snapshot_own_writes() {
        let snapshot = Snapshot::new(
            make_ts(100, 0),
            TxnId::new(1),
            IsolationLevel::SnapshotIsolation,
            HashSet::new(),
        );

        // Own pending write is visible
        let version = Version::new(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("data"),
        );
        assert!(snapshot.is_visible(&version));
    }

    #[test]
    fn test_snapshot_committed_visible() {
        let snapshot = Snapshot::new(
            make_ts(100, 0),
            TxnId::new(2),
            IsolationLevel::SnapshotIsolation,
            HashSet::new(),
        );

        // Committed version before snapshot is visible
        let version = Version::committed(
            VersionId::new(1),
            make_ts(50, 0),
            TxnId::new(1),
            Bytes::from("data"),
        );
        assert!(snapshot.is_visible(&version));
    }

    #[test]
    fn test_snapshot_future_invisible() {
        let snapshot = Snapshot::new(
            make_ts(100, 0),
            TxnId::new(2),
            IsolationLevel::SnapshotIsolation,
            HashSet::new(),
        );

        // Committed version after snapshot is invisible
        let version = Version::committed(
            VersionId::new(1),
            make_ts(150, 0),
            TxnId::new(1),
            Bytes::from("data"),
        );
        assert!(!snapshot.is_visible(&version));
    }

    #[test]
    fn test_snapshot_active_txn_invisible() {
        let mut active = HashSet::new();
        active.insert(TxnId::new(3));

        let snapshot = Snapshot::new(
            make_ts(100, 0),
            TxnId::new(2),
            IsolationLevel::SnapshotIsolation,
            active,
        );

        // Version from active transaction is invisible
        let version = Version::committed(
            VersionId::new(1),
            make_ts(50, 0),
            TxnId::new(3),
            Bytes::from("data"),
        );
        assert!(!snapshot.is_visible(&version));
    }

    #[test]
    fn test_snapshot_manager_begin_txn() {
        let manager = SnapshotManager::new();

        let info1 = manager.begin_txn(make_ts(100, 0), IsolationLevel::SnapshotIsolation);
        let info2 = manager.begin_txn(make_ts(110, 0), IsolationLevel::SnapshotIsolation);

        assert_eq!(info1.txn_id.as_u64(), 1);
        assert_eq!(info2.txn_id.as_u64(), 2);
        assert_eq!(manager.active_count(), 2);
    }

    #[test]
    fn test_snapshot_manager_take_snapshot() {
        let manager = SnapshotManager::new();

        let info1 = manager.begin_txn(make_ts(100, 0), IsolationLevel::SnapshotIsolation);
        let info2 = manager.begin_txn(make_ts(110, 0), IsolationLevel::SnapshotIsolation);

        let snapshot = manager.take_snapshot(info1.txn_id).unwrap();

        // Snapshot should see info2 as active
        assert!(snapshot.active_txns.contains(&info2.txn_id));
        assert!(!snapshot.active_txns.contains(&info1.txn_id));
    }

    #[test]
    fn test_snapshot_manager_commit() {
        let manager = SnapshotManager::new();

        let info = manager.begin_txn(make_ts(100, 0), IsolationLevel::SnapshotIsolation);
        assert_eq!(manager.active_count(), 1);

        let committed = manager.commit_txn(info.txn_id);
        assert!(committed.is_some());
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_snapshot_manager_abort() {
        let manager = SnapshotManager::new();

        let info = manager.begin_txn(make_ts(100, 0), IsolationLevel::SnapshotIsolation);

        let aborted = manager.abort_txn(info.txn_id);
        assert!(aborted.is_some());
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_snapshot_manager_min_active_ts() {
        let manager = SnapshotManager::new();

        // No active transactions
        assert_eq!(manager.min_active_ts(), HlcTimestamp::MAX);

        let _info1 = manager.begin_txn(make_ts(100, 0), IsolationLevel::SnapshotIsolation);
        assert_eq!(manager.min_active_ts(), make_ts(100, 0));

        let info2 = manager.begin_txn(make_ts(50, 0), IsolationLevel::SnapshotIsolation);
        assert_eq!(manager.min_active_ts(), make_ts(50, 0));

        manager.commit_txn(info2.txn_id);
        assert_eq!(manager.min_active_ts(), make_ts(100, 0));
    }

    #[test]
    fn test_transaction_info_conflicts() {
        let mut info1 =
            TransactionInfo::new(TxnId::new(1), make_ts(100, 0), IsolationLevel::Serializable);
        let mut info2 =
            TransactionInfo::new(TxnId::new(2), make_ts(100, 0), IsolationLevel::Serializable);

        // No conflicts initially
        assert!(!info1.has_rw_conflict(&info2));
        assert!(!info1.has_ww_conflict(&info2));

        // T1 reads key1, T2 writes key1
        info1.record_read(Bytes::from("key1"));
        info2.record_write(Bytes::from("key1"), VersionId::new(1));

        assert!(info1.has_rw_conflict(&info2)); // T1 reads what T2 writes
        assert!(!info2.has_rw_conflict(&info1)); // T2 doesn't read what T1 writes

        // Write-write conflict
        info1.record_write(Bytes::from("key2"), VersionId::new(2));
        info2.record_write(Bytes::from("key2"), VersionId::new(3));

        assert!(info1.has_ww_conflict(&info2));
    }

    #[test]
    fn test_ssi_validation_pass() {
        let manager = SnapshotManager::new();

        let info = manager.begin_txn(make_ts(100, 0), IsolationLevel::Serializable);

        // No conflicts, should validate
        let result = manager.validate_for_commit(info.txn_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ssi_validation_ww_conflict() {
        let manager = SnapshotManager::new();

        let info1 = manager.begin_txn(make_ts(100, 0), IsolationLevel::Serializable);
        let info2 = manager.begin_txn(make_ts(100, 0), IsolationLevel::Serializable);

        // Both write to same key
        manager.record_write(info1.txn_id, Bytes::from("key1"), VersionId::new(1));
        manager.record_write(info2.txn_id, Bytes::from("key1"), VersionId::new(2));

        let result = manager.validate_for_commit(info1.txn_id);
        assert!(matches!(
            result,
            Err(ConflictError::WriteWriteConflict { .. })
        ));
    }

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::SnapshotIsolation);
    }

    #[test]
    fn test_snapshot_at_timestamp() {
        let snapshot = Snapshot::at_timestamp(make_ts(100, 0));
        assert_eq!(*snapshot.read_ts(), make_ts(100, 0));
        assert_eq!(snapshot.txn_id(), TxnId::INVALID);
    }
}
