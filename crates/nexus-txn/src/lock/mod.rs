//! Lock management for transaction isolation.
//!
//! This module implements a hierarchical lock manager supporting:
//! - Row-level locking with shared (S) and exclusive (X) modes
//! - Intention locks (IS, IX) for table-level granularity hints
//! - Lock upgrading from S to X
//! - Wait queues for blocked lock requests
//!
//! # Lock Compatibility Matrix
//!
//! ```text
//!          │ S  │ X  │ IS │ IX │
//! ─────────┼────┼────┼────┼────┤
//!     S    │ ✓  │ ✗  │ ✓  │ ✗  │
//!     X    │ ✗  │ ✗  │ ✗  │ ✗  │
//!     IS   │ ✓  │ ✗  │ ✓  │ ✓  │
//!     IX   │ ✗  │ ✗  │ ✓  │ ✓  │
//! ```
//!
//! # Lock Ordering
//!
//! To prevent deadlocks through lock ordering:
//! - Acquire table locks before row locks
//! - Within same level, order by key bytes lexicographically

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use nexus_common::types::TxnId;
use parking_lot::{Mutex, RwLock};

/// Lock mode for a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Shared lock (read lock).
    Shared,
    /// Exclusive lock (write lock).
    Exclusive,
    /// Intention shared (table-level hint for row S locks).
    IntentionShared,
    /// Intention exclusive (table-level hint for row X locks).
    IntentionExclusive,
}

impl LockMode {
    /// Checks if this lock mode is compatible with another.
    pub fn is_compatible_with(&self, other: &LockMode) -> bool {
        use LockMode::*;
        matches!(
            (self, other),
            // Shared is compatible with Shared and IS
            (Shared, Shared) | (Shared, IntentionShared) |
            (IntentionShared, Shared) | (IntentionShared, IntentionShared) |
            (IntentionShared, IntentionExclusive) |
            // IS and IX are compatible with each other
            (IntentionExclusive, IntentionShared) | (IntentionExclusive, IntentionExclusive)
        )
    }

    /// Returns the stronger of two lock modes.
    pub fn stronger(self, other: LockMode) -> LockMode {
        use LockMode::*;
        match (self, other) {
            (Exclusive, _) | (_, Exclusive) => Exclusive,
            (IntentionExclusive, Shared) | (Shared, IntentionExclusive) => Exclusive,
            (Shared, Shared) => Shared,
            (Shared, _) | (_, Shared) => Shared,
            (IntentionExclusive, _) | (_, IntentionExclusive) => IntentionExclusive,
            (IntentionShared, IntentionShared) => IntentionShared,
        }
    }
}

impl fmt::Display for LockMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockMode::Shared => write!(f, "S"),
            LockMode::Exclusive => write!(f, "X"),
            LockMode::IntentionShared => write!(f, "IS"),
            LockMode::IntentionExclusive => write!(f, "IX"),
        }
    }
}

/// The type of resource being locked.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceId {
    /// A table-level lock.
    Table(u64),
    /// A row-level lock (table_id, row_key).
    Row(u64, Bytes),
    /// A page-level lock.
    Page(u64, u64),
}

impl ResourceId {
    /// Creates a table resource ID.
    pub fn table(table_id: u64) -> Self {
        ResourceId::Table(table_id)
    }

    /// Creates a row resource ID.
    pub fn row(table_id: u64, key: Bytes) -> Self {
        ResourceId::Row(table_id, key)
    }

    /// Creates a page resource ID.
    pub fn page(table_id: u64, page_id: u64) -> Self {
        ResourceId::Page(table_id, page_id)
    }

    /// Returns the table ID for this resource.
    pub fn table_id(&self) -> u64 {
        match self {
            ResourceId::Table(id) => *id,
            ResourceId::Row(id, _) => *id,
            ResourceId::Page(id, _) => *id,
        }
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceId::Table(id) => write!(f, "Table({})", id),
            ResourceId::Row(table, key) => write!(f, "Row({}, {:?})", table, key),
            ResourceId::Page(table, page) => write!(f, "Page({}, {})", table, page),
        }
    }
}

/// Result of a lock acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockResult {
    /// Lock was granted immediately.
    Granted,
    /// Lock request is waiting in queue.
    Waiting,
    /// Lock was upgraded from a weaker mode.
    Upgraded,
    /// Lock acquisition timed out.
    Timeout,
    /// Deadlock was detected and this transaction should abort.
    Deadlock,
    /// Transaction already holds the lock.
    AlreadyHeld,
}

impl LockResult {
    /// Returns true if the lock was successfully acquired.
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            LockResult::Granted | LockResult::Upgraded | LockResult::AlreadyHeld
        )
    }
}

/// A pending lock request.
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// Transaction requesting the lock.
    pub txn_id: TxnId,
    /// Requested lock mode.
    pub mode: LockMode,
    /// When the request was made.
    pub requested_at: Instant,
    /// Whether this is a waiting request.
    pub waiting: bool,
}

impl LockRequest {
    /// Creates a new lock request.
    pub fn new(txn_id: TxnId, mode: LockMode) -> Self {
        Self {
            txn_id,
            mode,
            requested_at: Instant::now(),
            waiting: false,
        }
    }
}

/// Information about a held lock.
#[derive(Debug)]
pub struct LockInfo {
    /// The resource being locked.
    pub resource: ResourceId,
    /// Current lock mode.
    pub mode: LockMode,
    /// Transactions holding the lock.
    pub holders: HashSet<TxnId>,
    /// Queue of waiting lock requests.
    pub wait_queue: VecDeque<LockRequest>,
}

impl LockInfo {
    /// Creates new lock info.
    pub fn new(resource: ResourceId) -> Self {
        Self {
            resource,
            mode: LockMode::Shared, // Will be set on first grant
            holders: HashSet::new(),
            wait_queue: VecDeque::new(),
        }
    }

    /// Checks if a lock mode can be granted.
    pub fn can_grant(&self, txn_id: TxnId, mode: LockMode) -> bool {
        if self.holders.is_empty() {
            return true;
        }

        // If this txn already holds the lock
        if self.holders.contains(&txn_id) {
            // Can always keep same mode, or upgrade if only holder
            if self.mode == mode || self.mode.is_compatible_with(&mode) {
                return true;
            }
            // Can upgrade if only holder
            if self.holders.len() == 1 {
                return true;
            }
        }

        // Check compatibility with current lock mode
        mode.is_compatible_with(&self.mode)
    }

    /// Grants a lock to a transaction.
    pub fn grant(&mut self, txn_id: TxnId, mode: LockMode) {
        if self.holders.is_empty() {
            self.mode = mode;
        } else if !mode.is_compatible_with(&self.mode)
            && self.holders.len() == 1
            && self.holders.contains(&txn_id)
        {
            // Upgrade
            self.mode = self.mode.stronger(mode);
        }
        self.holders.insert(txn_id);
    }

    /// Releases a lock held by a transaction.
    pub fn release(&mut self, txn_id: TxnId) -> bool {
        self.holders.remove(&txn_id)
    }

    /// Returns true if the lock is free.
    pub fn is_free(&self) -> bool {
        self.holders.is_empty() && self.wait_queue.is_empty()
    }
}

/// Statistics about the lock manager.
#[derive(Debug, Default)]
pub struct LockStats {
    /// Total lock acquisitions.
    pub acquisitions: AtomicU64,
    /// Total lock releases.
    pub releases: AtomicU64,
    /// Total lock waits.
    pub waits: AtomicU64,
    /// Total deadlocks detected.
    pub deadlocks: AtomicU64,
    /// Total lock upgrades.
    pub upgrades: AtomicU64,
    /// Total timeouts.
    pub timeouts: AtomicU64,
}

impl LockStats {
    /// Creates new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a successful acquisition.
    pub fn record_acquisition(&self) {
        self.acquisitions.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Records a release.
    pub fn record_release(&self) {
        self.releases.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Records a wait.
    pub fn record_wait(&self) {
        self.waits.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Records a deadlock.
    pub fn record_deadlock(&self) {
        self.deadlocks.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Records an upgrade.
    pub fn record_upgrade(&self) {
        self.upgrades.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Records a timeout.
    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

/// Configuration for the lock manager.
#[derive(Debug, Clone)]
pub struct LockManagerConfig {
    /// Default lock timeout.
    pub lock_timeout: Duration,
    /// Whether to enable deadlock detection.
    pub deadlock_detection: bool,
    /// Interval for deadlock detection checks.
    pub deadlock_check_interval: Duration,
}

impl Default for LockManagerConfig {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(30),
            deadlock_detection: true,
            deadlock_check_interval: Duration::from_millis(100),
        }
    }
}

/// The lock manager for managing transaction locks.
pub struct LockManager {
    /// All locks, keyed by resource.
    locks: RwLock<HashMap<ResourceId, LockInfo>>,
    /// Locks held by each transaction.
    txn_locks: RwLock<HashMap<TxnId, HashSet<ResourceId>>>,
    /// Wait-for graph edges (waiter -> holder).
    wait_for: Mutex<HashMap<TxnId, HashSet<TxnId>>>,
    /// Configuration.
    config: LockManagerConfig,
    /// Statistics.
    stats: LockStats,
}

impl LockManager {
    /// Creates a new lock manager with default configuration.
    pub fn new() -> Self {
        Self::with_config(LockManagerConfig::default())
    }

    /// Creates a lock manager with custom configuration.
    pub fn with_config(config: LockManagerConfig) -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            txn_locks: RwLock::new(HashMap::new()),
            wait_for: Mutex::new(HashMap::new()),
            config,
            stats: LockStats::new(),
        }
    }

    /// Tries to acquire a lock.
    pub fn try_lock(&self, txn_id: TxnId, resource: ResourceId, mode: LockMode) -> LockResult {
        let mut locks = self.locks.write();
        let lock_info = locks
            .entry(resource.clone())
            .or_insert_with(|| LockInfo::new(resource.clone()));

        // Check if already held
        if lock_info.holders.contains(&txn_id) && lock_info.mode == mode {
            return LockResult::AlreadyHeld;
        }

        // Try to grant
        if lock_info.can_grant(txn_id, mode) {
            let was_upgrade = lock_info.holders.contains(&txn_id) && lock_info.mode != mode;
            lock_info.grant(txn_id, mode);

            // Track locks per transaction
            self.txn_locks
                .write()
                .entry(txn_id)
                .or_insert_with(HashSet::new)
                .insert(resource);

            if was_upgrade {
                self.stats.record_upgrade();
                LockResult::Upgraded
            } else {
                self.stats.record_acquisition();
                LockResult::Granted
            }
        } else {
            // Need to wait
            let request = LockRequest::new(txn_id, mode);
            lock_info.wait_queue.push_back(request);

            // Update wait-for graph
            if self.config.deadlock_detection {
                let mut wait_for = self.wait_for.lock();
                let waiters = wait_for.entry(txn_id).or_insert_with(HashSet::new);
                for holder in &lock_info.holders {
                    waiters.insert(*holder);
                }
            }

            self.stats.record_wait();
            LockResult::Waiting
        }
    }

    /// Acquires a lock, blocking until granted or timeout.
    pub fn lock(
        &self,
        txn_id: TxnId,
        resource: ResourceId,
        mode: LockMode,
        timeout: Option<Duration>,
    ) -> LockResult {
        let start = Instant::now();
        let timeout = timeout.unwrap_or(self.config.lock_timeout);

        loop {
            let result = self.try_lock(txn_id, resource.clone(), mode);

            match result {
                LockResult::Waiting => {
                    // Check for timeout
                    if start.elapsed() >= timeout {
                        // Remove from wait queue
                        self.cancel_wait(txn_id, &resource);
                        self.stats.record_timeout();
                        return LockResult::Timeout;
                    }

                    // Check for deadlock
                    if self.config.deadlock_detection && self.has_deadlock(txn_id) {
                        self.cancel_wait(txn_id, &resource);
                        self.stats.record_deadlock();
                        return LockResult::Deadlock;
                    }

                    // Sleep briefly before retrying
                    std::thread::sleep(Duration::from_micros(100));
                }
                _ => return result,
            }
        }
    }

    /// Releases a lock.
    pub fn unlock(&self, txn_id: TxnId, resource: &ResourceId) -> bool {
        let mut locks = self.locks.write();

        if let Some(lock_info) = locks.get_mut(resource) {
            if lock_info.release(txn_id) {
                self.stats.record_release();

                // Remove from txn's lock set
                if let Some(txn_resources) = self.txn_locks.write().get_mut(&txn_id) {
                    txn_resources.remove(resource);
                }

                // Remove from wait-for graph
                self.wait_for.lock().remove(&txn_id);

                // Try to grant waiting requests
                self.process_wait_queue(lock_info);

                // Clean up empty lock entries
                if lock_info.is_free() {
                    locks.remove(resource);
                }

                return true;
            }
        }

        false
    }

    /// Releases all locks held by a transaction.
    pub fn release_all(&self, txn_id: TxnId) -> usize {
        let resources: Vec<ResourceId> = {
            let txn_locks = self.txn_locks.read();
            txn_locks
                .get(&txn_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

        let count = resources.len();
        for resource in resources {
            self.unlock(txn_id, &resource);
        }

        // Clean up
        self.txn_locks.write().remove(&txn_id);
        self.wait_for.lock().remove(&txn_id);

        count
    }

    /// Cancels a waiting lock request.
    fn cancel_wait(&self, txn_id: TxnId, resource: &ResourceId) {
        let mut locks = self.locks.write();
        if let Some(lock_info) = locks.get_mut(resource) {
            lock_info.wait_queue.retain(|r| r.txn_id != txn_id);
        }

        self.wait_for.lock().remove(&txn_id);
    }

    /// Processes the wait queue after a lock is released.
    fn process_wait_queue(&self, lock_info: &mut LockInfo) {
        while let Some(request) = lock_info.wait_queue.front() {
            if lock_info.can_grant(request.txn_id, request.mode) {
                let request = lock_info.wait_queue.pop_front().unwrap();
                lock_info.grant(request.txn_id, request.mode);

                // Track in txn's lock set
                self.txn_locks
                    .write()
                    .entry(request.txn_id)
                    .or_insert_with(HashSet::new)
                    .insert(lock_info.resource.clone());

                // Remove from wait-for graph
                self.wait_for.lock().remove(&request.txn_id);

                self.stats.record_acquisition();
            } else {
                break; // Can't grant this request, stop processing
            }
        }
    }

    /// Checks if there's a deadlock involving this transaction.
    fn has_deadlock(&self, start_txn: TxnId) -> bool {
        let wait_for = self.wait_for.lock();
        let mut visited = HashSet::new();
        let mut stack = vec![start_txn];

        while let Some(txn) = stack.pop() {
            if txn == start_txn && visited.contains(&txn) {
                return true; // Cycle detected
            }

            if visited.insert(txn) {
                if let Some(holders) = wait_for.get(&txn) {
                    for holder in holders {
                        stack.push(*holder);
                    }
                }
            }
        }

        false
    }

    /// Returns statistics about the lock manager.
    pub fn stats(&self) -> &LockStats {
        &self.stats
    }

    /// Returns the number of active locks.
    pub fn lock_count(&self) -> usize {
        self.locks.read().len()
    }

    /// Returns the number of transactions holding locks.
    pub fn txn_count(&self) -> usize {
        self.txn_locks.read().len()
    }

    /// Returns the locks held by a transaction.
    pub fn get_txn_locks(&self, txn_id: TxnId) -> Vec<ResourceId> {
        self.txn_locks
            .read()
            .get(&txn_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for LockManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockManager")
            .field("lock_count", &self.lock_count())
            .field("txn_count", &self.txn_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_mode_compatibility() {
        use LockMode::*;

        // S-S compatible
        assert!(Shared.is_compatible_with(&Shared));

        // S-X not compatible
        assert!(!Shared.is_compatible_with(&Exclusive));
        assert!(!Exclusive.is_compatible_with(&Shared));

        // X-X not compatible
        assert!(!Exclusive.is_compatible_with(&Exclusive));

        // IS compatible with S, IS, IX
        assert!(IntentionShared.is_compatible_with(&Shared));
        assert!(IntentionShared.is_compatible_with(&IntentionShared));
        assert!(IntentionShared.is_compatible_with(&IntentionExclusive));

        // IX compatible with IS, IX
        assert!(IntentionExclusive.is_compatible_with(&IntentionShared));
        assert!(IntentionExclusive.is_compatible_with(&IntentionExclusive));

        // IX not compatible with S, X
        assert!(!IntentionExclusive.is_compatible_with(&Shared));
        assert!(!IntentionExclusive.is_compatible_with(&Exclusive));
    }

    #[test]
    fn test_lock_manager_basic() {
        let lm = LockManager::new();
        let txn1 = TxnId::new(1);
        let resource = ResourceId::row(1, Bytes::from("key1"));

        let result = lm.try_lock(txn1, resource.clone(), LockMode::Shared);
        assert_eq!(result, LockResult::Granted);

        // Same lock again
        let result = lm.try_lock(txn1, resource.clone(), LockMode::Shared);
        assert_eq!(result, LockResult::AlreadyHeld);

        assert!(lm.unlock(txn1, &resource));
    }

    #[test]
    fn test_shared_locks_concurrent() {
        let lm = LockManager::new();
        let txn1 = TxnId::new(1);
        let txn2 = TxnId::new(2);
        let resource = ResourceId::row(1, Bytes::from("key1"));

        // Both can acquire shared locks
        let result1 = lm.try_lock(txn1, resource.clone(), LockMode::Shared);
        assert_eq!(result1, LockResult::Granted);

        let result2 = lm.try_lock(txn2, resource.clone(), LockMode::Shared);
        assert_eq!(result2, LockResult::Granted);

        assert_eq!(lm.lock_count(), 1);
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let lm = LockManager::new();
        let txn1 = TxnId::new(1);
        let txn2 = TxnId::new(2);
        let resource = ResourceId::row(1, Bytes::from("key1"));

        // Txn1 gets exclusive lock
        let result = lm.try_lock(txn1, resource.clone(), LockMode::Exclusive);
        assert_eq!(result, LockResult::Granted);

        // Txn2 must wait for shared
        let result = lm.try_lock(txn2, resource.clone(), LockMode::Shared);
        assert_eq!(result, LockResult::Waiting);

        // Release exclusive - this should automatically grant to txn2 from wait queue
        lm.unlock(txn1, &resource);

        // Txn2 should now have the lock (granted from wait queue processing)
        // Calling try_lock again should return AlreadyHeld
        let result = lm.try_lock(txn2, resource.clone(), LockMode::Shared);
        assert_eq!(result, LockResult::AlreadyHeld);
    }

    #[test]
    fn test_lock_upgrade() {
        let lm = LockManager::new();
        let txn1 = TxnId::new(1);
        let resource = ResourceId::row(1, Bytes::from("key1"));

        // Get shared lock
        let result = lm.try_lock(txn1, resource.clone(), LockMode::Shared);
        assert_eq!(result, LockResult::Granted);

        // Upgrade to exclusive
        let result = lm.try_lock(txn1, resource.clone(), LockMode::Exclusive);
        assert_eq!(result, LockResult::Upgraded);
    }

    #[test]
    fn test_release_all() {
        let lm = LockManager::new();
        let txn1 = TxnId::new(1);
        let r1 = ResourceId::row(1, Bytes::from("key1"));
        let r2 = ResourceId::row(1, Bytes::from("key2"));
        let r3 = ResourceId::row(1, Bytes::from("key3"));

        lm.try_lock(txn1, r1, LockMode::Shared);
        lm.try_lock(txn1, r2, LockMode::Exclusive);
        lm.try_lock(txn1, r3, LockMode::Shared);

        assert_eq!(lm.get_txn_locks(txn1).len(), 3);

        let released = lm.release_all(txn1);
        assert_eq!(released, 3);
        assert_eq!(lm.get_txn_locks(txn1).len(), 0);
    }

    #[test]
    fn test_resource_id() {
        let table = ResourceId::table(42);
        assert_eq!(table.table_id(), 42);

        let row = ResourceId::row(42, Bytes::from("key"));
        assert_eq!(row.table_id(), 42);

        let page = ResourceId::page(42, 100);
        assert_eq!(page.table_id(), 42);
    }

    #[test]
    fn test_lock_stats() {
        let lm = LockManager::new();
        let txn1 = TxnId::new(1);
        let resource = ResourceId::row(1, Bytes::from("key1"));

        lm.try_lock(txn1, resource.clone(), LockMode::Shared);
        assert_eq!(lm.stats().acquisitions.load(AtomicOrdering::Relaxed), 1);

        lm.unlock(txn1, &resource);
        assert_eq!(lm.stats().releases.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn test_lock_mode_display() {
        assert_eq!(format!("{}", LockMode::Shared), "S");
        assert_eq!(format!("{}", LockMode::Exclusive), "X");
        assert_eq!(format!("{}", LockMode::IntentionShared), "IS");
        assert_eq!(format!("{}", LockMode::IntentionExclusive), "IX");
    }
}
