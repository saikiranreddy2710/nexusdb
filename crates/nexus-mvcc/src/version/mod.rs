//! Version chain storage and management.
//!
//! This module implements MVCC version chains, which store multiple versions
//! of each record for snapshot isolation. Each version is tagged with:
//! - Begin timestamp (when it became visible)
//! - End timestamp (when it was superseded/deleted)
//! - Transaction ID that created it
//!
//! # Version Chain Structure
//!
//! ```text
//! Record Key: "user:1"
//! ┌─────────────────────────────────────────────────────┐
//! │ Version 3 (latest)                                   │
//! │ begin_ts: 150, end_ts: MAX, txn: 5                  │
//! │ value: "Alice (updated)"                             │
//! │                     ↓                                │
//! │ Version 2                                            │
//! │ begin_ts: 100, end_ts: 150, txn: 3                  │
//! │ value: "Alice"                                       │
//! │                     ↓                                │
//! │ Version 1 (oldest visible)                           │
//! │ begin_ts: 50, end_ts: 100, txn: 1                   │
//! │ value: "Initial Alice"                               │
//! └─────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use bytes::Bytes;
use nexus_common::types::TxnId;
use parking_lot::RwLock;

use crate::hlc::HlcTimestamp;

/// A unique identifier for a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionId(u64);

impl VersionId {
    /// Invalid version ID.
    pub const INVALID: Self = Self(0);

    /// Creates a new version ID.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw u64 value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Checks if this is a valid version ID.
    pub const fn is_valid(self) -> bool {
        self.0 != 0
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// The state of a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionState {
    /// Version is active and visible.
    Active,
    /// Version has been committed.
    Committed,
    /// Version has been aborted.
    Aborted,
    /// Version is pending (transaction in progress).
    Pending,
    /// Version has been garbage collected.
    Collected,
}

/// A single version of a record.
#[derive(Debug, Clone)]
pub struct Version {
    /// Unique identifier for this version.
    pub id: VersionId,
    /// Begin timestamp (when this version became visible).
    pub begin_ts: HlcTimestamp,
    /// End timestamp (when this version was superseded).
    /// MAX means still visible.
    pub end_ts: HlcTimestamp,
    /// Transaction that created this version.
    pub created_by: TxnId,
    /// Transaction that deleted/superseded this version.
    pub deleted_by: TxnId,
    /// The actual data.
    pub data: Bytes,
    /// Current state of the version.
    pub state: VersionState,
}

impl Version {
    /// Creates a new version.
    pub fn new(id: VersionId, begin_ts: HlcTimestamp, created_by: TxnId, data: Bytes) -> Self {
        Self {
            id,
            begin_ts,
            end_ts: HlcTimestamp::MAX,
            created_by,
            deleted_by: TxnId::INVALID,
            data,
            state: VersionState::Pending,
        }
    }

    /// Creates a committed version.
    pub fn committed(
        id: VersionId,
        begin_ts: HlcTimestamp,
        created_by: TxnId,
        data: Bytes,
    ) -> Self {
        Self {
            id,
            begin_ts,
            end_ts: HlcTimestamp::MAX,
            created_by,
            deleted_by: TxnId::INVALID,
            data,
            state: VersionState::Committed,
        }
    }

    /// Checks if this version is visible at the given timestamp.
    pub fn is_visible_at(&self, ts: &HlcTimestamp) -> bool {
        self.state == VersionState::Committed && &self.begin_ts <= ts && ts < &self.end_ts
    }

    /// Checks if this version is visible to the given transaction.
    pub fn is_visible_to(&self, txn_id: TxnId, read_ts: &HlcTimestamp) -> bool {
        // Visible if:
        // 1. Created by this transaction (can see own writes), or
        // 2. Committed and within timestamp range
        if self.created_by == txn_id {
            return self.state != VersionState::Aborted;
        }

        self.is_visible_at(read_ts)
    }

    /// Marks this version as committed.
    pub fn commit(&mut self, commit_ts: HlcTimestamp) {
        self.begin_ts = commit_ts;
        self.state = VersionState::Committed;
    }

    /// Marks this version as aborted.
    pub fn abort(&mut self) {
        self.state = VersionState::Aborted;
    }

    /// Marks this version as deleted (superseded).
    pub fn mark_deleted(&mut self, end_ts: HlcTimestamp, deleted_by: TxnId) {
        self.end_ts = end_ts;
        self.deleted_by = deleted_by;
    }

    /// Returns the size of this version in bytes.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.len()
    }

    /// Checks if this version can be garbage collected.
    pub fn can_gc(&self, oldest_active_ts: &HlcTimestamp) -> bool {
        // Can GC if:
        // 1. Aborted, or
        // 2. End timestamp is before oldest active transaction
        self.state == VersionState::Aborted
            || (self.state == VersionState::Committed && &self.end_ts < oldest_active_ts)
    }
}

/// A chain of versions for a single record.
#[derive(Debug)]
pub struct VersionChain {
    /// The key for this chain.
    key: Bytes,
    /// All versions, newest first.
    versions: RwLock<Vec<Version>>,
    /// Total size in bytes.
    total_size: AtomicU64,
}

impl VersionChain {
    /// Creates a new empty version chain.
    pub fn new(key: Bytes) -> Self {
        Self {
            key,
            versions: RwLock::new(Vec::new()),
            total_size: AtomicU64::new(0),
        }
    }

    /// Returns the key for this chain.
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns the number of versions in the chain.
    pub fn len(&self) -> usize {
        self.versions.read().len()
    }

    /// Returns true if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.versions.read().is_empty()
    }

    /// Returns the total size of all versions.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(AtomicOrdering::Relaxed)
    }

    /// Adds a new version to the chain.
    pub fn add_version(&self, version: Version) {
        let size = version.size() as u64;
        let mut versions = self.versions.write();
        versions.insert(0, version); // Insert at front (newest first)
        self.total_size.fetch_add(size, AtomicOrdering::Relaxed);
    }

    /// Gets the visible version for a transaction.
    pub fn get_visible(&self, txn_id: TxnId, read_ts: &HlcTimestamp) -> Option<Version> {
        let versions = self.versions.read();
        for version in versions.iter() {
            if version.is_visible_to(txn_id, read_ts) {
                return Some(version.clone());
            }
        }
        None
    }

    /// Gets the latest committed version.
    pub fn get_latest_committed(&self) -> Option<Version> {
        let versions = self.versions.read();
        versions
            .iter()
            .find(|v| v.state == VersionState::Committed && v.end_ts == HlcTimestamp::MAX)
            .cloned()
    }

    /// Gets the latest version (regardless of state).
    pub fn get_latest(&self) -> Option<Version> {
        let versions = self.versions.read();
        versions.first().cloned()
    }

    /// Commits a version.
    pub fn commit_version(&self, version_id: VersionId, commit_ts: HlcTimestamp) -> bool {
        let mut versions = self.versions.write();
        for version in versions.iter_mut() {
            if version.id == version_id {
                version.commit(commit_ts);
                return true;
            }
        }
        false
    }

    /// Aborts a version.
    pub fn abort_version(&self, version_id: VersionId) -> bool {
        let mut versions = self.versions.write();
        for version in versions.iter_mut() {
            if version.id == version_id {
                version.abort();
                return true;
            }
        }
        false
    }

    /// Marks a version as deleted.
    pub fn delete_version(
        &self,
        version_id: VersionId,
        end_ts: HlcTimestamp,
        deleted_by: TxnId,
    ) -> bool {
        let mut versions = self.versions.write();
        for version in versions.iter_mut() {
            if version.id == version_id {
                version.mark_deleted(end_ts, deleted_by);
                return true;
            }
        }
        false
    }

    /// Garbage collects old versions.
    /// Returns the number of versions collected.
    pub fn gc(&self, oldest_active_ts: &HlcTimestamp) -> usize {
        let mut versions = self.versions.write();
        let before_len = versions.len();

        let mut freed_size = 0u64;
        versions.retain(|v| {
            if v.can_gc(oldest_active_ts) {
                freed_size += v.size() as u64;
                false
            } else {
                true
            }
        });

        self.total_size
            .fetch_sub(freed_size, AtomicOrdering::Relaxed);
        before_len - versions.len()
    }

    /// Returns all versions (for debugging/testing).
    pub fn all_versions(&self) -> Vec<Version> {
        self.versions.read().clone()
    }
}

/// Version ID generator.
#[derive(Debug)]
pub struct VersionIdGenerator {
    next_id: AtomicU64,
}

impl VersionIdGenerator {
    /// Creates a new generator.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    /// Creates a generator starting at a specific ID.
    pub fn starting_at(id: u64) -> Self {
        Self {
            next_id: AtomicU64::new(id),
        }
    }

    /// Generates the next version ID.
    pub fn next(&self) -> VersionId {
        VersionId::new(self.next_id.fetch_add(1, AtomicOrdering::SeqCst))
    }

    /// Peeks at the next ID without incrementing.
    pub fn peek(&self) -> VersionId {
        VersionId::new(self.next_id.load(AtomicOrdering::SeqCst))
    }
}

impl Default for VersionIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// A store for managing version chains.
#[derive(Debug)]
pub struct VersionStore {
    /// All version chains, indexed by key.
    chains: dashmap::DashMap<Bytes, Arc<VersionChain>>,
    /// Version ID generator.
    id_gen: VersionIdGenerator,
}

impl VersionStore {
    /// Creates a new empty version store.
    pub fn new() -> Self {
        Self {
            chains: dashmap::DashMap::new(),
            id_gen: VersionIdGenerator::new(),
        }
    }

    /// Returns the number of keys in the store.
    pub fn key_count(&self) -> usize {
        self.chains.len()
    }

    /// Gets or creates a version chain for a key.
    pub fn get_or_create_chain(&self, key: Bytes) -> Arc<VersionChain> {
        self.chains
            .entry(key.clone())
            .or_insert_with(|| Arc::new(VersionChain::new(key)))
            .clone()
    }

    /// Gets a version chain if it exists.
    pub fn get_chain(&self, key: &Bytes) -> Option<Arc<VersionChain>> {
        self.chains.get(key).map(|r| r.clone())
    }

    /// Creates a new version for a key.
    pub fn create_version(
        &self,
        key: Bytes,
        begin_ts: HlcTimestamp,
        created_by: TxnId,
        data: Bytes,
    ) -> VersionId {
        let chain = self.get_or_create_chain(key);
        let id = self.id_gen.next();
        let version = Version::new(id, begin_ts, created_by, data);
        chain.add_version(version);
        id
    }

    /// Gets the visible version for a key.
    pub fn get_visible(
        &self,
        key: &Bytes,
        txn_id: TxnId,
        read_ts: &HlcTimestamp,
    ) -> Option<Version> {
        self.chains
            .get(key)
            .and_then(|chain| chain.get_visible(txn_id, read_ts))
    }

    /// Commits a version.
    pub fn commit(&self, key: &Bytes, version_id: VersionId, commit_ts: HlcTimestamp) -> bool {
        if let Some(chain) = self.chains.get(key) {
            chain.commit_version(version_id, commit_ts)
        } else {
            false
        }
    }

    /// Aborts a version.
    pub fn abort(&self, key: &Bytes, version_id: VersionId) -> bool {
        if let Some(chain) = self.chains.get(key) {
            chain.abort_version(version_id)
        } else {
            false
        }
    }

    /// Garbage collects all chains.
    /// Returns the total number of versions collected.
    pub fn gc_all(&self, oldest_active_ts: &HlcTimestamp) -> usize {
        let mut total = 0;
        for chain in self.chains.iter() {
            total += chain.gc(oldest_active_ts);
        }
        total
    }

    /// Removes empty chains.
    pub fn prune_empty_chains(&self) -> usize {
        let mut removed = 0;
        self.chains.retain(|_, chain| {
            if chain.is_empty() {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }

    /// Returns the total size of all versions.
    pub fn total_size(&self) -> u64 {
        self.chains.iter().map(|c| c.total_size()).sum()
    }
}

impl Default for VersionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts(physical: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp::new(physical, logical, 1)
    }

    #[test]
    fn test_version_id() {
        let id = VersionId::new(42);
        assert!(id.is_valid());
        assert_eq!(id.as_u64(), 42);
        assert!(!VersionId::INVALID.is_valid());
    }

    #[test]
    fn test_version_visibility() {
        let version = Version::committed(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("data"),
        );

        // Visible at ts >= begin_ts
        assert!(version.is_visible_at(&make_ts(100, 0)));
        assert!(version.is_visible_at(&make_ts(200, 0)));

        // Not visible before begin_ts
        assert!(!version.is_visible_at(&make_ts(50, 0)));
    }

    #[test]
    fn test_version_visibility_with_end_ts() {
        let mut version = Version::committed(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("data"),
        );
        version.mark_deleted(make_ts(200, 0), TxnId::new(2));

        // Visible between begin and end
        assert!(version.is_visible_at(&make_ts(100, 0)));
        assert!(version.is_visible_at(&make_ts(150, 0)));

        // Not visible at or after end_ts
        assert!(!version.is_visible_at(&make_ts(200, 0)));
        assert!(!version.is_visible_at(&make_ts(300, 0)));
    }

    #[test]
    fn test_version_own_writes() {
        let version = Version::new(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(5),
            Bytes::from("data"),
        );

        // Creator can see their own pending writes
        assert!(version.is_visible_to(TxnId::new(5), &make_ts(50, 0)));

        // Others cannot see pending writes
        assert!(!version.is_visible_to(TxnId::new(6), &make_ts(150, 0)));
    }

    #[test]
    fn test_version_chain_add_and_get() {
        let chain = VersionChain::new(Bytes::from("key1"));

        let v1 = Version::committed(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        chain.add_version(v1);

        let v2 = Version::committed(
            VersionId::new(2),
            make_ts(200, 0),
            TxnId::new(2),
            Bytes::from("v2"),
        );
        chain.add_version(v2);

        assert_eq!(chain.len(), 2);

        // Get visible version at ts=150 (should be v1)
        let visible = chain.get_visible(TxnId::new(10), &make_ts(150, 0)).unwrap();
        assert_eq!(visible.id, VersionId::new(1));
    }

    #[test]
    fn test_version_chain_latest() {
        let chain = VersionChain::new(Bytes::from("key1"));

        let v1 = Version::committed(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        chain.add_version(v1);

        let latest = chain.get_latest().unwrap();
        assert_eq!(latest.id, VersionId::new(1));
    }

    #[test]
    fn test_version_chain_commit() {
        let chain = VersionChain::new(Bytes::from("key1"));

        let v1 = Version::new(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        chain.add_version(v1);

        // Before commit, not visible to others
        assert!(chain.get_visible(TxnId::new(2), &make_ts(150, 0)).is_none());

        // Commit
        chain.commit_version(VersionId::new(1), make_ts(100, 0));

        // Now visible
        assert!(chain.get_visible(TxnId::new(2), &make_ts(150, 0)).is_some());
    }

    #[test]
    fn test_version_chain_abort() {
        let chain = VersionChain::new(Bytes::from("key1"));

        let v1 = Version::new(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        chain.add_version(v1);

        chain.abort_version(VersionId::new(1));

        // Not visible even to creator after abort
        let latest = chain.get_latest().unwrap();
        assert_eq!(latest.state, VersionState::Aborted);
    }

    #[test]
    fn test_version_chain_gc() {
        let chain = VersionChain::new(Bytes::from("key1"));

        // Add old version
        let mut v1 = Version::committed(
            VersionId::new(1),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        v1.mark_deleted(make_ts(200, 0), TxnId::new(2));
        chain.add_version(v1);

        // Add current version
        let v2 = Version::committed(
            VersionId::new(2),
            make_ts(200, 0),
            TxnId::new(2),
            Bytes::from("v2"),
        );
        chain.add_version(v2);

        assert_eq!(chain.len(), 2);

        // GC with oldest_active_ts = 250 should remove v1
        let collected = chain.gc(&make_ts(250, 0));
        assert_eq!(collected, 1);
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn test_version_store() {
        let store = VersionStore::new();

        let key = Bytes::from("key1");
        let v_id = store.create_version(
            key.clone(),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("value1"),
        );

        assert_eq!(store.key_count(), 1);
        assert!(v_id.is_valid());

        // Commit
        store.commit(&key, v_id, make_ts(100, 0));

        // Now visible
        let visible = store
            .get_visible(&key, TxnId::new(2), &make_ts(150, 0))
            .unwrap();
        assert_eq!(visible.data.as_ref(), b"value1");
    }

    #[test]
    fn test_version_id_generator() {
        let gen = VersionIdGenerator::new();

        let id1 = gen.next();
        let id2 = gen.next();
        let id3 = gen.next();

        assert_eq!(id1.as_u64(), 1);
        assert_eq!(id2.as_u64(), 2);
        assert_eq!(id3.as_u64(), 3);
    }

    #[test]
    fn test_version_store_gc() {
        let store = VersionStore::new();
        let key = Bytes::from("key1");

        // Create and commit old version
        let v1 = store.create_version(
            key.clone(),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        store.commit(&key, v1, make_ts(100, 0));

        // Create and commit new version (supersedes v1)
        if let Some(chain) = store.get_chain(&key) {
            chain.delete_version(v1, make_ts(200, 0), TxnId::new(2));
        }
        let v2 = store.create_version(
            key.clone(),
            make_ts(200, 0),
            TxnId::new(2),
            Bytes::from("v2"),
        );
        store.commit(&key, v2, make_ts(200, 0));

        // GC should remove v1
        let collected = store.gc_all(&make_ts(250, 0));
        assert_eq!(collected, 1);
    }
}
