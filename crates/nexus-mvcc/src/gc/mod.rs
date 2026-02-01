//! Epoch-based garbage collection for MVCC.
//!
//! This module implements epoch-based reclamation (EBR) for safely collecting
//! old versions that are no longer visible to any active transaction.
//!
//! # Overview
//!
//! EBR works by:
//! 1. Dividing time into epochs (monotonically increasing)
//! 2. Each reader "pins" an epoch when starting a read
//! 3. When all readers leave an epoch, it becomes safe to reclaim
//! 4. Garbage is queued with the epoch when it was retired
//! 5. Background GC collects garbage from safe epochs
//!
//! # Key Components
//!
//! - [`Epoch`]: A monotonically increasing epoch number
//! - [`EpochGuard`]: RAII guard that pins an epoch during read operations
//! - [`EpochManager`]: Tracks active epochs and determines safe-to-reclaim
//! - [`GarbageCollector`]: Background collector that reclaims old versions
//!
//! # Safety
//!
//! The EBR scheme ensures that:
//! - No version is reclaimed while a transaction might still need it
//! - Garbage is collected in a timely manner to prevent memory bloat
//! - Lock-free operations where possible for high concurrency

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::hlc::HlcTimestamp;
use crate::version::VersionStore;

/// A monotonically increasing epoch number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(u64);

impl Epoch {
    /// The zero epoch.
    pub const ZERO: Self = Self(0);

    /// Creates a new epoch.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next epoch.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the previous epoch (saturating at 0).
    pub const fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch({})", self.0)
    }
}

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Epoch> for u64 {
    fn from(epoch: Epoch) -> Self {
        epoch.0
    }
}

/// Information about an active reader in an epoch.
#[derive(Debug, Clone)]
struct ReaderInfo {
    /// Unique reader ID.
    id: u64,
    /// Epoch when reader started.
    epoch: Epoch,
    /// When the reader started.
    started_at: Instant,
}

/// A guard that pins an epoch while held.
///
/// When dropped, the reader is unregistered from the epoch,
/// potentially allowing that epoch to be reclaimed.
pub struct EpochGuard {
    /// Reference to the epoch manager.
    manager: Arc<EpochManager>,
    /// The epoch that is pinned.
    epoch: Epoch,
    /// Unique reader ID.
    reader_id: u64,
}

impl EpochGuard {
    /// Returns the pinned epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl Drop for EpochGuard {
    fn drop(&mut self) {
        self.manager.unpin(self.reader_id);
    }
}

impl fmt::Debug for EpochGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EpochGuard")
            .field("epoch", &self.epoch)
            .field("reader_id", &self.reader_id)
            .finish()
    }
}

/// Configuration for the epoch manager.
#[derive(Debug, Clone)]
pub struct EpochConfig {
    /// How often to advance the epoch (in milliseconds).
    pub epoch_interval_ms: u64,
    /// Maximum number of epochs to lag behind before forcing collection.
    pub max_epoch_lag: u64,
    /// How long to wait for readers before timing out.
    pub reader_timeout: Duration,
}

impl Default for EpochConfig {
    fn default() -> Self {
        Self {
            epoch_interval_ms: 10,
            max_epoch_lag: 3,
            reader_timeout: Duration::from_secs(60),
        }
    }
}

/// Manages epochs and tracks active readers.
pub struct EpochManager {
    /// Current epoch.
    current_epoch: AtomicU64,
    /// Next reader ID to assign.
    next_reader_id: AtomicU64,
    /// Active readers.
    readers: RwLock<Vec<ReaderInfo>>,
    /// Minimum safe epoch (all readers have left epochs before this).
    safe_epoch: AtomicU64,
    /// Configuration.
    config: EpochConfig,
    /// Last time epoch was advanced.
    last_advance: Mutex<Instant>,
}

impl EpochManager {
    /// Creates a new epoch manager with default configuration.
    pub fn new() -> Self {
        Self::with_config(EpochConfig::default())
    }

    /// Creates an epoch manager with custom configuration.
    pub fn with_config(config: EpochConfig) -> Self {
        Self {
            current_epoch: AtomicU64::new(0),
            next_reader_id: AtomicU64::new(1),
            readers: RwLock::new(Vec::new()),
            safe_epoch: AtomicU64::new(0),
            config,
            last_advance: Mutex::new(Instant::now()),
        }
    }

    /// Returns the current epoch.
    pub fn current_epoch(&self) -> Epoch {
        Epoch::new(self.current_epoch.load(AtomicOrdering::Acquire))
    }

    /// Returns the minimum safe epoch (epochs before this can be reclaimed).
    pub fn safe_epoch(&self) -> Epoch {
        Epoch::new(self.safe_epoch.load(AtomicOrdering::Acquire))
    }

    /// Advances to the next epoch.
    ///
    /// This should be called periodically to make progress on GC.
    pub fn advance_epoch(&self) -> Epoch {
        let new_epoch = self.current_epoch.fetch_add(1, AtomicOrdering::AcqRel) + 1;
        *self.last_advance.lock() = Instant::now();
        self.update_safe_epoch();
        Epoch::new(new_epoch)
    }

    /// Tries to advance epoch if enough time has passed.
    pub fn try_advance_epoch(&self) -> Option<Epoch> {
        let elapsed = self.last_advance.lock().elapsed();
        if elapsed.as_millis() >= self.config.epoch_interval_ms as u128 {
            Some(self.advance_epoch())
        } else {
            None
        }
    }

    /// Pins the current epoch and returns a guard.
    ///
    /// While the guard is held, the pinned epoch (and all epochs after it)
    /// cannot be reclaimed.
    pub fn pin(self: &Arc<Self>) -> EpochGuard {
        let epoch = self.current_epoch();
        let reader_id = self.next_reader_id.fetch_add(1, AtomicOrdering::SeqCst);

        {
            let mut readers = self.readers.write();
            readers.push(ReaderInfo {
                id: reader_id,
                epoch,
                started_at: Instant::now(),
            });
        }

        EpochGuard {
            manager: Arc::clone(self),
            epoch,
            reader_id,
        }
    }

    /// Unpins a reader from its epoch.
    fn unpin(&self, reader_id: u64) {
        {
            let mut readers = self.readers.write();
            readers.retain(|r| r.id != reader_id);
        }
        self.update_safe_epoch();
    }

    /// Updates the safe epoch based on active readers.
    fn update_safe_epoch(&self) {
        let readers = self.readers.read();
        let current = self.current_epoch();

        let min_pinned = readers.iter().map(|r| r.epoch).min().unwrap_or(current);

        // Safe epoch is one before the minimum pinned epoch
        let safe = if min_pinned.as_u64() > 0 {
            min_pinned.prev()
        } else {
            Epoch::ZERO
        };

        self.safe_epoch
            .store(safe.as_u64(), AtomicOrdering::Release);
    }

    /// Returns the number of active readers.
    pub fn reader_count(&self) -> usize {
        self.readers.read().len()
    }

    /// Checks if an epoch is safe to reclaim.
    pub fn is_safe_to_reclaim(&self, epoch: Epoch) -> bool {
        epoch <= self.safe_epoch()
    }

    /// Cleans up timed-out readers.
    ///
    /// Returns the number of readers cleaned up.
    pub fn cleanup_stale_readers(&self) -> usize {
        let mut readers = self.readers.write();
        let before_len = readers.len();
        let timeout = self.config.reader_timeout;

        readers.retain(|r| r.started_at.elapsed() < timeout);

        let removed = before_len - readers.len();
        if removed > 0 {
            drop(readers);
            self.update_safe_epoch();
        }
        removed
    }

    /// Returns statistics about the epoch manager.
    pub fn stats(&self) -> EpochStats {
        let readers = self.readers.read();
        EpochStats {
            current_epoch: self.current_epoch(),
            safe_epoch: self.safe_epoch(),
            reader_count: readers.len(),
            oldest_reader_epoch: readers.iter().map(|r| r.epoch).min(),
        }
    }
}

impl Default for EpochManager {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EpochManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EpochManager")
            .field("current_epoch", &self.current_epoch())
            .field("safe_epoch", &self.safe_epoch())
            .field("reader_count", &self.reader_count())
            .finish()
    }
}

/// Statistics about the epoch manager.
#[derive(Debug, Clone)]
pub struct EpochStats {
    /// Current epoch.
    pub current_epoch: Epoch,
    /// Safe epoch for reclamation.
    pub safe_epoch: Epoch,
    /// Number of active readers.
    pub reader_count: usize,
    /// Oldest pinned epoch (if any).
    pub oldest_reader_epoch: Option<Epoch>,
}

/// A garbage entry waiting to be collected.
#[derive(Debug)]
struct GarbageEntry {
    /// Epoch when this garbage was retired.
    retired_epoch: Epoch,
    /// HLC timestamp of the oldest transaction that might need this.
    oldest_visible_ts: HlcTimestamp,
    /// Number of versions to potentially collect.
    version_count: usize,
}

/// Configuration for the garbage collector.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Minimum interval between GC runs.
    pub gc_interval: Duration,
    /// Maximum number of versions to collect per run.
    pub max_versions_per_run: usize,
    /// Threshold of garbage entries before forcing collection.
    pub force_gc_threshold: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            gc_interval: Duration::from_millis(100),
            max_versions_per_run: 10_000,
            force_gc_threshold: 1_000,
        }
    }
}

/// Garbage collector for MVCC versions.
///
/// This collector works in conjunction with the epoch manager to safely
/// reclaim old versions that are no longer visible to any transaction.
pub struct GarbageCollector {
    /// Epoch manager.
    epoch_manager: Arc<EpochManager>,
    /// Version store to collect from.
    version_store: Arc<VersionStore>,
    /// Queue of garbage entries.
    garbage_queue: Mutex<VecDeque<GarbageEntry>>,
    /// Configuration.
    config: GcConfig,
    /// Last GC run time.
    last_gc: Mutex<Instant>,
    /// Statistics.
    stats: GcStats,
}

/// Statistics about garbage collection.
#[derive(Debug, Default)]
pub struct GcStats {
    /// Total number of GC runs.
    pub runs: AtomicU64,
    /// Total versions collected.
    pub versions_collected: AtomicU64,
    /// Total chains pruned.
    pub chains_pruned: AtomicU64,
}

impl GcStats {
    /// Creates new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a GC run.
    pub fn record_run(&self, versions: usize, chains: usize) {
        self.runs.fetch_add(1, AtomicOrdering::Relaxed);
        self.versions_collected
            .fetch_add(versions as u64, AtomicOrdering::Relaxed);
        self.chains_pruned
            .fetch_add(chains as u64, AtomicOrdering::Relaxed);
    }

    /// Returns the total number of runs.
    pub fn total_runs(&self) -> u64 {
        self.runs.load(AtomicOrdering::Relaxed)
    }

    /// Returns the total versions collected.
    pub fn total_versions_collected(&self) -> u64 {
        self.versions_collected.load(AtomicOrdering::Relaxed)
    }

    /// Returns the total chains pruned.
    pub fn total_chains_pruned(&self) -> u64 {
        self.chains_pruned.load(AtomicOrdering::Relaxed)
    }
}

impl GarbageCollector {
    /// Creates a new garbage collector.
    pub fn new(epoch_manager: Arc<EpochManager>, version_store: Arc<VersionStore>) -> Self {
        Self::with_config(epoch_manager, version_store, GcConfig::default())
    }

    /// Creates a garbage collector with custom configuration.
    pub fn with_config(
        epoch_manager: Arc<EpochManager>,
        version_store: Arc<VersionStore>,
        config: GcConfig,
    ) -> Self {
        Self {
            epoch_manager,
            version_store,
            garbage_queue: Mutex::new(VecDeque::new()),
            config,
            last_gc: Mutex::new(Instant::now()),
            stats: GcStats::new(),
        }
    }

    /// Retires garbage for collection.
    ///
    /// The garbage won't be collected until it's safe according to the epoch manager.
    pub fn retire(&self, oldest_visible_ts: HlcTimestamp, version_count: usize) {
        let entry = GarbageEntry {
            retired_epoch: self.epoch_manager.current_epoch(),
            oldest_visible_ts,
            version_count,
        };

        let mut queue = self.garbage_queue.lock();
        queue.push_back(entry);

        // Force GC if queue is too large
        if queue.len() >= self.config.force_gc_threshold {
            drop(queue);
            self.try_collect();
        }
    }

    /// Tries to run garbage collection if it's time.
    pub fn try_collect(&self) -> GcResult {
        let elapsed = self.last_gc.lock().elapsed();
        if elapsed < self.config.gc_interval {
            return GcResult {
                versions_collected: 0,
                chains_pruned: 0,
                skipped: true,
            };
        }

        self.collect()
    }

    /// Runs garbage collection.
    pub fn collect(&self) -> GcResult {
        *self.last_gc.lock() = Instant::now();

        let safe_epoch = self.epoch_manager.safe_epoch();
        let mut queue = self.garbage_queue.lock();

        // Find oldest safe timestamp from garbage entries
        let mut oldest_safe_ts = HlcTimestamp::MAX;
        let mut versions_to_collect = 0;

        // Process entries that are safe to collect
        while let Some(entry) = queue.front() {
            if entry.retired_epoch <= safe_epoch {
                if entry.oldest_visible_ts < oldest_safe_ts {
                    oldest_safe_ts = entry.oldest_visible_ts;
                }
                versions_to_collect += entry.version_count;
                queue.pop_front();

                if versions_to_collect >= self.config.max_versions_per_run {
                    break;
                }
            } else {
                break; // Queue is ordered, so no more safe entries
            }
        }

        drop(queue);

        // If we have something to collect
        if oldest_safe_ts != HlcTimestamp::MAX {
            let versions_collected = self.version_store.gc_all(&oldest_safe_ts);
            let chains_pruned = self.version_store.prune_empty_chains();

            self.stats.record_run(versions_collected, chains_pruned);

            GcResult {
                versions_collected,
                chains_pruned,
                skipped: false,
            }
        } else {
            GcResult {
                versions_collected: 0,
                chains_pruned: 0,
                skipped: false,
            }
        }
    }

    /// Forces immediate garbage collection, ignoring timing constraints.
    pub fn force_collect(&self, oldest_active_ts: &HlcTimestamp) -> GcResult {
        *self.last_gc.lock() = Instant::now();

        // Clear the garbage queue
        self.garbage_queue.lock().clear();

        // Collect all versions older than the given timestamp
        let versions_collected = self.version_store.gc_all(oldest_active_ts);
        let chains_pruned = self.version_store.prune_empty_chains();

        self.stats.record_run(versions_collected, chains_pruned);

        GcResult {
            versions_collected,
            chains_pruned,
            skipped: false,
        }
    }

    /// Returns GC statistics.
    pub fn stats(&self) -> &GcStats {
        &self.stats
    }

    /// Returns the number of pending garbage entries.
    pub fn pending_count(&self) -> usize {
        self.garbage_queue.lock().len()
    }
}

impl fmt::Debug for GarbageCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GarbageCollector")
            .field("pending_count", &self.pending_count())
            .field("total_runs", &self.stats.total_runs())
            .field("total_collected", &self.stats.total_versions_collected())
            .finish()
    }
}

/// Result of a garbage collection run.
#[derive(Debug, Clone, Copy, Default)]
pub struct GcResult {
    /// Number of versions collected.
    pub versions_collected: usize,
    /// Number of empty chains pruned.
    pub chains_pruned: usize,
    /// Whether GC was skipped (not enough time elapsed).
    pub skipped: bool,
}

impl GcResult {
    /// Returns true if any work was done.
    pub fn did_work(&self) -> bool {
        !self.skipped && (self.versions_collected > 0 || self.chains_pruned > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use nexus_common::types::TxnId;

    fn make_ts(physical: u64, logical: u32) -> HlcTimestamp {
        HlcTimestamp::new(physical, logical, 1)
    }

    #[test]
    fn test_epoch_basics() {
        let epoch = Epoch::new(42);
        assert_eq!(epoch.as_u64(), 42);
        assert_eq!(epoch.next().as_u64(), 43);
        assert_eq!(epoch.prev().as_u64(), 41);

        assert_eq!(Epoch::ZERO.prev(), Epoch::ZERO); // Saturating
    }

    #[test]
    fn test_epoch_manager_advance() {
        let manager = EpochManager::new();

        assert_eq!(manager.current_epoch(), Epoch::new(0));

        let new_epoch = manager.advance_epoch();
        assert_eq!(new_epoch, Epoch::new(1));
        assert_eq!(manager.current_epoch(), Epoch::new(1));

        manager.advance_epoch();
        assert_eq!(manager.current_epoch(), Epoch::new(2));
    }

    #[test]
    fn test_epoch_guard_pin_unpin() {
        let manager = Arc::new(EpochManager::new());

        assert_eq!(manager.reader_count(), 0);

        let guard1 = manager.pin();
        assert_eq!(guard1.epoch(), Epoch::new(0));
        assert_eq!(manager.reader_count(), 1);

        manager.advance_epoch();

        let guard2 = manager.pin();
        assert_eq!(guard2.epoch(), Epoch::new(1));
        assert_eq!(manager.reader_count(), 2);

        drop(guard1);
        assert_eq!(manager.reader_count(), 1);

        drop(guard2);
        assert_eq!(manager.reader_count(), 0);
    }

    #[test]
    fn test_safe_epoch() {
        let manager = Arc::new(EpochManager::new());

        // No readers, safe epoch follows current
        assert_eq!(manager.safe_epoch(), Epoch::new(0));

        manager.advance_epoch();
        manager.advance_epoch();
        // Still Epoch::ZERO because we haven't updated safe_epoch without readers
        // Actually, when there are no readers, min_pinned is current, so safe = current - 1

        let guard = manager.pin(); // Pins epoch 2
        assert_eq!(guard.epoch(), Epoch::new(2));

        manager.advance_epoch();
        manager.advance_epoch();
        // Current is now 4, but guard pins 2

        // Safe epoch should be 1 (one before the pinned epoch 2)
        assert_eq!(manager.safe_epoch(), Epoch::new(1));

        // Epoch 0 and 1 are safe to reclaim
        assert!(manager.is_safe_to_reclaim(Epoch::new(0)));
        assert!(manager.is_safe_to_reclaim(Epoch::new(1)));
        assert!(!manager.is_safe_to_reclaim(Epoch::new(2)));
        assert!(!manager.is_safe_to_reclaim(Epoch::new(3)));

        drop(guard);

        // Now safe epoch should advance
        assert_eq!(manager.safe_epoch(), Epoch::new(3));
    }

    #[test]
    fn test_epoch_manager_stats() {
        let manager = Arc::new(EpochManager::new());

        let _guard = manager.pin();
        manager.advance_epoch();

        let stats = manager.stats();
        assert_eq!(stats.current_epoch, Epoch::new(1));
        assert_eq!(stats.reader_count, 1);
        assert_eq!(stats.oldest_reader_epoch, Some(Epoch::new(0)));
    }

    #[test]
    fn test_garbage_collector_basic() {
        let epoch_manager = Arc::new(EpochManager::new());
        let version_store = Arc::new(VersionStore::new());

        let gc = GarbageCollector::new(Arc::clone(&epoch_manager), Arc::clone(&version_store));

        // Create some versions
        let key = Bytes::from("key1");
        let v1 = version_store.create_version(
            key.clone(),
            make_ts(100, 0),
            TxnId::new(1),
            Bytes::from("v1"),
        );
        version_store.commit(&key, v1, make_ts(100, 0));

        // Mark old version as superseded
        if let Some(chain) = version_store.get_chain(&key) {
            chain.delete_version(v1, make_ts(200, 0), TxnId::new(2));
        }

        let v2 = version_store.create_version(
            key.clone(),
            make_ts(200, 0),
            TxnId::new(2),
            Bytes::from("v2"),
        );
        version_store.commit(&key, v2, make_ts(200, 0));

        // Retire garbage
        gc.retire(make_ts(150, 0), 1);

        // Advance epoch to make garbage safe
        epoch_manager.advance_epoch();
        epoch_manager.advance_epoch();

        // Force collect
        let result = gc.force_collect(&make_ts(250, 0));
        assert!(!result.skipped);
        assert_eq!(result.versions_collected, 1);
    }

    #[test]
    fn test_gc_stats() {
        let stats = GcStats::new();

        assert_eq!(stats.total_runs(), 0);
        assert_eq!(stats.total_versions_collected(), 0);

        stats.record_run(10, 2);
        assert_eq!(stats.total_runs(), 1);
        assert_eq!(stats.total_versions_collected(), 10);
        assert_eq!(stats.total_chains_pruned(), 2);

        stats.record_run(5, 1);
        assert_eq!(stats.total_runs(), 2);
        assert_eq!(stats.total_versions_collected(), 15);
        assert_eq!(stats.total_chains_pruned(), 3);
    }

    #[test]
    fn test_gc_result() {
        let skipped = GcResult {
            versions_collected: 0,
            chains_pruned: 0,
            skipped: true,
        };
        assert!(!skipped.did_work());

        let no_work = GcResult {
            versions_collected: 0,
            chains_pruned: 0,
            skipped: false,
        };
        assert!(!no_work.did_work());

        let did_work = GcResult {
            versions_collected: 5,
            chains_pruned: 0,
            skipped: false,
        };
        assert!(did_work.did_work());
    }

    #[test]
    fn test_epoch_config_default() {
        let config = EpochConfig::default();
        assert_eq!(config.epoch_interval_ms, 10);
        assert_eq!(config.max_epoch_lag, 3);
        assert_eq!(config.reader_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_gc_config_default() {
        let config = GcConfig::default();
        assert_eq!(config.gc_interval, Duration::from_millis(100));
        assert_eq!(config.max_versions_per_run, 10_000);
        assert_eq!(config.force_gc_threshold, 1_000);
    }

    #[test]
    fn test_epoch_ordering() {
        assert!(Epoch::new(0) < Epoch::new(1));
        assert!(Epoch::new(1) < Epoch::new(2));
        assert_eq!(Epoch::new(5), Epoch::new(5));
    }

    #[test]
    fn test_epoch_conversions() {
        let epoch = Epoch::new(42);
        let val: u64 = epoch.into();
        assert_eq!(val, 42);

        let epoch2: Epoch = 100u64.into();
        assert_eq!(epoch2.as_u64(), 100);
    }

    #[test]
    fn test_multiple_readers_safe_epoch() {
        let manager = Arc::new(EpochManager::new());

        // Advance to epoch 5
        for _ in 0..5 {
            manager.advance_epoch();
        }

        // Pin at different epochs
        let guard1 = manager.pin(); // Epoch 5
        manager.advance_epoch();
        let guard2 = manager.pin(); // Epoch 6
        manager.advance_epoch();
        let guard3 = manager.pin(); // Epoch 7

        // Safe epoch should be min(5, 6, 7) - 1 = 4
        assert_eq!(manager.safe_epoch(), Epoch::new(4));

        // Drop the oldest reader
        drop(guard1);

        // Safe epoch should now be min(6, 7) - 1 = 5
        assert_eq!(manager.safe_epoch(), Epoch::new(5));

        drop(guard2);
        drop(guard3);
    }

    #[test]
    fn test_gc_pending_count() {
        let epoch_manager = Arc::new(EpochManager::new());
        let version_store = Arc::new(VersionStore::new());
        let gc = GarbageCollector::new(epoch_manager, version_store);

        assert_eq!(gc.pending_count(), 0);

        gc.retire(make_ts(100, 0), 5);
        assert_eq!(gc.pending_count(), 1);

        gc.retire(make_ts(200, 0), 3);
        assert_eq!(gc.pending_count(), 2);
    }
}
