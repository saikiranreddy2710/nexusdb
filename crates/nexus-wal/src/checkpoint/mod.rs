//! Checkpoint management.
//!
//! Checkpoints are used to reduce recovery time by creating a consistent
//! snapshot of the database state. After a checkpoint, recovery only needs
//! to replay WAL records after the checkpoint.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use nexus_common::types::{Lsn, TxnId};

use crate::error::WalResult;
use crate::wal::Wal;

/// Checkpoint state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
    /// No checkpoint in progress.
    Idle,
    /// Checkpoint is starting.
    Starting,
    /// Checkpoint is in progress (flushing dirty pages).
    InProgress,
    /// Checkpoint is completing.
    Completing,
}

/// Information about a completed checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// Checkpoint ID.
    pub checkpoint_id: u64,
    /// LSN of the checkpoint begin record.
    pub begin_lsn: Lsn,
    /// LSN of the checkpoint end record.
    pub end_lsn: Lsn,
    /// Minimum recovery LSN.
    pub min_recovery_lsn: Lsn,
    /// Time taken to complete the checkpoint.
    pub duration: Duration,
    /// Number of dirty pages flushed.
    pub pages_flushed: u64,
}

/// Checkpoint manager.
///
/// Coordinates checkpoint operations with the WAL and buffer pool.
pub struct CheckpointManager {
    /// Current state.
    state: Mutex<CheckpointState>,
    /// Current checkpoint ID (if in progress).
    current_checkpoint_id: AtomicU64,
    /// Last completed checkpoint.
    last_checkpoint: Mutex<Option<CheckpointInfo>>,
    /// Whether a checkpoint is requested.
    checkpoint_requested: AtomicBool,
    /// Checkpoint interval.
    interval: Duration,
}

impl CheckpointManager {
    /// Creates a new checkpoint manager.
    pub fn new(interval: Duration) -> Self {
        Self {
            state: Mutex::new(CheckpointState::Idle),
            current_checkpoint_id: AtomicU64::new(0),
            last_checkpoint: Mutex::new(None),
            checkpoint_requested: AtomicBool::new(false),
            interval,
        }
    }

    /// Returns the current checkpoint state.
    pub fn state(&self) -> CheckpointState {
        *self.state.lock()
    }

    /// Returns true if a checkpoint is in progress.
    pub fn is_in_progress(&self) -> bool {
        let state = self.state.lock();
        !matches!(*state, CheckpointState::Idle)
    }

    /// Requests a checkpoint to be taken.
    pub fn request_checkpoint(&self) {
        self.checkpoint_requested.store(true, Ordering::Release);
    }

    /// Returns true if a checkpoint has been requested.
    pub fn is_requested(&self) -> bool {
        self.checkpoint_requested.load(Ordering::Acquire)
    }

    /// Clears the checkpoint request flag.
    pub fn clear_request(&self) {
        self.checkpoint_requested.store(false, Ordering::Release);
    }

    /// Returns the last completed checkpoint info.
    pub fn last_checkpoint(&self) -> Option<CheckpointInfo> {
        self.last_checkpoint.lock().clone()
    }

    /// Returns the checkpoint interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Performs a checkpoint.
    ///
    /// This is a simplified version - a full implementation would:
    /// 1. Log checkpoint begin with active transactions
    /// 2. Flush all dirty pages from buffer pool
    /// 3. Log checkpoint end with min recovery LSN
    /// 4. Optionally truncate old WAL segments
    pub fn checkpoint<F>(
        &self,
        wal: &Wal,
        get_active_txns: F,
        get_min_dirty_page_lsn: impl Fn() -> Lsn,
        flush_dirty_pages: impl Fn() -> WalResult<u64>,
    ) -> WalResult<CheckpointInfo>
    where
        F: Fn() -> Vec<TxnId>,
    {
        let start = Instant::now();

        // Update state
        {
            let mut state = self.state.lock();
            if *state != CheckpointState::Idle {
                // Already in progress
                return Err(crate::error::WalError::config_error(
                    "Checkpoint already in progress",
                ));
            }
            *state = CheckpointState::Starting;
        }

        // Generate checkpoint ID
        let checkpoint_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.current_checkpoint_id
            .store(checkpoint_id, Ordering::Release);

        // Get active transactions
        let active_txns = get_active_txns();

        // Log checkpoint begin
        let begin_lsn = wal.log_checkpoint_begin(active_txns)?;

        // Update state
        {
            *self.state.lock() = CheckpointState::InProgress;
        }

        // Flush dirty pages
        let pages_flushed = flush_dirty_pages()?;

        // Get minimum dirty page LSN (for recovery start point)
        let min_recovery_lsn = get_min_dirty_page_lsn();

        // Update state
        {
            *self.state.lock() = CheckpointState::Completing;
        }

        // Log checkpoint end
        let end_lsn = wal.log_checkpoint_end(checkpoint_id, begin_lsn, min_recovery_lsn)?;

        let duration = start.elapsed();

        // Create checkpoint info
        let info = CheckpointInfo {
            checkpoint_id,
            begin_lsn,
            end_lsn,
            min_recovery_lsn,
            duration,
            pages_flushed,
        };

        // Update last checkpoint
        {
            *self.last_checkpoint.lock() = Some(info.clone());
        }

        // Update state
        {
            *self.state.lock() = CheckpointState::Idle;
        }

        self.clear_request();

        Ok(info)
    }
}

impl std::fmt::Debug for CheckpointManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointManager")
            .field("state", &self.state())
            .field("interval", &self.interval)
            .field("is_requested", &self.is_requested())
            .finish()
    }
}

/// Simple fuzzy checkpoint without buffer pool integration.
///
/// This is useful for testing or simple scenarios where we just
/// need to mark a point in the WAL.
pub fn simple_checkpoint(wal: &Wal) -> WalResult<Lsn> {
    let begin_lsn = wal.log_checkpoint_begin(vec![])?;
    let checkpoint_id = begin_lsn.as_u64();
    let end_lsn = wal.log_checkpoint_end(checkpoint_id, begin_lsn, begin_lsn)?;
    Ok(end_lsn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SyncPolicy, WalConfig};
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_manager_creation() {
        let mgr = CheckpointManager::new(Duration::from_secs(60));
        assert_eq!(mgr.state(), CheckpointState::Idle);
        assert!(!mgr.is_in_progress());
        assert!(mgr.last_checkpoint().is_none());
    }

    #[test]
    fn test_checkpoint_request() {
        let mgr = CheckpointManager::new(Duration::from_secs(60));
        assert!(!mgr.is_requested());

        mgr.request_checkpoint();
        assert!(mgr.is_requested());

        mgr.clear_request();
        assert!(!mgr.is_requested());
    }

    #[test]
    fn test_simple_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let config = WalConfig::new(tmp.path())
            .with_segment_size(1024 * 1024)
            .with_max_record_size(256 * 1024)
            .with_sync_policy(SyncPolicy::EveryWrite)
            .with_preallocate_segments(false);

        let wal = Wal::new(config).unwrap();
        let end_lsn = simple_checkpoint(&wal).unwrap();

        assert!(end_lsn.is_valid());
        assert_eq!(wal.last_checkpoint_lsn(), end_lsn);
    }

    #[test]
    fn test_checkpoint_with_manager() {
        let tmp = TempDir::new().unwrap();
        let config = WalConfig::new(tmp.path())
            .with_segment_size(1024 * 1024)
            .with_max_record_size(256 * 1024)
            .with_sync_policy(SyncPolicy::EveryWrite)
            .with_preallocate_segments(false);

        let wal = Wal::new(config).unwrap();
        let mgr = CheckpointManager::new(Duration::from_secs(60));

        let info = mgr
            .checkpoint(
                &wal,
                || vec![],      // no active transactions
                || Lsn::new(1), // min dirty page LSN
                || Ok(0),       // no pages flushed
            )
            .unwrap();

        assert!(info.begin_lsn.is_valid());
        assert!(info.end_lsn > info.begin_lsn);
        assert_eq!(mgr.state(), CheckpointState::Idle);
        assert!(mgr.last_checkpoint().is_some());
    }
}
