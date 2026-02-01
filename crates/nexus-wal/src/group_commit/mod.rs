//! Group commit implementation.
//!
//! Group commit batches multiple fsync calls together for better throughput.
//! Instead of syncing after each write, we wait briefly to accumulate writes
//! and then sync them all at once.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use nexus_common::types::Lsn;

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::writer::WalWriter;

/// State for a pending sync request.
#[derive(Debug)]
struct PendingSync {
    /// The LSN that needs to be synced.
    lsn: Lsn,
    /// Whether the sync has completed.
    completed: bool,
    /// Any error that occurred.
    error: Option<String>,
}

/// Group commit manager.
///
/// Batches multiple sync requests and performs them together.
pub struct GroupCommitManager {
    /// Configuration.
    config: Arc<WalConfig>,
    /// WAL writer.
    writer: Arc<WalWriter>,
    /// Pending sync requests.
    pending: Mutex<Vec<PendingSync>>,
    /// Condition variable for waiting on sync completion.
    sync_complete: Condvar,
    /// Last synced LSN.
    synced_lsn: AtomicU64,
    /// Whether shutdown has been requested.
    shutdown: AtomicBool,
    /// Number of pending requests.
    pending_count: AtomicU64,
}

impl GroupCommitManager {
    /// Creates a new group commit manager.
    pub fn new(config: Arc<WalConfig>, writer: Arc<WalWriter>) -> Self {
        Self {
            config,
            writer,
            pending: Mutex::new(Vec::new()),
            sync_complete: Condvar::new(),
            synced_lsn: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            pending_count: AtomicU64::new(0),
        }
    }

    /// Requests a sync for the given LSN.
    ///
    /// This will either:
    /// 1. Return immediately if the LSN is already synced
    /// 2. Wait for an ongoing sync that will include this LSN
    /// 3. Trigger a new sync if needed
    pub fn request_sync(&self, lsn: Lsn) -> WalResult<()> {
        // Check if already synced
        if lsn.as_u64() <= self.synced_lsn.load(Ordering::Acquire) {
            return Ok(());
        }

        // Add to pending
        let pending_sync = PendingSync {
            lsn,
            completed: false,
            error: None,
        };

        {
            let mut pending = self.pending.lock();
            pending.push(pending_sync);
            self.pending_count.fetch_add(1, Ordering::Release);
        }

        // Check if we should trigger a sync
        let should_sync = self.pending_count.load(Ordering::Acquire)
            >= self.config.group_commit_batch_size as u64;

        if should_sync {
            self.do_sync()?;
        } else {
            // Wait for sync to complete or timeout
            let mut pending = self.pending.lock();
            let timeout = self.config.group_commit_timeout;
            let result = self.sync_complete.wait_for(&mut pending, timeout);

            if result.timed_out() {
                // Timeout - trigger sync ourselves
                drop(pending);
                self.do_sync()?;
            }
        }

        // Check result
        let synced = self.synced_lsn.load(Ordering::Acquire);
        if lsn.as_u64() <= synced {
            Ok(())
        } else {
            Err(WalError::GroupCommitTimeout {
                timeout_ms: self.config.group_commit_timeout.as_millis() as u64,
            })
        }
    }

    /// Performs a sync, completing all pending requests.
    pub fn do_sync(&self) -> WalResult<()> {
        // Get all pending requests
        let mut pending = self.pending.lock();
        if pending.is_empty() {
            return Ok(());
        }

        // Find the max LSN we need to sync
        let max_lsn = pending.iter().map(|p| p.lsn.as_u64()).max().unwrap_or(0);

        // Perform the sync
        drop(pending); // Release lock before I/O

        let result = self.writer.sync();

        // Update state
        let mut pending = self.pending.lock();

        match &result {
            Ok(()) => {
                self.synced_lsn.fetch_max(max_lsn, Ordering::AcqRel);
                for p in pending.iter_mut() {
                    p.completed = true;
                }
            }
            Err(e) => {
                for p in pending.iter_mut() {
                    p.error = Some(e.to_string());
                }
            }
        }

        // Clear completed
        pending.retain(|p| !p.completed && p.error.is_none());
        self.pending_count
            .store(pending.len() as u64, Ordering::Release);

        // Notify waiters
        self.sync_complete.notify_all();

        result
    }

    /// Forces an immediate flush of all pending writes.
    pub fn flush(&self) -> WalResult<()> {
        self.do_sync()
    }

    /// Returns the last synced LSN.
    pub fn synced_lsn(&self) -> Lsn {
        Lsn::new(self.synced_lsn.load(Ordering::Acquire))
    }

    /// Returns the number of pending sync requests.
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Acquire)
    }

    /// Shuts down the group commit manager.
    pub fn shutdown(&self) -> WalResult<()> {
        self.shutdown.store(true, Ordering::Release);
        self.do_sync()?;
        Ok(())
    }

    /// Returns true if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for GroupCommitManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCommitManager")
            .field("synced_lsn", &self.synced_lsn())
            .field("pending_count", &self.pending_count())
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> Arc<WalConfig> {
        Arc::new(
            WalConfig::new(dir)
                .with_segment_size(1024 * 1024)
                .with_group_commit_batch_size(10)
                .with_group_commit_timeout(Duration::from_millis(100))
                .with_preallocate_segments(false),
        )
    }

    #[test]
    fn test_group_commit_creation() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = Arc::new(WalWriter::new(Arc::clone(&config)));
        let gc = GroupCommitManager::new(config, writer);

        assert_eq!(gc.synced_lsn(), Lsn::new(0));
        assert_eq!(gc.pending_count(), 0);
        assert!(!gc.is_shutdown());
    }

    #[test]
    fn test_immediate_flush() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = Arc::new(WalWriter::new(Arc::clone(&config)));

        // Write something first
        use nexus_common::types::TxnId;
        writer
            .log_commit(TxnId::new(1), Lsn::INVALID, 12345)
            .unwrap();

        let gc = GroupCommitManager::new(config, writer);
        gc.flush().unwrap();
    }

    #[test]
    fn test_shutdown() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let writer = Arc::new(WalWriter::new(Arc::clone(&config)));
        let gc = GroupCommitManager::new(config, writer);

        gc.shutdown().unwrap();
        assert!(gc.is_shutdown());
    }
}
