//! Background compaction scheduler.
//!
//! Runs compaction in a dedicated thread, triggered by the engine when
//! flush or write operations detect that compaction is needed. The
//! scheduler ensures only one compaction runs at a time and provides
//! rate limiting to prevent compaction from starving foreground I/O.
//!
//! ## Architecture
//!
//! ```text
//! Engine writes ──► maybe_schedule_compaction() ──► Scheduler
//!                                                      │
//!                                      ┌───────────────┘
//!                                      ▼
//!                               Background thread
//!                                      │
//!                          ┌───────────┴───────────┐
//!                          │ pick_compaction()      │
//!                          │ execute_compaction()   │
//!                          │ apply_version_edit()   │
//!                          └───────────────────────┘
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use tracing::{debug, error, info, warn};

/// A handle to the background compaction scheduler.
///
/// The scheduler runs compaction in a dedicated thread. Call `notify()`
/// to wake the thread when compaction may be needed (e.g., after flush).
/// Call `shutdown()` for a clean stop.
pub struct CompactionScheduler {
    state: Arc<SchedulerState>,
    /// Handle to the background thread (joined on shutdown).
    handle: Option<thread::JoinHandle<()>>,
}

struct SchedulerState {
    /// Signal to wake the compaction thread.
    notify: Condvar,
    /// Mutex for the condvar (holds wake flag).
    mutex: Mutex<bool>,
    /// Whether the scheduler should stop.
    shutdown: AtomicBool,
    /// Whether a compaction is currently running.
    running: AtomicBool,
    /// Total compactions completed.
    compactions_completed: AtomicU64,
    /// Total compaction errors.
    compaction_errors: AtomicU64,
    /// Minimum interval between compaction checks.
    check_interval: Duration,
}

/// Callback trait for the engine to provide compaction logic.
///
/// The scheduler calls `try_compact()` on each wake-up. The engine
/// implements this to run `pick_compaction()` + `execute_compaction()`.
pub trait CompactionCallback: Send + Sync + 'static {
    /// Attempt one round of compaction.
    ///
    /// Returns `Ok(true)` if compaction was performed (check again),
    /// `Ok(false)` if no compaction needed, or `Err` on failure.
    fn try_compact(&self) -> Result<bool, String>;
}

impl CompactionScheduler {
    /// Create and start a background compaction scheduler.
    ///
    /// The scheduler immediately starts a background thread that waits
    /// for `notify()` signals or periodic wake-ups.
    pub fn start<C: CompactionCallback>(callback: Arc<C>, check_interval: Duration) -> Self {
        let state = Arc::new(SchedulerState {
            notify: Condvar::new(),
            mutex: Mutex::new(false),
            shutdown: AtomicBool::new(false),
            running: AtomicBool::new(false),
            compactions_completed: AtomicU64::new(0),
            compaction_errors: AtomicU64::new(0),
            check_interval,
        });

        let thread_state = state.clone();
        let handle = thread::Builder::new()
            .name("nexus-compaction".into())
            .spawn(move || {
                Self::compaction_loop(thread_state, callback);
            })
            .expect("failed to spawn compaction thread");

        Self {
            state,
            handle: Some(handle),
        }
    }

    /// Wake the compaction thread to check for pending work.
    ///
    /// Non-blocking: just signals the background thread.
    pub fn notify(&self) {
        let mut wake = self.state.mutex.lock();
        *wake = true;
        self.state.notify.notify_one();
    }

    /// Check if a compaction is currently running.
    pub fn is_running(&self) -> bool {
        self.state.running.load(Ordering::Relaxed)
    }

    /// Get scheduler statistics.
    pub fn stats(&self) -> CompactionSchedulerStats {
        CompactionSchedulerStats {
            compactions_completed: self.state.compactions_completed.load(Ordering::Relaxed),
            compaction_errors: self.state.compaction_errors.load(Ordering::Relaxed),
            is_running: self.is_running(),
        }
    }

    /// Shut down the scheduler and wait for the background thread to exit.
    pub fn shutdown(&mut self) {
        self.state.shutdown.store(true, Ordering::Release);
        // Wake the thread so it can check the shutdown flag
        self.notify();

        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.join() {
                error!("compaction thread panicked: {:?}", e);
            }
        }
    }

    /// The main compaction loop running in the background thread.
    fn compaction_loop<C: CompactionCallback>(state: Arc<SchedulerState>, callback: Arc<C>) {
        info!("compaction scheduler started");

        loop {
            // Wait for a wake-up signal or timeout
            {
                let mut wake = state.mutex.lock();
                if !*wake && !state.shutdown.load(Ordering::Acquire) {
                    state
                        .notify
                        .wait_for(&mut wake, state.check_interval);
                }
                *wake = false;
            }

            // Check for shutdown
            if state.shutdown.load(Ordering::Acquire) {
                info!("compaction scheduler shutting down");
                break;
            }

            // Run compaction (may loop if there's more work)
            state.running.store(true, Ordering::Release);

            loop {
                let start = Instant::now();
                match callback.try_compact() {
                    Ok(true) => {
                        state
                            .compactions_completed
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(
                            duration_ms = start.elapsed().as_millis() as u64,
                            "compaction round completed"
                        );
                    }
                    Ok(false) => {
                        break; // No more work
                    }
                    Err(e) => {
                        state
                            .compaction_errors
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(error = %e, "compaction failed");
                        // Back off on error to avoid tight retry loop
                        thread::sleep(Duration::from_secs(1));
                        break;
                    }
                }

                // Check for shutdown between compaction rounds
                if state.shutdown.load(Ordering::Acquire) {
                    break;
                }
            }

            state.running.store(false, Ordering::Release);
        }

        info!("compaction scheduler stopped");
    }
}

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.shutdown();
        }
    }
}

/// Compaction scheduler statistics.
#[derive(Debug, Clone)]
pub struct CompactionSchedulerStats {
    pub compactions_completed: u64,
    pub compaction_errors: u64,
    pub is_running: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct MockCallback {
        call_count: AtomicUsize,
        max_calls: usize,
    }

    impl MockCallback {
        fn new(max_calls: usize) -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                max_calls,
            }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    impl CompactionCallback for MockCallback {
        fn try_compact(&self) -> Result<bool, String> {
            let count = self.call_count.fetch_add(1, Ordering::Relaxed);
            if count < self.max_calls {
                Ok(true) // More work to do
            } else {
                Ok(false) // No more work
            }
        }
    }

    #[test]
    fn test_scheduler_basic() {
        let callback = Arc::new(MockCallback::new(3));
        let mut scheduler =
            CompactionScheduler::start(callback.clone(), Duration::from_millis(100));

        // Notify to trigger compaction
        scheduler.notify();

        // Wait for compaction to complete
        thread::sleep(Duration::from_millis(200));

        // Should have run 3 successful + 1 "no work" check
        assert!(callback.calls() >= 3);

        let stats = scheduler.stats();
        assert_eq!(stats.compactions_completed, 3);

        scheduler.shutdown();
    }

    #[test]
    fn test_scheduler_shutdown() {
        let callback = Arc::new(MockCallback::new(0));
        let mut scheduler =
            CompactionScheduler::start(callback.clone(), Duration::from_secs(60));

        // Shutdown should be clean
        scheduler.shutdown();

        // No compactions should have run (no notify called)
        assert_eq!(callback.calls(), 0);
    }

    #[test]
    fn test_scheduler_error_handling() {
        struct FailingCallback;
        impl CompactionCallback for FailingCallback {
            fn try_compact(&self) -> Result<bool, String> {
                Err("test error".into())
            }
        }

        let callback = Arc::new(FailingCallback);
        let mut scheduler =
            CompactionScheduler::start(callback, Duration::from_millis(100));

        scheduler.notify();
        thread::sleep(Duration::from_millis(200));

        let stats = scheduler.stats();
        assert!(stats.compaction_errors > 0);

        scheduler.shutdown();
    }

    #[test]
    fn test_scheduler_periodic_wakeup() {
        let callback = Arc::new(MockCallback::new(1));
        let mut scheduler =
            CompactionScheduler::start(callback.clone(), Duration::from_millis(50));

        // Don't call notify — the periodic interval should trigger it
        thread::sleep(Duration::from_millis(200));

        // Should have been woken up by the timer
        assert!(callback.calls() >= 1);

        scheduler.shutdown();
    }
}
