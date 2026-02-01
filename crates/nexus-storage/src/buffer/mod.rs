//! Buffer Pool Manager for NexusDB.
//!
//! The buffer pool manages a fixed-size pool of in-memory page frames,
//! providing efficient caching of disk pages with the following features:
//!
//! - **Page Caching**: Keep frequently accessed pages in memory
//! - **Pin/Unpin**: Reference counting for safe concurrent access
//! - **Dirty Tracking**: Track modified pages for write-back
//! - **Eviction Policy**: Clock algorithm for page replacement
//! - **Async I/O**: Integration with async file operations
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        BufferPool                                │
//! │  ┌─────────────────────────────────────────────────────────────┐ │
//! │  │                    Page Table                                │ │
//! │  │              HashMap<PageId, FrameId>                        │ │
//! │  └─────────────────────────────────────────────────────────────┘ │
//! │                              │                                   │
//! │                              ▼                                   │
//! │  ┌─────────────────────────────────────────────────────────────┐ │
//! │  │                    Frame Array                               │ │
//! │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐       │ │
//! │  │  │ Frame 0 │ │ Frame 1 │ │ Frame 2 │ ... │ Frame N │       │ │
//! │  │  │ ─────── │ │ ─────── │ │ ─────── │     │ ─────── │       │ │
//! │  │  │ page_id │ │ page_id │ │ page_id │     │ page_id │       │ │
//! │  │  │ data[]  │ │ data[]  │ │ data[]  │     │ data[]  │       │ │
//! │  │  │ dirty   │ │ dirty   │ │ dirty   │     │ dirty   │       │ │
//! │  │  │ pin_cnt │ │ pin_cnt │ │ pin_cnt │     │ pin_cnt │       │ │
//! │  │  └─────────┘ └─────────┘ └─────────┘     └─────────┘       │ │
//! │  └─────────────────────────────────────────────────────────────┘ │
//! │                              │                                   │
//! │                              ▼                                   │
//! │  ┌─────────────────────────────────────────────────────────────┐ │
//! │  │                 Clock Replacer                               │ │
//! │  │           (tracks eviction candidates)                       │ │
//! │  └─────────────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use nexus_storage::buffer::{BufferPool, BufferPoolConfig};
//! use nexus_common::types::PageId;
//! use std::sync::Arc;
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a buffer pool with 1000 frames
//!     let config = BufferPoolConfig::new(1000);
//!     let pool = BufferPool::new(config)?;
//!     
//!     // Fetch a page (reads from disk if not cached)
//!     let page_guard = pool.fetch_page(PageId::new(42)).await?;
//!     
//!     // Read page data
//!     let data = page_guard.data();
//!     
//!     // Page is automatically unpinned when guard is dropped
//!     Ok(())
//! }
//! ```

mod config;
mod error;
mod eviction;
mod frame;
mod latch;
mod pool;

pub use config::BufferPoolConfig;
pub use error::{BufferError, BufferResult};
pub use eviction::ClockReplacer;
pub use frame::{BufferFrame, FrameId};
pub use latch::{PageReadGuard, PageWriteGuard};
pub use pool::BufferPool;

/// Frame ID type alias for clarity.
pub type FrameIndex = usize;

/// Statistics for buffer pool monitoring.
#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    /// Total number of page fetches.
    pub fetches: u64,
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses (required disk read).
    pub misses: u64,
    /// Number of pages evicted.
    pub evictions: u64,
    /// Number of dirty pages flushed.
    pub flushes: u64,
    /// Current number of pinned frames.
    pub pinned_frames: usize,
    /// Current number of dirty frames.
    pub dirty_frames: usize,
}

impl BufferPoolStats {
    /// Returns the cache hit ratio (0.0 to 1.0).
    pub fn hit_ratio(&self) -> f64 {
        if self.fetches == 0 {
            0.0
        } else {
            self.hits as f64 / self.fetches as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_hit_ratio() {
        let mut stats = BufferPoolStats::default();
        assert_eq!(stats.hit_ratio(), 0.0);

        stats.fetches = 100;
        stats.hits = 80;
        assert!((stats.hit_ratio() - 0.8).abs() < f64::EPSILON);
    }
}
