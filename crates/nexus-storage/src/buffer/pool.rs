//! Buffer pool implementation.
//!
//! The buffer pool manages a fixed-size cache of pages in memory,
//! handling page fetching, eviction, and flushing.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use nexus_common::types::PageId;
use parking_lot::RwLock;

use super::config::BufferPoolConfig;
use super::error::{BufferError, BufferResult};
use super::eviction::ClockReplacer;
use super::frame::{BufferFrame, FrameId};
use super::latch::{PageReadGuard, PageWriteGuard};
use super::BufferPoolStats;
use crate::file::{FileHandle, FileManager, OpenOptions, StandardFile};

/// The buffer pool manages page caching for the database.
///
/// It provides:
/// - Automatic page caching with LRU-like eviction (clock algorithm)
/// - Thread-safe concurrent access with fine-grained locking
/// - Dirty page tracking and write-back
/// - Integration with async I/O
pub struct BufferPool {
    /// Configuration.
    config: BufferPoolConfig,
    /// Array of buffer frames.
    frames: Vec<Arc<BufferFrame>>,
    /// Page table: maps PageId -> FrameId.
    page_table: RwLock<HashMap<PageId, FrameId>>,
    /// Clock replacer for eviction.
    replacer: ClockReplacer,
    /// File manager for I/O.
    file_manager: FileManager,
    /// Data file handle (lazily initialized).
    data_file: RwLock<Option<Arc<StandardFile>>>,
    /// Data file path.
    data_file_path: RwLock<Option<std::path::PathBuf>>,
    /// Fetch counter for statistics.
    fetch_count: AtomicU64,
    /// Hit counter for statistics.
    hit_count: AtomicU64,
    /// Miss counter for statistics.
    miss_count: AtomicU64,
    /// Eviction counter for statistics.
    eviction_count: AtomicU64,
    /// Flush counter for statistics.
    flush_count: AtomicU64,
    /// Shutdown flag.
    shutdown: AtomicBool,
}

impl BufferPool {
    /// Creates a new buffer pool with the given configuration.
    pub fn new(config: BufferPoolConfig) -> BufferResult<Self> {
        config
            .validate()
            .map_err(|e| BufferError::Config { message: e.to_string() })?;

        let num_frames = config.num_frames;
        let page_size = config.page_size;

        // Create frame array
        let frames: Vec<Arc<BufferFrame>> = (0..num_frames)
            .map(|i| Arc::new(BufferFrame::new(FrameId::new(i), page_size)))
            .collect();

        let file_manager = FileManager::new()
            .map_err(|e| BufferError::Config { message: e.to_string() })?;

        Ok(Self {
            config,
            frames,
            page_table: RwLock::new(HashMap::with_capacity(num_frames)),
            replacer: ClockReplacer::new(num_frames),
            file_manager,
            data_file: RwLock::new(None),
            data_file_path: RwLock::new(None),
            fetch_count: AtomicU64::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Opens a data file for the buffer pool.
    pub async fn open(&self, path: impl AsRef<Path>) -> BufferResult<()> {
        let path = path.as_ref();
        let options = OpenOptions::for_database();
        let file = self.file_manager.open(path, options).await?;
        
        *self.data_file.write() = Some(file);
        *self.data_file_path.write() = Some(path.to_path_buf());
        
        Ok(())
    }

    /// Fetches a page for reading.
    ///
    /// If the page is in the buffer pool, returns it directly.
    /// Otherwise, reads it from disk into a frame.
    pub async fn fetch_page(&self, page_id: PageId) -> BufferResult<PageReadGuard> {
        self.check_shutdown()?;
        self.fetch_count.fetch_add(1, Ordering::Relaxed);

        // Check if page is already in buffer pool
        if let Some(frame) = self.get_cached_frame(page_id) {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            frame.pin();
            return Ok(PageReadGuard::new(frame, page_id));
        }

        // Page not in cache, need to fetch from disk
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        let frame = self.fetch_from_disk(page_id).await?;
        Ok(PageReadGuard::new(frame, page_id))
    }

    /// Fetches a page for writing.
    ///
    /// Similar to fetch_page but returns a write guard.
    pub async fn fetch_page_for_write(&self, page_id: PageId) -> BufferResult<PageWriteGuard> {
        self.check_shutdown()?;
        self.fetch_count.fetch_add(1, Ordering::Relaxed);

        // Check if page is already in buffer pool
        if let Some(frame) = self.get_cached_frame(page_id) {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            frame.pin();
            return Ok(PageWriteGuard::new(frame, page_id));
        }

        // Page not in cache, need to fetch from disk
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        let frame = self.fetch_from_disk(page_id).await?;
        Ok(PageWriteGuard::new(frame, page_id))
    }

    /// Creates a new page in the buffer pool.
    ///
    /// Allocates a new page ID and returns a write guard to initialize it.
    pub async fn new_page(&self) -> BufferResult<PageWriteGuard> {
        self.check_shutdown()?;

        // Get a frame for the new page
        let frame = self.get_frame_for_new_page().await?;
        let page_id = frame.page_id();

        Ok(PageWriteGuard::new(frame, page_id))
    }

    /// Allocates a new page with a specific page ID.
    ///
    /// This is used when you know the page ID you want (e.g., from a free list).
    pub async fn allocate_page(&self, page_id: PageId) -> BufferResult<PageWriteGuard> {
        self.check_shutdown()?;

        if !page_id.is_valid() {
            return Err(BufferError::InvalidPageId { page_id });
        }

        // Check if page already exists
        {
            let page_table = self.page_table.read();
            if page_table.contains_key(&page_id) {
                return Err(BufferError::PageAlreadyExists { page_id });
            }
        }

        // Get a free or evicted frame
        let frame = self.get_or_evict_frame().await?;
        
        // Initialize the frame for the new page
        frame.set_page_id(page_id);
        frame.set_dirty(true);
        frame.pin();

        // Add to page table
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame.frame_id());
        }

        Ok(PageWriteGuard::new(frame, page_id))
    }

    /// Flushes a specific page to disk.
    pub async fn flush_page(&self, page_id: PageId) -> BufferResult<()> {
        let frame = {
            let page_table = self.page_table.read();
            match page_table.get(&page_id) {
                Some(&frame_id) => Arc::clone(&self.frames[frame_id.index()]),
                None => return Err(BufferError::PageNotFound { page_id }),
            }
        };

        if frame.is_dirty() {
            self.write_page_to_disk(&frame).await?;
            frame.set_dirty(false);
            self.flush_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Flushes all dirty pages to disk.
    pub async fn flush_all(&self) -> BufferResult<usize> {
        let mut flushed = 0;

        for frame in &self.frames {
            if frame.is_dirty() && !frame.is_empty() {
                self.write_page_to_disk(frame).await?;
                frame.set_dirty(false);
                flushed += 1;
            }
        }

        self.flush_count.fetch_add(flushed as u64, Ordering::Relaxed);
        Ok(flushed)
    }

    /// Evicts a page from the buffer pool (writes to disk if dirty).
    pub async fn evict_page(&self, page_id: PageId) -> BufferResult<()> {
        let frame = {
            let page_table = self.page_table.read();
            match page_table.get(&page_id) {
                Some(&frame_id) => Arc::clone(&self.frames[frame_id.index()]),
                None => return Ok(()), // Page not in pool, nothing to evict
            }
        };

        if frame.is_pinned() {
            // Can't evict a pinned page
            return Err(BufferError::NoFreeFrames);
        }

        // Flush if dirty
        if frame.is_dirty() {
            self.write_page_to_disk(&frame).await?;
            self.flush_count.fetch_add(1, Ordering::Relaxed);
        }

        // Remove from page table
        {
            let mut page_table = self.page_table.write();
            page_table.remove(&page_id);
        }

        // Reset the frame
        frame.reset();
        self.eviction_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Returns statistics about the buffer pool.
    pub fn stats(&self) -> BufferPoolStats {
        let mut pinned = 0;
        let mut dirty = 0;

        for frame in &self.frames {
            if frame.is_pinned() {
                pinned += 1;
            }
            if frame.is_dirty() {
                dirty += 1;
            }
        }

        BufferPoolStats {
            fetches: self.fetch_count.load(Ordering::Relaxed),
            hits: self.hit_count.load(Ordering::Relaxed),
            misses: self.miss_count.load(Ordering::Relaxed),
            evictions: self.eviction_count.load(Ordering::Relaxed),
            flushes: self.flush_count.load(Ordering::Relaxed),
            pinned_frames: pinned,
            dirty_frames: dirty,
        }
    }

    /// Returns the number of frames in the buffer pool.
    pub fn num_frames(&self) -> usize {
        self.config.num_frames
    }

    /// Returns the page size.
    pub fn page_size(&self) -> usize {
        self.config.page_size
    }

    /// Returns true if a page is in the buffer pool.
    pub fn contains(&self, page_id: PageId) -> bool {
        self.page_table.read().contains_key(&page_id)
    }

    /// Shuts down the buffer pool, flushing all dirty pages.
    pub async fn shutdown(&self) -> BufferResult<()> {
        self.shutdown.store(true, Ordering::Release);
        self.flush_all().await?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// Checks if the buffer pool is shutting down.
    fn check_shutdown(&self) -> BufferResult<()> {
        if self.shutdown.load(Ordering::Acquire) {
            Err(BufferError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    /// Gets a cached frame if the page is in the buffer pool.
    fn get_cached_frame(&self, page_id: PageId) -> Option<Arc<BufferFrame>> {
        let page_table = self.page_table.read();
        page_table
            .get(&page_id)
            .map(|frame_id| Arc::clone(&self.frames[frame_id.index()]))
    }

    /// Fetches a page from disk into a frame.
    async fn fetch_from_disk(&self, page_id: PageId) -> BufferResult<Arc<BufferFrame>> {
        // Get a frame (free or evicted)
        let frame = self.get_or_evict_frame().await?;

        // Read page from disk
        self.read_page_from_disk(page_id, &frame).await?;

        // Set up the frame
        frame.set_page_id(page_id);
        frame.set_dirty(false);
        frame.pin();

        // Add to page table
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame.frame_id());
        }

        Ok(frame)
    }

    /// Gets a frame for a new page.
    async fn get_frame_for_new_page(&self) -> BufferResult<Arc<BufferFrame>> {
        let frame = self.get_or_evict_frame().await?;

        // Generate a new page ID (simple increment for now)
        // In a real system, this would use a free page list
        let page_id = {
            let page_table = self.page_table.read();
            let max_id = page_table
                .keys()
                .map(|p| p.as_u64())
                .max()
                .unwrap_or(0);
            PageId::new(max_id + 1)
        };

        // Initialize the frame
        frame.set_page_id(page_id);
        frame.set_dirty(true);
        frame.pin();

        // Zero the page data
        {
            let mut data = frame.write_data();
            data.fill(0);
        }

        // Add to page table
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame.frame_id());
        }

        Ok(frame)
    }

    /// Gets a free frame or evicts one.
    async fn get_or_evict_frame(&self) -> BufferResult<Arc<BufferFrame>> {
        // First, try to find a free frame
        if let Some(frame_id) = self.replacer.find_free_frame(&self.frames) {
            return Ok(Arc::clone(&self.frames[frame_id.index()]));
        }

        // No free frames, need to evict
        self.evict_and_get_frame().await
    }

    /// Evicts a page and returns the freed frame.
    async fn evict_and_get_frame(&self) -> BufferResult<Arc<BufferFrame>> {
        // Find victim using clock algorithm
        let victim_id = self.replacer.find_victim(&self.frames);

        let frame_id = victim_id.ok_or(BufferError::NoFreeFrames)?;
        let frame = Arc::clone(&self.frames[frame_id.index()]);

        // Flush if dirty
        if frame.is_dirty() {
            self.write_page_to_disk(&frame).await?;
            self.flush_count.fetch_add(1, Ordering::Relaxed);
        }

        // Remove from page table
        let old_page_id = frame.page_id();
        if old_page_id.is_valid() {
            let mut page_table = self.page_table.write();
            page_table.remove(&old_page_id);
        }

        // Reset the frame
        frame.reset();
        self.eviction_count.fetch_add(1, Ordering::Relaxed);

        Ok(frame)
    }

    /// Reads a page from disk into a frame.
    async fn read_page_from_disk(&self, page_id: PageId, frame: &BufferFrame) -> BufferResult<()> {
        let file = self.data_file.read();
        let file = file.as_ref().ok_or_else(|| {
            BufferError::Config { message: "data file not opened".to_string() }
        })?;

        let offset = page_id.as_u64() * self.config.page_size as u64;
        let mut data = frame.write_data();

        file.read_exact_at(&mut data[..], offset).await?;

        Ok(())
    }

    /// Writes a page from a frame to disk.
    async fn write_page_to_disk(&self, frame: &BufferFrame) -> BufferResult<()> {
        let file = self.data_file.read();
        let file = file.as_ref().ok_or_else(|| {
            BufferError::Config { message: "data file not opened".to_string() }
        })?;

        let page_id = frame.page_id();
        let offset = page_id.as_u64() * self.config.page_size as u64;
        let data = frame.read_data();

        file.write_all_at(&data[..], offset).await?;

        Ok(())
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("num_frames", &self.config.num_frames)
            .field("page_size", &self.config.page_size)
            .field("pages_cached", &self.page_table.read().len())
            .field("stats", &self.stats())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PAGE_SIZE;
    use tempfile::tempdir;

    fn create_test_pool(num_frames: usize) -> BufferPool {
        let config = BufferPoolConfig::new(num_frames);
        BufferPool::new(config).unwrap()
    }

    #[test]
    fn test_pool_creation() {
        let pool = create_test_pool(100);
        assert_eq!(pool.num_frames(), 100);
        assert_eq!(pool.page_size(), PAGE_SIZE);
    }

    #[tokio::test]
    async fn test_allocate_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        let guard = pool.allocate_page(PageId::new(1)).await.unwrap();
        assert_eq!(guard.page_id(), PageId::new(1));
        assert!(pool.contains(PageId::new(1)));
        
        drop(guard);
        
        // Should still be in pool (but unpinned)
        assert!(pool.contains(PageId::new(1)));
    }

    #[tokio::test]
    async fn test_allocate_duplicate_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        let _guard = pool.allocate_page(PageId::new(1)).await.unwrap();
        
        // Try to allocate same page - should fail
        let result = pool.allocate_page(PageId::new(1)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_flush_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        // Allocate and write to a page
        {
            let mut guard = pool.allocate_page(PageId::new(0)).await.unwrap();
            let mut data = guard.data_mut();
            data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        }

        // Flush the page
        pool.flush_page(PageId::new(0)).await.unwrap();

        let stats = pool.stats();
        assert!(stats.flushes > 0);
    }

    #[tokio::test]
    async fn test_fetch_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        // Allocate a page and write data
        {
            let mut guard = pool.allocate_page(PageId::new(0)).await.unwrap();
            let mut data = guard.data_mut();
            data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        }

        // Flush and evict
        pool.flush_page(PageId::new(0)).await.unwrap();
        pool.evict_page(PageId::new(0)).await.unwrap();
        
        assert!(!pool.contains(PageId::new(0)));

        // Fetch the page back
        let guard = pool.fetch_page(PageId::new(0)).await.unwrap();
        let data = guard.data();
        assert_eq!(&data[0..4], &[1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        // Allocate a page
        {
            let _guard = pool.allocate_page(PageId::new(0)).await.unwrap();
        }

        // Fetch it again (should be a cache hit)
        {
            let _guard = pool.fetch_page(PageId::new(0)).await.unwrap();
        }

        let stats = pool.stats();
        assert!(stats.hits >= 1);
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        // Do some operations
        for i in 0..5 {
            let _guard = pool.allocate_page(PageId::new(i)).await.unwrap();
        }

        let stats = pool.stats();
        assert_eq!(stats.dirty_frames, 5);
    }

    #[tokio::test]
    async fn test_flush_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        // Allocate multiple pages
        for i in 0..5 {
            let mut guard = pool.allocate_page(PageId::new(i)).await.unwrap();
            guard.mark_dirty();
        }

        let flushed = pool.flush_all().await.unwrap();
        assert_eq!(flushed, 5);

        let stats = pool.stats();
        assert_eq!(stats.dirty_frames, 0);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let pool = create_test_pool(10);
        pool.open(&path).await.unwrap();

        pool.shutdown().await.unwrap();

        // Operations after shutdown should fail
        let result = pool.allocate_page(PageId::new(1)).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_contains() {
        let pool = create_test_pool(10);
        assert!(!pool.contains(PageId::new(1)));
    }
}
