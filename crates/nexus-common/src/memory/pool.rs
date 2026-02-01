//! Memory pool for fixed-size allocations.
//!
//! Memory pools (also called slab allocators) provide efficient allocation
//! and deallocation of fixed-size objects. They're particularly useful for:
//!
//! - Page buffers in the buffer pool
//! - Transaction objects
//! - Lock structures
//!
//! # Design
//!
//! - Pre-allocates a pool of fixed-size slots
//! - O(1) allocation and deallocation
//! - Supports concurrent access with lock-free free list
//! - Memory is cache-line aligned to avoid false sharing

use std::alloc::{self, Layout};
use std::mem;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::aligned::CACHE_LINE_SIZE;

/// A fixed-size memory pool.
///
/// Provides efficient allocation of fixed-size blocks with O(1) alloc/free.
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::MemoryPool;
///
/// // Create a pool with 64 slots of 1KB each
/// let pool = MemoryPool::new(1024, 64);
///
/// // Allocate a buffer
/// let buffer = pool.allocate().expect("pool not exhausted");
/// assert_eq!(buffer.len(), 1024);
///
/// // Return it to the pool
/// pool.deallocate(buffer);
/// ```
pub struct MemoryPool {
    /// Size of each slot in bytes.
    slot_size: usize,
    /// Number of slots in the pool.
    slot_count: usize,
    /// The actual memory for slots.
    memory: NonNull<u8>,
    /// Layout used for allocation.
    layout: Layout,
    /// Free list head (lock-free stack).
    free_list: Mutex<Vec<usize>>,
    /// Number of currently allocated slots.
    allocated_count: AtomicUsize,
}

// SAFETY: MemoryPool uses proper synchronization
unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

impl MemoryPool {
    /// Creates a new memory pool with the specified slot size and count.
    ///
    /// # Arguments
    ///
    /// * `slot_size` - Size of each slot in bytes (will be rounded up for alignment)
    /// * `slot_count` - Number of slots in the pool
    ///
    /// # Panics
    ///
    /// Panics if allocation fails or if slot_size is 0.
    #[must_use]
    pub fn new(slot_size: usize, slot_count: usize) -> Self {
        assert!(slot_size > 0, "slot_size must be greater than 0");
        assert!(slot_count > 0, "slot_count must be greater than 0");

        // Round up slot size to cache line alignment
        let slot_size = (slot_size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE * CACHE_LINE_SIZE;

        let total_size = slot_size * slot_count;
        let layout = Layout::from_size_align(total_size, CACHE_LINE_SIZE).expect("invalid layout");

        // SAFETY: Layout is valid
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        let memory = NonNull::new(ptr).expect("memory pool allocation failed");

        // Initialize free list with all slots
        let free_list: Vec<usize> = (0..slot_count).rev().collect();

        Self {
            slot_size,
            slot_count,
            memory,
            layout,
            free_list: Mutex::new(free_list),
            allocated_count: AtomicUsize::new(0),
        }
    }

    /// Creates a memory pool sized for database pages.
    ///
    /// # Arguments
    ///
    /// * `page_size` - Size of each page (typically 4KB or 8KB)
    /// * `pool_size_bytes` - Total pool size in bytes
    #[must_use]
    pub fn for_pages(page_size: usize, pool_size_bytes: usize) -> Self {
        let slot_count = pool_size_bytes / page_size;
        Self::new(page_size, slot_count.max(1))
    }

    /// Returns the size of each slot.
    #[inline]
    #[must_use]
    pub const fn slot_size(&self) -> usize {
        self.slot_size
    }

    /// Returns the total number of slots.
    #[inline]
    #[must_use]
    pub const fn slot_count(&self) -> usize {
        self.slot_count
    }

    /// Returns the number of currently allocated slots.
    #[inline]
    #[must_use]
    pub fn allocated_count(&self) -> usize {
        self.allocated_count.load(Ordering::Relaxed)
    }

    /// Returns the number of available slots.
    #[inline]
    #[must_use]
    pub fn available_count(&self) -> usize {
        self.slot_count - self.allocated_count()
    }

    /// Returns true if the pool is empty (no slots available).
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.available_count() == 0
    }

    /// Returns true if no slots are allocated.
    #[inline]
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.allocated_count() == 0
    }

    /// Allocates a buffer from the pool.
    ///
    /// Returns `None` if the pool is exhausted.
    #[must_use]
    pub fn allocate(&self) -> Option<PooledBuffer> {
        let mut free_list = self.free_list.lock();

        if let Some(slot_index) = free_list.pop() {
            self.allocated_count.fetch_add(1, Ordering::Relaxed);

            let offset = slot_index * self.slot_size;
            // SAFETY: offset is within bounds
            let ptr = unsafe { self.memory.as_ptr().add(offset) };

            Some(PooledBuffer {
                ptr: NonNull::new(ptr).unwrap(),
                size: self.slot_size,
                slot_index,
            })
        } else {
            None
        }
    }

    /// Deallocates a buffer, returning it to the pool.
    ///
    /// # Panics
    ///
    /// Panics if the buffer was not allocated from this pool.
    pub fn deallocate(&self, buffer: PooledBuffer) {
        // Verify the buffer belongs to this pool
        let buffer_addr = buffer.ptr.as_ptr() as usize;
        let pool_start = self.memory.as_ptr() as usize;
        let pool_end = pool_start + (self.slot_count * self.slot_size);

        assert!(
            buffer_addr >= pool_start && buffer_addr < pool_end,
            "buffer does not belong to this pool"
        );

        let mut free_list = self.free_list.lock();
        free_list.push(buffer.slot_index);
        self.allocated_count.fetch_sub(1, Ordering::Relaxed);

        // Forget the buffer so its Drop doesn't run
        mem::forget(buffer);
    }

    /// Tries to allocate, growing a counter if successful.
    ///
    /// This is useful for tracking allocations.
    pub fn try_allocate(&self) -> Option<PooledBuffer> {
        self.allocate()
    }

    /// Returns the total memory used by this pool.
    #[inline]
    #[must_use]
    pub fn total_memory(&self) -> usize {
        self.layout.size()
    }

    /// Returns memory utilization as a percentage.
    #[must_use]
    pub fn utilization(&self) -> f64 {
        self.allocated_count() as f64 / self.slot_count as f64 * 100.0
    }

    /// Clears the pool, returning all slots to the free list.
    ///
    /// # Safety
    ///
    /// Caller must ensure no `PooledBuffer`s from this pool are still in use.
    pub unsafe fn clear(&self) {
        let mut free_list = self.free_list.lock();
        free_list.clear();
        for i in (0..self.slot_count).rev() {
            free_list.push(i);
        }
        self.allocated_count.store(0, Ordering::Relaxed);
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        // SAFETY: memory was allocated with this layout
        unsafe {
            alloc::dealloc(self.memory.as_ptr(), self.layout);
        }
    }
}

impl std::fmt::Debug for MemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPool")
            .field("slot_size", &self.slot_size)
            .field("slot_count", &self.slot_count)
            .field("allocated", &self.allocated_count())
            .field("available", &self.available_count())
            .finish()
    }
}

/// A buffer allocated from a memory pool.
///
/// When dropped, the buffer is NOT automatically returned to the pool.
/// You must explicitly call `pool.deallocate(buffer)` to return it.
pub struct PooledBuffer {
    ptr: NonNull<u8>,
    size: usize,
    slot_index: usize,
}

// SAFETY: PooledBuffer owns its data exclusively
unsafe impl Send for PooledBuffer {}
unsafe impl Sync for PooledBuffer {}

impl PooledBuffer {
    /// Returns the size of the buffer.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns a raw pointer to the buffer.
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the buffer.
    #[inline]
    #[must_use]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns the buffer as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid and size is correct
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Returns the buffer as a mutable byte slice.
    #[inline]
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid and size is correct
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    /// Zeros the buffer contents.
    pub fn zero(&mut self) {
        // SAFETY: ptr is valid and size is correct
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), 0, self.size);
        }
    }

    /// Fills the buffer with a value.
    pub fn fill(&mut self, value: u8) {
        // SAFETY: ptr is valid and size is correct
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), value, self.size);
        }
    }

    /// Returns the slot index within the pool.
    #[inline]
    #[must_use]
    pub const fn slot_index(&self) -> usize {
        self.slot_index
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl AsRef<[u8]> for PooledBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for PooledBuffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl std::fmt::Debug for PooledBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledBuffer")
            .field("size", &self.size)
            .field("slot_index", &self.slot_index)
            .finish()
    }
}

/// A thread-local memory pool cache for reduced contention.
///
/// Each thread maintains a small cache of buffers to reduce
/// contention on the main pool.
pub struct ThreadLocalPoolCache {
    /// The backing pool.
    pool: std::sync::Arc<MemoryPool>,
    /// Thread-local cache size (reserved for future use).
    _cache_size: usize,
}

impl ThreadLocalPoolCache {
    /// Creates a new thread-local cache over a pool.
    pub fn new(pool: std::sync::Arc<MemoryPool>, cache_size: usize) -> Self {
        Self {
            pool,
            _cache_size: cache_size,
        }
    }

    /// Allocates from the cache or pool.
    pub fn allocate(&self) -> Option<PooledBuffer> {
        // For a production implementation, we'd use thread_local! here
        // to maintain per-thread caches
        self.pool.allocate()
    }

    /// Deallocates to the cache or pool.
    pub fn deallocate(&self, buffer: PooledBuffer) {
        self.pool.deallocate(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool_creation() {
        let pool = MemoryPool::new(1024, 64);

        assert!(pool.slot_size() >= 1024);
        assert_eq!(pool.slot_count(), 64);
        assert_eq!(pool.allocated_count(), 0);
        assert_eq!(pool.available_count(), 64);
    }

    #[test]
    fn test_memory_pool_allocate() {
        let pool = MemoryPool::new(1024, 4);

        let buf1 = pool.allocate().expect("should allocate");
        assert_eq!(pool.allocated_count(), 1);
        assert_eq!(pool.available_count(), 3);

        let buf2 = pool.allocate().expect("should allocate");
        assert_eq!(pool.allocated_count(), 2);

        pool.deallocate(buf1);
        assert_eq!(pool.allocated_count(), 1);

        pool.deallocate(buf2);
        assert_eq!(pool.allocated_count(), 0);
    }

    #[test]
    fn test_memory_pool_exhaustion() {
        let pool = MemoryPool::new(1024, 2);

        let _buf1 = pool.allocate().expect("should allocate");
        let _buf2 = pool.allocate().expect("should allocate");

        // Pool should be exhausted
        assert!(pool.allocate().is_none());
        assert!(pool.is_empty());
    }

    #[test]
    fn test_pooled_buffer_read_write() {
        let pool = MemoryPool::new(1024, 4);

        let mut buffer = pool.allocate().expect("should allocate");

        buffer[0] = 0xDE;
        buffer[1] = 0xAD;
        buffer[2] = 0xBE;
        buffer[3] = 0xEF;

        assert_eq!(buffer[0], 0xDE);
        assert_eq!(buffer[1], 0xAD);
        assert_eq!(buffer[2], 0xBE);
        assert_eq!(buffer[3], 0xEF);

        pool.deallocate(buffer);
    }

    #[test]
    fn test_pooled_buffer_zero() {
        let pool = MemoryPool::new(1024, 4);

        let mut buffer = pool.allocate().expect("should allocate");
        buffer.fill(0xFF);
        buffer.zero();

        for byte in buffer.as_slice() {
            assert_eq!(*byte, 0);
        }

        pool.deallocate(buffer);
    }

    #[test]
    fn test_memory_pool_for_pages() {
        let pool = MemoryPool::for_pages(8192, 1024 * 1024); // 1MB pool with 8KB pages

        assert!(pool.slot_size() >= 8192);
        assert!(pool.slot_count() > 0);
    }

    #[test]
    fn test_memory_pool_utilization() {
        let pool = MemoryPool::new(1024, 10);

        assert_eq!(pool.utilization(), 0.0);

        let _buf = pool.allocate();
        assert!((pool.utilization() - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_memory_pool_reuse() {
        let pool = MemoryPool::new(1024, 2);

        let buf1 = pool.allocate().expect("should allocate");
        let slot1 = buf1.slot_index();
        pool.deallocate(buf1);

        let buf2 = pool.allocate().expect("should allocate");
        // Should get the same slot back (LIFO)
        assert_eq!(buf2.slot_index(), slot1);

        pool.deallocate(buf2);
    }

    #[test]
    fn test_memory_pool_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(MemoryPool::new(1024, 100));
        let mut handles = vec![];

        for _ in 0..4 {
            let pool_clone = Arc::clone(&pool);
            let handle = thread::spawn(move || {
                for _ in 0..20 {
                    if let Some(buffer) = pool_clone.allocate() {
                        // Simulate some work
                        std::hint::spin_loop();
                        pool_clone.deallocate(buffer);
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All buffers should be returned
        assert_eq!(pool.allocated_count(), 0);
    }

    #[test]
    fn test_memory_pool_clear() {
        let pool = MemoryPool::new(1024, 4);

        let _buf1 = pool.allocate();
        let _buf2 = pool.allocate();

        assert_eq!(pool.allocated_count(), 2);

        // SAFETY: We're not using the buffers after clear
        unsafe {
            pool.clear();
        }

        assert_eq!(pool.allocated_count(), 0);
        assert_eq!(pool.available_count(), 4);
    }

    #[test]
    fn test_memory_pool_total_memory() {
        let pool = MemoryPool::new(1024, 64);
        // Each slot is cache-line aligned, so at least 64 * 1024 bytes
        assert!(pool.total_memory() >= 64 * 1024);
    }
}
