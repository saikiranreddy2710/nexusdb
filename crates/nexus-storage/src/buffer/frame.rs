//! Buffer frame - a slot in the buffer pool that holds a page.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use nexus_common::types::{Lsn, PageId};
use parking_lot::RwLock;

/// Frame identifier - index into the buffer pool's frame array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(pub usize);

impl FrameId {
    /// Invalid frame ID.
    pub const INVALID: Self = Self(usize::MAX);

    /// Creates a new frame ID.
    #[inline]
    pub const fn new(id: usize) -> Self {
        Self(id)
    }

    /// Returns the raw index.
    #[inline]
    pub const fn index(self) -> usize {
        self.0
    }

    /// Checks if this is a valid frame ID.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 != usize::MAX
    }
}

impl From<usize> for FrameId {
    fn from(id: usize) -> Self {
        Self::new(id)
    }
}

impl From<FrameId> for usize {
    fn from(id: FrameId) -> Self {
        id.0
    }
}

/// State of a buffer frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)] // Will be used when implementing async I/O states
pub enum FrameState {
    /// Frame is free and available.
    Free = 0,
    /// Frame is in use.
    InUse = 1,
    /// Frame is being read from disk.
    Reading = 2,
    /// Frame is being written to disk.
    Writing = 3,
}

/// A buffer frame holds a single page in memory.
///
/// Each frame has:
/// - A data buffer for the page contents
/// - Metadata (page_id, dirty flag, pin count)
/// - A lock for concurrent access
///
/// The frame uses atomic operations for pin count and dirty flag
/// to minimize lock contention.
pub struct BufferFrame {
    /// Frame ID (index in the frame array).
    frame_id: FrameId,
    /// Page data buffer.
    data: RwLock<Vec<u8>>,
    /// Page ID stored in this frame (INVALID if empty).
    page_id: AtomicU64,
    /// Pin count (number of active references).
    pin_count: AtomicU32,
    /// Whether the page is dirty (modified since last flush).
    dirty: AtomicBool,
    /// Reference bit for clock eviction.
    ref_bit: AtomicBool,
    /// Last modified LSN.
    lsn: AtomicU64,
}

impl BufferFrame {
    /// Creates a new empty buffer frame.
    pub fn new(frame_id: FrameId, page_size: usize) -> Self {
        Self {
            frame_id,
            data: RwLock::new(vec![0u8; page_size]),
            page_id: AtomicU64::new(PageId::INVALID.as_u64()),
            pin_count: AtomicU32::new(0),
            dirty: AtomicBool::new(false),
            ref_bit: AtomicBool::new(false),
            lsn: AtomicU64::new(Lsn::INVALID.as_u64()),
        }
    }

    /// Returns the frame ID.
    #[inline]
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Returns the page ID stored in this frame.
    #[inline]
    pub fn page_id(&self) -> PageId {
        PageId::new(self.page_id.load(Ordering::Acquire))
    }

    /// Sets the page ID for this frame.
    #[inline]
    pub fn set_page_id(&self, page_id: PageId) {
        self.page_id.store(page_id.as_u64(), Ordering::Release);
    }

    /// Returns true if this frame is empty (no page assigned).
    #[inline]
    pub fn is_empty(&self) -> bool {
        !self.page_id().is_valid()
    }

    /// Returns the current pin count.
    #[inline]
    pub fn pin_count(&self) -> u32 {
        self.pin_count.load(Ordering::Acquire)
    }

    /// Increments the pin count and returns the new value.
    #[inline]
    pub fn pin(&self) -> u32 {
        let count = self.pin_count.fetch_add(1, Ordering::AcqRel) + 1;
        // Set reference bit when pinned (for clock algorithm)
        self.ref_bit.store(true, Ordering::Release);
        count
    }

    /// Decrements the pin count and returns the new value.
    ///
    /// # Panics
    ///
    /// Panics if the pin count is already 0.
    #[inline]
    pub fn unpin(&self) -> u32 {
        let old = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "unpinned frame with pin_count = 0");
        old - 1
    }

    /// Returns true if the frame is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count() > 0
    }

    /// Returns true if the frame is dirty.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Marks the frame as dirty.
    #[inline]
    pub fn set_dirty(&self, dirty: bool) {
        self.dirty.store(dirty, Ordering::Release);
    }

    /// Returns the reference bit (for clock algorithm).
    #[inline]
    pub fn ref_bit(&self) -> bool {
        self.ref_bit.load(Ordering::Acquire)
    }

    /// Clears the reference bit.
    #[inline]
    pub fn clear_ref_bit(&self) {
        self.ref_bit.store(false, Ordering::Release);
    }

    /// Returns the LSN of the last modification.
    #[inline]
    pub fn lsn(&self) -> Lsn {
        Lsn::new(self.lsn.load(Ordering::Acquire))
    }

    /// Sets the LSN.
    #[inline]
    pub fn set_lsn(&self, lsn: Lsn) {
        self.lsn.store(lsn.as_u64(), Ordering::Release);
    }

    /// Returns a read lock on the page data.
    #[inline]
    pub fn read_data(&self) -> parking_lot::RwLockReadGuard<'_, Vec<u8>> {
        self.data.read()
    }

    /// Returns a write lock on the page data.
    #[inline]
    pub fn write_data(&self) -> parking_lot::RwLockWriteGuard<'_, Vec<u8>> {
        self.data.write()
    }

    /// Copies data into the frame.
    pub fn copy_from(&self, data: &[u8]) {
        let mut guard = self.data.write();
        guard[..data.len()].copy_from_slice(data);
    }

    /// Copies data from the frame.
    pub fn copy_to(&self, buf: &mut [u8]) {
        let guard = self.data.read();
        buf.copy_from_slice(&guard[..buf.len()]);
    }

    /// Resets the frame to empty state.
    pub fn reset(&self) {
        self.page_id
            .store(PageId::INVALID.as_u64(), Ordering::Release);
        self.pin_count.store(0, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        self.ref_bit.store(false, Ordering::Release);
        self.lsn.store(Lsn::INVALID.as_u64(), Ordering::Release);
        // Optionally zero the data
        // self.data.write().fill(0);
    }

    /// Returns true if this frame can be evicted.
    ///
    /// A frame can be evicted if:
    /// - It is not pinned
    /// - It has a valid page (not empty)
    #[inline]
    pub fn is_evictable(&self) -> bool {
        !self.is_pinned() && !self.is_empty()
    }
}

impl std::fmt::Debug for BufferFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferFrame")
            .field("frame_id", &self.frame_id)
            .field("page_id", &self.page_id())
            .field("pin_count", &self.pin_count())
            .field("dirty", &self.is_dirty())
            .field("ref_bit", &self.ref_bit())
            .field("lsn", &self.lsn())
            .finish()
    }
}

// Safety: BufferFrame uses atomic operations and RwLock for thread-safety.
unsafe impl Send for BufferFrame {}
unsafe impl Sync for BufferFrame {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_creation() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);
        assert_eq!(frame.frame_id().index(), 0);
        assert!(!frame.page_id().is_valid());
        assert!(frame.is_empty());
        assert_eq!(frame.pin_count(), 0);
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_pin_unpin() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);
        assert!(!frame.is_pinned());

        let count = frame.pin();
        assert_eq!(count, 1);
        assert!(frame.is_pinned());
        assert!(frame.ref_bit()); // Should be set on pin

        let count = frame.pin();
        assert_eq!(count, 2);

        let count = frame.unpin();
        assert_eq!(count, 1);
        assert!(frame.is_pinned());

        let count = frame.unpin();
        assert_eq!(count, 0);
        assert!(!frame.is_pinned());
    }

    #[test]
    fn test_dirty_flag() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);
        assert!(!frame.is_dirty());

        frame.set_dirty(true);
        assert!(frame.is_dirty());

        frame.set_dirty(false);
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_page_id() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);
        assert!(frame.is_empty());

        frame.set_page_id(PageId::new(42));
        assert!(!frame.is_empty());
        assert_eq!(frame.page_id(), PageId::new(42));
    }

    #[test]
    fn test_data_access() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);

        // Write data
        {
            let mut data = frame.write_data();
            data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        }

        // Read data
        {
            let data = frame.read_data();
            assert_eq!(&data[0..4], &[1, 2, 3, 4]);
        }
    }

    #[test]
    fn test_copy_operations() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);

        let input = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        frame.copy_from(&input);

        let mut output = vec![0u8; 8];
        frame.copy_to(&mut output);
        assert_eq!(output, input);
    }

    #[test]
    fn test_reset() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);
        frame.set_page_id(PageId::new(42));
        frame.pin();
        frame.set_dirty(true);
        frame.set_lsn(Lsn::new(1000));

        frame.reset();

        assert!(frame.is_empty());
        assert_eq!(frame.pin_count(), 0);
        assert!(!frame.is_dirty());
        assert!(!frame.ref_bit());
        assert_eq!(frame.lsn(), Lsn::INVALID);
    }

    #[test]
    fn test_is_evictable() {
        let frame = BufferFrame::new(FrameId::new(0), 8192);

        // Empty frame is not evictable
        assert!(!frame.is_evictable());

        // Assign a page
        frame.set_page_id(PageId::new(42));
        assert!(frame.is_evictable());

        // Pinned frame is not evictable
        frame.pin();
        assert!(!frame.is_evictable());

        frame.unpin();
        assert!(frame.is_evictable());
    }

    #[test]
    fn test_frame_id() {
        assert!(!FrameId::INVALID.is_valid());
        assert!(FrameId::new(0).is_valid());
        assert_eq!(FrameId::new(42).index(), 42);
    }
}
