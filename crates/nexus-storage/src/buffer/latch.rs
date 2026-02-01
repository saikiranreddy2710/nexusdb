//! Page latches (read/write guards) for safe concurrent access.
//!
//! This module provides RAII guards that automatically unpin pages
//! when they go out of scope.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use nexus_common::types::PageId;

use super::frame::BufferFrame;

/// Read guard for a page in the buffer pool.
///
/// This guard:
/// - Provides read-only access to page data
/// - Keeps the frame pinned while held
/// - Automatically unpins when dropped
pub struct PageReadGuard {
    frame: Arc<BufferFrame>,
    #[allow(dead_code)]
    page_id: PageId,
}

impl PageReadGuard {
    /// Creates a new read guard.
    pub(crate) fn new(frame: Arc<BufferFrame>, page_id: PageId) -> Self {
        Self { frame, page_id }
    }

    /// Returns the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        self.frame.page_id()
    }

    /// Returns a reference to the page data.
    #[inline]
    pub fn data(&self) -> impl Deref<Target = [u8]> + '_ {
        PageDataRef {
            guard: self.frame.read_data(),
        }
    }

    /// Returns the frame ID.
    #[inline]
    pub fn frame_id(&self) -> super::frame::FrameId {
        self.frame.frame_id()
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        self.frame.unpin();
    }
}

impl std::fmt::Debug for PageReadGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageReadGuard")
            .field("page_id", &self.page_id())
            .field("frame_id", &self.frame.frame_id())
            .finish()
    }
}

/// Helper struct to provide Deref for page data.
struct PageDataRef<'a> {
    guard: parking_lot::RwLockReadGuard<'a, Vec<u8>>,
}

impl<'a> Deref for PageDataRef<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// Write guard for a page in the buffer pool.
///
/// This guard:
/// - Provides read-write access to page data
/// - Keeps the frame pinned while held
/// - Marks the page dirty when modified
/// - Automatically unpins when dropped
pub struct PageWriteGuard {
    frame: Arc<BufferFrame>,
    #[allow(dead_code)]
    page_id: PageId,
    /// Track if the page was modified.
    modified: bool,
}

impl PageWriteGuard {
    /// Creates a new write guard.
    pub(crate) fn new(frame: Arc<BufferFrame>, page_id: PageId) -> Self {
        Self {
            frame,
            page_id,
            modified: false,
        }
    }

    /// Returns the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        self.frame.page_id()
    }

    /// Returns a reference to the page data.
    #[inline]
    pub fn data(&self) -> impl Deref<Target = [u8]> + '_ {
        PageDataRef {
            guard: self.frame.read_data(),
        }
    }

    /// Returns a mutable reference to the page data.
    ///
    /// This marks the page as dirty.
    #[inline]
    pub fn data_mut(&mut self) -> impl DerefMut<Target = [u8]> + '_ {
        self.modified = true;
        self.frame.set_dirty(true);
        PageDataMut {
            guard: self.frame.write_data(),
        }
    }

    /// Marks the page as dirty without getting a mutable reference.
    #[inline]
    pub fn mark_dirty(&mut self) {
        self.modified = true;
        self.frame.set_dirty(true);
    }

    /// Returns the frame ID.
    #[inline]
    pub fn frame_id(&self) -> super::frame::FrameId {
        self.frame.frame_id()
    }

    /// Returns true if the page was modified.
    #[inline]
    pub fn is_modified(&self) -> bool {
        self.modified
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        self.frame.unpin();
    }
}

impl std::fmt::Debug for PageWriteGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageWriteGuard")
            .field("page_id", &self.page_id())
            .field("frame_id", &self.frame.frame_id())
            .field("modified", &self.modified)
            .finish()
    }
}

/// Helper struct to provide DerefMut for page data.
struct PageDataMut<'a> {
    guard: parking_lot::RwLockWriteGuard<'a, Vec<u8>>,
}

impl<'a> Deref for PageDataMut<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for PageDataMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::super::frame::FrameId;
    use super::*;

    #[test]
    fn test_read_guard_unpins_on_drop() {
        let frame = Arc::new(BufferFrame::new(FrameId::new(0), 8192));
        frame.set_page_id(PageId::new(42));
        frame.pin(); // Initial pin for the guard

        assert_eq!(frame.pin_count(), 1);

        {
            let _guard = PageReadGuard::new(Arc::clone(&frame), PageId::new(42));
            // Guard holds the pin
        }

        // Pin should be released after guard is dropped
        assert_eq!(frame.pin_count(), 0);
    }

    #[test]
    fn test_write_guard_marks_dirty() {
        let frame = Arc::new(BufferFrame::new(FrameId::new(0), 8192));
        frame.set_page_id(PageId::new(42));
        frame.pin();

        assert!(!frame.is_dirty());

        {
            let mut guard = PageWriteGuard::new(Arc::clone(&frame), PageId::new(42));
            let _ = guard.data_mut();
        }

        // Frame should be dirty
        assert!(frame.is_dirty());
        // Pin should be released
        assert_eq!(frame.pin_count(), 0);
    }

    #[test]
    fn test_write_guard_mark_dirty_explicit() {
        let frame = Arc::new(BufferFrame::new(FrameId::new(0), 8192));
        frame.set_page_id(PageId::new(42));
        frame.pin();

        {
            let mut guard = PageWriteGuard::new(Arc::clone(&frame), PageId::new(42));
            guard.mark_dirty();
            assert!(guard.is_modified());
        }

        assert!(frame.is_dirty());
    }

    #[test]
    fn test_read_guard_data_access() {
        let frame = Arc::new(BufferFrame::new(FrameId::new(0), 8192));
        frame.set_page_id(PageId::new(42));

        // Write some data
        {
            let mut data = frame.write_data();
            data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        }

        frame.pin();
        let guard = PageReadGuard::new(Arc::clone(&frame), PageId::new(42));

        let data = guard.data();
        assert_eq!(&data[0..4], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_write_guard_data_access() {
        let frame = Arc::new(BufferFrame::new(FrameId::new(0), 8192));
        frame.set_page_id(PageId::new(42));
        frame.pin();

        {
            let mut guard = PageWriteGuard::new(Arc::clone(&frame), PageId::new(42));

            {
                let mut data = guard.data_mut();
                data[0..4].copy_from_slice(&[5, 6, 7, 8]);
            }
        }

        let data = frame.read_data();
        assert_eq!(&data[0..4], &[5, 6, 7, 8]);
    }
}
