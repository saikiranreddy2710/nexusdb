//! Clock eviction policy for buffer pool.
//!
//! The Clock algorithm is a simple and efficient approximation of LRU
//! that uses a reference bit instead of maintaining an ordered list.
//!
//! How it works:
//! 1. Each frame has a reference bit set when accessed
//! 2. A clock hand sweeps through frames looking for eviction candidates
//! 3. If a frame's reference bit is set, clear it and move on
//! 4. If a frame's reference bit is clear, evict it
//!
//! This provides O(1) access and good cache behavior with minimal overhead.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::frame::{BufferFrame, FrameId};

/// Clock-based page replacement algorithm.
///
/// The replacer tracks which frames can be evicted and uses a clock
/// algorithm to select victims. Only unpinned frames are candidates
/// for eviction.
pub struct ClockReplacer {
    /// Number of frames in the buffer pool.
    num_frames: usize,
    /// Current position of the clock hand.
    clock_hand: AtomicUsize,
}

impl ClockReplacer {
    /// Creates a new clock replacer for the given number of frames.
    pub fn new(num_frames: usize) -> Self {
        Self {
            num_frames,
            clock_hand: AtomicUsize::new(0),
        }
    }

    /// Finds a frame to evict using the clock algorithm.
    ///
    /// Returns the frame ID of the victim, or None if no frame can be evicted
    /// (all frames are pinned).
    ///
    /// This method sweeps through the frames starting from the current clock
    /// hand position. For each frame:
    /// - If pinned: skip
    /// - If empty: skip (no page to evict)
    /// - If ref_bit is set: clear it and continue
    /// - If ref_bit is clear: select as victim
    ///
    /// The clock hand advances with each check, and we limit sweeps to
    /// prevent infinite loops when all frames are pinned.
    pub fn find_victim(&self, frames: &[Arc<BufferFrame>]) -> Option<FrameId> {
        // We'll sweep through at most 2 * num_frames to give reference bits
        // a chance to be cleared
        let max_sweeps = 2 * self.num_frames;

        for _ in 0..max_sweeps {
            let pos = self.advance_hand();
            let frame = &frames[pos];

            // Skip if pinned
            if frame.is_pinned() {
                continue;
            }

            // Skip if empty (no page to evict)
            if frame.is_empty() {
                continue;
            }

            // Check reference bit
            if frame.ref_bit() {
                // Clear reference bit and continue
                frame.clear_ref_bit();
                continue;
            }

            // Found a victim!
            return Some(FrameId::new(pos));
        }

        // All frames are pinned or have reference bits set
        None
    }

    /// Finds a free (empty) frame.
    ///
    /// This is faster than eviction since no disk I/O is needed.
    pub fn find_free_frame(&self, frames: &[Arc<BufferFrame>]) -> Option<FrameId> {
        // Start from clock hand position for better distribution
        let start = self.clock_hand.load(Ordering::Relaxed);

        for i in 0..self.num_frames {
            let pos = (start + i) % self.num_frames;
            let frame = &frames[pos];

            if frame.is_empty() && !frame.is_pinned() {
                return Some(FrameId::new(pos));
            }
        }

        None
    }

    /// Advances the clock hand and returns the previous position.
    #[inline]
    fn advance_hand(&self) -> usize {
        loop {
            let current = self.clock_hand.load(Ordering::Relaxed);
            let next = (current + 1) % self.num_frames;
            if self
                .clock_hand
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return current;
            }
            // CAS failed, retry
        }
    }

    /// Returns the current clock hand position.
    #[inline]
    pub fn hand_position(&self) -> usize {
        self.clock_hand.load(Ordering::Relaxed)
    }

    /// Resets the clock hand to position 0.
    pub fn reset(&self) {
        self.clock_hand.store(0, Ordering::Release);
    }
}

impl std::fmt::Debug for ClockReplacer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClockReplacer")
            .field("num_frames", &self.num_frames)
            .field("clock_hand", &self.hand_position())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::types::PageId;

    fn create_frames(count: usize) -> Vec<Arc<BufferFrame>> {
        (0..count)
            .map(|i| Arc::new(BufferFrame::new(FrameId::new(i), 8192)))
            .collect()
    }

    #[test]
    fn test_find_free_frame() {
        let frames = create_frames(10);
        let replacer = ClockReplacer::new(10);

        // All frames are empty, should find the first one
        let victim = replacer.find_free_frame(&frames);
        assert!(victim.is_some());
    }

    #[test]
    fn test_find_victim_all_empty() {
        let frames = create_frames(10);
        let replacer = ClockReplacer::new(10);

        // All frames are empty, no victim (nothing to evict)
        let victim = replacer.find_victim(&frames);
        assert!(victim.is_none());
    }

    #[test]
    fn test_find_victim_simple() {
        let frames = create_frames(10);
        let replacer = ClockReplacer::new(10);

        // Assign pages to some frames
        frames[0].set_page_id(PageId::new(0));
        frames[1].set_page_id(PageId::new(1));
        frames[2].set_page_id(PageId::new(2));

        // Clear reference bits (simulate no recent access)
        frames[0].clear_ref_bit();
        frames[1].clear_ref_bit();
        frames[2].clear_ref_bit();

        // Should find frame 0 as victim
        let victim = replacer.find_victim(&frames);
        assert_eq!(victim, Some(FrameId::new(0)));
    }

    #[test]
    fn test_find_victim_skips_pinned() {
        let frames = create_frames(10);
        let replacer = ClockReplacer::new(10);

        // Assign pages
        frames[0].set_page_id(PageId::new(0));
        frames[1].set_page_id(PageId::new(1));

        // Pin frame 0
        frames[0].pin();
        frames[0].clear_ref_bit();
        frames[1].clear_ref_bit();

        // Should skip frame 0 and find frame 1
        let victim = replacer.find_victim(&frames);
        assert_eq!(victim, Some(FrameId::new(1)));
    }

    #[test]
    fn test_find_victim_clears_ref_bit() {
        let frames = create_frames(10);
        let replacer = ClockReplacer::new(10);

        // Assign pages with reference bit set
        frames[0].set_page_id(PageId::new(0));
        frames[0].pin();
        frames[0].unpin(); // This sets ref_bit
        assert!(frames[0].ref_bit());

        frames[1].set_page_id(PageId::new(1));
        frames[1].clear_ref_bit();

        // First sweep clears frame 0's ref_bit, finds frame 1
        let victim = replacer.find_victim(&frames);
        assert_eq!(victim, Some(FrameId::new(1)));

        // Frame 0's ref_bit should now be cleared
        assert!(!frames[0].ref_bit());
    }

    #[test]
    fn test_find_victim_all_pinned() {
        let frames = create_frames(3);
        let replacer = ClockReplacer::new(3);

        // Assign and pin all frames
        for (i, frame) in frames.iter().enumerate() {
            frame.set_page_id(PageId::new(i as u64));
            frame.pin();
        }

        // No victim should be found
        let victim = replacer.find_victim(&frames);
        assert!(victim.is_none());
    }

    #[test]
    fn test_clock_hand_advances() {
        let frames = create_frames(10);
        let replacer = ClockReplacer::new(10);

        // Set up frames 0-4 with pages
        for i in 0..5 {
            frames[i].set_page_id(PageId::new(i as u64));
            frames[i].clear_ref_bit();
        }

        // Find victims and check hand advances
        let v1 = replacer.find_victim(&frames);
        assert_eq!(v1, Some(FrameId::new(0)));
        frames[0].set_page_id(PageId::INVALID); // Mark as evicted

        let v2 = replacer.find_victim(&frames);
        assert_eq!(v2, Some(FrameId::new(1)));
    }

    #[test]
    fn test_hand_wraps_around() {
        let frames = create_frames(3);
        let replacer = ClockReplacer::new(3);

        // Advance hand past the end
        for _ in 0..5 {
            replacer.advance_hand();
        }

        // Hand should wrap around
        assert!(replacer.hand_position() < 3);
    }
}
