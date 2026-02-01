//! Page layout and disk format for NexusDB.
//!
//! This module implements the on-disk page format for NexusDB's storage engine.
//! All data is organized into fixed-size pages (default 8KB) that can be:
//!
//! - **Data Pages**: Store key-value records in a slotted format
//! - **Index Pages**: Store B+tree internal nodes with separators and child pointers
//! - **Overflow Pages**: Store large values that don't fit in a single page
//! - **Free Pages**: Available for allocation
//!
//! # Page Format
//!
//! ```text
//! +------------------+
//! |   Page Header    |  32 bytes
//! +------------------+
//! |   Slot Array     |  grows downward (4 bytes per slot)
//! |        ↓         |
//! +------------------+
//! |   Free Space     |
//! +------------------+
//! |        ↑         |
//! |   Record Data    |  grows upward
//! +------------------+
//! ```
//!
//! The slot array and record data grow toward each other, maximizing space usage.

mod checksum;
mod header;
mod slotted;
mod types;

pub use checksum::{compute_checksum, compute_page_checksum, verify_checksum};
pub use header::{PageHeader, PAGE_HEADER_SIZE};
pub use slotted::{RecordRef, Slot, SlotId, SlottedPage};
pub use types::{PageFlags, PageType};

/// Default page size (8 KB).
pub const PAGE_SIZE: usize = nexus_common::constants::DEFAULT_PAGE_SIZE;

/// Minimum usable space in a page (after header and overhead).
pub const MIN_USABLE_SPACE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - 8;

/// Magic bytes for page validation.
pub const PAGE_MAGIC: u16 = nexus_common::constants::PAGE_MAGIC;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_constants() {
        assert!(PAGE_SIZE.is_power_of_two());
        assert!(MIN_USABLE_SPACE > 0);
        assert!(PAGE_HEADER_SIZE < PAGE_SIZE);
    }
}
