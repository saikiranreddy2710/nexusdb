//! SSTable (Sorted String Table) format for persistent key-value storage.
//!
//! An SSTable is an immutable, sorted file containing key-value entries.
//! It uses a block-based format with prefix compression, bloom filters
//! for fast negative lookups, and an index block for efficient seeks.
//!
//! ## File Layout
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │ Data Block 0                                  │
//! │ Data Block 0 Trailer (compression + checksum) │
//! │ Data Block 1                                  │
//! │ Data Block 1 Trailer                          │
//! │ ...                                           │
//! │ Data Block N                                  │
//! │ Data Block N Trailer                          │
//! ├──────────────────────────────────────────────┤
//! │ Filter Block (bloom filter)                   │
//! │ Filter Block Trailer                          │
//! ├──────────────────────────────────────────────┤
//! │ Meta-Index Block (filter block handle, etc.)  │
//! │ Meta-Index Block Trailer                      │
//! ├──────────────────────────────────────────────┤
//! │ Index Block (data block handles + last keys)  │
//! │ Index Block Trailer                           │
//! ├──────────────────────────────────────────────┤
//! │ Footer (48 bytes)                             │
//! └──────────────────────────────────────────────┘
//! ```

pub mod block;
pub mod footer;
pub mod reader;
pub mod writer;

pub use block::{BlockBuilder, BlockHandle, BlockReader};
pub use footer::{Footer, FOOTER_SIZE, TABLE_MAGIC};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

/// Metadata about an SSTable file.
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// Unique numeric identifier for this SSTable.
    pub id: u64,
    /// Level in the LSM tree (0 = freshly flushed).
    pub level: usize,
    /// Smallest key in the SSTable (user key only).
    pub smallest_key: Vec<u8>,
    /// Largest key in the SSTable (user key only).
    pub largest_key: Vec<u8>,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of data entries in the SSTable.
    pub entry_count: u64,
    /// Minimum sequence number of entries.
    pub min_sequence: u64,
    /// Maximum sequence number of entries.
    pub max_sequence: u64,
    /// Number of data blocks.
    pub data_block_count: u64,
}

impl SSTableInfo {
    /// Check if this SSTable's key range overlaps with a given range.
    pub fn overlaps(&self, smallest: &[u8], largest: &[u8]) -> bool {
        !(self.largest_key.as_slice() < smallest || self.smallest_key.as_slice() > largest)
    }

    /// Check if this SSTable might contain the given key.
    pub fn might_contain_key(&self, key: &[u8]) -> bool {
        key >= self.smallest_key.as_slice() && key <= self.largest_key.as_slice()
    }
}
