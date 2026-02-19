//! Caching layer for the LSM-tree engine.
//!
//! Provides two levels of caching to minimize disk I/O:
//!
//! - **BlockCache**: Sharded LRU cache for SSTable data blocks.
//!   Eliminates repeated reads of the same block from different queries.
//!
//! - **TableCache**: LRU cache of open SSTableReader handles.
//!   Eliminates repeated file open/footer-read/index-parse per SSTable.
//!
//! ```text
//!   LsmEngine::get(key)
//!     │
//!     ├── MemTable lookup (in-memory)
//!     │
//!     └── SSTable lookup:
//!           │
//!           ├── TableCache.get_or_open(file_id)
//!           │     └── Opens file once, caches reader
//!           │
//!           └── SSTableReader.get(key)
//!                 └── BlockCache.get(file_id, offset)
//!                       └── Reads block once, caches data
//! ```

pub mod block_cache;
pub mod table_cache;

pub use block_cache::{BlockCache, BlockCacheKey, BlockCacheStats, CachedBlock};
pub use table_cache::{TableCache, TableCacheStats};
