//! # nexus-kv: LSM-tree Key-Value Storage Engine
//!
//! A high-performance, log-structured merge-tree (LSM-tree) key-value storage
//! engine for NexusDB. Designed for high write throughput with efficient reads
//! through bloom filters, block caching, and leveled compaction.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────┐
//! │                  Write Path                       │
//! │  PUT(k,v) → WAL → MemTable (Skip List)          │
//! │                     │                             │
//! │            (when full, freeze)                    │
//! │                     ▼                             │
//! │          Immutable MemTable                       │
//! │                     │                             │
//! │            (background flush)                     │
//! │                     ▼                             │
//! │          SSTable (Level 0)                        │
//! │                     │                             │
//! │         (background compaction)                   │
//! │                     ▼                             │
//! │   L0 → L1 → L2 → ... → Lmax                     │
//! └──────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────┐
//! │                  Read Path                        │
//! │  GET(k) → MemTable                               │
//! │         → Immutable MemTables                     │
//! │         → L0 SSTables (bloom filter check)       │
//! │         → L1 SSTables (binary search + bloom)    │
//! │         → L2 SSTables ...                        │
//! │         → Lmax SSTables                          │
//! └──────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! - **MemTable**: Concurrent skip list for in-memory writes
//! - **SSTable**: Sorted String Table with block-based format
//! - **Bloom Filter**: Per-SSTable filter for fast negative lookups
//! - **Compaction**: Background merge of SSTables across levels
//! - **Manifest**: Version tracking for atomic state transitions
//! - **Iterator**: Merge iterator across all levels for range scans

pub mod cache;
pub mod compaction;
pub mod config;
pub mod engine;
pub mod error;
pub mod iterator;
pub mod manifest;
pub mod memtable;
pub mod sstable;

pub use cache::{BlockCache, TableCache};
pub use config::{CompactionStrategy, CompressionType, LsmConfig};
pub use engine::LsmEngine;
pub use error::{KvError, KvResult};
