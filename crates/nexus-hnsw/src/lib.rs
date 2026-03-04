//! `nexus-hnsw` — HNSW vector index for NexusDB.
//!
//! Provides an in-memory Hierarchical Navigable Small World (HNSW) graph
//! for approximate nearest-neighbor search on high-dimensional vectors.
//!
//! # Features
//!
//! - **Multiple distance metrics**: L2, Cosine, Inner Product, Manhattan
//! - **Configurable parameters**: M, ef_construction, ef_search
//! - **Thread-safe**: concurrent reads, exclusive writes via `RwLock`
//! - **Serialization**: save / load index to/from bytes
//! - **Soft-delete**: tombstone-based deletion
//!
//! # Example
//!
//! ```
//! use nexus_hnsw::{HnswIndex, HnswConfig, DistanceMetric};
//!
//! let config = HnswConfig::new(3, DistanceMetric::L2);
//! let index = HnswIndex::new(config).unwrap();
//!
//! index.insert(1, &[1.0, 0.0, 0.0]).unwrap();
//! index.insert(2, &[0.0, 1.0, 0.0]).unwrap();
//! index.insert(3, &[0.0, 0.0, 1.0]).unwrap();
//!
//! let results = index.search(&[0.9, 0.1, 0.0], 2).unwrap();
//! assert_eq!(results[0].id, 1); // closest to [1,0,0]
//! ```

pub mod distance;
pub mod error;
pub mod hnsw;

// Re-exports for convenience.
pub use distance::DistanceMetric;
pub use error::{HnswError, HnswResult};
pub use hnsw::{HnswConfig, HnswIndex, HnswStats, SearchResult, VectorId};
