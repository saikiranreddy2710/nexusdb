//! SageTree - A novel B-tree storage engine for NexusDB.
//!
//! SageTree combines several advanced techniques to achieve superior performance:
//!
//! ## Key Features
//!
//! - **Bw-tree Style Delta Chains**: Updates are applied as delta records instead of
//!   in-place modifications, reducing write amplification by up to 10x compared to
//!   traditional LSM trees.
//!
//! - **Fractional Cascading**: Range queries benefit from hints passed between
//!   sibling nodes, reducing the search time within each leaf.
//!
//! - **MVCC Support**: Each entry carries version information for snapshot isolation
//!   and multi-version concurrency control.
//!
//! - **Lock-Free Reads**: Delta chains allow reads to proceed without taking locks,
//!   improving read concurrency.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                      SageTree                           │
//! │  ┌─────────────────────────────────────────────────┐   │
//! │  │               Internal Nodes                     │   │
//! │  │   [key1 | key2 | key3 | ...]                   │   │
//! │  │   /    \      |       \                         │   │
//! │  └─────────────────────────────────────────────────┘   │
//! │  ┌─────────────────────────────────────────────────┐   │
//! │  │            Leaf Nodes with Delta Chains          │   │
//! │  │   ┌─────────┐   ┌─────────┐   ┌─────────┐      │   │
//! │  │   │ Delta 3 │──▶│ Delta 2 │──▶│ Delta 1 │──▶Base │  │
//! │  │   └─────────┘   └─────────┘   └─────────┘      │   │
//! │  └─────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use nexus_storage::sagetree::{SageTree, SageTreeConfig, KeyRange};
//! use nexus_common::types::{Key, Value};
//!
//! // Create a tree with default configuration
//! let tree = SageTree::new();
//!
//! // Insert key-value pairs
//! tree.insert(Key::from_str("user:1"), Value::from_str("Alice")).unwrap();
//! tree.insert(Key::from_str("user:2"), Value::from_str("Bob")).unwrap();
//!
//! // Point lookup
//! let value = tree.get(&Key::from_str("user:1")).unwrap();
//! assert_eq!(value.unwrap().as_bytes(), b"Alice");
//!
//! // Range scan
//! let range = KeyRange::prefix(Key::from_str("user:"));
//! let users = tree.scan(range).unwrap();
//! assert_eq!(users.len(), 2);
//!
//! // Update
//! tree.update(&Key::from_str("user:1"), Value::from_str("Alice Smith")).unwrap();
//!
//! // Delete
//! tree.delete(&Key::from_str("user:2")).unwrap();
//! ```
//!
//! ## Configuration
//!
//! The tree can be configured with various parameters:
//!
//! ```rust,ignore
//! let config = SageTreeConfig::new()
//!     .with_page_size(16384)           // 16KB pages
//!     .with_branching_factor(256)       // Max children per node
//!     .with_fill_factor(0.75)           // Target 75% fill
//!     .with_max_delta_chain_length(8)   // Consolidate after 8 deltas
//!     .with_delta_chains(true)          // Enable delta chains
//!     .with_fractional_cascading(true); // Enable range query optimization
//!
//! let tree = SageTree::with_config(config);
//! ```

mod config;
mod cursor;
mod delta;
mod error;
mod node;
mod tree;

// Re-export main types
pub use config::SageTreeConfig;
pub use cursor::{
    CascadingHint, CursorEntry, CursorPosition, CursorState, CursorStats, Direction, KeyRange,
    LeafCursor, TraversalFrame, TraversalPath,
};
pub use delta::{DeltaChain, DeltaNode, DeltaRecord, DeltaType, SplitInfo, DELTA_MAGIC};
pub use error::{SageTreeError, SageTreeResult};
pub use node::{
    InternalEntry, InternalNode, LeafEntry, LeafNode, Node, NodeFlags, NodeHeader, NodeType,
    VersionInfo, NODE_HEADER_SIZE, NODE_MAGIC, VERSION_INFO_SIZE,
};
pub use tree::{PageAllocator, SageTree, TreeStats};
