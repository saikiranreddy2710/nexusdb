//! # nexus-common
//!
//! Common types, errors, and utilities for NexusDB.
//!
//! This crate provides the foundational types and abstractions used across
//! all NexusDB components. It includes:
//!
//! - **Types**: Core identifiers (`PageId`, `TxnId`, `LSN`), keys, values, and timestamps
//! - **Errors**: Unified error handling with `NexusError`
//! - **Config**: Database configuration structures
//! - **Constants**: System-wide constants and limits
//!
//! ## Example
//!
//! ```rust
//! use nexus_common::types::{PageId, TxnId, Key, Value};
//! use nexus_common::error::NexusResult;
//!
//! fn example() -> NexusResult<()> {
//!     let page_id = PageId::new(42);
//!     let txn_id = TxnId::new(1);
//!     let key = Key::from_bytes(b"hello");
//!     let value = Value::from_bytes(b"world");
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod constants;
pub mod config;
pub mod error;
pub mod types;

// Re-export commonly used items at the crate root
pub use constants::*;
pub use error::{NexusError, NexusResult};
pub use types::{
    Key, Lsn, NodeId, PageId, Timestamp, TxnId, Value,
};
