//! Type definitions for NexusDB.
//!
//! This module contains all core type definitions used across the database.

mod ids;
mod keys;
mod timestamps;

pub use ids::{Lsn, NodeId, PageId, TxnId};
pub use keys::{Key, Value, MAX_KEY_SIZE, MAX_VALUE_SIZE};
pub use timestamps::{Epoch, HlcTimestamp, Timestamp};
