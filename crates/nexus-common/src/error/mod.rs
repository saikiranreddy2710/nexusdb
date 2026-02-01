//! Error handling for NexusDB.
//!
//! This module provides a unified error type and result alias used
//! across all NexusDB components.

mod database;

pub use database::{ErrorCode, NexusError};

/// Result type alias for NexusDB operations.
pub type NexusResult<T> = std::result::Result<T, NexusError>;
