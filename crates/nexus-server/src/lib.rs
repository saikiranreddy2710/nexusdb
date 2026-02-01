//! # nexus-server
//!
//! Network server for NexusDB.
//!
//! This crate implements:
//! - gRPC server interface
//! - Connection management
//! - Query routing
//! - Cluster coordination

#![warn(missing_docs)]
#![warn(clippy::all)]

/// gRPC service implementation
pub mod grpc;

/// Connection pool
pub mod connection;

/// Request routing
pub mod router;

/// Server configuration
pub mod config;
