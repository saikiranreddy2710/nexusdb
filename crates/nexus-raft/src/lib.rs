//! # nexus-raft
//!
//! Raft++ consensus protocol for NexusDB.
//!
//! This crate implements enhanced Raft with:
//! - Parallel commit for reduced latency
//! - Leader leases for local reads
//! - Multi-Raft for sharding
//! - Pipelining for throughput

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Raft node implementation
pub mod node;

/// Log replication
pub mod replication;

/// Leader election
pub mod election;

/// State machine interface
pub mod state_machine;

/// RPC message types
pub mod rpc;
