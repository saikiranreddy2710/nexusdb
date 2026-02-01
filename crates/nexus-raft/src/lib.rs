//! # nexus-raft
//!
//! Raft++ consensus protocol for NexusDB.
//!
//! This crate implements enhanced Raft with:
//! - Parallel commit for reduced latency
//! - Leader leases for local reads
//! - Multi-Raft for sharding
//! - Pipelining for throughput
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                         RaftNode                            │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
//! │  │  Election   │  │ Replication │  │   State Machine     │ │
//! │  │  (Leader    │  │ (Log sync,  │  │   (Apply commands,  │ │
//! │  │   election) │  │  heartbeats)│  │    snapshots)       │ │
//! │  └─────────────┘  └─────────────┘  └─────────────────────┘ │
//! │                           │                                 │
//! │                    ┌──────┴──────┐                         │
//! │                    │  RaftLog    │                         │
//! │                    │ (Persistent │                         │
//! │                    │   log)      │                         │
//! │                    └─────────────┘                         │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use nexus_raft::{RaftNode, RaftConfig, StateMachine};
//!
//! // Create a Raft node
//! let config = RaftConfig::default();
//! let node = RaftNode::new(1, config, my_state_machine);
//!
//! // Process a tick (call periodically)
//! let messages = node.tick();
//!
//! // Handle incoming messages
//! let responses = node.handle_message(from, message);
//!
//! // Propose a command (leader only)
//! node.propose(command)?;
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Raft log storage.
pub mod log;

/// Raft node implementation.
pub mod node;

/// Log replication.
pub mod replication;

/// Leader election.
pub mod election;

/// State machine interface.
pub mod state_machine;

/// RPC message types.
pub mod rpc;

// Re-export key types for convenience
pub use log::RaftLog;
pub use node::{RaftConfig, RaftNode, Role};
pub use rpc::{
    AppendEntries, AppendResponse, InstallSnapshot, LogEntry, NodeId, OutboundMessage,
    RaftMessage, RequestVote, SnapshotResponse, Term, VoteResponse,
};
pub use state_machine::StateMachine;

/// A log index (position in the Raft log).
///
/// Index 0 is reserved for the "null" entry before the log starts.
/// The first real entry is at index 1.
pub type LogIndex = u64;

/// Result type for Raft operations.
pub type Result<T> = std::result::Result<T, RaftError>;

/// Errors that can occur in Raft operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftError {
    /// Not the leader, cannot accept proposals.
    NotLeader {
        /// The current leader, if known.
        leader_hint: Option<NodeId>,
    },
    /// The proposal channel is full.
    ProposalDropped,
    /// The log is compacted past the requested index.
    LogCompacted {
        /// The first available index.
        first_index: LogIndex,
    },
    /// Invalid configuration.
    InvalidConfig(String),
    /// Snapshot is being installed.
    SnapshotInProgress,
    /// Internal error.
    Internal(String),
}

impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftError::NotLeader { leader_hint } => {
                if let Some(leader) = leader_hint {
                    write!(f, "not leader, try node {}", leader)
                } else {
                    write!(f, "not leader, leader unknown")
                }
            }
            RaftError::ProposalDropped => write!(f, "proposal dropped"),
            RaftError::LogCompacted { first_index } => {
                write!(f, "log compacted, first available index: {}", first_index)
            }
            RaftError::InvalidConfig(msg) => write!(f, "invalid config: {}", msg),
            RaftError::SnapshotInProgress => write!(f, "snapshot in progress"),
            RaftError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for RaftError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_error_display() {
        let err = RaftError::NotLeader {
            leader_hint: Some(3),
        };
        assert!(err.to_string().contains("node 3"));

        let err = RaftError::LogCompacted { first_index: 100 };
        assert!(err.to_string().contains("100"));
    }
}
