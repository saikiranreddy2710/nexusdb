//! Cluster membership and configuration changes.
//!
//! This module implements Raft's joint consensus protocol for safe membership
//! changes. It supports adding and removing nodes from the cluster without
//! risking split-brain scenarios.
//!
//! # Joint Consensus Protocol
//!
//! When changing membership, we use a two-phase protocol:
//!
//! 1. **Transition phase**: The cluster operates under joint consensus where
//!    both the old and new configurations must agree. Majorities are required
//!    from BOTH configurations.
//!
//! 2. **Commit phase**: Once the joint configuration is committed, the cluster
//!    transitions to the new configuration alone.
//!
//! ```text
//! Old Config ──▶ Joint Config (Cold ∪ Cnew) ──▶ New Config
//!     │                  │                           │
//!     └── requires ──────┴── both majorities ────────┘
//!            majority
//! ```
//!
//! # Single-server changes
//!
//! As an optimization, single server additions/removals can use a simpler
//! protocol that doesn't require joint consensus, since adding or removing
//! one server at a time maintains the intersection property.
//!
//! # Usage
//!
//! ```ignore
//! use nexus_raft::membership::{Configuration, MembershipChange};
//!
//! // Current configuration
//! let config = Configuration::new(vec![1, 2, 3]);
//!
//! // Propose adding node 4
//! let change = MembershipChange::AddNode(4);
//! let new_config = config.apply_change(change)?;
//! ```

use std::collections::{HashMap, HashSet};
use std::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::rpc::NodeId;
use crate::{LogIndex, RaftError, Result};

/// A cluster configuration.
///
/// Represents the set of nodes that form the Raft cluster at a given point
/// in time. Configurations can be in one of two states:
///
/// - **Simple**: A single set of voters
/// - **Joint**: Two sets of voters (old and new) operating under joint consensus
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Configuration {
    /// The current (or new) set of voters.
    pub voters: HashSet<NodeId>,
    /// The old set of voters (only during joint consensus).
    pub voters_outgoing: HashSet<NodeId>,
    /// Non-voting learners (receive log entries but don't vote).
    pub learners: HashSet<NodeId>,
    /// The log index where this configuration was committed.
    pub committed_index: LogIndex,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            voters: HashSet::new(),
            voters_outgoing: HashSet::new(),
            learners: HashSet::new(),
            committed_index: 0,
        }
    }
}

impl Configuration {
    /// Creates a new simple configuration with the given voters.
    pub fn new(voters: Vec<NodeId>) -> Self {
        Self {
            voters: voters.into_iter().collect(),
            voters_outgoing: HashSet::new(),
            learners: HashSet::new(),
            committed_index: 0,
        }
    }

    /// Creates a configuration with voters and learners.
    pub fn with_learners(voters: Vec<NodeId>, learners: Vec<NodeId>) -> Self {
        Self {
            voters: voters.into_iter().collect(),
            voters_outgoing: HashSet::new(),
            learners: learners.into_iter().collect(),
            committed_index: 0,
        }
    }

    /// Returns true if this is a joint configuration (transitioning).
    pub fn is_joint(&self) -> bool {
        !self.voters_outgoing.is_empty()
    }

    /// Returns true if the given node is a voter.
    pub fn is_voter(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id) || self.voters_outgoing.contains(&node_id)
    }

    /// Returns true if the given node is a learner.
    pub fn is_learner(&self, node_id: NodeId) -> bool {
        self.learners.contains(&node_id)
    }

    /// Returns true if the given node is in the configuration at all.
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.is_voter(node_id) || self.is_learner(node_id)
    }

    /// Returns all voters (from both configurations if joint).
    pub fn all_voters(&self) -> HashSet<NodeId> {
        let mut all = self.voters.clone();
        all.extend(&self.voters_outgoing);
        all
    }

    /// Returns all nodes (voters and learners).
    pub fn all_nodes(&self) -> HashSet<NodeId> {
        let mut all = self.all_voters();
        all.extend(&self.learners);
        all
    }

    /// Returns the number of voters in the current configuration.
    pub fn voter_count(&self) -> usize {
        self.voters.len()
    }

    /// Calculates the majority needed for the current configuration.
    pub fn quorum_size(&self) -> usize {
        quorum_size(self.voters.len())
    }

    /// Calculates the majority needed for the outgoing configuration.
    pub fn quorum_size_outgoing(&self) -> usize {
        quorum_size(self.voters_outgoing.len())
    }

    /// Checks if a set of nodes forms a quorum.
    ///
    /// For joint configurations, requires majorities from BOTH configurations.
    pub fn has_quorum(&self, nodes: &HashSet<NodeId>) -> bool {
        let incoming_votes = self.voters.intersection(nodes).count();
        let has_incoming_majority = incoming_votes >= self.quorum_size();

        if self.is_joint() {
            let outgoing_votes = self.voters_outgoing.intersection(nodes).count();
            let has_outgoing_majority = outgoing_votes >= self.quorum_size_outgoing();
            has_incoming_majority && has_outgoing_majority
        } else {
            has_incoming_majority
        }
    }

    /// Checks if a given vote count forms a quorum.
    ///
    /// This is a convenience method when you only have counts, not node sets.
    /// For joint configurations, use `has_quorum` with actual node sets.
    pub fn has_quorum_count(&self, count: usize) -> bool {
        count >= self.quorum_size()
    }

    /// Creates a joint configuration for transitioning to a new set of voters.
    pub fn enter_joint(&self, new_voters: HashSet<NodeId>) -> Self {
        Self {
            voters: new_voters,
            voters_outgoing: self.voters.clone(),
            learners: self.learners.clone(),
            committed_index: 0, // Will be set when committed
        }
    }

    /// Exits joint consensus, keeping only the new configuration.
    pub fn leave_joint(&self) -> Self {
        Self {
            voters: self.voters.clone(),
            voters_outgoing: HashSet::new(),
            learners: self.learners.clone(),
            committed_index: 0, // Will be set when committed
        }
    }

    /// Applies a membership change, returning the new configuration.
    ///
    /// For single-node changes (add/remove one node), this uses a simplified
    /// protocol. For multi-node changes, it enters joint consensus.
    pub fn apply_change(&self, change: MembershipChange) -> Result<Self> {
        // Cannot apply changes during joint consensus
        if self.is_joint() {
            return Err(RaftError::ConfigChangeInProgress);
        }

        match change {
            MembershipChange::AddNode(node_id) => {
                if self.is_voter(node_id) {
                    return Err(RaftError::NodeAlreadyExists(node_id));
                }
                let mut new_voters = self.voters.clone();
                new_voters.insert(node_id);
                // Remove from learners if present
                let mut new_learners = self.learners.clone();
                new_learners.remove(&node_id);

                Ok(Self {
                    voters: new_voters,
                    voters_outgoing: HashSet::new(),
                    learners: new_learners,
                    committed_index: 0,
                })
            }
            MembershipChange::RemoveNode(node_id) => {
                if !self.is_voter(node_id) {
                    return Err(RaftError::NodeNotFound(node_id));
                }
                if self.voters.len() == 1 {
                    return Err(RaftError::LastNode);
                }
                let mut new_voters = self.voters.clone();
                new_voters.remove(&node_id);

                Ok(Self {
                    voters: new_voters,
                    voters_outgoing: HashSet::new(),
                    learners: self.learners.clone(),
                    committed_index: 0,
                })
            }
            MembershipChange::AddLearner(node_id) => {
                if self.contains(node_id) {
                    return Err(RaftError::NodeAlreadyExists(node_id));
                }
                let mut new_learners = self.learners.clone();
                new_learners.insert(node_id);

                Ok(Self {
                    voters: self.voters.clone(),
                    voters_outgoing: HashSet::new(),
                    learners: new_learners,
                    committed_index: 0,
                })
            }
            MembershipChange::RemoveLearner(node_id) => {
                if !self.is_learner(node_id) {
                    return Err(RaftError::NodeNotFound(node_id));
                }
                let mut new_learners = self.learners.clone();
                new_learners.remove(&node_id);

                Ok(Self {
                    voters: self.voters.clone(),
                    voters_outgoing: HashSet::new(),
                    learners: new_learners,
                    committed_index: 0,
                })
            }
            MembershipChange::PromoteLearner(node_id) => {
                if !self.is_learner(node_id) {
                    return Err(RaftError::NodeNotFound(node_id));
                }
                let mut new_voters = self.voters.clone();
                new_voters.insert(node_id);
                let mut new_learners = self.learners.clone();
                new_learners.remove(&node_id);

                Ok(Self {
                    voters: new_voters,
                    voters_outgoing: HashSet::new(),
                    learners: new_learners,
                    committed_index: 0,
                })
            }
            MembershipChange::ReplaceConfiguration(new_voters) => {
                if new_voters.is_empty() {
                    return Err(RaftError::EmptyConfiguration);
                }
                // This requires joint consensus
                let new_voters_set: HashSet<_> = new_voters.into_iter().collect();
                Ok(self.enter_joint(new_voters_set))
            }
        }
    }

    /// Serializes the configuration to bytes.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Version byte
        buf.put_u8(1);

        // Voters
        buf.put_u32_le(self.voters.len() as u32);
        for &v in &self.voters {
            buf.put_u64_le(v);
        }

        // Outgoing voters
        buf.put_u32_le(self.voters_outgoing.len() as u32);
        for &v in &self.voters_outgoing {
            buf.put_u64_le(v);
        }

        // Learners
        buf.put_u32_le(self.learners.len() as u32);
        for &l in &self.learners {
            buf.put_u64_le(l);
        }

        // Committed index
        buf.put_u64_le(self.committed_index);

        buf.freeze()
    }

    /// Deserializes a configuration from bytes.
    pub fn from_bytes(mut data: Bytes) -> Result<Self> {
        if data.is_empty() {
            return Err(RaftError::InvalidConfiguration("empty data".into()));
        }

        let version = data.get_u8();
        if version != 1 {
            return Err(RaftError::InvalidConfiguration(format!(
                "unsupported version: {}",
                version
            )));
        }

        // Voters
        let voters_len = data.get_u32_le() as usize;
        let mut voters = HashSet::with_capacity(voters_len);
        for _ in 0..voters_len {
            voters.insert(data.get_u64_le());
        }

        // Outgoing voters
        let outgoing_len = data.get_u32_le() as usize;
        let mut voters_outgoing = HashSet::with_capacity(outgoing_len);
        for _ in 0..outgoing_len {
            voters_outgoing.insert(data.get_u64_le());
        }

        // Learners
        let learners_len = data.get_u32_le() as usize;
        let mut learners = HashSet::with_capacity(learners_len);
        for _ in 0..learners_len {
            learners.insert(data.get_u64_le());
        }

        // Committed index
        let committed_index = data.get_u64_le();

        Ok(Self {
            voters,
            voters_outgoing,
            learners,
            committed_index,
        })
    }
}

impl fmt::Display for Configuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let voters: Vec<_> = self.voters.iter().collect();
        if self.is_joint() {
            let outgoing: Vec<_> = self.voters_outgoing.iter().collect();
            write!(f, "Joint({:?} <- {:?})", voters, outgoing)
        } else {
            write!(f, "Config({:?})", voters)
        }
    }
}

/// A membership change request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipChange {
    /// Add a new voter to the cluster.
    AddNode(NodeId),
    /// Remove a voter from the cluster.
    RemoveNode(NodeId),
    /// Add a non-voting learner.
    AddLearner(NodeId),
    /// Remove a learner.
    RemoveLearner(NodeId),
    /// Promote a learner to voter.
    PromoteLearner(NodeId),
    /// Replace the entire configuration (requires joint consensus).
    ReplaceConfiguration(Vec<NodeId>),
}

impl MembershipChange {
    /// Serializes the change to bytes.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        match self {
            MembershipChange::AddNode(id) => {
                buf.put_u8(0);
                buf.put_u64_le(*id);
            }
            MembershipChange::RemoveNode(id) => {
                buf.put_u8(1);
                buf.put_u64_le(*id);
            }
            MembershipChange::AddLearner(id) => {
                buf.put_u8(2);
                buf.put_u64_le(*id);
            }
            MembershipChange::RemoveLearner(id) => {
                buf.put_u8(3);
                buf.put_u64_le(*id);
            }
            MembershipChange::PromoteLearner(id) => {
                buf.put_u8(4);
                buf.put_u64_le(*id);
            }
            MembershipChange::ReplaceConfiguration(nodes) => {
                buf.put_u8(5);
                buf.put_u32_le(nodes.len() as u32);
                for &id in nodes {
                    buf.put_u64_le(id);
                }
            }
        }

        buf.freeze()
    }

    /// Deserializes a change from bytes.
    pub fn from_bytes(mut data: Bytes) -> Result<Self> {
        if data.is_empty() {
            return Err(RaftError::InvalidConfiguration("empty change".into()));
        }

        let change_type = data.get_u8();
        match change_type {
            0 => Ok(MembershipChange::AddNode(data.get_u64_le())),
            1 => Ok(MembershipChange::RemoveNode(data.get_u64_le())),
            2 => Ok(MembershipChange::AddLearner(data.get_u64_le())),
            3 => Ok(MembershipChange::RemoveLearner(data.get_u64_le())),
            4 => Ok(MembershipChange::PromoteLearner(data.get_u64_le())),
            5 => {
                let len = data.get_u32_le() as usize;
                let mut nodes = Vec::with_capacity(len);
                for _ in 0..len {
                    nodes.push(data.get_u64_le());
                }
                Ok(MembershipChange::ReplaceConfiguration(nodes))
            }
            _ => Err(RaftError::InvalidConfiguration(format!(
                "unknown change type: {}",
                change_type
            ))),
        }
    }
}

impl fmt::Display for MembershipChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MembershipChange::AddNode(id) => write!(f, "AddNode({})", id),
            MembershipChange::RemoveNode(id) => write!(f, "RemoveNode({})", id),
            MembershipChange::AddLearner(id) => write!(f, "AddLearner({})", id),
            MembershipChange::RemoveLearner(id) => write!(f, "RemoveLearner({})", id),
            MembershipChange::PromoteLearner(id) => write!(f, "PromoteLearner({})", id),
            MembershipChange::ReplaceConfiguration(nodes) => {
                write!(f, "ReplaceConfig({:?})", nodes)
            }
        }
    }
}

/// Tracks the progress of a configuration change.
#[derive(Debug, Clone)]
pub struct ChangeProgress {
    /// The configuration change being applied.
    pub change: MembershipChange,
    /// The log index of the configuration entry.
    pub config_index: LogIndex,
    /// Whether the configuration has been committed.
    pub committed: bool,
    /// For joint consensus: whether we're in the second phase.
    pub in_joint: bool,
}

impl ChangeProgress {
    /// Creates a new change progress tracker.
    pub fn new(change: MembershipChange, config_index: LogIndex) -> Self {
        Self {
            change,
            config_index,
            committed: false,
            in_joint: false,
        }
    }
}

/// Configuration manager for handling membership changes.
#[derive(Debug)]
pub struct ConfigurationManager {
    /// The current configuration.
    current: Configuration,
    /// Pending configuration change (if any).
    pending_change: Option<ChangeProgress>,
    /// History of configuration changes for debugging.
    history: Vec<(LogIndex, Configuration)>,
}

impl ConfigurationManager {
    /// Creates a new configuration manager.
    pub fn new(initial_config: Configuration) -> Self {
        Self {
            current: initial_config,
            pending_change: None,
            history: Vec::new(),
        }
    }

    /// Returns the current configuration.
    pub fn current(&self) -> &Configuration {
        &self.current
    }

    /// Returns true if a configuration change is in progress.
    pub fn has_pending_change(&self) -> bool {
        self.pending_change.is_some()
    }

    /// Returns the pending change, if any.
    pub fn pending_change(&self) -> Option<&ChangeProgress> {
        self.pending_change.as_ref()
    }

    /// Proposes a membership change.
    ///
    /// Returns the new configuration if the change can be applied.
    pub fn propose_change(&mut self, change: MembershipChange) -> Result<Configuration> {
        if self.pending_change.is_some() {
            return Err(RaftError::ConfigChangeInProgress);
        }

        self.current.apply_change(change)
    }

    /// Starts a configuration change.
    ///
    /// Call this after the configuration entry has been appended to the log.
    pub fn start_change(
        &mut self,
        change: MembershipChange,
        new_config: Configuration,
        log_index: LogIndex,
    ) {
        let in_joint = new_config.is_joint();
        self.pending_change = Some(ChangeProgress {
            change,
            config_index: log_index,
            committed: false,
            in_joint,
        });
        self.current = new_config;
    }

    /// Called when the configuration entry is committed.
    pub fn on_config_committed(&mut self, commit_index: LogIndex) {
        if let Some(ref mut progress) = self.pending_change {
            if progress.config_index <= commit_index && !progress.committed {
                progress.committed = true;
                self.current.committed_index = progress.config_index;

                // Record in history
                self.history
                    .push((progress.config_index, self.current.clone()));

                // If not in joint consensus, change is complete
                if !progress.in_joint {
                    self.pending_change = None;
                }
            }
        }
    }

    /// Completes joint consensus by transitioning to the new configuration.
    ///
    /// Returns the final configuration entry to append to the log.
    pub fn complete_joint_consensus(&mut self) -> Option<Configuration> {
        if let Some(ref progress) = self.pending_change {
            if progress.committed && progress.in_joint {
                let final_config = self.current.leave_joint();
                return Some(final_config);
            }
        }
        None
    }

    /// Applies the final (non-joint) configuration.
    pub fn apply_final_config(&mut self, config: Configuration, log_index: LogIndex) {
        self.current = config;
        self.current.committed_index = log_index;
        self.pending_change = None;

        self.history.push((log_index, self.current.clone()));
    }

    /// Restores the configuration from a snapshot.
    pub fn restore(&mut self, config: Configuration) {
        self.current = config;
        self.pending_change = None;
        self.history.clear();
    }

    /// Returns the configuration at a given log index.
    pub fn config_at(&self, index: LogIndex) -> Option<&Configuration> {
        // Search history in reverse order
        for (idx, config) in self.history.iter().rev() {
            if *idx <= index {
                return Some(config);
            }
        }
        None
    }
}

/// Quorum tracker for counting votes with joint consensus support.
#[derive(Debug)]
pub struct QuorumTracker {
    /// Configuration for quorum calculation.
    config: Configuration,
    /// Votes received (node_id -> voted).
    votes: HashMap<NodeId, bool>,
}

impl QuorumTracker {
    /// Creates a new quorum tracker.
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            votes: HashMap::new(),
        }
    }

    /// Records a vote from a node.
    pub fn record_vote(&mut self, node_id: NodeId, granted: bool) {
        self.votes.insert(node_id, granted);
    }

    /// Returns true if we have achieved quorum.
    pub fn has_quorum(&self) -> bool {
        let granted: HashSet<NodeId> = self
            .votes
            .iter()
            .filter_map(|(&id, &granted)| if granted { Some(id) } else { None })
            .collect();

        self.config.has_quorum(&granted)
    }

    /// Returns the number of positive votes.
    pub fn granted_count(&self) -> usize {
        self.votes.values().filter(|&&v| v).count()
    }

    /// Returns the number of negative votes.
    pub fn rejected_count(&self) -> usize {
        self.votes.values().filter(|&&v| !v).count()
    }

    /// Returns true if quorum is impossible (too many rejections).
    pub fn is_rejected(&self) -> bool {
        // For simple configs, check if too many rejections
        if !self.config.is_joint() {
            let _rejected = self.rejected_count();
            let remaining = self.config.voter_count() - self.votes.len();
            let max_possible = self.granted_count() + remaining;
            return max_possible < self.config.quorum_size();
        }

        // For joint configs, check both quorums
        // This is more complex - for now, be conservative
        false
    }
}

/// Calculates the quorum size for a given number of voters.
fn quorum_size(voters: usize) -> usize {
    voters / 2 + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_configuration_new() {
        let config = Configuration::new(vec![1, 2, 3]);
        assert_eq!(config.voter_count(), 3);
        assert!(!config.is_joint());
        assert!(config.is_voter(1));
        assert!(config.is_voter(2));
        assert!(config.is_voter(3));
        assert!(!config.is_voter(4));
    }

    #[test]
    fn test_quorum_size() {
        assert_eq!(quorum_size(1), 1);
        assert_eq!(quorum_size(2), 2);
        assert_eq!(quorum_size(3), 2);
        assert_eq!(quorum_size(4), 3);
        assert_eq!(quorum_size(5), 3);
    }

    #[test]
    fn test_has_quorum_simple() {
        let config = Configuration::new(vec![1, 2, 3]);

        // 2 out of 3 is quorum
        let votes: HashSet<_> = vec![1, 2].into_iter().collect();
        assert!(config.has_quorum(&votes));

        // 1 out of 3 is not quorum
        let votes: HashSet<_> = vec![1].into_iter().collect();
        assert!(!config.has_quorum(&votes));
    }

    #[test]
    fn test_joint_consensus_quorum() {
        let old_config = Configuration::new(vec![1, 2, 3]);
        let new_voters: HashSet<_> = vec![1, 2, 4].into_iter().collect();
        let joint = old_config.enter_joint(new_voters);

        assert!(joint.is_joint());

        // Need majority of both {1,2,3} and {1,2,4}
        // {1,2} satisfies both
        let votes: HashSet<_> = vec![1, 2].into_iter().collect();
        assert!(joint.has_quorum(&votes));

        // {1,4} only satisfies new config, not old
        let votes: HashSet<_> = vec![1, 4].into_iter().collect();
        assert!(!joint.has_quorum(&votes));

        // {2,3} only satisfies old config, not new
        let votes: HashSet<_> = vec![2, 3].into_iter().collect();
        assert!(!joint.has_quorum(&votes));
    }

    #[test]
    fn test_leave_joint() {
        let old = Configuration::new(vec![1, 2, 3]);
        let joint = old.enter_joint(vec![1, 2, 4].into_iter().collect());
        let new = joint.leave_joint();

        assert!(!new.is_joint());
        assert!(new.is_voter(1));
        assert!(new.is_voter(2));
        assert!(new.is_voter(4));
        assert!(!new.is_voter(3));
    }

    #[test]
    fn test_add_node() {
        let config = Configuration::new(vec![1, 2, 3]);
        let new_config = config.apply_change(MembershipChange::AddNode(4)).unwrap();

        assert!(new_config.is_voter(4));
        assert_eq!(new_config.voter_count(), 4);
    }

    #[test]
    fn test_remove_node() {
        let config = Configuration::new(vec![1, 2, 3]);
        let new_config = config
            .apply_change(MembershipChange::RemoveNode(3))
            .unwrap();

        assert!(!new_config.is_voter(3));
        assert_eq!(new_config.voter_count(), 2);
    }

    #[test]
    fn test_remove_last_node_fails() {
        let config = Configuration::new(vec![1]);
        let result = config.apply_change(MembershipChange::RemoveNode(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_add_learner() {
        let config = Configuration::new(vec![1, 2, 3]);
        let new_config = config
            .apply_change(MembershipChange::AddLearner(4))
            .unwrap();

        assert!(new_config.is_learner(4));
        assert!(!new_config.is_voter(4));
    }

    #[test]
    fn test_promote_learner() {
        let config = Configuration::with_learners(vec![1, 2, 3], vec![4]);
        let new_config = config
            .apply_change(MembershipChange::PromoteLearner(4))
            .unwrap();

        assert!(new_config.is_voter(4));
        assert!(!new_config.is_learner(4));
    }

    #[test]
    fn test_configuration_serialization() {
        let config = Configuration::with_learners(vec![1, 2, 3], vec![4, 5]);
        let bytes = config.to_bytes();
        let restored = Configuration::from_bytes(bytes).unwrap();

        assert_eq!(config.voters, restored.voters);
        assert_eq!(config.learners, restored.learners);
    }

    #[test]
    fn test_membership_change_serialization() {
        let changes = vec![
            MembershipChange::AddNode(42),
            MembershipChange::RemoveNode(7),
            MembershipChange::AddLearner(99),
            MembershipChange::ReplaceConfiguration(vec![1, 2, 3, 4, 5]),
        ];

        for change in changes {
            let bytes = change.to_bytes();
            let restored = MembershipChange::from_bytes(bytes).unwrap();
            assert_eq!(change, restored);
        }
    }

    #[test]
    fn test_config_manager() {
        let initial = Configuration::new(vec![1, 2, 3]);
        let mut manager = ConfigurationManager::new(initial);

        assert!(!manager.has_pending_change());

        // Propose adding node 4
        let new_config = manager
            .propose_change(MembershipChange::AddNode(4))
            .unwrap();
        manager.start_change(MembershipChange::AddNode(4), new_config, 10);

        assert!(manager.has_pending_change());
        assert!(manager.current().is_voter(4));

        // Commit the change
        manager.on_config_committed(10);

        assert!(!manager.has_pending_change());
    }

    #[test]
    fn test_quorum_tracker() {
        let config = Configuration::new(vec![1, 2, 3]);
        let mut tracker = QuorumTracker::new(config);

        // Vote from self
        tracker.record_vote(1, true);
        assert!(!tracker.has_quorum());

        // Vote from peer 2
        tracker.record_vote(2, true);
        assert!(tracker.has_quorum());
    }

    #[test]
    fn test_cannot_change_during_joint() {
        let old = Configuration::new(vec![1, 2, 3]);
        let joint = old.enter_joint(vec![1, 2, 4].into_iter().collect());

        // Cannot apply another change during joint consensus
        let result = joint.apply_change(MembershipChange::AddNode(5));
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_configuration() {
        let config = Configuration::new(vec![1, 2, 3]);
        let new_config = config
            .apply_change(MembershipChange::ReplaceConfiguration(vec![4, 5, 6]))
            .unwrap();

        // Should be in joint consensus
        assert!(new_config.is_joint());
        assert!(new_config.is_voter(1)); // Old voter
        assert!(new_config.is_voter(4)); // New voter
    }

    #[test]
    fn test_all_nodes() {
        let config = Configuration::with_learners(vec![1, 2, 3], vec![4, 5]);
        let all = config.all_nodes();

        assert_eq!(all.len(), 5);
        assert!(all.contains(&1));
        assert!(all.contains(&4));
    }
}
