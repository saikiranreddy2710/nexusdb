//! Main Raft node implementation.
//!
//! This module provides the `RaftNode` struct which coordinates all Raft
//! components: election, replication, log, and state machine.
//!
//! # Usage
//!
//! ```ignore
//! // Create a node
//! let config = RaftConfig {
//!     node_id: 1,
//!     peers: vec![2, 3, 4, 5],
//!     ..Default::default()
//! };
//! let mut node = RaftNode::new(config, state_machine);
//!
//! // Main loop
//! loop {
//!     // Process tick (call every ~50ms)
//!     let messages = node.tick();
//!     for msg in messages {
//!         send_to_peer(msg.to, msg.message);
//!     }
//!
//!     // Handle incoming messages
//!     let responses = node.handle_message(from, message);
//!     for msg in responses {
//!         send_to_peer(msg.to, msg.message);
//!     }
//!
//!     // Apply committed entries
//!     node.apply_committed();
//! }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::election::ElectionState;
use crate::log::RaftLog;
use crate::replication::{get_entries_to_apply, handle_append_entries, ReplicationState};
use crate::rpc::{
    AppendEntries, AppendResponse, EntryType, InstallSnapshot, LogEntry, NodeId, OutboundMessage,
    RaftMessage, RequestVote, SnapshotResponse, Term, VoteResponse,
};
use crate::state_machine::{ApplyResult, Snapshot, StateMachine};
use crate::{LogIndex, RaftError, Result};

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// IDs of peer nodes (excluding self).
    pub peers: Vec<NodeId>,
    /// Maximum entries per AppendEntries RPC.
    pub max_entries_per_rpc: usize,
    /// Enable pre-vote protocol (reduces disruptions from network partitions).
    pub pre_vote: bool,
    /// Enable leader lease (allows local reads on leader).
    pub leader_lease: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            peers: Vec::new(),
            max_entries_per_rpc: 100,
            pre_vote: false,
            leader_lease: false,
        }
    }
}

impl RaftConfig {
    /// Creates a new config with the given node ID.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    /// Sets the peers.
    pub fn with_peers(mut self, peers: Vec<NodeId>) -> Self {
        self.peers = peers;
        self
    }

    /// Returns the total cluster size (including self).
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
}

/// The role of a Raft node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Following a leader.
    Follower,
    /// Running for leader.
    Candidate,
    /// The cluster leader.
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Follower => write!(f, "Follower"),
            Role::Candidate => write!(f, "Candidate"),
            Role::Leader => write!(f, "Leader"),
        }
    }
}

/// A pending proposal waiting to be committed.
#[derive(Debug)]
#[allow(dead_code)]
struct PendingProposal {
    /// The log index of the proposal.
    index: LogIndex,
    /// The term when the proposal was made.
    term: Term,
}

/// The main Raft node.
pub struct RaftNode<S: StateMachine> {
    /// Node configuration.
    config: RaftConfig,
    /// Current role.
    role: Role,
    /// Election state.
    election: ElectionState,
    /// Replication state (only when leader).
    replication: Option<ReplicationState>,
    /// The Raft log.
    log: Arc<RaftLog>,
    /// The state machine.
    state_machine: Arc<Mutex<S>>,
    /// Commit index.
    commit_index: LogIndex,
    /// Last applied index.
    last_applied: LogIndex,
    /// Pending proposals (for leader).
    pending_proposals: VecDeque<PendingProposal>,
    /// Snapshot being received (for follower).
    incoming_snapshot: Option<Vec<u8>>,
}

impl<S: StateMachine> RaftNode<S> {
    /// Creates a new Raft node.
    pub fn new(config: RaftConfig, state_machine: S) -> Self {
        let cluster_size = config.cluster_size();
        let last_applied = state_machine.last_applied();

        Self {
            config,
            role: Role::Follower,
            election: ElectionState::new(cluster_size),
            replication: None,
            log: Arc::new(RaftLog::new()),
            state_machine: Arc::new(Mutex::new(state_machine)),
            commit_index: last_applied,
            last_applied,
            pending_proposals: VecDeque::new(),
            incoming_snapshot: None,
        }
    }

    /// Returns the node ID.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Returns the current role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Returns true if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Returns the current term.
    pub fn current_term(&self) -> Term {
        self.election.current_term()
    }

    /// Returns the known leader, if any.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.election.leader_id()
    }

    /// Returns the commit index.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Returns the last applied index.
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Returns the log.
    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Processes a tick.
    ///
    /// Call this periodically (e.g., every 50ms) to drive timeouts and
    /// heartbeats. Returns messages to send to peers.
    pub fn tick(&mut self) -> Vec<OutboundMessage> {
        let mut messages = Vec::new();

        match self.role {
            Role::Follower | Role::Candidate => {
                if self.election.tick() {
                    // Election timeout - start election
                    self.start_election(&mut messages);
                }
            }
            Role::Leader => {
                if self.election.should_send_heartbeat() {
                    // Send heartbeats
                    self.send_heartbeats(&mut messages);
                    self.election.reset_heartbeat_timer();
                }
                self.election.tick();
            }
        }

        messages
    }

    /// Starts a new election.
    fn start_election(&mut self, messages: &mut Vec<OutboundMessage>) {
        self.role = Role::Candidate;
        let request = self.election.start_election(self.config.node_id, &self.log);

        // Send RequestVote to all peers
        for &peer in &self.config.peers {
            messages.push(OutboundMessage::new(
                peer,
                RaftMessage::RequestVote(request.clone()),
            ));
        }

        // Check if we already have majority (single-node cluster)
        if self.election.has_majority() {
            self.become_leader(messages);
        }
    }

    /// Becomes the leader.
    fn become_leader(&mut self, messages: &mut Vec<OutboundMessage>) {
        self.role = Role::Leader;
        self.election.become_leader(self.config.node_id);

        // Initialize replication state
        let last_index = self.log.last_index();
        self.replication = Some(ReplicationState::new(
            &self.config.peers,
            last_index,
            self.commit_index,
            self.config.cluster_size(),
        ));

        // Notify state machine
        self.state_machine.lock().on_become_leader();

        // Append no-op entry for leader establishment
        let noop = LogEntry::noop(self.election.current_term(), last_index + 1);
        self.log.append(noop).ok();

        // Send initial heartbeats
        self.send_heartbeats(messages);
    }

    /// Becomes a follower.
    fn become_follower(&mut self) {
        if self.role != Role::Follower {
            self.role = Role::Follower;
            self.replication = None;
            self.pending_proposals.clear();
            self.state_machine.lock().on_become_follower();
        }
    }

    /// Sends heartbeats to all followers.
    fn send_heartbeats(&mut self, messages: &mut Vec<OutboundMessage>) {
        if let Some(ref mut replication) = self.replication {
            for &peer in &self.config.peers {
                if let Some(ae) = replication.prepare_append_entries(
                    peer,
                    self.election.current_term(),
                    self.config.node_id,
                    &self.log,
                ) {
                    messages.push(OutboundMessage::new(peer, RaftMessage::AppendEntries(ae)));
                }
            }
        }
    }

    /// Handles an incoming message.
    ///
    /// Returns messages to send in response.
    pub fn handle_message(&mut self, from: NodeId, message: RaftMessage) -> Vec<OutboundMessage> {
        let mut messages = Vec::new();

        // Check term and step down if needed
        let msg_term = message.term();
        if msg_term > self.election.current_term() {
            self.election.maybe_update_term(msg_term);
            self.become_follower();
        }

        match message {
            RaftMessage::RequestVote(rv) => {
                self.handle_request_vote(from, rv, &mut messages);
            }
            RaftMessage::VoteResponse(vr) => {
                self.handle_vote_response(from, vr, &mut messages);
            }
            RaftMessage::AppendEntries(ae) => {
                self.handle_append_entries(from, ae, &mut messages);
            }
            RaftMessage::AppendResponse(ar) => {
                self.handle_append_response(from, ar, &mut messages);
            }
            RaftMessage::InstallSnapshot(is) => {
                self.handle_install_snapshot(from, is, &mut messages);
            }
            RaftMessage::SnapshotResponse(sr) => {
                self.handle_snapshot_response(from, sr, &mut messages);
            }
        }

        messages
    }

    /// Handles a RequestVote RPC.
    fn handle_request_vote(
        &mut self,
        from: NodeId,
        request: RequestVote,
        messages: &mut Vec<OutboundMessage>,
    ) {
        let (response, term_updated) = self.election.handle_request_vote(&request, &self.log);

        if term_updated {
            self.become_follower();
        }

        messages.push(OutboundMessage::new(
            from,
            RaftMessage::VoteResponse(response),
        ));
    }

    /// Handles a VoteResponse RPC.
    fn handle_vote_response(
        &mut self,
        from: NodeId,
        response: VoteResponse,
        messages: &mut Vec<OutboundMessage>,
    ) {
        // Ignore if not candidate or term mismatch
        if self.role != Role::Candidate || response.term != self.election.current_term() {
            return;
        }

        if self.election.record_vote(from, response.vote_granted) {
            // Won election
            self.become_leader(messages);
        }
    }

    /// Handles an AppendEntries RPC.
    fn handle_append_entries(
        &mut self,
        from: NodeId,
        request: AppendEntries,
        messages: &mut Vec<OutboundMessage>,
    ) {
        // If valid AppendEntries, we know the sender is the leader
        if request.term >= self.election.current_term() {
            self.become_follower();
            self.election.set_leader(from);
            self.election.reset_election_timer();
        }

        let response = handle_append_entries(&request, &self.log, self.election.current_term());

        // Update commit index
        if response.success && request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(self.log.last_index());
        }

        messages.push(OutboundMessage::new(
            from,
            RaftMessage::AppendResponse(response),
        ));
    }

    /// Handles an AppendResponse RPC.
    fn handle_append_response(
        &mut self,
        from: NodeId,
        response: AppendResponse,
        _messages: &mut Vec<OutboundMessage>,
    ) {
        // Ignore if not leader or term mismatch
        if self.role != Role::Leader || response.term != self.election.current_term() {
            return;
        }

        if let Some(ref mut replication) = self.replication {
            let commit_updated = replication.handle_append_response(
                from,
                &response,
                self.election.current_term(),
                self.log.last_index(),
                &self.log,
            );

            if commit_updated {
                self.commit_index = replication.commit_index();
            }
        }
    }

    /// Handles an InstallSnapshot RPC.
    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        request: InstallSnapshot,
        messages: &mut Vec<OutboundMessage>,
    ) {
        // Reset election timer - we heard from leader
        if request.term >= self.election.current_term() {
            self.become_follower();
            self.election.set_leader(from);
            self.election.reset_election_timer();
        }

        // Stale term - reject
        if request.term < self.election.current_term() {
            messages.push(OutboundMessage::new(
                from,
                RaftMessage::SnapshotResponse(SnapshotResponse::new(self.election.current_term())),
            ));
            return;
        }

        // Accumulate snapshot chunks
        if request.offset == 0 {
            self.incoming_snapshot = Some(Vec::new());
        }

        if let Some(ref mut snapshot_data) = self.incoming_snapshot {
            snapshot_data.extend_from_slice(&request.data);

            if request.done {
                // Snapshot complete - apply it
                let meta = crate::log::SnapshotMeta {
                    last_included_index: request.last_included_index,
                    last_included_term: request.last_included_term,
                    size: snapshot_data.len() as u64,
                };

                let snapshot =
                    Snapshot::new(meta.clone(), Bytes::from(std::mem::take(snapshot_data)));

                // Restore state machine
                self.state_machine.lock().restore(&snapshot);

                // Reset log
                self.log = Arc::new(RaftLog::from_snapshot(meta));
                self.last_applied = request.last_included_index;
                self.commit_index = request.last_included_index;

                self.incoming_snapshot = None;
            }
        }

        messages.push(OutboundMessage::new(
            from,
            RaftMessage::SnapshotResponse(SnapshotResponse::new(self.election.current_term())),
        ));
    }

    /// Handles a SnapshotResponse RPC.
    fn handle_snapshot_response(
        &mut self,
        from: NodeId,
        response: SnapshotResponse,
        _messages: &mut Vec<OutboundMessage>,
    ) {
        // Ignore if not leader or term mismatch
        if self.role != Role::Leader || response.term != self.election.current_term() {
            return;
        }

        // Mark snapshot as complete
        if let Some(ref mut replication) = self.replication {
            let last_included = self.log.snapshot_meta().last_included_index;
            replication.snapshot_complete(from, last_included);
        }
    }

    /// Proposes a command to be replicated.
    ///
    /// Only the leader can accept proposals. Returns the log index if
    /// successful, or an error if not the leader.
    pub fn propose(&mut self, data: Bytes) -> Result<LogIndex> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader {
                leader_hint: self.election.leader_id(),
            });
        }

        let term = self.election.current_term();
        let entry = self.log.create_entry(term, EntryType::Command, data);
        let index = entry.index;

        self.log.append(entry)?;

        self.pending_proposals
            .push_back(PendingProposal { index, term });

        Ok(index)
    }

    /// Applies committed entries to the state machine.
    ///
    /// Returns the results of applied entries.
    pub fn apply_committed(&mut self) -> Vec<(LogIndex, ApplyResult)> {
        let entries = get_entries_to_apply(&self.log, self.commit_index, self.last_applied);

        let mut results = Vec::with_capacity(entries.len());
        let mut sm = self.state_machine.lock();

        for entry in entries {
            let index = entry.index;
            let result = sm.apply(&entry);
            self.last_applied = index;
            results.push((index, result));
        }

        // Update replication state's last_applied
        if let Some(ref mut replication) = self.replication {
            replication.set_last_applied(self.last_applied);
        }

        results
    }

    /// Creates a snapshot of the current state.
    pub fn create_snapshot(&self) -> Snapshot {
        let sm = self.state_machine.lock();
        let data = sm.snapshot();

        Snapshot::new(
            crate::log::SnapshotMeta {
                last_included_index: self.last_applied,
                last_included_term: self.log.term_at(self.last_applied).unwrap_or(0),
                size: data.len() as u64,
            },
            data,
        )
    }

    /// Compacts the log up to the last applied index.
    pub fn compact_log(&self) -> Result<()> {
        let last_applied = self.last_applied;
        let term = self.log.term_at(last_applied).unwrap_or(0);
        self.log.compact(last_applied, term)
    }

    /// Adds a peer to the cluster.
    pub fn add_peer(&mut self, peer_id: NodeId) {
        self.config.peers.push(peer_id);

        if let Some(ref mut replication) = self.replication {
            replication.add_peer(peer_id, self.log.last_index());
        }
    }

    /// Removes a peer from the cluster.
    pub fn remove_peer(&mut self, peer_id: NodeId) {
        self.config.peers.retain(|&p| p != peer_id);

        if let Some(ref mut replication) = self.replication {
            replication.remove_peer(peer_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::NoOpStateMachine;

    fn make_node(id: NodeId, peers: Vec<NodeId>) -> RaftNode<NoOpStateMachine> {
        let config = RaftConfig::new(id).with_peers(peers);
        RaftNode::new(config, NoOpStateMachine::new())
    }

    #[test]
    fn test_new_node() {
        let node = make_node(1, vec![2, 3, 4]);

        assert_eq!(node.node_id(), 1);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 0);
        assert!(node.leader_id().is_none());
    }

    #[test]
    fn test_single_node_election() {
        let mut node = make_node(1, vec![]);

        // Single node should become leader after timeout
        loop {
            let messages = node.tick();
            if node.role() == Role::Leader {
                break;
            }
            assert!(messages.is_empty()); // No peers to send to
        }

        assert!(node.is_leader());
        assert_eq!(node.leader_id(), Some(1));
    }

    #[test]
    fn test_election_timeout_starts_election() {
        let mut node = make_node(1, vec![2, 3]);

        // Tick until election starts
        let mut messages = Vec::new();
        loop {
            messages = node.tick();
            if node.role() == Role::Candidate {
                break;
            }
        }

        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.current_term(), 1);

        // Should have sent RequestVote to peers
        assert_eq!(messages.len(), 2);
        for msg in messages {
            match msg.message {
                RaftMessage::RequestVote(rv) => {
                    assert_eq!(rv.term, 1);
                    assert_eq!(rv.candidate_id, 1);
                }
                _ => panic!("Expected RequestVote"),
            }
        }
    }

    #[test]
    fn test_receive_vote_and_become_leader() {
        let mut node = make_node(1, vec![2, 3]);

        // Start election
        loop {
            node.tick();
            if node.role() == Role::Candidate {
                break;
            }
        }

        // Receive vote from node 2
        let vote = VoteResponse::grant(1);
        let messages = node.handle_message(2, RaftMessage::VoteResponse(vote));

        // Should become leader (have 2 votes: self + node 2 = majority of 3)
        assert_eq!(node.role(), Role::Leader);

        // Should send AppendEntries (heartbeats)
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let mut node = make_node(1, vec![2, 3]);

        // Become leader
        loop {
            node.tick();
            if node.role() == Role::Candidate {
                break;
            }
        }
        let vote = VoteResponse::grant(1);
        node.handle_message(2, RaftMessage::VoteResponse(vote));
        assert!(node.is_leader());

        // Receive message with higher term
        let ae = AppendEntries::heartbeat(5, 2, 0, 0, 0);
        node.handle_message(2, RaftMessage::AppendEntries(ae));

        // Should step down
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.leader_id(), Some(2));
    }

    #[test]
    fn test_propose_on_leader() {
        let mut node = make_node(1, vec![]);

        // Become leader
        loop {
            node.tick();
            if node.is_leader() {
                break;
            }
        }

        // Propose command
        let index = node.propose(Bytes::from("cmd1")).unwrap();
        assert_eq!(index, 2); // Index 1 is the no-op

        let index = node.propose(Bytes::from("cmd2")).unwrap();
        assert_eq!(index, 3);
    }

    #[test]
    fn test_propose_on_follower_fails() {
        let node = make_node(1, vec![2, 3]);
        assert_eq!(node.role(), Role::Follower);

        let mut node = node;
        let result = node.propose(Bytes::from("cmd"));
        assert!(result.is_err());

        match result {
            Err(RaftError::NotLeader { .. }) => {}
            _ => panic!("Expected NotLeader error"),
        }
    }

    #[test]
    fn test_handle_append_entries() {
        let mut node = make_node(1, vec![2, 3]);

        // Receive AppendEntries from leader
        let entries = vec![LogEntry::command(1, 1, Bytes::from("cmd1"))];
        let ae = AppendEntries::new(1, 2, 0, 0, entries, 0);

        let messages = node.handle_message(2, RaftMessage::AppendEntries(ae));

        // Should respond with success
        assert_eq!(messages.len(), 1);
        match &messages[0].message {
            RaftMessage::AppendResponse(ar) => {
                assert!(ar.success);
                assert_eq!(ar.match_index, 1);
            }
            _ => panic!("Expected AppendResponse"),
        }

        // Log should have the entry
        assert_eq!(node.log().last_index(), 1);
    }

    #[test]
    fn test_commit_index_update() {
        let mut node = make_node(1, vec![2, 3]);

        // Receive AppendEntries with commit index
        let entries = vec![
            LogEntry::command(1, 1, Bytes::from("cmd1")),
            LogEntry::command(1, 2, Bytes::from("cmd2")),
        ];
        let ae = AppendEntries::new(1, 2, 0, 0, entries, 1);

        node.handle_message(2, RaftMessage::AppendEntries(ae));

        // Commit index should be updated
        assert_eq!(node.commit_index(), 1);
    }

    #[test]
    fn test_apply_committed() {
        let mut node = make_node(1, vec![2, 3]);

        // Add entries and set commit
        let entries = vec![
            LogEntry::command(1, 1, Bytes::from("cmd1")),
            LogEntry::command(1, 2, Bytes::from("cmd2")),
        ];
        let ae = AppendEntries::new(1, 2, 0, 0, entries, 2);
        node.handle_message(2, RaftMessage::AppendEntries(ae));

        // Apply committed
        let results = node.apply_committed();

        assert_eq!(results.len(), 2);
        assert_eq!(node.last_applied(), 2);
    }

    #[test]
    fn test_heartbeat_on_leader() {
        let mut node = make_node(1, vec![2, 3]);

        // Become leader
        loop {
            node.tick();
            if node.role() == Role::Candidate {
                break;
            }
        }
        let initial_messages =
            node.handle_message(2, RaftMessage::VoteResponse(VoteResponse::grant(1)));
        assert!(node.is_leader());

        // The initial heartbeats are sent when becoming leader
        assert!(!initial_messages.is_empty());
        for msg in &initial_messages {
            assert!(matches!(msg.message, RaftMessage::AppendEntries(_)));
        }

        // Simulate receiving responses to clear in_flight
        for msg in &initial_messages {
            let response = AppendResponse::success(1, 1);
            node.handle_message(msg.to, RaftMessage::AppendResponse(response));
        }

        // Reset heartbeat timer
        node.election.reset_heartbeat_timer();

        // Tick until heartbeat interval
        let mut messages = Vec::new();
        for _ in 0..10 {
            messages = node.tick();
            if !messages.is_empty() {
                break;
            }
        }

        // Should send heartbeats to peers
        assert!(!messages.is_empty());
        for msg in messages {
            assert!(matches!(msg.message, RaftMessage::AppendEntries(_)));
        }
    }

    #[test]
    fn test_reject_stale_append_entries() {
        let mut node = make_node(1, vec![2, 3]);

        // Advance term
        let ae = AppendEntries::heartbeat(5, 2, 0, 0, 0);
        node.handle_message(2, RaftMessage::AppendEntries(ae));
        assert_eq!(node.current_term(), 5);

        // Receive stale AppendEntries
        let ae = AppendEntries::heartbeat(3, 3, 0, 0, 0);
        let messages = node.handle_message(3, RaftMessage::AppendEntries(ae));

        // Should reject
        match &messages[0].message {
            RaftMessage::AppendResponse(ar) => {
                assert!(!ar.success);
                assert_eq!(ar.term, 5);
            }
            _ => panic!("Expected AppendResponse"),
        }
    }

    #[test]
    fn test_add_remove_peer() {
        let mut node = make_node(1, vec![2, 3]);

        node.add_peer(4);
        assert!(node.config.peers.contains(&4));

        node.remove_peer(2);
        assert!(!node.config.peers.contains(&2));
    }
}
