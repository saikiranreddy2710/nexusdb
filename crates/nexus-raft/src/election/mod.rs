//! Leader election for Raft consensus.
//!
//! This module implements the leader election protocol:
//! 1. Followers wait for heartbeats; if none received, become Candidate
//! 2. Candidates increment term, vote for self, request votes from peers
//! 3. Candidate receiving majority becomes Leader
//! 4. Leaders send periodic heartbeats to maintain authority
//!
//! # Election Safety
//!
//! - At most one leader can be elected per term
//! - A node only votes for a candidate with an up-to-date log
//! - Randomized election timeouts prevent split votes
//!
//! # Timing
//!
//! This module uses tick-based timing for testability:
//! - `election_timeout`: 10-20 ticks (randomized)
//! - `heartbeat_interval`: 3 ticks
//!
//! The caller should call `tick()` at regular intervals (e.g., 50ms).

use std::collections::HashSet;

use rand::Rng;

use crate::log::RaftLog;
use crate::rpc::{NodeId, RequestVote, Term, VoteResponse};

/// Minimum election timeout in ticks.
pub const MIN_ELECTION_TIMEOUT: u64 = 10;

/// Maximum election timeout in ticks.
pub const MAX_ELECTION_TIMEOUT: u64 = 20;

/// Heartbeat interval in ticks.
pub const HEARTBEAT_INTERVAL: u64 = 3;

/// Election state for a Raft node.
#[derive(Debug)]
pub struct ElectionState {
    /// Current term.
    current_term: Term,
    /// Node we voted for in current term (if any).
    voted_for: Option<NodeId>,
    /// Known leader for current term.
    leader_id: Option<NodeId>,
    /// Votes received in current election (when Candidate).
    votes_received: HashSet<NodeId>,
    /// Ticks since last reset (election timeout for Follower/Candidate, heartbeat for Leader).
    ticks_since_reset: u64,
    /// Current election timeout (randomized).
    election_timeout: u64,
    /// Total number of nodes in the cluster.
    cluster_size: usize,
}

impl ElectionState {
    /// Creates a new election state.
    pub fn new(cluster_size: usize) -> Self {
        let election_timeout = Self::random_election_timeout();
        Self {
            current_term: 0,
            voted_for: None,
            leader_id: None,
            votes_received: HashSet::new(),
            ticks_since_reset: 0,
            election_timeout,
            cluster_size,
        }
    }

    /// Returns the current term.
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    /// Returns who we voted for in the current term.
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the known leader for the current term.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Sets the known leader.
    pub fn set_leader(&mut self, leader_id: NodeId) {
        self.leader_id = Some(leader_id);
    }

    /// Returns the number of votes required for a majority.
    pub fn quorum_size(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Generates a random election timeout.
    fn random_election_timeout() -> u64 {
        rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT)
    }

    /// Resets the election timer with a new random timeout.
    pub fn reset_election_timer(&mut self) {
        self.ticks_since_reset = 0;
        self.election_timeout = Self::random_election_timeout();
    }

    /// Resets the heartbeat timer (for leaders).
    pub fn reset_heartbeat_timer(&mut self) {
        self.ticks_since_reset = 0;
    }

    /// Advances time by one tick.
    ///
    /// Returns `true` if the timer has expired (election timeout or heartbeat).
    pub fn tick(&mut self) -> bool {
        self.ticks_since_reset += 1;
        self.ticks_since_reset >= self.election_timeout
    }

    /// Checks if heartbeat interval has elapsed (for leaders).
    pub fn should_send_heartbeat(&self) -> bool {
        self.ticks_since_reset >= HEARTBEAT_INTERVAL
    }

    /// Updates term if the given term is higher.
    ///
    /// Returns `true` if the term was updated (meaning we should become follower).
    pub fn maybe_update_term(&mut self, term: Term) -> bool {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.leader_id = None;
            self.votes_received.clear();
            self.reset_election_timer();
            true
        } else {
            false
        }
    }

    /// Starts a new election.
    ///
    /// Returns the RequestVote message to send to peers.
    pub fn start_election(&mut self, node_id: NodeId, log: &RaftLog) -> RequestVote {
        // Increment term
        self.current_term += 1;

        // Vote for self
        self.voted_for = Some(node_id);
        self.votes_received.clear();
        self.votes_received.insert(node_id);

        // Reset election timer
        self.reset_election_timer();

        // Clear leader
        self.leader_id = None;

        // Create RequestVote
        RequestVote::new(
            self.current_term,
            node_id,
            log.last_index(),
            log.last_term(),
        )
    }

    /// Records a vote received.
    ///
    /// Returns `true` if we now have a majority.
    pub fn record_vote(&mut self, from: NodeId, granted: bool) -> bool {
        if granted {
            self.votes_received.insert(from);
            self.has_majority()
        } else {
            false
        }
    }

    /// Checks if we have received a majority of votes.
    pub fn has_majority(&self) -> bool {
        self.votes_received.len() >= self.quorum_size()
    }

    /// Returns the number of votes received.
    pub fn vote_count(&self) -> usize {
        self.votes_received.len()
    }

    /// Handles an incoming RequestVote request.
    ///
    /// Returns the VoteResponse to send back, and whether we updated our term.
    pub fn handle_request_vote(
        &mut self,
        request: &RequestVote,
        log: &RaftLog,
    ) -> (VoteResponse, bool) {
        let mut term_updated = false;

        // If request term is higher, update our term
        if request.term > self.current_term {
            self.maybe_update_term(request.term);
            term_updated = true;
        }

        // Deny if request term is lower
        if request.term < self.current_term {
            return (VoteResponse::deny(self.current_term), term_updated);
        }

        // Check if we can vote for this candidate
        let can_vote = match self.voted_for {
            None => true,
            Some(id) => id == request.candidate_id,
        };

        // Check if candidate's log is at least as up-to-date as ours
        let log_ok = log.is_up_to_date(request.last_log_index, request.last_log_term);

        if can_vote && log_ok {
            // Grant vote
            self.voted_for = Some(request.candidate_id);
            self.reset_election_timer();
            (VoteResponse::grant(self.current_term), term_updated)
        } else {
            // Deny vote
            (VoteResponse::deny(self.current_term), term_updated)
        }
    }

    /// Called when becoming leader.
    pub fn become_leader(&mut self, node_id: NodeId) {
        self.leader_id = Some(node_id);
        self.reset_heartbeat_timer();
    }

    /// Restores persistent state (called during recovery).
    pub fn restore(&mut self, term: Term, voted_for: Option<NodeId>) {
        self.current_term = term;
        self.voted_for = voted_for;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::LogEntry;
    use crate::LogIndex;
    use bytes::Bytes;

    fn make_log_with_entries(entries: Vec<(Term, LogIndex)>) -> RaftLog {
        let log = RaftLog::new();
        for (term, index) in entries {
            log.append(LogEntry::command(term, index, Bytes::from("cmd")))
                .unwrap();
        }
        log
    }

    #[test]
    fn test_new_election_state() {
        let state = ElectionState::new(5);
        assert_eq!(state.current_term(), 0);
        assert_eq!(state.voted_for(), None);
        assert_eq!(state.leader_id(), None);
        assert_eq!(state.quorum_size(), 3);
    }

    #[test]
    fn test_quorum_sizes() {
        assert_eq!(ElectionState::new(1).quorum_size(), 1);
        assert_eq!(ElectionState::new(2).quorum_size(), 2);
        assert_eq!(ElectionState::new(3).quorum_size(), 2);
        assert_eq!(ElectionState::new(4).quorum_size(), 3);
        assert_eq!(ElectionState::new(5).quorum_size(), 3);
        assert_eq!(ElectionState::new(7).quorum_size(), 4);
    }

    #[test]
    fn test_start_election() {
        let mut state = ElectionState::new(3);
        let log = make_log_with_entries(vec![(1, 1), (1, 2), (2, 3)]);

        let request = state.start_election(1, &log);

        assert_eq!(state.current_term(), 1);
        assert_eq!(state.voted_for(), Some(1));
        assert_eq!(state.vote_count(), 1); // Self-vote
        assert!(!state.has_majority()); // Need 2 for quorum of 3

        assert_eq!(request.term, 1);
        assert_eq!(request.candidate_id, 1);
        assert_eq!(request.last_log_index, 3);
        assert_eq!(request.last_log_term, 2);
    }

    #[test]
    fn test_record_vote() {
        let mut state = ElectionState::new(3);
        let log = RaftLog::new();
        state.start_election(1, &log);

        // Self-vote counts, need one more
        assert!(!state.has_majority());

        // Receive vote from node 2
        let has_majority = state.record_vote(2, true);
        assert!(has_majority);
        assert_eq!(state.vote_count(), 2);

        // Denied vote doesn't count
        state.record_vote(3, false);
        assert_eq!(state.vote_count(), 2);
    }

    #[test]
    fn test_handle_request_vote_grant() {
        let mut state = ElectionState::new(3);
        let log = make_log_with_entries(vec![(1, 1)]);

        // Candidate with higher term and up-to-date log
        let request = RequestVote::new(1, 2, 1, 1);
        let (response, term_updated) = state.handle_request_vote(&request, &log);

        assert!(response.vote_granted);
        assert!(term_updated);
        assert_eq!(state.voted_for(), Some(2));
        assert_eq!(state.current_term(), 1);
    }

    #[test]
    fn test_handle_request_vote_deny_stale_term() {
        let mut state = ElectionState::new(3);
        state.current_term = 5;
        let log = RaftLog::new();

        // Request from old term
        let request = RequestVote::new(3, 2, 0, 0);
        let (response, term_updated) = state.handle_request_vote(&request, &log);

        assert!(!response.vote_granted);
        assert!(!term_updated);
        assert_eq!(response.term, 5);
    }

    #[test]
    fn test_handle_request_vote_deny_already_voted() {
        let mut state = ElectionState::new(3);
        let log = RaftLog::new();

        // First vote
        let request1 = RequestVote::new(1, 2, 0, 0);
        let (response1, _) = state.handle_request_vote(&request1, &log);
        assert!(response1.vote_granted);

        // Second candidate in same term - denied
        let request2 = RequestVote::new(1, 3, 0, 0);
        let (response2, _) = state.handle_request_vote(&request2, &log);
        assert!(!response2.vote_granted);
    }

    #[test]
    fn test_handle_request_vote_deny_stale_log() {
        let mut state = ElectionState::new(3);
        let log = make_log_with_entries(vec![(2, 1), (2, 2)]); // Our log: term 2, index 2

        // Candidate with older log
        let request = RequestVote::new(3, 2, 1, 1); // Their log: term 1, index 1
        let (response, _) = state.handle_request_vote(&request, &log);

        assert!(!response.vote_granted);
        assert_eq!(state.voted_for(), None); // Did not vote for anyone
    }

    #[test]
    fn test_maybe_update_term() {
        let mut state = ElectionState::new(3);
        state.current_term = 5;
        state.voted_for = Some(1);
        state.leader_id = Some(1);
        state.votes_received.insert(1);

        // Lower term - no update
        assert!(!state.maybe_update_term(3));
        assert_eq!(state.current_term(), 5);

        // Higher term - update and clear state
        assert!(state.maybe_update_term(10));
        assert_eq!(state.current_term(), 10);
        assert_eq!(state.voted_for(), None);
        assert_eq!(state.leader_id(), None);
        assert_eq!(state.vote_count(), 0);
    }

    #[test]
    fn test_election_timeout() {
        let mut state = ElectionState::new(3);

        // Tick until timeout
        let mut ticked = 0;
        while !state.tick() {
            ticked += 1;
            assert!(ticked <= MAX_ELECTION_TIMEOUT);
        }

        assert!(ticked >= MIN_ELECTION_TIMEOUT);
        assert!(ticked <= MAX_ELECTION_TIMEOUT);
    }

    #[test]
    fn test_heartbeat_interval() {
        let mut state = ElectionState::new(3);
        state.reset_heartbeat_timer();

        assert!(!state.should_send_heartbeat());
        state.tick();
        assert!(!state.should_send_heartbeat());
        state.tick();
        assert!(!state.should_send_heartbeat());
        state.tick();
        assert!(state.should_send_heartbeat());
    }

    #[test]
    fn test_become_leader() {
        let mut state = ElectionState::new(3);
        state.become_leader(1);

        assert_eq!(state.leader_id(), Some(1));
    }

    #[test]
    fn test_restore() {
        let mut state = ElectionState::new(3);
        state.restore(10, Some(5));

        assert_eq!(state.current_term(), 10);
        assert_eq!(state.voted_for(), Some(5));
    }

    #[test]
    fn test_log_comparison() {
        // Test that voting logic correctly compares logs
        let mut state = ElectionState::new(3);

        // Our log: term 2, index 3
        let log = make_log_with_entries(vec![(1, 1), (2, 2), (2, 3)]);

        // Candidate with higher term but shorter log - should win (term is more important)
        let request = RequestVote::new(5, 2, 1, 3);
        let (response, _) = state.handle_request_vote(&request, &log);
        assert!(response.vote_granted);
    }
}
