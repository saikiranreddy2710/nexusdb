//! Log replication for Raft consensus.
//!
//! This module handles replicating log entries from the leader to followers:
//!
//! 1. Leader maintains `next_index` and `match_index` for each follower
//! 2. Leader sends AppendEntries with entries starting at `next_index`
//! 3. Follower responds with success/failure
//! 4. On success: update `match_index` and `next_index`
//! 5. On failure: decrement `next_index` and retry (with fast rollback hints)
//!
//! # Commit Index
//!
//! An entry is committed when replicated to a majority of nodes.
//! The leader tracks `match_index` for all followers and updates `commit_index`
//! when a majority have replicated an entry.

use std::collections::HashMap;

use crate::log::RaftLog;
use crate::rpc::{AppendEntries, AppendResponse, LogEntry, NodeId, Term};
use crate::LogIndex;

/// Maximum number of entries to send in a single AppendEntries RPC.
pub const MAX_ENTRIES_PER_RPC: usize = 100;

/// Replication progress for a single follower.
#[derive(Debug, Clone)]
pub struct FollowerProgress {
    /// The next log index to send to this follower.
    pub next_index: LogIndex,
    /// The highest log index known to be replicated on this follower.
    pub match_index: LogIndex,
    /// Number of consecutive failures (for backoff).
    pub failures: u32,
    /// Whether we're currently waiting for a response.
    pub in_flight: bool,
    /// Whether we're sending a snapshot instead of entries.
    pub snapshot_in_progress: bool,
}

impl FollowerProgress {
    /// Creates progress for a new follower.
    ///
    /// Initialized with next_index = last_log_index + 1 (optimistic).
    pub fn new(last_log_index: LogIndex) -> Self {
        Self {
            next_index: last_log_index + 1,
            match_index: 0,
            failures: 0,
            in_flight: false,
            snapshot_in_progress: false,
        }
    }

    /// Updates progress after a successful AppendEntries response.
    pub fn on_success(&mut self, match_index: LogIndex) {
        self.match_index = match_index;
        self.next_index = match_index + 1;
        self.failures = 0;
        self.in_flight = false;
    }

    /// Updates progress after a failed AppendEntries response.
    ///
    /// Uses conflict hints for fast rollback if available.
    pub fn on_failure(&mut self, conflict_index: Option<LogIndex>, log: &RaftLog) {
        self.failures += 1;
        self.in_flight = false;

        if let Some(hint) = conflict_index {
            // Fast rollback using conflict hint
            self.next_index = hint;
        } else {
            // Slow rollback: decrement by 1
            self.next_index = self.next_index.saturating_sub(1).max(1);
        }

        // Don't go below the log's first index
        let first = log.first_index();
        if self.next_index < first {
            self.next_index = first;
            self.snapshot_in_progress = true;
        }
    }

    /// Checks if we should send entries to this follower.
    pub fn should_send(&self) -> bool {
        !self.in_flight && !self.snapshot_in_progress
    }

    /// Marks that we've sent an RPC and are waiting for response.
    pub fn mark_in_flight(&mut self) {
        self.in_flight = true;
    }

    /// Checks if the follower needs a snapshot.
    pub fn needs_snapshot(&self) -> bool {
        self.snapshot_in_progress
    }
}

/// Replication state for the leader.
#[derive(Debug)]
pub struct ReplicationState {
    /// Progress for each follower.
    progress: HashMap<NodeId, FollowerProgress>,
    /// Our commit index.
    commit_index: LogIndex,
    /// Index of last entry applied to state machine.
    last_applied: LogIndex,
    /// Total cluster size (for quorum calculation).
    cluster_size: usize,
}

impl ReplicationState {
    /// Creates a new replication state.
    pub fn new(
        peers: &[NodeId],
        last_log_index: LogIndex,
        commit_index: LogIndex,
        cluster_size: usize,
    ) -> Self {
        let mut progress = HashMap::new();
        for &peer in peers {
            progress.insert(peer, FollowerProgress::new(last_log_index));
        }

        Self {
            progress,
            commit_index,
            last_applied: commit_index,
            cluster_size,
        }
    }

    /// Returns the current commit index.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Returns the last applied index.
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Sets the last applied index.
    pub fn set_last_applied(&mut self, index: LogIndex) {
        self.last_applied = index;
    }

    /// Gets progress for a specific follower.
    pub fn get_progress(&self, node_id: NodeId) -> Option<&FollowerProgress> {
        self.progress.get(&node_id)
    }

    /// Gets mutable progress for a specific follower.
    pub fn get_progress_mut(&mut self, node_id: NodeId) -> Option<&mut FollowerProgress> {
        self.progress.get_mut(&node_id)
    }

    /// Returns the number of votes required for a majority.
    fn quorum_size(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Updates commit index based on current replication state.
    ///
    /// Returns true if commit index was advanced.
    pub fn maybe_update_commit_index(
        &mut self,
        leader_last_index: LogIndex,
        current_term: Term,
        log: &RaftLog,
    ) -> bool {
        // Find the highest index replicated on a majority
        // We need quorum_size nodes to have replicated the entry
        // Leader counts as 1, so we need (quorum_size - 1) followers

        let mut match_indices: Vec<LogIndex> =
            self.progress.values().map(|p| p.match_index).collect();

        // Add leader's own index
        match_indices.push(leader_last_index);
        match_indices.sort_unstable();
        match_indices.reverse();

        // The nth element (0-indexed) is the highest index with n+1 replicas
        // For quorum, we need quorum_size replicas
        let quorum_idx = self.quorum_size() - 1;

        if quorum_idx < match_indices.len() {
            let new_commit = match_indices[quorum_idx];

            // Only commit entries from current term (Raft safety requirement)
            if new_commit > self.commit_index {
                // Verify the entry at new_commit is from current term
                if let Some(term) = log.term_at(new_commit) {
                    if term == current_term {
                        self.commit_index = new_commit;
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Prepares an AppendEntries message for a follower.
    pub fn prepare_append_entries(
        &mut self,
        follower_id: NodeId,
        term: Term,
        leader_id: NodeId,
        log: &RaftLog,
    ) -> Option<AppendEntries> {
        let progress = self.progress.get_mut(&follower_id)?;

        if !progress.should_send() {
            return None;
        }

        // Get entries to send
        let result = log.entries_for_follower(progress.next_index, MAX_ENTRIES_PER_RPC);

        match result {
            Ok((prev_index, prev_term, entries)) => {
                progress.mark_in_flight();

                Some(AppendEntries::new(
                    term,
                    leader_id,
                    prev_index,
                    prev_term,
                    entries,
                    self.commit_index,
                ))
            }
            Err(_) => {
                // Log is compacted, need to send snapshot
                progress.snapshot_in_progress = true;
                None
            }
        }
    }

    /// Handles an AppendEntries response from a follower.
    ///
    /// Returns true if the commit index was updated.
    pub fn handle_append_response(
        &mut self,
        from: NodeId,
        response: &AppendResponse,
        current_term: Term,
        leader_last_index: LogIndex,
        log: &RaftLog,
    ) -> bool {
        if let Some(progress) = self.progress.get_mut(&from) {
            if response.success {
                progress.on_success(response.match_index);
                return self.maybe_update_commit_index(leader_last_index, current_term, log);
            } else {
                progress.on_failure(response.conflict_index, log);
            }
        }
        false
    }

    /// Returns followers that need entries sent.
    pub fn followers_needing_entries(&self) -> Vec<NodeId> {
        self.progress
            .iter()
            .filter(|(_, p)| p.should_send())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Returns followers that need snapshots.
    pub fn followers_needing_snapshots(&self) -> Vec<NodeId> {
        self.progress
            .iter()
            .filter(|(_, p)| p.needs_snapshot())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Marks a snapshot as complete for a follower.
    pub fn snapshot_complete(&mut self, follower_id: NodeId, last_included_index: LogIndex) {
        if let Some(progress) = self.progress.get_mut(&follower_id) {
            progress.snapshot_in_progress = false;
            progress.match_index = last_included_index;
            progress.next_index = last_included_index + 1;
        }
    }

    /// Adds a new peer to track.
    pub fn add_peer(&mut self, peer_id: NodeId, last_log_index: LogIndex) {
        self.progress
            .insert(peer_id, FollowerProgress::new(last_log_index));
        self.cluster_size += 1;
    }

    /// Removes a peer from tracking.
    pub fn remove_peer(&mut self, peer_id: NodeId) {
        self.progress.remove(&peer_id);
        self.cluster_size = self.cluster_size.saturating_sub(1);
    }
}

/// Handles AppendEntries on the follower side.
pub fn handle_append_entries(
    request: &AppendEntries,
    log: &RaftLog,
    current_term: Term,
) -> AppendResponse {
    // Reject if term is stale
    if request.term < current_term {
        return AppendResponse::failure(current_term, log.last_index());
    }

    // Check if we have the prev_log entry
    if !log.match_term(request.prev_log_index, request.prev_log_term) {
        // Log doesn't match - send conflict hints for fast rollback
        if let Some(our_term) = log.term_at(request.prev_log_index) {
            // We have an entry at prev_log_index but with different term
            // Find the first entry with this term for fast rollback
            if let Some(first_of_term) = log.find_first_index_of_term(our_term) {
                return AppendResponse::failure_with_hint(
                    current_term,
                    log.last_index(),
                    first_of_term,
                    our_term,
                );
            }
        }

        // We don't have prev_log_index at all, or couldn't find hint
        // Suggest our last index + 1 as the conflict point
        return AppendResponse::failure(current_term, log.last_index());
    }

    // Append entries (handles conflicts internally)
    if let Err(_) = log.append_entries(request.entries.clone()) {
        return AppendResponse::failure(current_term, log.last_index());
    }

    AppendResponse::success(current_term, log.last_index())
}

/// Returns entries that are ready to be applied to the state machine.
pub fn get_entries_to_apply(
    log: &RaftLog,
    commit_index: LogIndex,
    last_applied: LogIndex,
) -> Vec<LogEntry> {
    if commit_index <= last_applied {
        return Vec::new();
    }

    log.get_range(last_applied + 1, commit_index + 1)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_follower_progress_new() {
        let progress = FollowerProgress::new(10);
        assert_eq!(progress.next_index, 11);
        assert_eq!(progress.match_index, 0);
        assert!(progress.should_send());
    }

    #[test]
    fn test_follower_progress_on_success() {
        let mut progress = FollowerProgress::new(10);
        progress.in_flight = true;
        progress.failures = 3;

        progress.on_success(15);

        assert_eq!(progress.match_index, 15);
        assert_eq!(progress.next_index, 16);
        assert_eq!(progress.failures, 0);
        assert!(!progress.in_flight);
    }

    #[test]
    fn test_follower_progress_on_failure() {
        let log = make_log_with_entries(vec![(1, 1), (1, 2), (1, 3)]);
        let mut progress = FollowerProgress::new(10);
        // next_index starts at 11 (last_log_index + 1)
        assert_eq!(progress.next_index, 11);
        progress.in_flight = true;

        // Without hint - slow rollback: 11 -> 10
        progress.on_failure(None, &log);
        assert_eq!(progress.next_index, 10);
        assert_eq!(progress.failures, 1);

        // With hint - fast rollback to 5
        progress.on_failure(Some(5), &log);
        assert_eq!(progress.next_index, 5);
        assert_eq!(progress.failures, 2);
    }

    #[test]
    fn test_replication_state_new() {
        let peers = vec![2, 3, 4];
        let state = ReplicationState::new(&peers, 10, 5, 4);

        assert_eq!(state.commit_index(), 5);
        assert!(state.get_progress(2).is_some());
        assert!(state.get_progress(3).is_some());
        assert!(state.get_progress(4).is_some());
        assert!(state.get_progress(1).is_none());
    }

    #[test]
    fn test_maybe_update_commit_index() {
        let log = make_log_with_entries(vec![(1, 1), (1, 2), (2, 3), (2, 4), (2, 5)]);

        let peers = vec![2, 3];
        let mut state = ReplicationState::new(&peers, 5, 0, 3);

        // Initially no majority
        assert!(!state.maybe_update_commit_index(5, 2, &log));
        assert_eq!(state.commit_index(), 0);

        // Node 2 matches index 3
        state.get_progress_mut(2).unwrap().match_index = 3;
        // Leader has 5, node 2 has 3, node 3 has 0
        // Sorted: [5, 3, 0], quorum index 1 -> 3
        // But entry at 3 is term 2, current term is 2 - OK
        assert!(state.maybe_update_commit_index(5, 2, &log));
        assert_eq!(state.commit_index(), 3);

        // Node 3 matches index 5
        state.get_progress_mut(3).unwrap().match_index = 5;
        // Leader has 5, node 2 has 3, node 3 has 5
        // Sorted: [5, 5, 3], quorum index 1 -> 5
        assert!(state.maybe_update_commit_index(5, 2, &log));
        assert_eq!(state.commit_index(), 5);
    }

    #[test]
    fn test_commit_only_current_term() {
        // Raft requires that we only commit entries from the current term
        let log = make_log_with_entries(vec![
            (1, 1),
            (1, 2),
            (1, 3), // All entries from term 1
        ]);

        let peers = vec![2, 3];
        let mut state = ReplicationState::new(&peers, 3, 0, 3);

        state.get_progress_mut(2).unwrap().match_index = 3;
        state.get_progress_mut(3).unwrap().match_index = 3;

        // We're in term 2, but all entries are from term 1
        // Should NOT commit because entries are from old term
        assert!(!state.maybe_update_commit_index(3, 2, &log));
        assert_eq!(state.commit_index(), 0);

        // If we're in term 1, it should commit
        assert!(state.maybe_update_commit_index(3, 1, &log));
        assert_eq!(state.commit_index(), 3);
    }

    #[test]
    fn test_prepare_append_entries() {
        let log = make_log_with_entries(vec![(1, 1), (1, 2), (1, 3)]);

        let peers = vec![2];
        let mut state = ReplicationState::new(&peers, 3, 0, 2);

        // Follower 2 starts at next_index = 4 (nothing to send beyond current log)
        let ae = state.prepare_append_entries(2, 1, 1, &log);
        assert!(ae.is_some());
        let ae = ae.unwrap();
        assert_eq!(ae.prev_log_index, 3);
        assert_eq!(ae.prev_log_term, 1);
        assert!(ae.entries.is_empty()); // Already caught up

        // Simulate follower needing entries
        state.get_progress_mut(2).unwrap().next_index = 2;
        state.get_progress_mut(2).unwrap().in_flight = false;

        let ae = state.prepare_append_entries(2, 1, 1, &log);
        assert!(ae.is_some());
        let ae = ae.unwrap();
        assert_eq!(ae.prev_log_index, 1);
        assert_eq!(ae.prev_log_term, 1);
        assert_eq!(ae.entries.len(), 2); // Entries 2 and 3
    }

    #[test]
    fn test_handle_append_entries_success() {
        let log = make_log_with_entries(vec![(1, 1), (1, 2)]);

        let request = AppendEntries::new(
            1,
            1,
            2,
            1,
            vec![LogEntry::command(1, 3, Bytes::from("cmd3"))],
            2,
        );

        let response = handle_append_entries(&request, &log, 1);
        assert!(response.success);
        assert_eq!(response.match_index, 3);
        assert_eq!(log.last_index(), 3);
    }

    #[test]
    fn test_handle_append_entries_stale_term() {
        let log = make_log_with_entries(vec![(1, 1)]);

        let request = AppendEntries::heartbeat(0, 1, 0, 0, 0);

        let response = handle_append_entries(&request, &log, 1);
        assert!(!response.success);
        assert_eq!(response.term, 1);
    }

    #[test]
    fn test_handle_append_entries_log_mismatch() {
        let log = make_log_with_entries(vec![(1, 1), (1, 2), (1, 3)]);

        // Leader thinks we have term 2 at index 2
        let request = AppendEntries::new(2, 1, 2, 2, vec![], 0);

        let response = handle_append_entries(&request, &log, 2);
        assert!(!response.success);
        // Should include conflict hint
        assert!(response.conflict_index.is_some() || response.conflict_term.is_some());
    }

    #[test]
    fn test_get_entries_to_apply() {
        let log = make_log_with_entries(vec![(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)]);

        // Nothing applied yet, commit at 3
        let entries = get_entries_to_apply(&log, 3, 0);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[2].index, 3);

        // Already applied 2, commit at 5
        let entries = get_entries_to_apply(&log, 5, 2);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 3);
        assert_eq!(entries[2].index, 5);

        // Already applied up to commit
        let entries = get_entries_to_apply(&log, 3, 3);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_followers_needing_entries() {
        let peers = vec![2, 3, 4];
        let mut state = ReplicationState::new(&peers, 10, 5, 4);

        // All should need entries initially
        let needing = state.followers_needing_entries();
        assert_eq!(needing.len(), 3);

        // Mark one in flight
        state.get_progress_mut(2).unwrap().in_flight = true;
        let needing = state.followers_needing_entries();
        assert_eq!(needing.len(), 2);
        assert!(!needing.contains(&2));
    }

    #[test]
    fn test_add_remove_peer() {
        let peers = vec![2, 3];
        let mut state = ReplicationState::new(&peers, 10, 5, 3);

        assert_eq!(state.progress.len(), 2);

        state.add_peer(4, 10);
        assert_eq!(state.progress.len(), 3);
        assert!(state.get_progress(4).is_some());

        state.remove_peer(2);
        assert_eq!(state.progress.len(), 2);
        assert!(state.get_progress(2).is_none());
    }
}
