//! Raft RPC message types.
//!
//! This module defines the core message types for the Raft protocol:
//! - `RequestVote`: Used during leader election
//! - `AppendEntries`: Used for log replication and heartbeats
//! - `InstallSnapshot`: Used for catching up slow followers
//!
//! # Message Flow
//!
//! ```text
//! Leader Election:
//!   Candidate ──RequestVote──▶ Follower
//!   Candidate ◀──VoteResponse── Follower
//!
//! Log Replication:
//!   Leader ──AppendEntries──▶ Follower
//!   Leader ◀──AppendResponse── Follower
//!
//! Snapshot Transfer:
//!   Leader ──InstallSnapshot──▶ Follower
//!   Leader ◀──SnapshotResponse── Follower
//! ```

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::LogIndex;

/// A Raft term number (monotonically increasing epoch).
pub type Term = u64;

/// A Raft node identifier.
pub type NodeId = u64;

/// A log entry in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// The term when entry was received by leader.
    pub term: Term,
    /// The position in the log.
    pub index: LogIndex,
    /// The type of log entry.
    pub entry_type: EntryType,
    /// The command data (for Command entries).
    pub data: Bytes,
}

impl LogEntry {
    /// Creates a new command entry.
    pub fn command(term: Term, index: LogIndex, data: Bytes) -> Self {
        Self {
            term,
            index,
            entry_type: EntryType::Command,
            data,
        }
    }

    /// Creates a new no-op entry (used for leader establishment).
    pub fn noop(term: Term, index: LogIndex) -> Self {
        Self {
            term,
            index,
            entry_type: EntryType::Noop,
            data: Bytes::new(),
        }
    }

    /// Creates a configuration change entry.
    pub fn config(term: Term, index: LogIndex, data: Bytes) -> Self {
        Self {
            term,
            index,
            entry_type: EntryType::Config,
            data,
        }
    }

    /// Returns the serialized size of this entry.
    pub fn size(&self) -> usize {
        // term(8) + index(8) + entry_type(1) + data_len(8) + data
        25 + self.data.len()
    }
}

/// The type of a log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// A client command to be applied to the state machine.
    Command,
    /// A no-op entry (used for leader establishment).
    Noop,
    /// A configuration change entry.
    Config,
}

/// RequestVote RPC arguments.
///
/// Sent by candidates to gather votes during leader election.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVote {
    /// Candidate's term.
    pub term: Term,
    /// Candidate requesting vote.
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry.
    pub last_log_index: LogIndex,
    /// Term of candidate's last log entry.
    pub last_log_term: Term,
}

impl RequestVote {
    /// Creates a new RequestVote message.
    pub fn new(
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

/// RequestVote RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteResponse {
    /// Current term of the responder.
    pub term: Term,
    /// True means candidate received vote.
    pub vote_granted: bool,
}

impl VoteResponse {
    /// Creates a grant response.
    pub fn grant(term: Term) -> Self {
        Self {
            term,
            vote_granted: true,
        }
    }

    /// Creates a deny response.
    pub fn deny(term: Term) -> Self {
        Self {
            term,
            vote_granted: false,
        }
    }
}

/// AppendEntries RPC arguments.
///
/// Used by leader to replicate log entries and send heartbeats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntries {
    /// Leader's term.
    pub term: Term,
    /// So follower can redirect clients.
    pub leader_id: NodeId,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: LogIndex,
    /// Term of prev_log_index entry.
    pub prev_log_term: Term,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: LogIndex,
}

impl AppendEntries {
    /// Creates a new AppendEntries message.
    pub fn new(
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    /// Creates a heartbeat (AppendEntries with no entries).
    pub fn heartbeat(
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        leader_commit: LogIndex,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
        }
    }

    /// Returns true if this is a heartbeat.
    pub fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }
}

/// AppendEntries RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResponse {
    /// Current term of the responder.
    pub term: Term,
    /// True if follower contained entry matching prev_log_index and prev_log_term.
    pub success: bool,
    /// The follower's last log index (for faster catch-up).
    pub match_index: LogIndex,
    /// Hint for next index to try (optimization for fast rollback).
    pub conflict_index: Option<LogIndex>,
    /// Term of the conflicting entry (if any).
    pub conflict_term: Option<Term>,
}

impl AppendResponse {
    /// Creates a success response.
    pub fn success(term: Term, match_index: LogIndex) -> Self {
        Self {
            term,
            success: true,
            match_index,
            conflict_index: None,
            conflict_term: None,
        }
    }

    /// Creates a failure response.
    pub fn failure(term: Term, match_index: LogIndex) -> Self {
        Self {
            term,
            success: false,
            match_index,
            conflict_index: None,
            conflict_term: None,
        }
    }

    /// Creates a failure response with conflict hints.
    pub fn failure_with_hint(
        term: Term,
        match_index: LogIndex,
        conflict_index: LogIndex,
        conflict_term: Term,
    ) -> Self {
        Self {
            term,
            success: false,
            match_index,
            conflict_index: Some(conflict_index),
            conflict_term: Some(conflict_term),
        }
    }
}

/// InstallSnapshot RPC arguments.
///
/// Used for transferring snapshots to slow followers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshot {
    /// Leader's term.
    pub term: Term,
    /// So follower can redirect clients.
    pub leader_id: NodeId,
    /// The snapshot replaces all entries up through and including this index.
    pub last_included_index: LogIndex,
    /// Term of last_included_index.
    pub last_included_term: Term,
    /// Byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// Raw bytes of the snapshot chunk.
    pub data: Bytes,
    /// True if this is the last chunk.
    pub done: bool,
}

impl InstallSnapshot {
    /// Creates a new InstallSnapshot message.
    pub fn new(
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        offset: u64,
        data: Bytes,
        done: bool,
    ) -> Self {
        Self {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset,
            data,
            done,
        }
    }
}

/// InstallSnapshot RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotResponse {
    /// Current term of the responder.
    pub term: Term,
}

impl SnapshotResponse {
    /// Creates a new response.
    pub fn new(term: Term) -> Self {
        Self { term }
    }
}

/// All possible Raft RPC messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    /// RequestVote request.
    RequestVote(RequestVote),
    /// RequestVote response.
    VoteResponse(VoteResponse),
    /// AppendEntries request.
    AppendEntries(AppendEntries),
    /// AppendEntries response.
    AppendResponse(AppendResponse),
    /// InstallSnapshot request.
    InstallSnapshot(InstallSnapshot),
    /// InstallSnapshot response.
    SnapshotResponse(SnapshotResponse),
}

impl RaftMessage {
    /// Returns the term in the message.
    pub fn term(&self) -> Term {
        match self {
            RaftMessage::RequestVote(m) => m.term,
            RaftMessage::VoteResponse(m) => m.term,
            RaftMessage::AppendEntries(m) => m.term,
            RaftMessage::AppendResponse(m) => m.term,
            RaftMessage::InstallSnapshot(m) => m.term,
            RaftMessage::SnapshotResponse(m) => m.term,
        }
    }

    /// Returns a short name for the message type.
    pub fn type_name(&self) -> &'static str {
        match self {
            RaftMessage::RequestVote(_) => "RequestVote",
            RaftMessage::VoteResponse(_) => "VoteResponse",
            RaftMessage::AppendEntries(_) => "AppendEntries",
            RaftMessage::AppendResponse(_) => "AppendResponse",
            RaftMessage::InstallSnapshot(_) => "InstallSnapshot",
            RaftMessage::SnapshotResponse(_) => "SnapshotResponse",
        }
    }
}

impl fmt::Display for RaftMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(term={})", self.type_name(), self.term())
    }
}

/// An outbound message to send to a peer.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// The target node.
    pub to: NodeId,
    /// The message to send.
    pub message: RaftMessage,
}

impl OutboundMessage {
    /// Creates a new outbound message.
    pub fn new(to: NodeId, message: RaftMessage) -> Self {
        Self { to, message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_command() {
        let entry = LogEntry::command(1, 5, Bytes::from("set x=1"));
        assert_eq!(entry.term, 1);
        assert_eq!(entry.index, 5);
        assert_eq!(entry.entry_type, EntryType::Command);
    }

    #[test]
    fn test_log_entry_noop() {
        let entry = LogEntry::noop(2, 10);
        assert_eq!(entry.term, 2);
        assert_eq!(entry.index, 10);
        assert_eq!(entry.entry_type, EntryType::Noop);
        assert!(entry.data.is_empty());
    }

    #[test]
    fn test_request_vote() {
        let rv = RequestVote::new(5, 1, 100, 4);
        assert_eq!(rv.term, 5);
        assert_eq!(rv.candidate_id, 1);
        assert_eq!(rv.last_log_index, 100);
        assert_eq!(rv.last_log_term, 4);
    }

    #[test]
    fn test_vote_response() {
        let grant = VoteResponse::grant(5);
        assert!(grant.vote_granted);
        assert_eq!(grant.term, 5);

        let deny = VoteResponse::deny(5);
        assert!(!deny.vote_granted);
    }

    #[test]
    fn test_append_entries_heartbeat() {
        let hb = AppendEntries::heartbeat(3, 1, 50, 2, 45);
        assert!(hb.is_heartbeat());
        assert_eq!(hb.term, 3);
        assert_eq!(hb.leader_id, 1);
    }

    #[test]
    fn test_append_entries_with_entries() {
        let entries = vec![
            LogEntry::command(3, 51, Bytes::from("cmd1")),
            LogEntry::command(3, 52, Bytes::from("cmd2")),
        ];
        let ae = AppendEntries::new(3, 1, 50, 2, entries.clone(), 45);
        assert!(!ae.is_heartbeat());
        assert_eq!(ae.entries.len(), 2);
    }

    #[test]
    fn test_append_response() {
        let success = AppendResponse::success(3, 52);
        assert!(success.success);
        assert_eq!(success.match_index, 52);

        let failure = AppendResponse::failure(3, 50);
        assert!(!failure.success);

        let failure_hint = AppendResponse::failure_with_hint(3, 50, 45, 2);
        assert!(!failure_hint.success);
        assert_eq!(failure_hint.conflict_index, Some(45));
        assert_eq!(failure_hint.conflict_term, Some(2));
    }

    #[test]
    fn test_install_snapshot() {
        let snap = InstallSnapshot::new(5, 1, 100, 4, 0, Bytes::from("snapshot data"), true);
        assert_eq!(snap.term, 5);
        assert_eq!(snap.last_included_index, 100);
        assert!(snap.done);
    }

    #[test]
    fn test_raft_message_term() {
        let msg = RaftMessage::RequestVote(RequestVote::new(10, 1, 50, 9));
        assert_eq!(msg.term(), 10);
        assert_eq!(msg.type_name(), "RequestVote");
    }

    #[test]
    fn test_outbound_message() {
        let msg = OutboundMessage::new(2, RaftMessage::VoteResponse(VoteResponse::grant(5)));
        assert_eq!(msg.to, 2);
    }

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry::command(1, 5, Bytes::from("test"));
        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: LogEntry = bincode::deserialize(&serialized).unwrap();
        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_raft_message_serialization() {
        let msg = RaftMessage::AppendEntries(AppendEntries::heartbeat(3, 1, 50, 2, 45));
        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: RaftMessage = bincode::deserialize(&serialized).unwrap();
        assert_eq!(msg, deserialized);
    }
}
