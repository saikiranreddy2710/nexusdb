//! Raft StateMachine backed by NexusDB's StorageEngine.
//!
//! Implements `nexus_raft::StateMachine` to apply committed log entries
//! (SQL commands) to the database. Enables distributed consensus — writes
//! are proposed to Raft leader, committed across the cluster, then applied
//! to each node's local storage.
//!
//! # Command Format
//!
//! Log entries carry serialized `RaftCommand`:
//! ```text
//! { "db": "nexusdb", "sql": "INSERT INTO users VALUES (1, 'Alice')" }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, error, info};

use nexus_raft::state_machine::{ApplyResult, Snapshot, StateMachine};
use nexus_raft::LogIndex;

use super::Database;

/// A SQL command to be replicated via Raft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RaftCommand {
    /// Target database name.
    pub db: String,
    /// SQL statement to execute.
    pub sql: String,
}

impl RaftCommand {
    /// Creates a new Raft command.
    pub fn new(db: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            db: db.into(),
            sql: sql.into(),
        }
    }

    /// Serializes to bytes for Raft log entry.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    /// Deserializes from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok()
    }
}

/// Raft StateMachine that applies SQL commands to a NexusDB Database.
///
/// Each committed log entry is deserialized as a `RaftCommand` and executed
/// against the local database via a temporary session.
pub struct DatabaseStateMachine {
    /// Reference to the database engine.
    db: Arc<Database>,
    /// Last applied log index (tracked for recovery).
    last_applied_index: AtomicU64,
}

impl DatabaseStateMachine {
    /// Creates a new state machine backed by the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            last_applied_index: AtomicU64::new(0),
        }
    }
}

impl StateMachine for DatabaseStateMachine {
    /// Applies a committed log entry by executing the SQL command.
    fn apply(&mut self, entry: &nexus_raft::rpc::LogEntry) -> ApplyResult {
        let index = entry.index;
        let data = &entry.data;

        // Deserialize the command
        let cmd = match RaftCommand::from_bytes(data) {
            Some(cmd) => cmd,
            None => {
                // Could be a no-op entry (leader election) or config change
                debug!("Raft apply index {}: no-op or non-SQL entry", index);
                self.last_applied_index.store(index, Ordering::Release);
                return ApplyResult::ok();
            }
        };

        debug!("Raft apply index {}: db={} sql={}", index, cmd.db, cmd.sql);

        // Create a temporary session and execute the command
        let session_id = self.db.create_session(&cmd.db);
        let result = {
            let session_arc = match self.db.get_session(session_id) {
                Some(s) => s,
                None => {
                    error!("Raft apply: failed to create session for index {}", index);
                    self.last_applied_index.store(index, Ordering::Release);
                    return ApplyResult::failure(Bytes::from("session creation failed"));
                }
            };
            let mut session = session_arc.write().unwrap();
            session.execute(&cmd.sql)
        };
        self.db.close_session(session_id);

        self.last_applied_index.store(index, Ordering::Release);

        match result {
            Ok(stmt_result) => {
                let result_str = stmt_result.display();
                ApplyResult::success(Bytes::from(result_str))
            }
            Err(e) => {
                error!("Raft apply index {} failed: {}", index, e);
                ApplyResult::failure(Bytes::from(e.to_string()))
            }
        }
    }

    /// Creates a snapshot of the current database state.
    ///
    /// For now, returns an empty snapshot. Full snapshot support (serializing
    /// the entire database state) will be added when snapshot transfer between
    /// nodes is implemented.
    fn snapshot(&self) -> Bytes {
        // TODO: Serialize full database state for snapshot transfer
        info!("Raft snapshot requested (stub — returning empty)");
        Bytes::new()
    }

    /// Restores state from a snapshot.
    fn restore(&mut self, _snapshot: &Snapshot) {
        // TODO: Deserialize and restore full database state
        info!("Raft restore requested (stub — no-op)");
    }

    /// Returns the last applied log index.
    fn last_applied(&self) -> LogIndex {
        self.last_applied_index.load(Ordering::Acquire)
    }

    /// Called when this node becomes leader.
    fn on_become_leader(&mut self) {
        info!("This node is now the Raft LEADER");
    }

    /// Called when this node becomes follower.
    fn on_become_follower(&mut self) {
        info!("This node is now a Raft FOLLOWER");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_command_roundtrip() {
        let cmd = RaftCommand::new("nexusdb", "INSERT INTO users VALUES (1, 'Alice')");
        let bytes = cmd.to_bytes();
        let decoded = RaftCommand::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.db, "nexusdb");
        assert_eq!(decoded.sql, "INSERT INTO users VALUES (1, 'Alice')");
    }

    #[test]
    fn test_raft_command_invalid_bytes() {
        assert!(RaftCommand::from_bytes(b"not json").is_none());
    }
}
