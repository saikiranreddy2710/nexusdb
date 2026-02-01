//! State machine interface for Raft consensus.
//!
//! The state machine is the component that actually executes commands and
//! maintains application state. In NexusDB, this would be the database engine.
//!
//! # Responsibilities
//!
//! - Apply committed commands in order
//! - Create snapshots for log compaction
//! - Restore state from snapshots
//!
//! # Safety
//!
//! All nodes must apply the same commands in the same order, so the state
//! machine must be deterministic. The same sequence of commands must always
//! produce the same state.

use bytes::Bytes;

use crate::log::SnapshotMeta;
use crate::rpc::LogEntry;
use crate::LogIndex;

/// The result of applying a command to the state machine.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    /// The result data to return to the client.
    pub data: Bytes,
    /// Whether the command was successful.
    pub success: bool,
}

impl ApplyResult {
    /// Creates a successful result.
    pub fn success(data: Bytes) -> Self {
        Self {
            data,
            success: true,
        }
    }

    /// Creates an empty successful result.
    pub fn ok() -> Self {
        Self {
            data: Bytes::new(),
            success: true,
        }
    }

    /// Creates a failure result.
    pub fn failure(error: Bytes) -> Self {
        Self {
            data: error,
            success: false,
        }
    }
}

/// A snapshot of the state machine.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Metadata about the snapshot.
    pub meta: SnapshotMeta,
    /// The serialized state machine state.
    pub data: Bytes,
}

impl Snapshot {
    /// Creates a new snapshot.
    pub fn new(meta: SnapshotMeta, data: Bytes) -> Self {
        Self { meta, data }
    }
}

/// Trait for the replicated state machine.
///
/// Implement this trait for your application to use Raft consensus.
/// The state machine must be deterministic: the same sequence of commands
/// must always produce the same state.
///
/// # Example
///
/// ```ignore
/// struct KeyValueStore {
///     data: HashMap<String, String>,
/// }
///
/// impl StateMachine for KeyValueStore {
///     fn apply(&mut self, entry: &LogEntry) -> ApplyResult {
///         // Parse and execute the command
///         let cmd: Command = deserialize(&entry.data);
///         match cmd {
///             Command::Set(k, v) => {
///                 self.data.insert(k, v);
///                 ApplyResult::ok()
///             }
///             Command::Get(k) => {
///                 let value = self.data.get(&k).cloned().unwrap_or_default();
///                 ApplyResult::success(value.into())
///             }
///         }
///     }
///
///     fn snapshot(&self) -> Bytes {
///         serialize(&self.data)
///     }
///
///     fn restore(&mut self, snapshot: &Snapshot) {
///         self.data = deserialize(&snapshot.data);
///     }
/// }
/// ```
pub trait StateMachine: Send + Sync {
    /// Applies a committed log entry to the state machine.
    ///
    /// This is called for each entry after it is committed. Entries are
    /// applied in order, and each entry is applied exactly once.
    ///
    /// For no-op entries (used for leader establishment), this should
    /// be a no-op that returns success.
    fn apply(&mut self, entry: &LogEntry) -> ApplyResult;

    /// Creates a snapshot of the current state.
    ///
    /// The snapshot should contain all state necessary to restore the
    /// state machine to its current state.
    fn snapshot(&self) -> Bytes;

    /// Restores state from a snapshot.
    ///
    /// This is called when the node receives a snapshot from the leader.
    /// After restoration, the state machine should be in the same state
    /// as the leader was at the snapshot's last included index.
    fn restore(&mut self, snapshot: &Snapshot);

    /// Returns the last applied index.
    ///
    /// This should return the index of the last log entry that was applied.
    /// Used during recovery to determine where to resume applying entries.
    fn last_applied(&self) -> LogIndex;

    /// Called when the node becomes leader.
    ///
    /// This is a hook for any leader-specific initialization.
    fn on_become_leader(&mut self) {}

    /// Called when the node becomes follower.
    ///
    /// This is a hook for any cleanup when losing leadership.
    fn on_become_follower(&mut self) {}
}

/// A no-op state machine for testing.
///
/// This state machine accepts all commands but doesn't actually do anything.
/// Useful for testing Raft consensus without a real application.
#[derive(Debug, Default)]
pub struct NoOpStateMachine {
    last_applied: LogIndex,
    snapshot_data: Bytes,
}

impl NoOpStateMachine {
    /// Creates a new no-op state machine.
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateMachine for NoOpStateMachine {
    fn apply(&mut self, entry: &LogEntry) -> ApplyResult {
        self.last_applied = entry.index;
        ApplyResult::ok()
    }

    fn snapshot(&self) -> Bytes {
        self.snapshot_data.clone()
    }

    fn restore(&mut self, snapshot: &Snapshot) {
        self.last_applied = snapshot.meta.last_included_index;
        self.snapshot_data = snapshot.data.clone();
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

/// A state machine that records all applied commands.
///
/// Useful for testing and debugging.
#[derive(Debug, Default)]
pub struct RecordingStateMachine {
    /// Applied entries in order.
    pub entries: Vec<LogEntry>,
    /// Last applied index.
    last_applied: LogIndex,
}

impl RecordingStateMachine {
    /// Creates a new recording state machine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of applied entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if no entries have been applied.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Gets an applied entry by index.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        self.entries.iter().find(|e| e.index == index)
    }
}

impl StateMachine for RecordingStateMachine {
    fn apply(&mut self, entry: &LogEntry) -> ApplyResult {
        self.entries.push(entry.clone());
        self.last_applied = entry.index;
        ApplyResult::success(entry.data.clone())
    }

    fn snapshot(&self) -> Bytes {
        // Serialize entries for snapshot
        bincode::serialize(&self.entries)
            .map(Bytes::from)
            .unwrap_or_default()
    }

    fn restore(&mut self, snapshot: &Snapshot) {
        if let Ok(entries) = bincode::deserialize::<Vec<LogEntry>>(&snapshot.data) {
            self.entries = entries;
            self.last_applied = snapshot.meta.last_included_index;
        }
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

/// Helper to apply multiple entries to a state machine.
pub fn apply_entries(
    state_machine: &mut dyn StateMachine,
    entries: &[LogEntry],
) -> Vec<ApplyResult> {
    entries.iter().map(|e| state_machine.apply(e)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::EntryType;

    fn make_entry(term: u64, index: u64, data: &str) -> LogEntry {
        LogEntry {
            term,
            index,
            entry_type: EntryType::Command,
            data: Bytes::from(data.to_string()),
        }
    }

    #[test]
    fn test_apply_result() {
        let success = ApplyResult::success(Bytes::from("result"));
        assert!(success.success);
        assert_eq!(success.data, Bytes::from("result"));

        let ok = ApplyResult::ok();
        assert!(ok.success);
        assert!(ok.data.is_empty());

        let failure = ApplyResult::failure(Bytes::from("error"));
        assert!(!failure.success);
        assert_eq!(failure.data, Bytes::from("error"));
    }

    #[test]
    fn test_noop_state_machine() {
        let mut sm = NoOpStateMachine::new();

        assert_eq!(sm.last_applied(), 0);

        let entry = make_entry(1, 1, "cmd1");
        let result = sm.apply(&entry);
        assert!(result.success);
        assert_eq!(sm.last_applied(), 1);

        let entry = make_entry(1, 2, "cmd2");
        sm.apply(&entry);
        assert_eq!(sm.last_applied(), 2);
    }

    #[test]
    fn test_recording_state_machine() {
        let mut sm = RecordingStateMachine::new();

        assert!(sm.is_empty());

        let entry1 = make_entry(1, 1, "cmd1");
        let entry2 = make_entry(1, 2, "cmd2");

        sm.apply(&entry1);
        sm.apply(&entry2);

        assert_eq!(sm.len(), 2);
        assert_eq!(sm.last_applied(), 2);

        let retrieved = sm.get(1).unwrap();
        assert_eq!(retrieved.data, Bytes::from("cmd1"));

        let retrieved = sm.get(2).unwrap();
        assert_eq!(retrieved.data, Bytes::from("cmd2"));
    }

    #[test]
    fn test_recording_snapshot_restore() {
        let mut sm1 = RecordingStateMachine::new();

        let entries = vec![
            make_entry(1, 1, "cmd1"),
            make_entry(1, 2, "cmd2"),
            make_entry(1, 3, "cmd3"),
        ];

        for entry in &entries {
            sm1.apply(entry);
        }

        // Create snapshot
        let snapshot = Snapshot::new(
            SnapshotMeta {
                last_included_index: 3,
                last_included_term: 1,
                size: 0,
            },
            sm1.snapshot(),
        );

        // Restore to new state machine
        let mut sm2 = RecordingStateMachine::new();
        sm2.restore(&snapshot);

        assert_eq!(sm2.len(), 3);
        assert_eq!(sm2.last_applied(), 3);
        assert!(sm2.get(1).is_some());
        assert!(sm2.get(2).is_some());
        assert!(sm2.get(3).is_some());
    }

    #[test]
    fn test_apply_entries_helper() {
        let mut sm = RecordingStateMachine::new();

        let entries = vec![
            make_entry(1, 1, "cmd1"),
            make_entry(1, 2, "cmd2"),
            make_entry(1, 3, "cmd3"),
        ];

        let results = apply_entries(&mut sm, &entries);

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.success));
        assert_eq!(sm.len(), 3);
    }

    #[test]
    fn test_noop_entry() {
        let mut sm = RecordingStateMachine::new();

        let noop = LogEntry {
            term: 1,
            index: 1,
            entry_type: EntryType::Noop,
            data: Bytes::new(),
        };

        let result = sm.apply(&noop);
        assert!(result.success);
        assert_eq!(sm.last_applied(), 1);
    }
}
