//! Raft log storage.
//!
//! The Raft log is an append-only sequence of entries that records all commands
//! that have been proposed to the cluster. Each entry contains:
//! - A term number (the leader's term when the entry was created)
//! - A log index (position in the log)
//! - Entry data (command, no-op, or configuration change)
//!
//! # Log Invariants
//!
//! 1. Log indices are monotonically increasing starting from 1
//! 2. If two entries have the same index and term, they are identical
//! 3. If two entries have the same index and term, all preceding entries are identical
//!
//! # Compaction
//!
//! When the log grows too large, it can be compacted by creating a snapshot.
//! After compaction, entries up to `snapshot_index` are discarded, and
//! `first_index()` returns `snapshot_index + 1`.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::rpc::{EntryType, LogEntry, Term};
use crate::{LogIndex, RaftError, Result};

/// Metadata about a snapshot.
#[derive(Debug, Clone, Default)]
pub struct SnapshotMeta {
    /// The last log index included in the snapshot.
    pub last_included_index: LogIndex,
    /// The term of the last included entry.
    pub last_included_term: Term,
    /// Size of the snapshot in bytes.
    pub size: u64,
}

/// The Raft log storage.
///
/// This implementation uses an in-memory VecDeque for storage with support
/// for log compaction via snapshots. The log is thread-safe and can be
/// accessed concurrently.
///
/// # Index Mapping
///
/// Due to log compaction, the physical index in the VecDeque differs from
/// the logical Raft log index. We track `offset` to handle this:
///
/// ```text
/// logical_index = physical_index + offset
/// physical_index = logical_index - offset
/// offset = snapshot.last_included_index
/// ```
#[derive(Debug)]
pub struct RaftLog {
    /// The log entries (after compaction point).
    entries: RwLock<VecDeque<LogEntry>>,
    /// The offset for index calculation (entries before this are compacted).
    offset: RwLock<LogIndex>,
    /// Snapshot metadata.
    snapshot_meta: RwLock<SnapshotMeta>,
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftLog {
    /// Creates a new empty Raft log.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(VecDeque::new()),
            offset: RwLock::new(0),
            snapshot_meta: RwLock::new(SnapshotMeta::default()),
        }
    }

    /// Creates a Raft log initialized from a snapshot.
    pub fn from_snapshot(meta: SnapshotMeta) -> Self {
        Self {
            entries: RwLock::new(VecDeque::new()),
            offset: RwLock::new(meta.last_included_index),
            snapshot_meta: RwLock::new(meta),
        }
    }

    /// Returns the index of the first entry in the log.
    ///
    /// After compaction, this is `snapshot.last_included_index + 1`.
    /// For a fresh log, this is 1 (or 0 if empty).
    pub fn first_index(&self) -> LogIndex {
        let offset = *self.offset.read();
        if offset == 0 {
            // Fresh log: first entry will be at index 1
            1
        } else {
            // After compaction: first available is after snapshot
            offset + 1
        }
    }

    /// Returns the index of the last entry in the log.
    ///
    /// Returns 0 if the log is empty (before any entries).
    pub fn last_index(&self) -> LogIndex {
        let entries = self.entries.read();
        let offset = *self.offset.read();

        if entries.is_empty() {
            offset // Return snapshot index if no entries
        } else {
            entries.back().map(|e| e.index).unwrap_or(offset)
        }
    }

    /// Returns the term of the last entry in the log.
    ///
    /// Returns 0 if the log is empty.
    pub fn last_term(&self) -> Term {
        let entries = self.entries.read();

        if entries.is_empty() {
            self.snapshot_meta.read().last_included_term
        } else {
            entries.back().map(|e| e.term).unwrap_or(0)
        }
    }

    /// Returns the term of the entry at the given index.
    ///
    /// Returns `None` if the index is out of bounds or compacted.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }

        let offset = *self.offset.read();

        // Check if this is the snapshot index
        if index == offset {
            let meta = self.snapshot_meta.read();
            if meta.last_included_index == index {
                return Some(meta.last_included_term);
            }
        }

        // Check if compacted
        if index <= offset && offset > 0 {
            return None;
        }

        let entries = self.entries.read();
        let physical_index = if offset == 0 {
            index.checked_sub(1)?
        } else {
            index.checked_sub(offset + 1)?
        };

        entries.get(physical_index as usize).map(|e| e.term)
    }

    /// Gets the entry at the given index.
    ///
    /// Returns `None` if the index is out of bounds or compacted.
    pub fn get(&self, index: LogIndex) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }

        let offset = *self.offset.read();

        // Check if compacted
        if index <= offset && offset > 0 {
            return None;
        }

        let entries = self.entries.read();
        let physical_index = if offset == 0 {
            index.checked_sub(1)?
        } else {
            index.checked_sub(offset + 1)?
        };

        entries.get(physical_index as usize).cloned()
    }

    /// Gets entries in the range [start, end).
    ///
    /// Returns an error if the range is compacted.
    pub fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<LogEntry>> {
        if start >= end {
            return Ok(Vec::new());
        }

        let offset = *self.offset.read();
        let first = self.first_index();

        if start < first && offset > 0 {
            return Err(RaftError::LogCompacted { first_index: first });
        }

        let entries = self.entries.read();
        let mut result = Vec::with_capacity((end - start) as usize);

        for index in start..end {
            let physical_index = if offset == 0 {
                index.saturating_sub(1)
            } else {
                index.saturating_sub(offset + 1)
            };

            if let Some(entry) = entries.get(physical_index as usize) {
                result.push(entry.clone());
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Appends a single entry to the log.
    ///
    /// The entry's index must be exactly `last_index() + 1`.
    pub fn append(&self, entry: LogEntry) -> Result<()> {
        let mut entries = self.entries.write();
        let offset = *self.offset.read();

        let expected_index = if entries.is_empty() {
            offset + 1
        } else {
            entries.back().map(|e| e.index + 1).unwrap_or(offset + 1)
        };

        if entry.index != expected_index {
            return Err(RaftError::Internal(format!(
                "log gap: expected index {}, got {}",
                expected_index, entry.index
            )));
        }

        entries.push_back(entry);
        Ok(())
    }

    /// Appends multiple entries to the log.
    ///
    /// Used during log replication from the leader.
    pub fn append_entries(&self, new_entries: Vec<LogEntry>) -> Result<()> {
        if new_entries.is_empty() {
            return Ok(());
        }

        let mut entries = self.entries.write();
        let offset = *self.offset.read();

        for entry in new_entries {
            let physical_index = if offset == 0 {
                entry.index.saturating_sub(1)
            } else {
                entry.index.saturating_sub(offset + 1)
            };

            if physical_index < entries.len() as u64 {
                // Check for conflict
                let existing = &entries[physical_index as usize];
                if existing.term != entry.term {
                    // Conflict! Truncate from this point
                    entries.truncate(physical_index as usize);
                    entries.push_back(entry);
                }
                // If terms match, entry is already there (idempotent)
            } else if physical_index == entries.len() as u64 {
                // Append new entry
                entries.push_back(entry);
            } else {
                // Gap in log - should not happen in correct Raft
                return Err(RaftError::Internal(format!(
                    "log gap at index {}",
                    entry.index
                )));
            }
        }

        Ok(())
    }

    /// Truncates the log, removing all entries from `from_index` onwards.
    ///
    /// Used when a follower receives conflicting entries from a new leader.
    pub fn truncate_from(&self, from_index: LogIndex) -> Result<()> {
        let mut entries = self.entries.write();
        let offset = *self.offset.read();
        let first = if offset == 0 { 1 } else { offset + 1 };

        if from_index < first {
            return Err(RaftError::LogCompacted { first_index: first });
        }

        let physical_index = if offset == 0 {
            from_index.saturating_sub(1)
        } else {
            from_index.saturating_sub(offset + 1)
        };

        if physical_index < entries.len() as u64 {
            entries.truncate(physical_index as usize);
        }

        Ok(())
    }

    /// Creates an entry for appending to the log.
    ///
    /// This is a convenience method that automatically assigns the next index.
    pub fn create_entry(&self, term: Term, entry_type: EntryType, data: Bytes) -> LogEntry {
        LogEntry {
            term,
            index: self.last_index() + 1,
            entry_type,
            data,
        }
    }

    /// Returns the number of entries in the log (excluding compacted entries).
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true if the log has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Returns snapshot metadata.
    pub fn snapshot_meta(&self) -> SnapshotMeta {
        self.snapshot_meta.read().clone()
    }

    /// Compacts the log up to the given index (inclusive).
    ///
    /// All entries up to and including `compact_index` are removed.
    /// The term at `compact_index` must be provided for the snapshot metadata.
    pub fn compact(&self, compact_index: LogIndex, compact_term: Term) -> Result<()> {
        let mut entries = self.entries.write();
        let mut offset = self.offset.write();
        let mut meta = self.snapshot_meta.write();

        // Can't compact beyond what we have
        let last = if entries.is_empty() {
            *offset
        } else {
            entries.back().map(|e| e.index).unwrap_or(*offset)
        };

        if compact_index > last {
            return Err(RaftError::Internal(format!(
                "cannot compact to {} beyond last index {}",
                compact_index, last
            )));
        }

        // Calculate how many entries to remove
        let current_first = if *offset == 0 { 1 } else { *offset + 1 };

        if compact_index >= current_first {
            let entries_to_remove = (compact_index - current_first + 1) as usize;
            for _ in 0..entries_to_remove.min(entries.len()) {
                entries.pop_front();
            }
        }

        // Update offset and snapshot metadata
        *offset = compact_index;
        meta.last_included_index = compact_index;
        meta.last_included_term = compact_term;

        Ok(())
    }

    /// Checks if the given (index, term) pair matches our log.
    ///
    /// This is used by followers to verify the leader's prev_log_index/term.
    pub fn match_term(&self, index: LogIndex, term: Term) -> bool {
        if index == 0 {
            return term == 0;
        }

        self.term_at(index).map(|t| t == term).unwrap_or(false)
    }

    /// Checks if a candidate's log is at least as up-to-date as ours.
    ///
    /// Used during leader election to determine vote eligibility.
    /// The candidate's log is "at least as up-to-date" if:
    /// 1. The candidate's last term is greater than ours, OR
    /// 2. The terms are equal and the candidate's last index is >= ours
    ///
    /// Returns true if we should vote for a candidate with this log state.
    pub fn is_up_to_date(&self, candidate_last_index: LogIndex, candidate_last_term: Term) -> bool {
        let our_last_term = self.last_term();
        let our_last_index = self.last_index();

        if candidate_last_term != our_last_term {
            candidate_last_term > our_last_term
        } else {
            candidate_last_index >= our_last_index
        }
    }

    /// Finds the first index with the given term.
    ///
    /// Used for fast log rollback during replication conflicts.
    pub fn find_first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        let entries = self.entries.read();
        let offset = *self.offset.read();

        for (i, entry) in entries.iter().enumerate() {
            if entry.term == term {
                return Some(if offset == 0 {
                    i as LogIndex + 1
                } else {
                    offset + 1 + i as LogIndex
                });
            }
        }

        None
    }

    /// Returns entries that need to be replicated to a follower.
    ///
    /// Given the follower's `next_index`, returns entries starting from that index,
    /// along with the prev_log_index and prev_log_term for the AppendEntries RPC.
    pub fn entries_for_follower(
        &self,
        next_index: LogIndex,
        max_entries: usize,
    ) -> Result<(LogIndex, Term, Vec<LogEntry>)> {
        let prev_index = next_index.saturating_sub(1);
        let prev_term = self
            .term_at(prev_index)
            .ok_or_else(|| RaftError::LogCompacted {
                first_index: self.first_index(),
            })?;

        let end_index = (next_index + max_entries as u64).min(self.last_index() + 1);
        let entries = self.get_range(next_index, end_index)?;

        Ok((prev_index, prev_term, entries))
    }
}

/// Thread-safe shared reference to a RaftLog.
pub type SharedRaftLog = Arc<RaftLog>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_log() {
        let log = RaftLog::new();
        assert_eq!(log.first_index(), 1);
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_append_single() {
        let log = RaftLog::new();
        let entry = LogEntry::command(1, 1, Bytes::from("cmd1"));
        log.append(entry.clone()).unwrap();

        assert_eq!(log.len(), 1);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.get(1), Some(entry));
    }

    #[test]
    fn test_append_multiple() {
        let log = RaftLog::new();

        for i in 1..=5 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        assert_eq!(log.len(), 5);
        assert_eq!(log.last_index(), 5);

        for i in 1..=5 {
            assert!(log.get(i).is_some());
            assert_eq!(log.term_at(i), Some(1));
        }
    }

    #[test]
    fn test_append_gap_error() {
        let log = RaftLog::new();
        let entry = LogEntry::command(1, 5, Bytes::from("cmd")); // Gap: should be index 1
        assert!(log.append(entry).is_err());
    }

    #[test]
    fn test_get_range() {
        let log = RaftLog::new();

        for i in 1..=10 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        let range = log.get_range(3, 7).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].index, 3);
        assert_eq!(range[3].index, 6);

        // Empty range
        let empty = log.get_range(5, 5).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_truncate() {
        let log = RaftLog::new();

        for i in 1..=10 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        log.truncate_from(6).unwrap();
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.len(), 5);
        assert!(log.get(6).is_none());
    }

    #[test]
    fn test_append_entries_with_conflict() {
        let log = RaftLog::new();

        // Initial entries from term 1
        for i in 1..=5 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("old{}", i)));
            log.append(entry).unwrap();
        }

        // New leader sends entries starting from index 3 with term 2
        let new_entries = vec![
            LogEntry::command(2, 3, Bytes::from("new3")),
            LogEntry::command(2, 4, Bytes::from("new4")),
            LogEntry::command(2, 5, Bytes::from("new5")),
        ];

        log.append_entries(new_entries).unwrap();

        // Check that entries 1-2 are preserved, 3-5 are replaced
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(4), Some(2));
        assert_eq!(log.term_at(5), Some(2));
    }

    #[test]
    fn test_match_term() {
        let log = RaftLog::new();

        let entry1 = LogEntry::command(1, 1, Bytes::from("cmd1"));
        let entry2 = LogEntry::command(2, 2, Bytes::from("cmd2"));
        log.append(entry1).unwrap();
        log.append(entry2).unwrap();

        assert!(log.match_term(0, 0)); // Base case
        assert!(log.match_term(1, 1));
        assert!(log.match_term(2, 2));
        assert!(!log.match_term(1, 2)); // Wrong term
        assert!(!log.match_term(3, 2)); // Beyond log
    }

    #[test]
    fn test_is_up_to_date() {
        let log = RaftLog::new();

        // Empty log (our term 0, index 0)
        // Candidate with (0, 0) is up-to-date
        assert!(log.is_up_to_date(0, 0));
        // Candidate with (1, 1) is more up-to-date (has entries we don't)
        assert!(log.is_up_to_date(1, 1));

        // Add some entries
        log.append(LogEntry::command(1, 1, Bytes::from("cmd1")))
            .unwrap();
        log.append(LogEntry::command(2, 2, Bytes::from("cmd2")))
            .unwrap();

        // Our log: term 2, index 2
        // Candidate with (1, 1): behind us - NOT up-to-date
        assert!(!log.is_up_to_date(1, 1));
        // Candidate with (2, 2): same as us - up-to-date
        assert!(log.is_up_to_date(2, 2));
        // Candidate with (2, 1): lower term - NOT up-to-date
        assert!(!log.is_up_to_date(2, 1));
        // Candidate with (3, 2): same term, longer log - up-to-date
        assert!(log.is_up_to_date(3, 2));
        // Candidate with (1, 3): higher term - up-to-date (term wins)
        assert!(log.is_up_to_date(1, 3));
    }

    #[test]
    fn test_compact() {
        let log = RaftLog::new();

        for i in 1..=10 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        // Compact up to index 5
        log.compact(5, 1).unwrap();

        assert_eq!(log.first_index(), 6);
        assert_eq!(log.last_index(), 10);
        assert_eq!(log.len(), 5);

        // Can't get compacted entries
        assert!(log.get(5).is_none());
        assert!(log.get(4).is_none());

        // Can still get remaining entries
        assert!(log.get(6).is_some());
        assert!(log.get(10).is_some());

        // Snapshot metadata updated
        let meta = log.snapshot_meta();
        assert_eq!(meta.last_included_index, 5);
        assert_eq!(meta.last_included_term, 1);
    }

    #[test]
    fn test_from_snapshot() {
        let meta = SnapshotMeta {
            last_included_index: 100,
            last_included_term: 5,
            size: 1024,
        };

        let log = RaftLog::from_snapshot(meta);
        assert_eq!(log.first_index(), 101);
        assert_eq!(log.last_index(), 100);
        assert_eq!(log.last_term(), 5);

        // Can append starting at index 101
        let entry = LogEntry::command(5, 101, Bytes::from("cmd"));
        log.append(entry).unwrap();
        assert_eq!(log.last_index(), 101);
    }

    #[test]
    fn test_find_first_index_of_term() {
        let log = RaftLog::new();

        // Term 1: indices 1-3
        // Term 2: indices 4-6
        // Term 3: indices 7-10
        for i in 1..=10 {
            let term = if i <= 3 {
                1
            } else if i <= 6 {
                2
            } else {
                3
            };
            let entry = LogEntry::command(term, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        assert_eq!(log.find_first_index_of_term(1), Some(1));
        assert_eq!(log.find_first_index_of_term(2), Some(4));
        assert_eq!(log.find_first_index_of_term(3), Some(7));
        assert_eq!(log.find_first_index_of_term(4), None);
    }

    #[test]
    fn test_entries_for_follower() {
        let log = RaftLog::new();

        for i in 1..=10 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        // Get entries starting at index 5
        let (prev_index, prev_term, entries) = log.entries_for_follower(5, 3).unwrap();
        assert_eq!(prev_index, 4);
        assert_eq!(prev_term, 1);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 5);
        assert_eq!(entries[2].index, 7);
    }

    #[test]
    fn test_create_entry() {
        let log = RaftLog::new();

        let entry1 = log.create_entry(1, EntryType::Command, Bytes::from("cmd1"));
        assert_eq!(entry1.index, 1);
        log.append(entry1).unwrap();

        let entry2 = log.create_entry(1, EntryType::Command, Bytes::from("cmd2"));
        assert_eq!(entry2.index, 2);
        log.append(entry2).unwrap();

        let noop = log.create_entry(2, EntryType::Noop, Bytes::new());
        assert_eq!(noop.index, 3);
    }

    #[test]
    fn test_term_at_snapshot_boundary() {
        let meta = SnapshotMeta {
            last_included_index: 100,
            last_included_term: 5,
            size: 1024,
        };

        let log = RaftLog::from_snapshot(meta);

        // Term at snapshot index should work
        assert_eq!(log.term_at(100), Some(5));

        // Before snapshot should fail
        assert_eq!(log.term_at(99), None);

        // Add new entry and verify
        log.append(LogEntry::command(6, 101, Bytes::from("cmd")))
            .unwrap();
        assert_eq!(log.term_at(101), Some(6));
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let log = Arc::new(RaftLog::new());

        // Append entries sequentially first
        for i in 1..=100 {
            let entry = LogEntry::command(1, i, Bytes::from(format!("cmd{}", i)));
            log.append(entry).unwrap();
        }

        // Then read concurrently
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let log = Arc::clone(&log);
                thread::spawn(move || {
                    for i in 1..=100 {
                        assert!(log.get(i).is_some());
                        assert_eq!(log.term_at(i), Some(1));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
