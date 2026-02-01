//! Persistent Raft state (term and vote).
//!
//! This module provides durable storage for Raft's persistent state:
//! - `current_term`: The latest term server has seen
//! - `voted_for`: The candidate that received vote in current term
//!
//! The state is stored in a simple file format with atomic writes.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::rpc::{NodeId, Term};

use super::files::{STATE_FILE, STATE_FILE_TMP};
use super::{StorageError, StorageResult};

/// Magic number for state file.
const STATE_MAGIC: u32 = 0x52535441; // "RSTA"

/// Version of the state file format.
const STATE_VERSION: u32 = 1;

/// Size of the state file.
const STATE_FILE_SIZE: usize = 4 + 4 + 8 + 8 + 4; // magic + version + term + voted_for + checksum

/// Persistent Raft state.
///
/// Stores current term and voted_for durably on disk.
/// Uses atomic file replacement for crash safety.
#[derive(Debug)]
pub struct RaftState {
    /// Directory containing the state file.
    dir: PathBuf,
    /// Current term (cached in memory).
    current_term: AtomicU64,
    /// Node we voted for (cached in memory). 0 means no vote.
    voted_for: AtomicU64,
    /// Whether we have a vote (separate from voted_for to distinguish None from node 0).
    has_vote: AtomicBool,
    /// Whether to sync writes.
    sync_writes: bool,
    /// Lock for write operations.
    write_lock: RwLock<()>,
}

impl RaftState {
    /// Creates a new RaftState, loading from disk if the file exists.
    pub fn new<P: AsRef<Path>>(dir: P, sync_writes: bool) -> StorageResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let state = Self {
            dir: dir.clone(),
            current_term: AtomicU64::new(0),
            voted_for: AtomicU64::new(0),
            has_vote: AtomicBool::new(false),
            sync_writes,
            write_lock: RwLock::new(()),
        };

        // Try to load existing state
        let state_path = dir.join(STATE_FILE);
        if state_path.exists() {
            state.load()?;
        }

        Ok(state)
    }

    /// Returns the current term.
    pub fn current_term(&self) -> Term {
        self.current_term.load(Ordering::Acquire)
    }

    /// Returns the node we voted for (if any).
    pub fn voted_for(&self) -> Option<NodeId> {
        if self.has_vote.load(Ordering::Acquire) {
            Some(self.voted_for.load(Ordering::Acquire))
        } else {
            None
        }
    }

    /// Saves the current term and vote.
    ///
    /// Uses atomic file replacement: write to temp file, sync, rename.
    pub fn save(&self, term: Term, voted_for: Option<NodeId>) -> StorageResult<()> {
        let _guard = self.write_lock.write();

        let tmp_path = self.dir.join(STATE_FILE_TMP);
        let state_path = self.dir.join(STATE_FILE);

        // Build the state buffer
        let mut buf = [0u8; STATE_FILE_SIZE];

        // Magic
        buf[0..4].copy_from_slice(&STATE_MAGIC.to_le_bytes());
        // Version
        buf[4..8].copy_from_slice(&STATE_VERSION.to_le_bytes());
        // Term
        buf[8..16].copy_from_slice(&term.to_le_bytes());
        // Voted for (use u64::MAX as sentinel for None)
        let voted_for_value = voted_for.unwrap_or(u64::MAX);
        buf[16..24].copy_from_slice(&voted_for_value.to_le_bytes());
        // Checksum (CRC32 of the rest)
        let checksum = crc32fast::hash(&buf[0..24]);
        buf[24..28].copy_from_slice(&checksum.to_le_bytes());

        // Write to temp file
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        file.write_all(&buf)?;

        if self.sync_writes {
            file.sync_all()?;
        }
        drop(file);

        // Atomic rename
        fs::rename(&tmp_path, &state_path)?;

        // Sync the directory to ensure the rename is durable
        if self.sync_writes {
            if let Ok(dir) = File::open(&self.dir) {
                let _ = dir.sync_all();
            }
        }

        // Update in-memory state
        self.current_term.store(term, Ordering::Release);
        if let Some(node_id) = voted_for {
            self.voted_for.store(node_id, Ordering::Release);
            self.has_vote.store(true, Ordering::Release);
        } else {
            self.voted_for.store(0, Ordering::Release);
            self.has_vote.store(false, Ordering::Release);
        }

        Ok(())
    }

    /// Loads the state from disk.
    fn load(&self) -> StorageResult<()> {
        let state_path = self.dir.join(STATE_FILE);

        let mut file = File::open(&state_path)?;
        let mut buf = [0u8; STATE_FILE_SIZE];
        file.read_exact(&mut buf)?;

        // Verify magic
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != STATE_MAGIC {
            return Err(StorageError::Corrupted(format!(
                "invalid magic: expected {:08x}, got {:08x}",
                STATE_MAGIC, magic
            )));
        }

        // Verify version
        let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        if version != STATE_VERSION {
            return Err(StorageError::Corrupted(format!(
                "unsupported version: {}",
                version
            )));
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]);
        let computed_checksum = crc32fast::hash(&buf[0..24]);
        if stored_checksum != computed_checksum {
            return Err(StorageError::Corrupted(format!(
                "checksum mismatch: expected {:08x}, got {:08x}",
                stored_checksum, computed_checksum
            )));
        }

        // Read term
        let term = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);

        // Read voted_for
        let voted_for_value = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);

        // Update in-memory state
        self.current_term.store(term, Ordering::Release);
        if voted_for_value != u64::MAX {
            self.voted_for.store(voted_for_value, Ordering::Release);
            self.has_vote.store(true, Ordering::Release);
        } else {
            self.voted_for.store(0, Ordering::Release);
            self.has_vote.store(false, Ordering::Release);
        }

        Ok(())
    }

    /// Updates the term if the given term is higher.
    ///
    /// If the term is updated, voted_for is cleared.
    /// Returns true if the term was updated.
    pub fn maybe_update_term(&self, term: Term) -> StorageResult<bool> {
        if term > self.current_term() {
            self.save(term, None)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Records a vote for the given candidate in the current term.
    pub fn vote_for(&self, candidate: NodeId) -> StorageResult<()> {
        self.save(self.current_term(), Some(candidate))
    }

    /// Increments the term and votes for self (starting an election).
    pub fn start_election(&self, self_id: NodeId) -> StorageResult<Term> {
        let new_term = self.current_term() + 1;
        self.save(new_term, Some(self_id))?;
        Ok(new_term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_state() {
        let tmp = TempDir::new().unwrap();
        let state = RaftState::new(tmp.path(), true).unwrap();

        assert_eq!(state.current_term(), 0);
        assert_eq!(state.voted_for(), None);
    }

    #[test]
    fn test_save_and_load() {
        let tmp = TempDir::new().unwrap();

        // Save state
        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            state.save(5, Some(42)).unwrap();
            assert_eq!(state.current_term(), 5);
            assert_eq!(state.voted_for(), Some(42));
        }

        // Load state in new instance
        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            assert_eq!(state.current_term(), 5);
            assert_eq!(state.voted_for(), Some(42));
        }
    }

    #[test]
    fn test_save_without_vote() {
        let tmp = TempDir::new().unwrap();

        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            state.save(10, None).unwrap();
        }

        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            assert_eq!(state.current_term(), 10);
            assert_eq!(state.voted_for(), None);
        }
    }

    #[test]
    fn test_maybe_update_term() {
        let tmp = TempDir::new().unwrap();
        let state = RaftState::new(tmp.path(), true).unwrap();

        // Higher term should update
        assert!(state.maybe_update_term(5).unwrap());
        assert_eq!(state.current_term(), 5);
        assert_eq!(state.voted_for(), None);

        // Vote for someone
        state.vote_for(42).unwrap();
        assert_eq!(state.voted_for(), Some(42));

        // Even higher term should clear vote
        assert!(state.maybe_update_term(10).unwrap());
        assert_eq!(state.current_term(), 10);
        assert_eq!(state.voted_for(), None);

        // Same or lower term should not update
        assert!(!state.maybe_update_term(10).unwrap());
        assert!(!state.maybe_update_term(5).unwrap());
    }

    #[test]
    fn test_start_election() {
        let tmp = TempDir::new().unwrap();
        let state = RaftState::new(tmp.path(), true).unwrap();

        let new_term = state.start_election(1).unwrap();
        assert_eq!(new_term, 1);
        assert_eq!(state.current_term(), 1);
        assert_eq!(state.voted_for(), Some(1));

        let new_term = state.start_election(1).unwrap();
        assert_eq!(new_term, 2);
        assert_eq!(state.current_term(), 2);
        assert_eq!(state.voted_for(), Some(1));
    }

    #[test]
    fn test_corrupted_magic() {
        let tmp = TempDir::new().unwrap();

        // Write a file with bad magic
        let state_path = tmp.path().join(STATE_FILE);
        let mut buf = [0u8; STATE_FILE_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        fs::write(&state_path, &buf).unwrap();

        let result = RaftState::new(tmp.path(), true);
        assert!(matches!(result, Err(StorageError::Corrupted(_))));
    }

    #[test]
    fn test_corrupted_checksum() {
        let tmp = TempDir::new().unwrap();

        // Create valid state first
        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            state.save(5, Some(42)).unwrap();
        }

        // Corrupt the checksum
        let state_path = tmp.path().join(STATE_FILE);
        let mut buf = fs::read(&state_path).unwrap();
        buf[24] ^= 0xFF; // Flip some bits
        fs::write(&state_path, &buf).unwrap();

        let result = RaftState::new(tmp.path(), true);
        assert!(matches!(result, Err(StorageError::Corrupted(_))));
    }

    #[test]
    fn test_vote_for_node_zero() {
        let tmp = TempDir::new().unwrap();

        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            state.save(5, Some(0)).unwrap();
            assert_eq!(state.voted_for(), Some(0));
        }

        // Reload and verify node 0 vote is preserved
        {
            let state = RaftState::new(tmp.path(), true).unwrap();
            assert_eq!(state.current_term(), 5);
            assert_eq!(state.voted_for(), Some(0));
        }
    }
}
