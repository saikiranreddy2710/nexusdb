//! Tamper-proof audit logging with hash-chain integrity.
//!
//! Every security-relevant event is recorded as an [`AuditEntry`] linked
//! to its predecessor via a SHA-256 hash, forming an append-only chain.
//! Modifying or deleting any entry breaks the chain, making tampering
//! detectable.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The type of audited event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditAction {
    /// Successful authentication.
    AuthSuccess,
    /// Failed authentication attempt.
    AuthFailure,
    /// Account lockout triggered.
    AccountLocked,
    /// SQL query executed.
    Query,
    /// Schema change (CREATE/ALTER/DROP).
    SchemaChange,
    /// Data modification (INSERT/UPDATE/DELETE).
    DataModification,
    /// Permission grant or revoke.
    PermissionChange,
    /// User created or dropped.
    UserChange,
    /// Connection opened.
    Connect,
    /// Connection closed.
    Disconnect,
}

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
    /// The user who performed the action (or "anonymous").
    pub username: String,
    /// The action category.
    pub action: AuditAction,
    /// Target object (e.g. table name, database name).
    pub target: Option<String>,
    /// Freeform detail string (e.g. the SQL statement).
    pub detail: Option<String>,
    /// Client IP address (if available).
    pub client_ip: Option<String>,
    /// Whether the action succeeded.
    pub success: bool,
    /// SHA-256 hash of the previous entry (hex-encoded).
    pub prev_hash: String,
    /// SHA-256 hash of this entry (hex-encoded).
    pub hash: String,
}

impl AuditEntry {
    /// Compute the hash of this entry's content (excluding the hash field itself).
    fn compute_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.sequence.to_le_bytes());
        hasher.update(self.timestamp_ms.to_le_bytes());
        hasher.update(self.username.as_bytes());
        hasher.update(format!("{:?}", self.action).as_bytes());
        if let Some(ref t) = self.target {
            hasher.update(t.as_bytes());
        }
        if let Some(ref d) = self.detail {
            hasher.update(d.as_bytes());
        }
        hasher.update(if self.success { &[1u8] } else { &[0u8] });
        hasher.update(self.prev_hash.as_bytes());

        let result = hasher.finalize();
        result.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// In-memory tamper-proof audit log with hash-chain integrity.
#[derive(Debug)]
pub struct AuditLog {
    entries: Mutex<Vec<AuditEntry>>,
    next_seq: AtomicU64,
    last_hash: Mutex<String>,
    /// Maximum number of entries to retain in memory.
    max_entries: usize,
}

impl AuditLog {
    /// Create a new audit log.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            next_seq: AtomicU64::new(1),
            last_hash: Mutex::new("0".repeat(64)), // genesis hash
            max_entries,
        }
    }

    /// Record an audit event.
    pub fn record(
        &self,
        username: impl Into<String>,
        action: AuditAction,
        target: Option<String>,
        detail: Option<String>,
        client_ip: Option<String>,
        success: bool,
    ) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let prev_hash = self.last_hash.lock().clone();

        let mut entry = AuditEntry {
            sequence: seq,
            timestamp_ms: now_millis(),
            username: username.into(),
            action,
            target,
            detail,
            client_ip,
            success,
            prev_hash,
            hash: String::new(),
        };

        entry.hash = entry.compute_hash();
        *self.last_hash.lock() = entry.hash.clone();

        let mut entries = self.entries.lock();
        entries.push(entry);

        // Evict oldest entries if over capacity
        if entries.len() > self.max_entries {
            let drain = entries.len() - self.max_entries;
            entries.drain(..drain);
        }
    }

    /// Convenience: record a successful query.
    pub fn record_query(&self, username: &str, sql: &str, success: bool) {
        self.record(
            username,
            AuditAction::Query,
            None,
            Some(sql.to_string()),
            None,
            success,
        );
    }

    /// Convenience: record an authentication event.
    pub fn record_auth(&self, username: &str, success: bool, client_ip: Option<String>) {
        self.record(
            username,
            if success {
                AuditAction::AuthSuccess
            } else {
                AuditAction::AuthFailure
            },
            None,
            None,
            client_ip,
            success,
        );
    }

    /// Get the total number of entries.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    /// Get all entries (clone).
    pub fn entries(&self) -> Vec<AuditEntry> {
        self.entries.lock().clone()
    }

    /// Get entries filtered by action.
    pub fn entries_by_action(&self, action: AuditAction) -> Vec<AuditEntry> {
        self.entries
            .lock()
            .iter()
            .filter(|e| e.action == action)
            .cloned()
            .collect()
    }

    /// Get entries for a specific user.
    pub fn entries_by_user(&self, username: &str) -> Vec<AuditEntry> {
        self.entries
            .lock()
            .iter()
            .filter(|e| e.username == username)
            .cloned()
            .collect()
    }

    /// Verify the integrity of the hash chain.
    ///
    /// Returns `true` if all entries are properly linked and no entry
    /// has been tampered with. Returns `false` if any hash is invalid.
    pub fn verify_integrity(&self) -> bool {
        let entries = self.entries.lock();
        let genesis = "0".repeat(64);
        let mut expected_prev = &genesis;

        for entry in entries.iter() {
            // Check prev_hash links to previous entry
            if entry.prev_hash != *expected_prev {
                return false;
            }
            // Check the entry's own hash is correct
            let computed = entry.compute_hash();
            if entry.hash != computed {
                return false;
            }
            expected_prev = &entry.hash;
        }

        true
    }

    /// Export entries as JSON (for SIEM integration).
    pub fn export_json(&self) -> String {
        let entries = self.entries.lock();
        serde_json::to_string_pretty(&*entries).unwrap_or_default()
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_record_and_retrieve() {
        let log = AuditLog::new(1000);
        log.record_query("alice", "SELECT * FROM users", true);
        log.record_query("bob", "DROP TABLE users", false);

        assert_eq!(log.len(), 2);
        let entries = log.entries();
        assert_eq!(entries[0].username, "alice");
        assert_eq!(entries[1].username, "bob");
        assert!(entries[0].success);
        assert!(!entries[1].success);
    }

    #[test]
    fn test_hash_chain_integrity() {
        let log = AuditLog::new(1000);

        for i in 0..10 {
            log.record_query("user", &format!("SELECT {}", i), true);
        }

        assert!(log.verify_integrity());
    }

    #[test]
    fn test_tamper_detection() {
        let log = AuditLog::new(1000);
        log.record_query("alice", "SELECT 1", true);
        log.record_query("alice", "SELECT 2", true);
        log.record_query("alice", "SELECT 3", true);

        assert!(log.verify_integrity());

        // Tamper with an entry
        {
            let mut entries = log.entries.lock();
            entries[1].detail = Some("MODIFIED".to_string());
        }

        // Chain should now be broken
        assert!(!log.verify_integrity());
    }

    #[test]
    fn test_entry_eviction() {
        let log = AuditLog::new(5);

        for i in 0..10 {
            log.record_query("u", &format!("q{}", i), true);
        }

        assert_eq!(log.len(), 5);
        // Oldest entries should have been evicted
        let entries = log.entries();
        assert_eq!(entries[0].sequence, 6);
    }

    #[test]
    fn test_filter_by_action() {
        let log = AuditLog::new(1000);
        log.record_auth("alice", true, None);
        log.record_query("alice", "SELECT 1", true);
        log.record_auth("bob", false, None);

        let auth_entries = log.entries_by_action(AuditAction::AuthSuccess);
        assert_eq!(auth_entries.len(), 1);
        assert_eq!(auth_entries[0].username, "alice");

        let fail_entries = log.entries_by_action(AuditAction::AuthFailure);
        assert_eq!(fail_entries.len(), 1);
    }

    #[test]
    fn test_filter_by_user() {
        let log = AuditLog::new(1000);
        log.record_query("alice", "q1", true);
        log.record_query("bob", "q2", true);
        log.record_query("alice", "q3", true);

        let alice = log.entries_by_user("alice");
        assert_eq!(alice.len(), 2);
    }

    #[test]
    fn test_export_json() {
        let log = AuditLog::new(1000);
        log.record_query("alice", "SELECT 1", true);

        let json = log.export_json();
        assert!(json.contains("alice"));
        assert!(json.contains("SELECT 1"));
    }
}
