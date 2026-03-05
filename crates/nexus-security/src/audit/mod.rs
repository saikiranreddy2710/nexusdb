//! Tamper-proof audit logging with hash-chain integrity.
//!
//! Every security-relevant event is recorded as an [`AuditEntry`] linked
//! to its predecessor via a SHA-256 hash, forming an append-only chain.
//! Modifying or deleting any entry breaks the chain, making tampering
//! detectable.
//!
//! # Thread Safety
//!
//! All state is protected by a single `Mutex` to guarantee atomicity of
//! sequence allocation, hash chaining, and entry insertion.  This prevents
//! the race condition where two concurrent `record()` calls could observe
//! the same `prev_hash`.

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

impl AuditAction {
    /// Returns a stable string representation for each variant.
    ///
    /// Unlike `Debug`, this is guaranteed to be consistent across compiler
    /// versions and is safe to use in hashing and serialization contexts.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditAction::AuthSuccess => "AuthSuccess",
            AuditAction::AuthFailure => "AuthFailure",
            AuditAction::AccountLocked => "AccountLocked",
            AuditAction::Query => "Query",
            AuditAction::SchemaChange => "SchemaChange",
            AuditAction::DataModification => "DataModification",
            AuditAction::PermissionChange => "PermissionChange",
            AuditAction::UserChange => "UserChange",
            AuditAction::Connect => "Connect",
            AuditAction::Disconnect => "Disconnect",
        }
    }
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
        hasher.update(self.action.as_str().as_bytes());
        if let Some(ref t) = self.target {
            hasher.update(t.as_bytes());
        }
        if let Some(ref d) = self.detail {
            hasher.update(d.as_bytes());
        }
        match &self.client_ip {
            Some(ip) => hasher.update(ip.as_bytes()),
            None => hasher.update(b"<no-ip>"),
        }
        hasher.update(if self.success { &[1u8] } else { &[0u8] });
        hasher.update(self.prev_hash.as_bytes());

        let result = hasher.finalize();
        result.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Genesis hash (64 hex zeros).
fn genesis_hash() -> String {
    "0".repeat(64)
}

/// Consolidated inner state protected by a single mutex to prevent
/// race conditions in the hash chain.
#[derive(Debug)]
struct AuditLogInner {
    entries: Vec<AuditEntry>,
    next_seq: u64,
    last_hash: String,
    /// Hash at the start of the retained window.  After eviction, the first
    /// remaining entry's `prev_hash` will NOT match genesis.  This field
    /// records what the valid "chain start" hash should be so that
    /// `verify_integrity` still works after eviction.
    chain_start_hash: String,
}

/// In-memory tamper-proof audit log with hash-chain integrity.
#[derive(Debug)]
pub struct AuditLog {
    inner: Mutex<AuditLogInner>,
    /// Maximum number of entries to retain in memory.
    max_entries: usize,
}

impl AuditLog {
    /// Create a new audit log.
    pub fn new(max_entries: usize) -> Self {
        let genesis = genesis_hash();
        Self {
            inner: Mutex::new(AuditLogInner {
                entries: Vec::new(),
                next_seq: 1,
                last_hash: genesis.clone(),
                chain_start_hash: genesis,
            }),
            max_entries,
        }
    }

    /// Record an audit event.
    ///
    /// The entire operation (sequence allocation, hash computation, insertion,
    /// and eviction) is performed under a single lock to guarantee chain
    /// integrity even under concurrent access.
    pub fn record(
        &self,
        username: impl Into<String>,
        action: AuditAction,
        target: Option<String>,
        detail: Option<String>,
        client_ip: Option<String>,
        success: bool,
    ) {
        let mut inner = self.inner.lock();

        let seq = inner.next_seq;
        inner.next_seq += 1;

        let mut entry = AuditEntry {
            sequence: seq,
            timestamp_ms: now_millis(),
            username: username.into(),
            action,
            target,
            detail,
            client_ip,
            success,
            prev_hash: inner.last_hash.clone(),
            hash: String::new(),
        };

        entry.hash = entry.compute_hash();
        inner.last_hash = entry.hash.clone();
        inner.entries.push(entry);

        // Evict oldest entries if over capacity
        if inner.entries.len() > self.max_entries {
            let drain = inner.entries.len() - self.max_entries;
            // The last evicted entry's hash becomes the new chain start
            if let Some(last_evicted) = inner.entries.get(drain - 1) {
                inner.chain_start_hash = last_evicted.hash.clone();
            }
            inner.entries.drain(..drain);
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
        self.inner.lock().entries.len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().entries.is_empty()
    }

    /// Get all entries (clone).
    pub fn entries(&self) -> Vec<AuditEntry> {
        self.inner.lock().entries.clone()
    }

    /// Get entries filtered by action.
    pub fn entries_by_action(&self, action: AuditAction) -> Vec<AuditEntry> {
        self.inner
            .lock()
            .entries
            .iter()
            .filter(|e| e.action == action)
            .cloned()
            .collect()
    }

    /// Get entries for a specific user.
    pub fn entries_by_user(&self, username: &str) -> Vec<AuditEntry> {
        self.inner
            .lock()
            .entries
            .iter()
            .filter(|e| e.username == username)
            .cloned()
            .collect()
    }

    /// Verify the integrity of the hash chain.
    ///
    /// Returns `true` if all entries are properly linked and no entry
    /// has been tampered with.  Correctly handles the case where oldest
    /// entries have been evicted (the chain starts from the last evicted
    /// entry's hash rather than genesis).
    pub fn verify_integrity(&self) -> bool {
        let inner = self.inner.lock();

        let mut expected_prev = &inner.chain_start_hash;

        for entry in inner.entries.iter() {
            // Check prev_hash links to previous entry (or chain start)
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
        let inner = self.inner.lock();
        serde_json::to_string_pretty(&inner.entries).unwrap_or_default()
    }

    /// Get the number of entries that have been evicted since creation.
    pub fn evicted_count(&self) -> u64 {
        let inner = self.inner.lock();
        let first_seq = inner.entries.first().map(|e| e.sequence).unwrap_or(1);
        first_seq.saturating_sub(1)
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
            let mut inner = log.inner.lock();
            inner.entries[1].detail = Some("MODIFIED".to_string());
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
    fn test_integrity_after_eviction() {
        // This test verifies that verify_integrity works correctly even
        // after entries have been evicted from the log.
        let log = AuditLog::new(5);

        for i in 0..10 {
            log.record_query("u", &format!("q{}", i), true);
        }

        // Integrity should still hold after eviction
        assert!(log.verify_integrity());
        assert_eq!(log.evicted_count(), 5);
    }

    #[test]
    fn test_tamper_detection_after_eviction() {
        let log = AuditLog::new(5);

        for i in 0..10 {
            log.record_query("u", &format!("q{}", i), true);
        }

        assert!(log.verify_integrity());

        // Tamper with the first remaining entry
        {
            let mut inner = log.inner.lock();
            inner.entries[0].detail = Some("TAMPERED".to_string());
        }

        assert!(!log.verify_integrity());
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

    #[test]
    fn test_concurrent_records_maintain_integrity() {
        use std::sync::Arc;
        use std::thread;

        let log = Arc::new(AuditLog::new(10_000));
        let mut handles = Vec::new();

        for t in 0..4 {
            let log = Arc::clone(&log);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    log.record_query(&format!("thread_{}", t), &format!("SELECT {}", i), true);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(log.len(), 400);
        // Hash chain must be intact despite concurrent writes
        assert!(log.verify_integrity());
    }
}
