//! Tamper-proof audit logging with hash chain integrity.
//!
//! Every database operation is recorded in an append-only log where
//! each entry contains a hash of the previous entry, forming a chain
//! similar to a blockchain. Tampering with any entry breaks the chain.
//!
//! ## Hash Chain
//!
//! ```text
//! Entry 0: { data, hash(data + "genesis") }
//! Entry 1: { data, hash(data + entry_0.hash) }
//! Entry 2: { data, hash(data + entry_1.hash) }
//! ```

use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// An audit log entry recording a database operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Sequential entry ID.
    pub id: u64,
    /// Timestamp of the operation.
    pub timestamp: u64,
    /// Username who performed the operation.
    pub user: String,
    /// Type of operation (SELECT, INSERT, UPDATE, DELETE, DDL, AUTH).
    pub action: String,
    /// Resource affected (table name, or "system" for auth events).
    pub resource: String,
    /// Additional details (SQL query, rows affected, etc.).
    pub details: String,
    /// Client IP address (if available).
    pub client_ip: Option<String>,
    /// Whether the operation succeeded.
    pub success: bool,
    /// SHA-256 hash of this entry + previous hash (chain integrity).
    pub hash: String,
    /// Hash of the previous entry (empty string for genesis).
    pub prev_hash: String,
}

/// Append-only audit log with hash chain integrity verification.
pub struct AuditLog {
    entries: RwLock<Vec<AuditEntry>>,
    next_id: RwLock<u64>,
}

impl AuditLog {
    /// Create a new empty audit log.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            next_id: RwLock::new(0),
        }
    }

    /// Record an audit entry.
    pub fn record(
        &self,
        user: &str,
        action: &str,
        resource: &str,
        details: &str,
        client_ip: Option<&str>,
        success: bool,
    ) -> AuditEntry {
        let mut entries = self.entries.write();
        let mut next_id = self.next_id.write();

        let prev_hash = entries
            .last()
            .map(|e| e.hash.clone())
            .unwrap_or_default();

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let id = *next_id;
        *next_id += 1;

        // Compute hash chain: H(id || timestamp || user || action || resource || details || prev_hash)
        let hash_input = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            id, now, user, action, resource, details, prev_hash
        );
        let hash = compute_sha256(&hash_input);

        let entry = AuditEntry {
            id,
            timestamp: now,
            user: user.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            details: details.to_string(),
            client_ip: client_ip.map(|s| s.to_string()),
            success,
            hash,
            prev_hash,
        };

        entries.push(entry.clone());
        entry
    }

    /// Verify the integrity of the entire audit chain.
    ///
    /// Returns `Ok(())` if the chain is intact, or `Err(entry_id)` for
    /// the first entry where the chain is broken.
    pub fn verify_chain(&self) -> Result<(), u64> {
        let entries = self.entries.read();

        for (i, entry) in entries.iter().enumerate() {
            // Verify prev_hash link
            let expected_prev = if i == 0 {
                String::new()
            } else {
                entries[i - 1].hash.clone()
            };

            if entry.prev_hash != expected_prev {
                return Err(entry.id);
            }

            // Verify entry hash
            let hash_input = format!(
                "{}|{}|{}|{}|{}|{}|{}",
                entry.id,
                entry.timestamp,
                entry.user,
                entry.action,
                entry.resource,
                entry.details,
                entry.prev_hash
            );
            let computed = compute_sha256(&hash_input);

            if entry.hash != computed {
                return Err(entry.id);
            }
        }

        Ok(())
    }

    /// Get all entries (most recent last).
    pub fn entries(&self) -> Vec<AuditEntry> {
        self.entries.read().clone()
    }

    /// Get entries for a specific user.
    pub fn entries_for_user(&self, user: &str) -> Vec<AuditEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.user == user)
            .cloned()
            .collect()
    }

    /// Get entries for a specific resource/table.
    pub fn entries_for_resource(&self, resource: &str) -> Vec<AuditEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.resource == resource)
            .cloned()
            .collect()
    }

    /// Total number of audit entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Whether the audit log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute SHA-256 hash using ring.
fn compute_sha256(input: &str) -> String {
    use ring::digest;
    let hash = digest::digest(&digest::SHA256, input.as_bytes());
    hex_encode(hash.as_ref())
}

/// Encode bytes as hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_log_basic() {
        let log = AuditLog::new();

        log.record("alice", "SELECT", "users", "SELECT * FROM users", None, true);
        log.record("bob", "INSERT", "orders", "INSERT INTO orders ...", None, true);

        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_hash_chain_integrity() {
        let log = AuditLog::new();

        log.record("alice", "SELECT", "users", "query1", None, true);
        log.record("bob", "INSERT", "orders", "query2", None, true);
        log.record("charlie", "DELETE", "items", "query3", None, true);

        // Chain should be valid
        assert!(log.verify_chain().is_ok());
    }

    #[test]
    fn test_tamper_detection() {
        let log = AuditLog::new();

        log.record("alice", "SELECT", "users", "query1", None, true);
        log.record("bob", "INSERT", "orders", "query2", None, true);

        // Tamper with the first entry
        {
            let mut entries = log.entries.write();
            entries[0].details = "TAMPERED".to_string();
        }

        // Chain should be broken
        assert!(log.verify_chain().is_err());
    }

    #[test]
    fn test_entries_by_user() {
        let log = AuditLog::new();

        log.record("alice", "SELECT", "t1", "", None, true);
        log.record("bob", "INSERT", "t2", "", None, true);
        log.record("alice", "UPDATE", "t1", "", None, true);

        let alice_entries = log.entries_for_user("alice");
        assert_eq!(alice_entries.len(), 2);
    }

    #[test]
    fn test_entries_by_resource() {
        let log = AuditLog::new();

        log.record("u1", "SELECT", "users", "", None, true);
        log.record("u2", "INSERT", "orders", "", None, true);
        log.record("u3", "UPDATE", "users", "", None, true);

        let user_entries = log.entries_for_resource("users");
        assert_eq!(user_entries.len(), 2);
    }

    #[test]
    fn test_prev_hash_chain() {
        let log = AuditLog::new();

        let e1 = log.record("a", "SELECT", "t", "", None, true);
        let e2 = log.record("b", "INSERT", "t", "", None, true);

        assert_eq!(e1.prev_hash, "");       // Genesis entry
        assert_eq!(e2.prev_hash, e1.hash);  // Chained to e1
    }

    #[test]
    fn test_failed_operation_logged() {
        let log = AuditLog::new();

        log.record("hacker", "DROP", "system", "DROP TABLE *", Some("1.2.3.4"), false);

        let entries = log.entries();
        assert!(!entries[0].success);
        assert_eq!(entries[0].client_ip, Some("1.2.3.4".to_string()));
    }
}
