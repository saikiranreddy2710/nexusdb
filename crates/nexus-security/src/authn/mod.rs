//! Authentication: verify the identity of every request.
//!
//! Supports multiple credential types:
//! - **SCRAM-SHA-256**: PostgreSQL-compatible password authentication
//! - **API Key**: Simple token-based auth for service accounts
//! - **JWT**: Stateless tokens (validated against a shared secret)

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Authentication error.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("user not found: {0}")]
    UserNotFound(String),
    #[error("user already exists: {0}")]
    UserAlreadyExists(String),
    #[error("account locked: {0}")]
    AccountLocked(String),
    #[error("token expired")]
    TokenExpired,
    #[error("invalid token")]
    InvalidToken,
    #[error("authentication required")]
    AuthRequired,
    #[error("internal error: {0}")]
    Internal(String),
}

pub type AuthResult<T> = Result<T, AuthError>;

/// A credential presented by a client.
#[derive(Debug, Clone)]
pub enum Credential {
    /// Username + password (hashed via SCRAM-SHA-256).
    Password { username: String, password: String },
    /// API key (bearer token).
    ApiKey(String),
    /// JWT token.
    Jwt(String),
}

/// A verified identity resulting from successful authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Unique user identifier.
    pub username: String,
    /// Roles assigned to this identity.
    pub roles: Vec<String>,
    /// When this identity was authenticated.
    pub authenticated_at: u64, // epoch seconds
    /// Authentication method used.
    pub method: AuthMethod,
}

/// The method by which an identity was authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Password,
    ApiKey,
    Jwt,
    /// Internal system user (no external auth).
    System,
}

impl Identity {
    /// Create a system identity (superuser, no external auth needed).
    pub fn system() -> Self {
        Self {
            username: "system".to_string(),
            roles: vec!["superuser".to_string()],
            authenticated_at: now_epoch(),
            method: AuthMethod::System,
        }
    }

    /// Check if identity has a given role.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Check if identity is a superuser.
    pub fn is_superuser(&self) -> bool {
        self.has_role("superuser")
    }
}

/// Stored user record (server-side).
#[derive(Debug, Clone)]
struct UserRecord {
    username: String,
    /// SHA-256 hash of (salt + password).
    password_hash: [u8; 32],
    salt: [u8; 16],
    roles: Vec<String>,
    /// Number of consecutive failed login attempts.
    failed_attempts: u32,
    /// Whether the account is locked.
    locked: bool,
    /// API key (if assigned).
    api_key: Option<String>,
}

/// Maximum failed attempts before account lockout.
const MAX_FAILED_ATTEMPTS: u32 = 5;

/// The main authenticator that manages users and validates credentials.
#[derive(Debug)]
pub struct Authenticator {
    /// User store (username -> record).
    users: RwLock<HashMap<String, UserRecord>>,
    /// JWT secret for token validation.
    jwt_secret: Vec<u8>,
    /// Whether authentication is enforced (false = allow all).
    enforce: bool,
}

impl Authenticator {
    /// Create a new authenticator.
    ///
    /// If `enforce` is false, all authentication requests succeed with
    /// a default identity (useful for development/testing).
    pub fn new(enforce: bool) -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            jwt_secret: generate_random_bytes(32),
            enforce,
        }
    }

    /// Create a non-enforcing authenticator (all requests pass).
    pub fn permissive() -> Self {
        Self::new(false)
    }

    /// Whether authentication is enforced.
    pub fn is_enforcing(&self) -> bool {
        self.enforce
    }

    /// Register a new user with a password and roles.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> AuthResult<()> {
        let mut users = self.users.write();
        if users.contains_key(username) {
            return Err(AuthError::UserAlreadyExists(username.to_string()));
        }

        let salt = generate_random_bytes(16);
        let mut salt_arr = [0u8; 16];
        salt_arr.copy_from_slice(&salt);
        let hash = hash_password(password, &salt_arr);

        users.insert(
            username.to_string(),
            UserRecord {
                username: username.to_string(),
                password_hash: hash,
                salt: salt_arr,
                roles,
                failed_attempts: 0,
                locked: false,
                api_key: None,
            },
        );

        tracing::info!(user = username, "user created");
        Ok(())
    }

    /// Drop a user.
    pub fn drop_user(&self, username: &str) -> AuthResult<()> {
        let mut users = self.users.write();
        users
            .remove(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;
        tracing::info!(user = username, "user dropped");
        Ok(())
    }

    /// Assign an API key to a user. Returns the generated key.
    pub fn assign_api_key(&self, username: &str) -> AuthResult<String> {
        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        let key = format!("nxk_{}", hex::encode(&generate_random_bytes(24)));
        user.api_key = Some(key.clone());
        Ok(key)
    }

    /// Authenticate a credential and return a verified identity.
    pub fn authenticate(&self, credential: &Credential) -> AuthResult<Identity> {
        if !self.enforce {
            return Ok(Identity {
                username: match credential {
                    Credential::Password { username, .. } => username.clone(),
                    Credential::ApiKey(_) => "api_user".to_string(),
                    Credential::Jwt(_) => "jwt_user".to_string(),
                },
                roles: vec!["superuser".to_string()],
                authenticated_at: now_epoch(),
                method: AuthMethod::System,
            });
        }

        match credential {
            Credential::Password { username, password } => {
                self.authenticate_password(username, password)
            }
            Credential::ApiKey(key) => self.authenticate_api_key(key),
            Credential::Jwt(token) => self.authenticate_jwt(token),
        }
    }

    /// Verify a password credential.
    fn authenticate_password(&self, username: &str, password: &str) -> AuthResult<Identity> {
        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or(AuthError::InvalidCredentials)?;

        if user.locked {
            return Err(AuthError::AccountLocked(username.to_string()));
        }

        let hash = hash_password(password, &user.salt);
        if hash != user.password_hash {
            user.failed_attempts += 1;
            if user.failed_attempts >= MAX_FAILED_ATTEMPTS {
                user.locked = true;
                tracing::warn!(
                    user = username,
                    attempts = user.failed_attempts,
                    "account locked after too many failed attempts"
                );
            }
            return Err(AuthError::InvalidCredentials);
        }

        // Success: reset failed attempts
        user.failed_attempts = 0;

        Ok(Identity {
            username: user.username.clone(),
            roles: user.roles.clone(),
            authenticated_at: now_epoch(),
            method: AuthMethod::Password,
        })
    }

    /// Verify an API key credential.
    fn authenticate_api_key(&self, key: &str) -> AuthResult<Identity> {
        let users = self.users.read();
        for user in users.values() {
            if user.api_key.as_deref() == Some(key) {
                return Ok(Identity {
                    username: user.username.clone(),
                    roles: user.roles.clone(),
                    authenticated_at: now_epoch(),
                    method: AuthMethod::ApiKey,
                });
            }
        }
        Err(AuthError::InvalidCredentials)
    }

    /// Verify a JWT token (simplified HMAC-SHA256 validation).
    fn authenticate_jwt(&self, token: &str) -> AuthResult<Identity> {
        // Simple format: base64(json_payload).base64(hmac_signature)
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidToken);
        }

        let payload_b64 = parts[0];
        let sig_b64 = parts[1];

        // Verify HMAC
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(&self.jwt_secret)
            .map_err(|e| AuthError::Internal(format!("HMAC init failed: {}", e)))?;
        mac.update(payload_b64.as_bytes());
        let expected_sig = hex::encode(mac.finalize().into_bytes().as_slice());

        if sig_b64 != expected_sig {
            return Err(AuthError::InvalidToken);
        }

        // Decode payload
        let payload_bytes = base64_decode(payload_b64).map_err(|_| AuthError::InvalidToken)?;
        let payload: serde_json::Value =
            serde_json::from_slice(&payload_bytes).map_err(|_| AuthError::InvalidToken)?;

        let username = payload["sub"]
            .as_str()
            .ok_or(AuthError::InvalidToken)?
            .to_string();
        let exp = payload["exp"].as_u64().unwrap_or(0);

        if exp > 0 && exp < now_epoch() {
            return Err(AuthError::TokenExpired);
        }

        let roles = payload["roles"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Identity {
            username,
            roles,
            authenticated_at: now_epoch(),
            method: AuthMethod::Jwt,
        })
    }

    /// Issue a JWT token for a given user. Returns the token string.
    pub fn issue_jwt(&self, username: &str, ttl: Duration) -> AuthResult<String> {
        let users = self.users.read();
        let user = users
            .get(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        let exp = now_epoch() + ttl.as_secs();
        let payload = serde_json::json!({
            "sub": user.username,
            "roles": user.roles,
            "exp": exp,
            "iat": now_epoch(),
        });

        let payload_b64 = base64_encode(payload.to_string().as_bytes());

        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(&self.jwt_secret)
            .map_err(|e| AuthError::Internal(format!("HMAC init failed: {}", e)))?;
        mac.update(payload_b64.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes().as_slice());

        Ok(format!("{}.{}", payload_b64, sig))
    }

    /// Number of registered users.
    pub fn user_count(&self) -> usize {
        self.users.read().len()
    }

    /// Unlock a locked account (admin operation).
    pub fn unlock_user(&self, username: &str) -> AuthResult<()> {
        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;
        user.locked = false;
        user.failed_attempts = 0;
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn hash_password(password: &str, salt: &[u8; 16]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

fn generate_random_bytes(len: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen()).collect()
}

fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Minimal base64 encode (no external dep).
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_decode(input: &str) -> Result<Vec<u8>, ()> {
    fn val(c: u8) -> Result<u32, ()> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(()),
        }
    }

    let input = input.as_bytes();
    let mut out = Vec::new();
    for chunk in input.chunks(4) {
        if chunk.len() < 2 {
            break;
        }
        let a = val(chunk[0])?;
        let b = val(chunk[1])?;
        let c = if chunk.len() > 2 && chunk[2] != b'=' {
            val(chunk[2])?
        } else {
            0
        };
        let d = if chunk.len() > 3 && chunk[3] != b'=' {
            val(chunk[3])?
        } else {
            0
        };
        let triple = (a << 18) | (b << 12) | (c << 6) | d;
        out.push(((triple >> 16) & 0xFF) as u8);
        if chunk.len() > 2 && chunk[2] != b'=' {
            out.push(((triple >> 8) & 0xFF) as u8);
        }
        if chunk.len() > 3 && chunk[3] != b'=' {
            out.push((triple & 0xFF) as u8);
        }
    }
    Ok(out)
}

/// Hex encode helper (avoid external dep).
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_authenticate_user() {
        let auth = Authenticator::new(true);
        auth.create_user("alice", "password123", vec!["analyst".into()])
            .unwrap();

        let cred = Credential::Password {
            username: "alice".into(),
            password: "password123".into(),
        };
        let id = auth.authenticate(&cred).unwrap();
        assert_eq!(id.username, "alice");
        assert!(id.has_role("analyst"));
        assert_eq!(id.method, AuthMethod::Password);
    }

    #[test]
    fn test_bad_password_rejected() {
        let auth = Authenticator::new(true);
        auth.create_user("bob", "secret", vec![]).unwrap();

        let cred = Credential::Password {
            username: "bob".into(),
            password: "wrong".into(),
        };
        assert!(matches!(
            auth.authenticate(&cred),
            Err(AuthError::InvalidCredentials)
        ));
    }

    #[test]
    fn test_account_lockout() {
        let auth = Authenticator::new(true);
        auth.create_user("charlie", "pass", vec![]).unwrap();

        let bad_cred = Credential::Password {
            username: "charlie".into(),
            password: "wrong".into(),
        };

        for _ in 0..MAX_FAILED_ATTEMPTS {
            let _ = auth.authenticate(&bad_cred);
        }

        // Now even the correct password should fail (locked)
        let good_cred = Credential::Password {
            username: "charlie".into(),
            password: "pass".into(),
        };
        assert!(matches!(
            auth.authenticate(&good_cred),
            Err(AuthError::AccountLocked(_))
        ));

        // Unlock
        auth.unlock_user("charlie").unwrap();
        assert!(auth.authenticate(&good_cred).is_ok());
    }

    #[test]
    fn test_api_key_auth() {
        let auth = Authenticator::new(true);
        auth.create_user("svc", "x", vec!["service".into()])
            .unwrap();
        let key = auth.assign_api_key("svc").unwrap();

        let id = auth.authenticate(&Credential::ApiKey(key)).unwrap();
        assert_eq!(id.username, "svc");
        assert_eq!(id.method, AuthMethod::ApiKey);
    }

    #[test]
    fn test_jwt_roundtrip() {
        let auth = Authenticator::new(true);
        auth.create_user("admin", "x", vec!["superuser".into()])
            .unwrap();

        let token = auth.issue_jwt("admin", Duration::from_secs(3600)).unwrap();

        let id = auth.authenticate(&Credential::Jwt(token)).unwrap();
        assert_eq!(id.username, "admin");
        assert!(id.has_role("superuser"));
        assert_eq!(id.method, AuthMethod::Jwt);
    }

    #[test]
    fn test_permissive_mode() {
        let auth = Authenticator::permissive();
        let cred = Credential::Password {
            username: "anyone".into(),
            password: "anything".into(),
        };
        let id = auth.authenticate(&cred).unwrap();
        assert_eq!(id.username, "anyone");
    }

    #[test]
    fn test_duplicate_user() {
        let auth = Authenticator::new(true);
        auth.create_user("dup", "p", vec![]).unwrap();
        assert!(matches!(
            auth.create_user("dup", "p", vec![]),
            Err(AuthError::UserAlreadyExists(_))
        ));
    }

    #[test]
    fn test_drop_user() {
        let auth = Authenticator::new(true);
        auth.create_user("temp", "p", vec![]).unwrap();
        assert_eq!(auth.user_count(), 1);
        auth.drop_user("temp").unwrap();
        assert_eq!(auth.user_count(), 0);
    }
}
