//! Authentication: identity verification for every request.
//!
//! Supports multiple authentication methods:
//! - Password-based with Argon2id hashing (OWASP recommended)
//! - JWT bearer tokens for stateless API authentication
//!
//! ## Password Security
//!
//! Uses Argon2id (winner of the Password Hashing Competition) with
//! parameters tuned for server-side verification (~100ms per hash).

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::SaltString;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use parking_lot::RwLock;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Authentication errors.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Invalid credentials (wrong password or unknown user).
    #[error("invalid credentials")]
    InvalidCredentials,
    /// Token is expired.
    #[error("token expired")]
    TokenExpired,
    /// Token is malformed or invalid.
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// User already exists.
    #[error("user already exists: {0}")]
    UserExists(String),
    /// User not found.
    #[error("user not found: {0}")]
    UserNotFound(String),
    /// Internal error during password hashing.
    #[error("hashing error: {0}")]
    HashingError(String),
}

/// A verified user identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIdentity {
    /// Username.
    pub username: String,
    /// Assigned roles.
    pub roles: Vec<String>,
    /// When the identity was authenticated.
    pub authenticated_at: u64,
}

/// Login credentials.
pub enum Credentials {
    /// Username + password.
    Password { username: String, password: String },
    /// JWT bearer token.
    Token(String),
}

/// Stored user record.
#[derive(Debug, Clone)]
struct UserRecord {
    username: String,
    password_hash: String,
    roles: Vec<String>,
    active: bool,
}

/// JWT claims structure.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,        // username
    roles: Vec<String>,
    iat: u64,           // issued at
    exp: u64,           // expiration
}

/// Main authenticator managing users and token verification.
pub struct Authenticator {
    /// User store (in-memory; production would use persistent storage).
    users: RwLock<HashMap<String, UserRecord>>,
    /// JWT signing key.
    jwt_secret: Vec<u8>,
    /// Token expiration duration.
    token_ttl: Duration,
}

impl Authenticator {
    /// Create a new authenticator with the given JWT secret.
    pub fn new(jwt_secret: &[u8], token_ttl: Duration) -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            jwt_secret: jwt_secret.to_vec(),
            token_ttl,
        }
    }

    /// Create an authenticator with default settings (for testing).
    pub fn default_for_testing() -> Self {
        Self::new(b"nexusdb-test-secret-key-32bytes!", Duration::from_secs(3600))
    }

    /// Create a new user with a password.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> Result<(), AuthError> {
        let mut users = self.users.write();
        if users.contains_key(username) {
            return Err(AuthError::UserExists(username.to_string()));
        }

        let hash = hash_password(password)?;
        users.insert(
            username.to_string(),
            UserRecord {
                username: username.to_string(),
                password_hash: hash,
                roles,
                active: true,
            },
        );

        Ok(())
    }

    /// Authenticate with credentials and return a user identity.
    pub fn authenticate(&self, credentials: &Credentials) -> Result<UserIdentity, AuthError> {
        match credentials {
            Credentials::Password { username, password } => {
                self.verify_password(username, password)
            }
            Credentials::Token(token) => self.verify_token(token),
        }
    }

    /// Issue a JWT token for an authenticated user.
    pub fn issue_token(&self, identity: &UserIdentity) -> Result<String, AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = Claims {
            sub: identity.username.clone(),
            roles: identity.roles.clone(),
            iat: now,
            exp: now + self.token_ttl.as_secs(),
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(&self.jwt_secret),
        )
        .map_err(|e| AuthError::InvalidToken(e.to_string()))
    }

    /// Drop a user.
    pub fn drop_user(&self, username: &str) -> Result<(), AuthError> {
        let mut users = self.users.write();
        if users.remove(username).is_none() {
            return Err(AuthError::UserNotFound(username.to_string()));
        }
        Ok(())
    }

    /// Change a user's password.
    pub fn change_password(
        &self,
        username: &str,
        new_password: &str,
    ) -> Result<(), AuthError> {
        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.password_hash = hash_password(new_password)?;
        Ok(())
    }

    /// List all usernames.
    pub fn list_users(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    fn verify_password(&self, username: &str, password: &str) -> Result<UserIdentity, AuthError> {
        let users = self.users.read();
        let user = users
            .get(username)
            .ok_or(AuthError::InvalidCredentials)?;

        if !user.active {
            return Err(AuthError::InvalidCredentials);
        }

        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|e| AuthError::HashingError(e.to_string()))?;

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| AuthError::InvalidCredentials)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(UserIdentity {
            username: username.to_string(),
            roles: user.roles.clone(),
            authenticated_at: now,
        })
    }

    fn verify_token(&self, token: &str) -> Result<UserIdentity, AuthError> {
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(&self.jwt_secret),
            &Validation::default(),
        )
        .map_err(|e| {
            if e.to_string().contains("ExpiredSignature") {
                AuthError::TokenExpired
            } else {
                AuthError::InvalidToken(e.to_string())
            }
        })?;

        Ok(UserIdentity {
            username: token_data.claims.sub,
            roles: token_data.claims.roles,
            authenticated_at: token_data.claims.iat,
        })
    }
}

/// Hash a password using Argon2id.
fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| AuthError::HashingError(e.to_string()))?;
    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_user_and_authenticate() {
        let auth = Authenticator::default_for_testing();
        auth.create_user("alice", "secure_password", vec!["admin".into()])
            .unwrap();

        let identity = auth
            .authenticate(&Credentials::Password {
                username: "alice".into(),
                password: "secure_password".into(),
            })
            .unwrap();

        assert_eq!(identity.username, "alice");
        assert_eq!(identity.roles, vec!["admin"]);
    }

    #[test]
    fn test_wrong_password() {
        let auth = Authenticator::default_for_testing();
        auth.create_user("bob", "correct_password", vec![]).unwrap();

        let result = auth.authenticate(&Credentials::Password {
            username: "bob".into(),
            password: "wrong_password".into(),
        });

        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[test]
    fn test_jwt_token_roundtrip() {
        let auth = Authenticator::default_for_testing();
        auth.create_user("charlie", "pass123", vec!["reader".into()])
            .unwrap();

        let identity = auth
            .authenticate(&Credentials::Password {
                username: "charlie".into(),
                password: "pass123".into(),
            })
            .unwrap();

        let token = auth.issue_token(&identity).unwrap();
        let verified = auth.authenticate(&Credentials::Token(token)).unwrap();

        assert_eq!(verified.username, "charlie");
        assert_eq!(verified.roles, vec!["reader"]);
    }

    #[test]
    fn test_duplicate_user() {
        let auth = Authenticator::default_for_testing();
        auth.create_user("dave", "pass", vec![]).unwrap();
        assert!(matches!(
            auth.create_user("dave", "pass2", vec![]),
            Err(AuthError::UserExists(_))
        ));
    }

    #[test]
    fn test_change_password() {
        let auth = Authenticator::default_for_testing();
        auth.create_user("eve", "old_pass", vec![]).unwrap();
        auth.change_password("eve", "new_pass").unwrap();

        // Old password should fail
        assert!(auth
            .authenticate(&Credentials::Password {
                username: "eve".into(),
                password: "old_pass".into(),
            })
            .is_err());

        // New password should work
        assert!(auth
            .authenticate(&Credentials::Password {
                username: "eve".into(),
                password: "new_pass".into(),
            })
            .is_ok());
    }

    #[test]
    fn test_drop_user() {
        let auth = Authenticator::default_for_testing();
        auth.create_user("frank", "pass", vec![]).unwrap();
        auth.drop_user("frank").unwrap();

        assert!(auth
            .authenticate(&Credentials::Password {
                username: "frank".into(),
                password: "pass".into(),
            })
            .is_err());
    }
}
