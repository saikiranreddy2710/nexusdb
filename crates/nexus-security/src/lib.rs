//! # nexus-security: Zero Trust Security Layer
//!
//! Provides defense-in-depth security for NexusDB:
//!
//! - **Authentication** (`auth`): Password hashing (Argon2id), JWT tokens
//! - **Authorization** (`authz`): RBAC with roles, permissions, row-level security
//! - **Encryption** (`crypto`): AES-256-GCM at-rest encryption with key hierarchy
//! - **Audit** (`audit`): Tamper-proof logging with hash chain integrity
//! - **TLS** (`tls`): TLS/mTLS configuration for transport security
//!
//! ## Zero Trust Principles
//!
//! 1. Never trust, always verify (every request authenticated)
//! 2. Least privilege access (minimum permissions needed)
//! 3. Assume breach (defense in depth)
//! 4. Continuous monitoring (audit everything)

pub mod audit;
pub mod auth;
pub mod authz;
pub mod crypto;
pub mod tls;

pub use auth::{Authenticator, Credentials, UserIdentity};
pub use authz::{Permission, Role, RoleManager};
pub use crypto::{DataEncryptor, EncryptedData};
pub use audit::{AuditEntry, AuditLog};
pub use tls::TlsConfig;
