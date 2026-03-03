//! # NexusDB Security Layer
//!
//! Zero Trust security framework implementing defense-in-depth:
//!
//! - **Authentication (`authn`)**: Verify identity on every request
//!   (SCRAM-SHA-256, JWT tokens, API keys)
//! - **Authorization (`authz`)**: RBAC + row-level security
//! - **Audit (`audit`)**: Tamper-proof, hash-chained audit log
//!
//! ## Zero Trust Principles
//!
//! 1. Never trust, always verify - every request is authenticated
//! 2. Least privilege - minimum permissions needed
//! 3. Assume breach - defense in depth with audit trail

pub mod audit;
pub mod authn;
pub mod authz;

pub use audit::{AuditEntry, AuditLog};
pub use authn::{AuthError, AuthResult, Authenticator, Credential, Identity};
pub use authz::{AccessDecision, Authorizer, Permission, Privilege, Role};
