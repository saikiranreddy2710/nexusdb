//! # NexusDB Database Engine
//!
//! This module provides the unified database interface that wires together
//! all NexusDB components:
//!
//! - SQL parsing and execution (`nexus-sql`)
//! - Transaction management (`nexus-txn`)
//! - MVCC storage (`nexus-mvcc`)
//! - Storage engine (`nexus-storage`)
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │                         Database                                │
//! │                            │                                    │
//! │    ┌───────────────────────┼───────────────────────┐           │
//! │    ▼                       ▼                       ▼           │
//! │ ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐   │
//! │ │ SessionManager│    │ StorageEngine   │    │ TxnManager   │   │
//! │ │              │    │                 │    │              │   │
//! │ │ - Sessions   │    │ - Catalog       │    │ - LockManager│   │
//! │ │ - Autocommit │    │ - TableStores   │    │ - MVCC       │   │
//! │ └──────────────┘    └─────────────────┘    └──────────────┘   │
//! │         │                    │                    │            │
//! │         └────────────────────┼────────────────────┘            │
//! │                              ▼                                  │
//! │                        Session                                  │
//! │                  (connection-level state)                       │
//! │                              │                                  │
//! │    ┌────────────────────────┼────────────────────────┐         │
//! │    ▼                        ▼                        ▼         │
//! │ ┌───────────────┐    ┌─────────────┐    ┌──────────────────┐  │
//! │ │  SQL Parser   │    │ Planner &   │    │ Query Executor   │  │
//! │ │               │    │ Optimizer   │    │                  │  │
//! │ └───────────────┘    └─────────────┘    └──────────────────┘  │
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Usage
//!
//! ```ignore
//! use nexus_server::database::Database;
//!
//! // Create database
//! let db = Database::open_memory()?;
//!
//! // Create a session
//! let session = db.create_session();
//!
//! // Execute SQL
//! session.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")?;
//! session.execute("INSERT INTO users VALUES (1, 'Alice')")?;
//!
//! let result = session.execute("SELECT * FROM users")?;
//! for row in result.rows() {
//!     println!("{:?}", row);
//! }
//!
//! // Or use transactions explicitly
//! session.begin()?;
//! session.execute("UPDATE users SET name = 'Bob' WHERE id = 1")?;
//! session.commit()?;
//! ```

mod engine;
mod error;
mod result;
mod session;

pub use engine::{Database, DatabaseConfig, DatabaseStats, DEFAULT_DATABASE_NAME};
pub use error::{DatabaseError, DatabaseResult};
pub use result::{ExecuteResult, StatementResult};
pub use session::{Session, SessionConfig, SessionId, SessionState};

// Re-export security types for convenience
pub use nexus_security::{
    AccessDecision, AuditLog, AuthError, Authenticator, Authorizer, Credential, Identity,
    Permission, Privilege, Role,
};
