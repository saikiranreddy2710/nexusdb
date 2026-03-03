//! Authorization: Role-Based Access Control (RBAC) with Row-Level Security.
//!
//! Determines whether an authenticated identity is allowed to perform
//! a specific operation on a specific resource.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::authn::Identity;

/// A privilege that can be granted on a database object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    /// All privileges (superuser shorthand).
    All,
}

impl Privilege {
    /// Check if `self` satisfies `required`.
    pub fn satisfies(&self, required: &Privilege) -> bool {
        *self == Privilege::All || self == required
    }
}

/// The target of a permission grant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Permission on a specific table.
    Table {
        database: String,
        table: String,
        privilege: Privilege,
    },
    /// Permission on all tables in a database.
    Database {
        database: String,
        privilege: Privilege,
    },
    /// Global server-level permission.
    Global { privilege: Privilege },
}

/// A named role that holds a set of permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
    /// Roles this role inherits from.
    pub inherits: Vec<String>,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            permissions: HashSet::new(),
            inherits: Vec::new(),
        }
    }

    pub fn with_permission(mut self, perm: Permission) -> Self {
        self.permissions.insert(perm);
        self
    }

    pub fn with_inherit(mut self, parent: impl Into<String>) -> Self {
        self.inherits.push(parent.into());
        self
    }
}

/// The result of an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessDecision {
    /// Access granted.
    Allow,
    /// Access denied with reason.
    Deny(String),
}

impl AccessDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, AccessDecision::Allow)
    }
}

/// The authorizer checks permissions for authenticated identities.
#[derive(Debug)]
pub struct Authorizer {
    /// Role definitions (role_name -> Role).
    roles: RwLock<HashMap<String, Role>>,
    /// Whether authorization is enforced.
    enforce: bool,
}

impl Authorizer {
    /// Create a new authorizer with built-in superuser role.
    pub fn new(enforce: bool) -> Self {
        let mut roles = HashMap::new();

        // Built-in superuser role has global ALL privilege
        roles.insert(
            "superuser".to_string(),
            Role::new("superuser").with_permission(Permission::Global {
                privilege: Privilege::All,
            }),
        );

        Self {
            roles: RwLock::new(roles),
            enforce,
        }
    }

    /// Create a non-enforcing authorizer (all requests pass).
    pub fn permissive() -> Self {
        Self::new(false)
    }

    /// Define a new role.
    pub fn create_role(&self, role: Role) {
        let mut roles = self.roles.write();
        roles.insert(role.name.clone(), role);
    }

    /// Drop a role.
    pub fn drop_role(&self, name: &str) {
        let mut roles = self.roles.write();
        roles.remove(name);
    }

    /// Grant a permission to a role.
    pub fn grant(&self, role_name: &str, perm: Permission) -> bool {
        let mut roles = self.roles.write();
        if let Some(role) = roles.get_mut(role_name) {
            role.permissions.insert(perm);
            true
        } else {
            false
        }
    }

    /// Revoke a permission from a role.
    pub fn revoke(&self, role_name: &str, perm: &Permission) -> bool {
        let mut roles = self.roles.write();
        if let Some(role) = roles.get_mut(role_name) {
            role.permissions.remove(perm);
            true
        } else {
            false
        }
    }

    /// Check whether an identity may perform `privilege` on `database.table`.
    pub fn check_table(
        &self,
        identity: &Identity,
        database: &str,
        table: &str,
        privilege: Privilege,
    ) -> AccessDecision {
        if !self.enforce {
            return AccessDecision::Allow;
        }

        if identity.is_superuser() {
            return AccessDecision::Allow;
        }

        let roles = self.roles.read();

        // Collect all effective permissions (including inherited)
        let mut visited = HashSet::new();
        let effective = self.collect_permissions(identity, &roles, &mut visited);

        // Check table-specific grant
        for perm in &effective {
            match perm {
                Permission::Global { privilege: p } if p.satisfies(&privilege) => {
                    return AccessDecision::Allow;
                }
                Permission::Database {
                    database: db,
                    privilege: p,
                } if db == database && p.satisfies(&privilege) => {
                    return AccessDecision::Allow;
                }
                Permission::Table {
                    database: db,
                    table: tbl,
                    privilege: p,
                } if db == database && tbl == table && p.satisfies(&privilege) => {
                    return AccessDecision::Allow;
                }
                _ => {}
            }
        }

        AccessDecision::Deny(format!(
            "user '{}' lacks {:?} on {}.{}",
            identity.username, privilege, database, table
        ))
    }

    /// Check database-level permission.
    pub fn check_database(
        &self,
        identity: &Identity,
        database: &str,
        privilege: Privilege,
    ) -> AccessDecision {
        if !self.enforce {
            return AccessDecision::Allow;
        }
        if identity.is_superuser() {
            return AccessDecision::Allow;
        }

        let roles = self.roles.read();
        let mut visited = HashSet::new();
        let effective = self.collect_permissions(identity, &roles, &mut visited);

        for perm in &effective {
            match perm {
                Permission::Global { privilege: p } if p.satisfies(&privilege) => {
                    return AccessDecision::Allow;
                }
                Permission::Database {
                    database: db,
                    privilege: p,
                } if db == database && p.satisfies(&privilege) => {
                    return AccessDecision::Allow;
                }
                _ => {}
            }
        }

        AccessDecision::Deny(format!(
            "user '{}' lacks {:?} on database '{}'",
            identity.username, privilege, database
        ))
    }

    /// Recursively collect all permissions for the identity's roles.
    fn collect_permissions(
        &self,
        identity: &Identity,
        roles: &HashMap<String, Role>,
        visited: &mut HashSet<String>,
    ) -> HashSet<Permission> {
        let mut perms = HashSet::new();

        for role_name in &identity.roles {
            self.collect_role_perms(role_name, roles, visited, &mut perms);
        }

        perms
    }

    fn collect_role_perms(
        &self,
        role_name: &str,
        roles: &HashMap<String, Role>,
        visited: &mut HashSet<String>,
        perms: &mut HashSet<Permission>,
    ) {
        if !visited.insert(role_name.to_string()) {
            return; // Cycle protection
        }

        if let Some(role) = roles.get(role_name) {
            perms.extend(role.permissions.iter().cloned());
            for parent in &role.inherits {
                self.collect_role_perms(parent, roles, visited, perms);
            }
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn analyst_identity() -> Identity {
        Identity {
            username: "alice".into(),
            roles: vec!["analyst".into()],
            authenticated_at: 0,
            method: crate::authn::AuthMethod::Password,
        }
    }

    #[test]
    fn test_superuser_always_allowed() {
        let authz = Authorizer::new(true);
        let id = Identity::system();
        let d = authz.check_table(&id, "db", "users", Privilege::Delete);
        assert!(d.is_allowed());
    }

    #[test]
    fn test_grant_table_select() {
        let authz = Authorizer::new(true);
        authz.create_role(Role::new("analyst"));
        authz.grant(
            "analyst",
            Permission::Table {
                database: "analytics".into(),
                table: "events".into(),
                privilege: Privilege::Select,
            },
        );

        let id = analyst_identity();
        assert!(authz
            .check_table(&id, "analytics", "events", Privilege::Select)
            .is_allowed());
        assert!(!authz
            .check_table(&id, "analytics", "events", Privilege::Delete)
            .is_allowed());
        assert!(!authz
            .check_table(&id, "analytics", "other", Privilege::Select)
            .is_allowed());
    }

    #[test]
    fn test_database_wide_grant() {
        let authz = Authorizer::new(true);
        authz.create_role(Role::new("analyst"));
        authz.grant(
            "analyst",
            Permission::Database {
                database: "analytics".into(),
                privilege: Privilege::Select,
            },
        );

        let id = analyst_identity();
        assert!(authz
            .check_table(&id, "analytics", "any_table", Privilege::Select)
            .is_allowed());
    }

    #[test]
    fn test_role_inheritance() {
        let authz = Authorizer::new(true);

        // base_reader can SELECT on public.*
        authz.create_role(
            Role::new("base_reader").with_permission(Permission::Database {
                database: "public".into(),
                privilege: Privilege::Select,
            }),
        );

        // analyst inherits base_reader and can also INSERT on analytics.events
        authz.create_role(
            Role::new("analyst")
                .with_inherit("base_reader")
                .with_permission(Permission::Table {
                    database: "analytics".into(),
                    table: "events".into(),
                    privilege: Privilege::Insert,
                }),
        );

        let id = analyst_identity();

        // Inherited from base_reader
        assert!(authz
            .check_table(&id, "public", "users", Privilege::Select)
            .is_allowed());

        // Direct grant
        assert!(authz
            .check_table(&id, "analytics", "events", Privilege::Insert)
            .is_allowed());
    }

    #[test]
    fn test_permissive_mode() {
        let authz = Authorizer::permissive();
        let id = analyst_identity();
        assert!(authz
            .check_table(&id, "any", "table", Privilege::Delete)
            .is_allowed());
    }

    #[test]
    fn test_revoke() {
        let authz = Authorizer::new(true);
        let perm = Permission::Table {
            database: "db".into(),
            table: "t".into(),
            privilege: Privilege::Select,
        };
        authz.create_role(Role::new("analyst"));
        authz.grant("analyst", perm.clone());

        let id = analyst_identity();
        assert!(authz
            .check_table(&id, "db", "t", Privilege::Select)
            .is_allowed());

        authz.revoke("analyst", &perm);
        assert!(!authz
            .check_table(&id, "db", "t", Privilege::Select)
            .is_allowed());
    }
}
