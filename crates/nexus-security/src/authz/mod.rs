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
    /// Whether this role is a built-in role that cannot be dropped.
    #[serde(default)]
    pub builtin: bool,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            permissions: HashSet::new(),
            inherits: Vec::new(),
            builtin: false,
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

        // Built-in superuser role has global ALL privilege and cannot be dropped
        let mut superuser_role = Role::new("superuser").with_permission(Permission::Global {
            privilege: Privilege::All,
        });
        superuser_role.builtin = true;
        roles.insert("superuser".to_string(), superuser_role);

        Self {
            roles: RwLock::new(roles),
            enforce,
        }
    }

    /// Create a non-enforcing authorizer (all requests pass).
    pub fn permissive() -> Self {
        Self::new(false)
    }

    /// Whether authorization is enforced.
    pub fn is_enforcing(&self) -> bool {
        self.enforce
    }

    /// Define a new role.
    ///
    /// Returns an error if the role name collides with a built-in role.
    pub fn create_role(&self, role: Role) -> Result<(), String> {
        let mut roles = self.roles.write();
        if let Some(existing) = roles.get(&role.name) {
            if existing.builtin {
                return Err(format!("cannot overwrite built-in role: {}", role.name));
            }
        }
        roles.insert(role.name.clone(), role);
        Ok(())
    }

    /// Drop a role.
    ///
    /// Returns `false` if the role does not exist or is a built-in role.
    pub fn drop_role(&self, name: &str) -> bool {
        let mut roles = self.roles.write();
        // Protect built-in roles from deletion
        if let Some(role) = roles.get(name) {
            if role.builtin {
                tracing::warn!(role = name, "cannot drop built-in role");
                return false;
            }
        }
        roles.remove(name).is_some()
    }

    /// Grant a permission to a role.
    ///
    /// Only a superuser is allowed to grant permissions. Returns an error
    /// if the caller is not a superuser, or `Ok(false)` if the role does
    /// not exist.
    pub fn grant(
        &self,
        role_name: &str,
        perm: Permission,
        caller: &Identity,
    ) -> Result<bool, String> {
        if !caller.is_superuser() {
            return Err(format!(
                "user '{}' is not authorized to grant permissions",
                caller.username
            ));
        }
        let mut roles = self.roles.write();
        if let Some(role) = roles.get_mut(role_name) {
            role.permissions.insert(perm);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Revoke a permission from a role.
    ///
    /// Only a superuser is allowed to revoke permissions. Returns an error
    /// if the caller is not a superuser, or `Ok(false)` if the role does
    /// not exist.
    pub fn revoke(
        &self,
        role_name: &str,
        perm: &Permission,
        caller: &Identity,
    ) -> Result<bool, String> {
        if !caller.is_superuser() {
            return Err(format!(
                "user '{}' is not authorized to revoke permissions",
                caller.username
            ));
        }
        let mut roles = self.roles.write();
        if let Some(role) = roles.get_mut(role_name) {
            role.permissions.remove(perm);
            Ok(true)
        } else {
            Ok(false)
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

    /// Check global server-level permission.
    pub fn check_global(&self, identity: &Identity, privilege: Privilege) -> AccessDecision {
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
            if let Permission::Global { privilege: p } = perm {
                if p.satisfies(&privilege) {
                    return AccessDecision::Allow;
                }
            }
        }

        AccessDecision::Deny(format!(
            "user '{}' lacks global {:?} privilege",
            identity.username, privilege
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

    fn superuser_caller() -> Identity {
        Identity::system()
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
        let su = superuser_caller();
        authz.create_role(Role::new("analyst")).unwrap();
        authz
            .grant(
                "analyst",
                Permission::Table {
                    database: "analytics".into(),
                    table: "events".into(),
                    privilege: Privilege::Select,
                },
                &su,
            )
            .unwrap();

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
        let su = superuser_caller();
        authz.create_role(Role::new("analyst")).unwrap();
        authz
            .grant(
                "analyst",
                Permission::Database {
                    database: "analytics".into(),
                    privilege: Privilege::Select,
                },
                &su,
            )
            .unwrap();

        let id = analyst_identity();
        assert!(authz
            .check_table(&id, "analytics", "any_table", Privilege::Select)
            .is_allowed());
    }

    #[test]
    fn test_role_inheritance() {
        let authz = Authorizer::new(true);

        // base_reader can SELECT on public.*
        authz
            .create_role(
                Role::new("base_reader").with_permission(Permission::Database {
                    database: "public".into(),
                    privilege: Privilege::Select,
                }),
            )
            .unwrap();

        // analyst inherits base_reader and can also INSERT on analytics.events
        authz
            .create_role(
                Role::new("analyst")
                    .with_inherit("base_reader")
                    .with_permission(Permission::Table {
                        database: "analytics".into(),
                        table: "events".into(),
                        privilege: Privilege::Insert,
                    }),
            )
            .unwrap();

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
        let su = superuser_caller();
        let perm = Permission::Table {
            database: "db".into(),
            table: "t".into(),
            privilege: Privilege::Select,
        };
        authz.create_role(Role::new("analyst")).unwrap();
        authz.grant("analyst", perm.clone(), &su).unwrap();

        let id = analyst_identity();
        assert!(authz
            .check_table(&id, "db", "t", Privilege::Select)
            .is_allowed());

        authz.revoke("analyst", &perm, &su).unwrap();
        assert!(!authz
            .check_table(&id, "db", "t", Privilege::Select)
            .is_allowed());
    }

    #[test]
    fn test_builtin_superuser_cannot_be_dropped() {
        let authz = Authorizer::new(true);

        // Attempting to drop the built-in superuser role should fail
        assert!(!authz.drop_role("superuser"));

        // Superuser role should still exist and work
        let id = Identity::system();
        assert!(authz
            .check_table(&id, "any", "table", Privilege::Delete)
            .is_allowed());
    }

    #[test]
    fn test_builtin_role_cannot_be_overwritten() {
        let authz = Authorizer::new(true);

        // Attempting to overwrite the built-in superuser role should return an error
        let result = authz.create_role(Role::new("superuser"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("built-in"));
    }

    #[test]
    fn test_custom_role_can_be_dropped() {
        let authz = Authorizer::new(true);
        authz.create_role(Role::new("temp_role")).unwrap();
        assert!(authz.drop_role("temp_role"));
    }

    #[test]
    fn test_check_global() {
        let authz = Authorizer::new(true);

        let id = Identity::system();
        assert!(authz.check_global(&id, Privilege::Create).is_allowed());

        let limited = analyst_identity();
        assert!(!authz.check_global(&limited, Privilege::Create).is_allowed());
    }

    #[test]
    fn test_grant_requires_superuser() {
        let authz = Authorizer::new(true);
        authz.create_role(Role::new("analyst")).unwrap();

        let non_su = analyst_identity();
        let result = authz.grant(
            "analyst",
            Permission::Global {
                privilege: Privilege::All,
            },
            &non_su,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not authorized"));
    }

    #[test]
    fn test_revoke_requires_superuser() {
        let authz = Authorizer::new(true);
        let su = superuser_caller();
        authz.create_role(Role::new("analyst")).unwrap();
        authz
            .grant(
                "analyst",
                Permission::Global {
                    privilege: Privilege::Select,
                },
                &su,
            )
            .unwrap();

        let non_su = analyst_identity();
        let result = authz.revoke(
            "analyst",
            &Permission::Global {
                privilege: Privilege::Select,
            },
            &non_su,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not authorized"));
    }
}
