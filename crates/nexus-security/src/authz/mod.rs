//! Authorization: RBAC with row-level security.
//!
//! Implements Role-Based Access Control with fine-grained permissions:
//! - Roles: named collections of permissions (admin, reader, writer)
//! - Permissions: (action, resource) pairs (SELECT on users, INSERT on orders)
//! - Row-Level Security: policies that filter rows based on user context

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Authorization errors.
#[derive(Debug, Error)]
pub enum AuthzError {
    /// User lacks required permission.
    #[error("permission denied: {user} cannot {action} on {resource}")]
    PermissionDenied {
        user: String,
        action: String,
        resource: String,
    },
    /// Role not found.
    #[error("role not found: {0}")]
    RoleNotFound(String),
    /// Role already exists.
    #[error("role already exists: {0}")]
    RoleExists(String),
}

/// A database action that can be permitted or denied.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    /// SELECT queries.
    Select,
    /// INSERT operations.
    Insert,
    /// UPDATE operations.
    Update,
    /// DELETE operations.
    Delete,
    /// CREATE TABLE/INDEX.
    Create,
    /// DROP TABLE/INDEX.
    Drop,
    /// ALTER TABLE.
    Alter,
    /// GRANT/REVOKE permissions.
    Grant,
    /// All actions.
    All,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Select => write!(f, "SELECT"),
            Action::Insert => write!(f, "INSERT"),
            Action::Update => write!(f, "UPDATE"),
            Action::Delete => write!(f, "DELETE"),
            Action::Create => write!(f, "CREATE"),
            Action::Drop => write!(f, "DROP"),
            Action::Alter => write!(f, "ALTER"),
            Action::Grant => write!(f, "GRANT"),
            Action::All => write!(f, "ALL"),
        }
    }
}

/// A permission: (action, resource). Resource is a table name or "*" for all.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Permission {
    pub action: Action,
    /// Table name, or "*" for all tables.
    pub resource: String,
}

impl Permission {
    pub fn new(action: Action, resource: impl Into<String>) -> Self {
        Self {
            action,
            resource: resource.into(),
        }
    }

    /// Permission for all actions on all tables.
    pub fn superuser() -> Self {
        Self::new(Action::All, "*")
    }

    /// Check if this permission covers the requested action on resource.
    pub fn covers(&self, action: &Action, resource: &str) -> bool {
        let action_match = self.action == Action::All || self.action == *action;
        let resource_match = self.resource == "*" || self.resource == resource;
        action_match && resource_match
    }
}

/// A named role with a set of permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            permissions: HashSet::new(),
        }
    }

    pub fn with_permission(mut self, permission: Permission) -> Self {
        self.permissions.insert(permission);
        self
    }

    pub fn has_permission(&self, action: &Action, resource: &str) -> bool {
        self.permissions
            .iter()
            .any(|p| p.covers(action, resource))
    }
}

/// A row-level security policy.
#[derive(Debug, Clone)]
pub struct RlsPolicy {
    /// Policy name.
    pub name: String,
    /// Table this policy applies to.
    pub table: String,
    /// Column to check against the user context.
    pub column: String,
    /// The user attribute to compare (e.g., "user_id", "department").
    pub user_attribute: String,
}

/// Role manager: stores roles and checks permissions.
pub struct RoleManager {
    /// Role definitions.
    roles: RwLock<HashMap<String, Role>>,
    /// User-to-role assignments: username -> set of role names.
    user_roles: RwLock<HashMap<String, HashSet<String>>>,
    /// Row-level security policies.
    rls_policies: RwLock<Vec<RlsPolicy>>,
}

impl RoleManager {
    /// Create a new role manager with a built-in superuser role.
    pub fn new() -> Self {
        let mut roles = HashMap::new();
        roles.insert(
            "superuser".to_string(),
            Role::new("superuser").with_permission(Permission::superuser()),
        );
        Self {
            roles: RwLock::new(roles),
            user_roles: RwLock::new(HashMap::new()),
            rls_policies: RwLock::new(Vec::new()),
        }
    }

    /// Create a new role.
    pub fn create_role(&self, role: Role) -> Result<(), AuthzError> {
        let mut roles = self.roles.write();
        if roles.contains_key(&role.name) {
            return Err(AuthzError::RoleExists(role.name.clone()));
        }
        roles.insert(role.name.clone(), role);
        Ok(())
    }

    /// Grant a permission to a role.
    pub fn grant_to_role(
        &self,
        role_name: &str,
        permission: Permission,
    ) -> Result<(), AuthzError> {
        let mut roles = self.roles.write();
        let role = roles
            .get_mut(role_name)
            .ok_or_else(|| AuthzError::RoleNotFound(role_name.to_string()))?;
        role.permissions.insert(permission);
        Ok(())
    }

    /// Assign a role to a user.
    pub fn assign_role(&self, username: &str, role_name: &str) -> Result<(), AuthzError> {
        {
            let roles = self.roles.read();
            if !roles.contains_key(role_name) {
                return Err(AuthzError::RoleNotFound(role_name.to_string()));
            }
        }
        let mut user_roles = self.user_roles.write();
        user_roles
            .entry(username.to_string())
            .or_default()
            .insert(role_name.to_string());
        Ok(())
    }

    /// Revoke a role from a user.
    pub fn revoke_role(&self, username: &str, role_name: &str) {
        let mut user_roles = self.user_roles.write();
        if let Some(roles) = user_roles.get_mut(username) {
            roles.remove(role_name);
        }
    }

    /// Check if a user has permission for an action on a resource.
    pub fn check_permission(
        &self,
        username: &str,
        action: &Action,
        resource: &str,
    ) -> Result<(), AuthzError> {
        let user_roles = self.user_roles.read();
        let role_names = user_roles.get(username);

        if let Some(role_names) = role_names {
            let roles = self.roles.read();
            for role_name in role_names {
                if let Some(role) = roles.get(role_name) {
                    if role.has_permission(action, resource) {
                        return Ok(());
                    }
                }
            }
        }

        Err(AuthzError::PermissionDenied {
            user: username.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
        })
    }

    /// Add a row-level security policy.
    pub fn add_rls_policy(&self, policy: RlsPolicy) {
        self.rls_policies.write().push(policy);
    }

    /// Get RLS policies for a table.
    pub fn get_rls_policies(&self, table: &str) -> Vec<RlsPolicy> {
        self.rls_policies
            .read()
            .iter()
            .filter(|p| p.table == table)
            .cloned()
            .collect()
    }

    /// List all role names.
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.read().keys().cloned().collect()
    }

    /// Get roles assigned to a user.
    pub fn get_user_roles(&self, username: &str) -> Vec<String> {
        self.user_roles
            .read()
            .get(username)
            .map(|r| r.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Default for RoleManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_covers() {
        let p = Permission::new(Action::Select, "users");
        assert!(p.covers(&Action::Select, "users"));
        assert!(!p.covers(&Action::Insert, "users"));
        assert!(!p.covers(&Action::Select, "orders"));

        let all = Permission::superuser();
        assert!(all.covers(&Action::Select, "users"));
        assert!(all.covers(&Action::Delete, "anything"));
    }

    #[test]
    fn test_role_permissions() {
        let role = Role::new("reader")
            .with_permission(Permission::new(Action::Select, "*"));

        assert!(role.has_permission(&Action::Select, "users"));
        assert!(role.has_permission(&Action::Select, "orders"));
        assert!(!role.has_permission(&Action::Insert, "users"));
    }

    #[test]
    fn test_rbac_flow() {
        let rm = RoleManager::new();

        rm.create_role(
            Role::new("analyst")
                .with_permission(Permission::new(Action::Select, "*")),
        )
        .unwrap();

        rm.assign_role("alice", "analyst").unwrap();

        assert!(rm.check_permission("alice", &Action::Select, "users").is_ok());
        assert!(rm.check_permission("alice", &Action::Insert, "users").is_err());
    }

    #[test]
    fn test_grant_permission() {
        let rm = RoleManager::new();
        rm.create_role(Role::new("writer")).unwrap();
        rm.grant_to_role("writer", Permission::new(Action::Insert, "orders"))
            .unwrap();
        rm.assign_role("bob", "writer").unwrap();

        assert!(rm.check_permission("bob", &Action::Insert, "orders").is_ok());
        assert!(rm.check_permission("bob", &Action::Insert, "users").is_err());
    }

    #[test]
    fn test_superuser() {
        let rm = RoleManager::new();
        rm.assign_role("admin", "superuser").unwrap();

        assert!(rm.check_permission("admin", &Action::Select, "anything").is_ok());
        assert!(rm.check_permission("admin", &Action::Drop, "everything").is_ok());
    }

    #[test]
    fn test_revoke_role() {
        let rm = RoleManager::new();
        rm.create_role(Role::new("temp")).unwrap();
        rm.assign_role("user1", "temp").unwrap();
        rm.revoke_role("user1", "temp");

        assert!(rm.get_user_roles("user1").is_empty());
    }

    #[test]
    fn test_rls_policy() {
        let rm = RoleManager::new();
        rm.add_rls_policy(RlsPolicy {
            name: "user_isolation".into(),
            table: "user_data".into(),
            column: "owner_id".into(),
            user_attribute: "user_id".into(),
        });

        let policies = rm.get_rls_policies("user_data");
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].column, "owner_id");

        assert!(rm.get_rls_policies("other_table").is_empty());
    }
}
