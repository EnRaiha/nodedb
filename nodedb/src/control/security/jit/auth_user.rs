// SPDX-License-Identifier: BUSL-1.1

//! JIT-provisioned auth user store: in-memory cache + redb persistence.
//!
//! Manages `_system.auth_users` records for externally-authenticated users.
//! Users are auto-created on first JWT authentication when JIT provisioning
//! is enabled. No passwords stored — these users authenticate exclusively
//! via external providers (JWT/OIDC).

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use tracing::info;

use crate::control::security::auth_context::AuthStatus;
use crate::control::security::catalog::{StoredAuthUser, SystemCatalog};

/// In-memory auth user record.
#[derive(Debug, Clone)]
pub struct AuthUserRecord {
    /// Unique identifier (from JWT `sub` or `user_id` claim).
    pub id: String,
    /// Username (display name).
    pub username: String,
    /// Email address.
    pub email: String,
    /// Tenant this user belongs to.
    pub tenant_id: u64,
    /// Identity provider name.
    pub provider: String,
    /// First authentication timestamp.
    pub first_seen: u64,
    /// Most recent authentication timestamp.
    pub last_seen: u64,
    /// Whether this user is active.
    pub is_active: bool,
    /// Account status.
    pub status: AuthStatus,
    /// External-only (no local password).
    pub is_external: bool,
    /// Last synced JWT claims.
    pub synced_claims: HashMap<String, String>,
}

impl AuthUserRecord {
    pub fn from_stored(s: &StoredAuthUser) -> Self {
        Self {
            id: s.id.clone(),
            username: s.username.clone(),
            email: s.email.clone(),
            tenant_id: s.tenant_id,
            provider: s.provider.clone(),
            first_seen: s.first_seen,
            last_seen: s.last_seen,
            is_active: s.is_active,
            status: s.status.parse().unwrap_or_default(),
            is_external: s.is_external,
            synced_claims: s.synced_claims.clone(),
        }
    }

    pub fn to_stored(&self) -> StoredAuthUser {
        StoredAuthUser {
            id: self.id.clone(),
            username: self.username.clone(),
            email: self.email.clone(),
            tenant_id: self.tenant_id,
            provider: self.provider.clone(),
            first_seen: self.first_seen,
            last_seen: self.last_seen,
            is_active: self.is_active,
            status: self.status.to_string(),
            is_external: self.is_external,
            synced_claims: self.synced_claims.clone(),
        }
    }
}

/// Thread-safe auth user store.
pub struct AuthUserStore {
    /// id → AuthUserRecord.
    users: RwLock<HashMap<String, AuthUserRecord>>,
    /// Optional catalog for persistence.
    catalog: Option<SystemCatalog>,
}

impl AuthUserStore {
    /// Create an in-memory-only store (for tests).
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            catalog: None,
        }
    }

    /// Open a persistent store, loading from redb.
    pub fn open(catalog: SystemCatalog) -> crate::Result<Self> {
        let stored = catalog.load_all_auth_users()?;
        let mut users = HashMap::with_capacity(stored.len());
        for s in &stored {
            let record = AuthUserRecord::from_stored(s);
            users.insert(record.id.clone(), record);
        }

        if !users.is_empty() {
            info!(count = users.len(), "auth users loaded from catalog");
        }

        Ok(Self {
            users: RwLock::new(users),
            catalog: Some(catalog),
        })
    }

    /// Get an auth user by ID.
    pub fn get(&self, id: &str) -> Option<AuthUserRecord> {
        let users = self.users.read();
        users.get(id).cloned()
    }

    /// Check if an auth user exists and is active.
    pub fn is_active(&self, id: &str) -> bool {
        self.get(id).is_some_and(|u| u.is_active)
    }

    /// Get the status of an auth user. Returns `None` if user doesn't exist.
    pub fn get_status(&self, id: &str) -> Option<AuthStatus> {
        self.get(id).map(|u| u.status)
    }

    /// Create or update an auth user record.
    pub fn upsert(&self, record: AuthUserRecord) -> crate::Result<()> {
        if let Some(ref catalog) = self.catalog {
            catalog.put_auth_user(&record.to_stored())?;
        }
        let mut users = self.users.write();
        users.insert(record.id.clone(), record);
        Ok(())
    }

    /// Update the `last_seen` timestamp for a user.
    pub fn touch(&self, id: &str) -> crate::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut users = self.users.write();
        if let Some(user) = users.get_mut(id) {
            user.last_seen = now;
            if let Some(ref catalog) = self.catalog {
                let _ = catalog.put_auth_user(&user.to_stored());
            }
        }
        Ok(())
    }

    /// Deactivate a user (blocks even with valid JWT).
    pub fn deactivate(&self, id: &str) -> crate::Result<bool> {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(id) {
            user.is_active = false;
            user.status = AuthStatus::Suspended;
            if let Some(ref catalog) = self.catalog {
                catalog.put_auth_user(&user.to_stored())?;
            }
            info!(user_id = %id, "auth user deactivated");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set the status of an auth user.
    pub fn set_status(&self, id: &str, status: AuthStatus) -> crate::Result<bool> {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(id) {
            user.status = status;
            user.is_active = matches!(
                status,
                AuthStatus::Active | AuthStatus::Restricted | AuthStatus::ReadOnly
            );
            if let Some(ref catalog) = self.catalog {
                catalog.put_auth_user(&user.to_stored())?;
            }
            info!(user_id = %id, status = %status, "auth user status changed");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all auth users, optionally filtered by active status.
    pub fn list(&self, active_only: bool) -> Vec<AuthUserRecord> {
        let users = self.users.read();
        users
            .values()
            .filter(|u| !active_only || u.is_active)
            .cloned()
            .collect()
    }

    /// Purge inactive users older than the given threshold.
    /// Returns the number of purged records.
    pub fn purge_inactive(&self, inactive_before_secs: u64) -> crate::Result<usize> {
        let to_purge: Vec<String> = {
            let users = self.users.read();
            users
                .values()
                .filter(|u| !u.is_active && u.last_seen < inactive_before_secs)
                .map(|u| u.id.clone())
                .collect()
        };

        let count = to_purge.len();
        if count > 0 {
            let mut users = self.users.write();
            for id in &to_purge {
                users.remove(id);
                if let Some(ref catalog) = self.catalog {
                    let _ = catalog.delete_auth_user(id);
                }
            }
            info!(purged = count, "inactive auth users purged");
        }

        Ok(count)
    }

    /// Total user count.
    pub fn count(&self) -> usize {
        let users = self.users.read();
        users.len()
    }

    /// Access the catalog (for shared use).
    pub fn catalog(&self) -> Option<&SystemCatalog> {
        self.catalog.as_ref()
    }
}

impl Default for AuthUserStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    fn test_user(id: &str) -> AuthUserRecord {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        AuthUserRecord {
            id: id.into(),
            username: format!("user_{id}"),
            email: format!("{id}@example.com"),
            tenant_id: 1,
            provider: "test".into(),
            first_seen: now,
            last_seen: now,
            is_active: true,
            status: AuthStatus::Active,
            is_external: true,
            synced_claims: HashMap::new(),
        }
    }

    #[test]
    fn upsert_and_get() {
        let store = AuthUserStore::new();
        store.upsert(test_user("u1")).unwrap();
        let user = store.get("u1").unwrap();
        assert_eq!(user.username, "user_u1");
        assert!(user.is_active);
    }

    #[test]
    fn deactivate_blocks_user() {
        let store = AuthUserStore::new();
        store.upsert(test_user("u1")).unwrap();
        assert!(store.is_active("u1"));

        store.deactivate("u1").unwrap();
        assert!(!store.is_active("u1"));
        assert_eq!(store.get_status("u1"), Some(AuthStatus::Suspended));
    }

    #[test]
    fn set_status() {
        let store = AuthUserStore::new();
        store.upsert(test_user("u1")).unwrap();

        store.set_status("u1", AuthStatus::ReadOnly).unwrap();
        assert_eq!(store.get_status("u1"), Some(AuthStatus::ReadOnly));
        assert!(store.is_active("u1")); // ReadOnly is still "active"

        store.set_status("u1", AuthStatus::Banned).unwrap();
        assert!(!store.is_active("u1"));
    }

    #[test]
    fn list_filters_active() {
        let store = AuthUserStore::new();
        store.upsert(test_user("u1")).unwrap();
        store.upsert(test_user("u2")).unwrap();
        store.deactivate("u2").unwrap();

        assert_eq!(store.list(true).len(), 1);
        assert_eq!(store.list(false).len(), 2);
    }

    #[test]
    fn auth_user_state_remains_enforced_after_panic_while_write_locked() {
        let store = AuthUserStore::new();
        store.upsert(test_user("existing")).unwrap();
        store.set_status("existing", AuthStatus::Banned).unwrap();

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _users = store.users.write();
            panic!("simulated interrupted JIT auth-user update");
        }));
        assert!(panic_result.is_err());

        // The existing denied status remains authoritative after the panic.
        assert_eq!(store.get_status("existing"), Some(AuthStatus::Banned));
        assert!(!store.is_active("existing"));

        // The cache remains mutable and readable for later authenticated users.
        store.upsert(test_user("later")).unwrap();
        store.set_status("later", AuthStatus::ReadOnly).unwrap();
        assert_eq!(store.get_status("later"), Some(AuthStatus::ReadOnly));
        assert!(store.get("later").is_some());
        assert!(store.is_active("later"));
    }

    #[test]
    fn nonexistent_user_returns_none() {
        let store = AuthUserStore::new();
        assert!(store.get("nonexistent").is_none());
        assert!(!store.is_active("nonexistent"));
    }
}
