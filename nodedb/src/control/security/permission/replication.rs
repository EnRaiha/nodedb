// SPDX-License-Identifier: BUSL-1.1

//! Applier-side helpers for replicated permissions and ownership.

use crate::control::security::catalog::{StoredOwner, StoredPermission};
use crate::control::security::identity::Permission;
use crate::control::security::time::now_secs;
use crate::types::TenantId;

use super::store::PermissionStore;
use super::types::{Grant, format_permission, owner_key, parse_permission};

/// Build a `StoredOwner` ready for proposing as `CatalogEntry::PutOwner`.
pub fn prepare_owner(
    object_type: &str,
    tenant_id: TenantId,
    object_name: &str,
    owner_username: &str,
) -> StoredOwner {
    StoredOwner {
        database_id: 0,
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
        tenant_id: tenant_id.as_u64(),
        owner_username: owner_username.to_string(),
    }
}

impl PermissionStore {
    /// Build a `StoredPermission` ready for replication without mutating state.
    pub fn prepare_permission(
        &self,
        target: &str,
        grantee: &str,
        permission: Permission,
        granted_by: &str,
    ) -> StoredPermission {
        StoredPermission {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission: format_permission(permission),
            granted_by: granted_by.to_string(),
            granted_at: now_secs(),
        }
    }

    /// Whether a grant already exists (proposer-side pre-check).
    pub fn permission_exists(&self, target: &str, grantee: &str, permission: Permission) -> bool {
        self.grants.read().contains(&Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission,
        })
    }

    /// Install a replicated permission grant into the in-memory cache.
    pub fn install_replicated_permission(&self, stored: &StoredPermission) {
        let Some(perm) = parse_permission(&stored.permission) else {
            tracing::warn!(
                permission = %stored.permission,
                "install_replicated_permission: unknown permission name — skipping"
            );
            return;
        };
        self.grants.write().insert(Grant {
            target: stored.target.clone(),
            grantee: stored.grantee.clone(),
            permission: perm,
        });
    }

    /// Remove a replicated permission grant from the in-memory cache.
    pub fn install_replicated_revoke(&self, target: &str, grantee: &str, permission: &str) -> bool {
        let Some(perm) = parse_permission(permission) else {
            tracing::warn!(
                permission,
                "install_replicated_revoke: unknown permission name — skipping"
            );
            return false;
        };
        self.grants.write().remove(&Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission: perm,
        })
    }

    /// Whether an owner record already exists (proposer-side pre-check).
    pub fn owner_exists(&self, object_type: &str, tenant_id: u64, object_name: &str) -> bool {
        let key = owner_key(object_type, 0, tenant_id, object_name);
        self.owners.read().contains_key(&key)
    }

    /// Install a replicated owner record into the in-memory cache.
    pub fn install_replicated_owner(&self, stored: &StoredOwner) {
        let key = owner_key(
            &stored.object_type,
            stored.database_id,
            stored.tenant_id,
            &stored.object_name,
        );
        self.owners
            .write()
            .insert(key, stored.owner_username.clone());
    }

    /// Remove all grants whose target matches the supplied string.
    pub fn remove_grants_for_target(&self, target: &str) -> usize {
        let mut grants = self.grants.write();
        let before = grants.len();
        grants.retain(|g| g.target != target);
        before - grants.len()
    }

    /// Remove a replicated owner record from the in-memory cache.
    pub fn install_replicated_remove_owner(
        &self,
        object_type: &str,
        tenant_id: u64,
        object_name: &str,
    ) -> bool {
        self.install_replicated_remove_owner_in_database(object_type, 0, tenant_id, object_name)
    }

    pub fn install_replicated_remove_owner_in_database(
        &self,
        object_type: &str,
        database_id: u64,
        tenant_id: u64,
        object_name: &str,
    ) -> bool {
        let key = owner_key(object_type, database_id, tenant_id, object_name);
        self.owners.write().remove(&key).is_some()
    }
}
