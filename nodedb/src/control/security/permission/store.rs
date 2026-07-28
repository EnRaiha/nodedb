// SPDX-License-Identifier: BUSL-1.1

//! `PermissionStore` — in-memory grants + ownership maps with redb persistence.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;

use crate::control::security::catalog::{StoredPermission, SystemCatalog};
use crate::control::security::identity::Permission;
use crate::control::security::time::now_secs;
use crate::types::TenantId;

use super::types::{Grant, format_permission, owner_key, parse_permission};

/// Permission store: grants + ownership with poison-free in-memory caches and
/// redb persistence.
pub struct PermissionStore {
    pub(super) grants: RwLock<HashSet<Grant>>,
    /// "collection:{tenant_id}:{name}" → owner username
    pub(super) owners: RwLock<HashMap<String, String>>,
}

impl Default for PermissionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PermissionStore {
    pub fn new() -> Self {
        Self {
            grants: RwLock::new(HashSet::new()),
            owners: RwLock::new(HashMap::new()),
        }
    }

    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored_perms = catalog.load_all_permissions()?;
        let mut grants = self.grants.write();
        for sp in stored_perms {
            if let Some(perm) = parse_permission(&sp.permission) {
                grants.insert(Grant {
                    target: sp.target,
                    grantee: sp.grantee,
                    permission: perm,
                });
            }
        }

        let stored_owners = catalog.load_all_owners()?;
        let mut owners = self.owners.write();
        for so in stored_owners {
            let key = owner_key(
                &so.object_type,
                so.database_id,
                so.tenant_id,
                &so.object_name,
            );
            owners.insert(key, so.owner_username);
        }

        let gc = grants.len();
        let oc = owners.len();
        if gc > 0 || oc > 0 {
            tracing::info!(grants = gc, owners = oc, "loaded permissions from catalog");
        }
        Ok(())
    }

    /// Grant a permission on a target to a grantee (role name or "user:username").
    pub fn grant(
        &self,
        target: &str,
        grantee: &str,
        permission: Permission,
        granted_by: &str,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let grant = Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission,
        };

        if let Some(catalog) = catalog {
            catalog.put_permission(&StoredPermission {
                target: target.to_string(),
                grantee: grantee.to_string(),
                permission: format_permission(permission),
                granted_by: granted_by.to_string(),
                granted_at: now_secs(),
            })?;
        }

        self.grants.write().insert(grant);
        Ok(())
    }

    /// Revoke a permission. Returns `true` if a grant was removed.
    pub fn revoke(
        &self,
        target: &str,
        grantee: &str,
        permission: Permission,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<bool> {
        let grant = Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission,
        };

        if let Some(catalog) = catalog {
            catalog.delete_permission(target, grantee, &format_permission(permission))?;
        }

        Ok(self.grants.write().remove(&grant))
    }

    /// List all grants for a grantee.
    pub fn grants_for(&self, grantee: &str) -> Vec<Grant> {
        self.grants
            .read()
            .iter()
            .filter(|g| g.grantee == grantee)
            .cloned()
            .collect()
    }

    /// Replace the entire in-memory grants + owners state with `other`.
    pub(crate) fn clear_and_install_from(&self, other: &Self) {
        let fresh_grants = other.snapshot_grants();
        let fresh_owners = other.snapshot_owners();
        let mut grants = self.grants.write();
        grants.clear();
        grants.extend(fresh_grants);
        drop(grants);
        let mut owners = self.owners.write();
        owners.clear();
        owners.extend(fresh_owners);
    }

    /// Deterministic snapshot of every grant held in memory.
    pub fn snapshot_grants(&self) -> Vec<Grant> {
        let mut out: Vec<Grant> = self.grants.read().iter().cloned().collect();
        out.sort_by(|a, b| {
            let a_key = (
                a.target.clone(),
                a.grantee.clone(),
                format_permission(a.permission),
            );
            let b_key = (
                b.target.clone(),
                b.grantee.clone(),
                format_permission(b.permission),
            );
            a_key.cmp(&b_key)
        });
        out
    }

    /// Deterministic snapshot of every owner held in memory as sorted pairs.
    pub fn snapshot_owners(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .owners
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// List all grants scoped to the given tenant ID prefix.
    pub fn all_grants(&self, tenant_id: TenantId) -> Vec<Grant> {
        let tid = tenant_id.as_u64();
        let col_prefix = format!("collection:{tid}:");
        let func_prefix = format!("function:{tid}:");
        self.grants
            .read()
            .iter()
            .filter(|g| g.target.starts_with(&col_prefix) || g.target.starts_with(&func_prefix))
            .cloned()
            .collect()
    }

    /// List all grants on a target.
    pub fn grants_on(&self, target: &str) -> Vec<Grant> {
        self.grants
            .read()
            .iter()
            .filter(|g| g.target == target)
            .cloned()
            .collect()
    }
}
