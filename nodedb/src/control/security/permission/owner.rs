// SPDX-License-Identifier: BUSL-1.1

//! Object ownership CRUD on `PermissionStore`.

use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::types::TenantId;

use super::store::PermissionStore;
use super::types::owner_key;

impl PermissionStore {
    /// Set the owner of an object in `database_id`. Cluster mode flows
    /// through catalog replication.
    pub fn set_owner(
        &self,
        object_type: &str,
        database_id: u64,
        tenant_id: TenantId,
        object_name: &str,
        owner_username: &str,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let key = owner_key(object_type, database_id, tenant_id.as_u64(), object_name);

        if let Some(catalog) = catalog {
            catalog.put_owner(&StoredOwner {
                database_id,
                object_type: object_type.to_string(),
                object_name: object_name.to_string(),
                tenant_id: tenant_id.as_u64(),
                owner_username: owner_username.to_string(),
            })?;
        }

        self.owners.write().insert(key, owner_username.to_string());
        Ok(())
    }

    /// Get the owner of an object in `database_id`.
    pub fn get_owner(
        &self,
        object_type: &str,
        database_id: u64,
        tenant_id: TenantId,
        object_name: &str,
    ) -> Option<String> {
        let key = owner_key(object_type, database_id, tenant_id.as_u64(), object_name);
        self.owners.read().get(&key).cloned()
    }
}
