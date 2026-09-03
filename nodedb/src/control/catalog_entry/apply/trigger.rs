// SPDX-License-Identifier: BUSL-1.1

//! Apply Trigger catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::trigger_types::StoredTrigger;
use crate::control::security::catalog::{SystemCatalog, catalog_err};

pub fn put(stored: &StoredTrigger, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_trigger(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_trigger '{}' (database {}, tenant {})",
                stored.name,
                stored.database_id.as_u64(),
                stored.tenant_id
            ),
            e,
        )
    })?;
    super::owner::put_parent_owner(
        object_type::TRIGGER,
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    )
}

pub fn delete(
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_trigger_in_database(database_id, tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_trigger '{name}' (database {}, tenant {tenant_id})",
                    database_id.as_u64()
                ),
                e,
            )
        })?;
    super::owner::delete_parent_owner(
        object_type::TRIGGER,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    )
}
