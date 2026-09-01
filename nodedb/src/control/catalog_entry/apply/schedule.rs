// SPDX-License-Identifier: BUSL-1.1

//! Apply Schedule catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::event::scheduler::types::ScheduleDef;

pub fn put(stored: &ScheduleDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_schedule(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_schedule '{}' (database {}, tenant {})",
                stored.name, stored.database_id, stored.tenant_id
            ),
            e,
        )
    })?;
    super::owner::put_parent_owner_in_database(
        object_type::SCHEDULE,
        stored.database_id,
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
        .delete_schedule_in_database(database_id, tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_schedule '{name}' (database {}, tenant {tenant_id})",
                    database_id.as_u64()
                ),
                e,
            )
        })?;
    super::owner::delete_parent_owner_in_database(
        object_type::SCHEDULE,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    )
}
