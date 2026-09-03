// SPDX-License-Identifier: BUSL-1.1

//! Apply MaterializedView catalog entries to `SystemCatalog` redb.

use tracing::debug;

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredMaterializedView, SystemCatalog, catalog_err};

pub fn put(stored: &StoredMaterializedView, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_materialized_view(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_materialized_view '{}' (database {}, tenant {})",
                stored.name, stored.database_id, stored.tenant_id
            ),
            e,
        )
    })?;
    super::owner::put_parent_owner(
        object_type::MATERIALIZED_VIEW,
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    )
}

pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_materialized_view(database_id, tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_materialized_view '{name}' \
                     (database {database_id}, tenant {tenant_id})"
                ),
                e,
            )
        })?;
    super::owner::delete_parent_owner(
        object_type::MATERIALIZED_VIEW,
        database_id,
        tenant_id,
        name,
        catalog,
    )?;

    // Preserve the target as inactive until post-apply reclaim succeeds. The
    // target is the view's own same-name collection in the same database.
    let found = super::collection::prepare_purge(database_id, tenant_id, name, catalog)?;
    debug!(
        view = %name,
        database = database_id,
        tenant = tenant_id,
        found,
        "catalog_entry: materialized view target purge preparation"
    );
    Ok(())
}
