// SPDX-License-Identifier: BUSL-1.1

//! Apply ChangeStream catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::event::cdc::stream_def::ChangeStreamDef;

pub fn put(stored: &ChangeStreamDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_change_stream(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_change_stream '{}' (database {}, tenant {})",
                stored.name,
                stored.database_id.as_u64(),
                stored.tenant_id
            ),
            e,
        )
    })?;
    // The owner row is keyed by the same database as the stream row. Writing
    // it under database 0 leaves an owner no `get_change_stream` can resolve,
    // which `verify_redb_integrity` reports as an orphan change_stream row and
    // which turns DROP USER reassignment into a hard failure.
    super::owner::put_parent_owner_in_database(
        object_type::CHANGE_STREAM,
        stored.database_id.as_u64(),
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
        .delete_change_stream(crate::types::DatabaseId::new(database_id), tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_change_stream '{name}' (database {database_id}, tenant {tenant_id})"
                ),
                e,
            )
        })?;
    super::owner::delete_parent_owner_in_database(
        object_type::CHANGE_STREAM,
        database_id,
        tenant_id,
        name,
        catalog,
    )
}
