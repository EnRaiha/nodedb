// SPDX-License-Identifier: BUSL-1.1

//! Apply custom type catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredCustomType, SystemCatalog, catalog_err};

pub fn put(stored: &StoredCustomType, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_custom_type(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_custom_type '{}' (database {}, tenant {})",
                stored.name, stored.database_id, stored.tenant_id
            ),
            e,
        )
    })
}

pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_custom_type(database_id, tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_custom_type '{name}' (database {database_id}, tenant {tenant_id})"
                ),
                e,
            )
        })
        .map(|_| ())
}
