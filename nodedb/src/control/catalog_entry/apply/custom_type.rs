// SPDX-License-Identifier: BUSL-1.1

//! Apply custom type catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredCustomType, SystemCatalog, catalog_err};

/// Write the type, letting the catalog assign its OID.
///
/// The entry's `oid` is ignored on the create path. A proposing node cannot
/// pick it: two nodes handling concurrent `CREATE TYPE` statements would pick
/// the same value and give two distinct types one identity. Every node runs
/// this path in identical log order over identical redb state, so the OID the
/// catalog assigns is the same on all of them.
pub fn put(stored: &StoredCustomType, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_custom_type_assigning_oid(stored)
        .map(|_| ())
        .map_err(|e| {
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
