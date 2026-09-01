// SPDX-License-Identifier: BUSL-1.1

//! Apply RLS policy catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredRlsPolicy, SystemCatalog, catalog_err};

pub fn put(stored: &StoredRlsPolicy, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_rls_policy(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_rls_policy '{}' on '{}' (tenant {})",
                stored.name, stored.collection, stored.tenant_id
            ),
            e,
        )
    })
}

pub fn delete(
    tenant_id: u64,
    collection: &str,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_rls_policy(tenant_id, collection, name)
        .map_err(|e| {
            catalog_err(
                &format!("delete_rls_policy '{name}' on '{collection}' (tenant {tenant_id})"),
                e,
            )
        })
        .map(|_| ())
}
