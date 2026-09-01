// SPDX-License-Identifier: BUSL-1.1

//! Apply redaction policy catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredRedactionPolicy, SystemCatalog, catalog_err};

pub fn put(stored: &StoredRedactionPolicy, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_redaction_policy(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_redaction_policy '{}' on '{}' (tenant {})",
                stored.name, stored.collection, stored.tenant_id
            ),
            e,
        )
    })
}

pub fn delete(
    tenant_id: u64,
    collection: &str,
    for_role: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_redaction_policy(tenant_id, collection, for_role)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_redaction_policy on '{collection}' for role '{for_role}' \
                     (tenant {tenant_id})"
                ),
                e,
            )
        })
        .map(|_| ())
}
