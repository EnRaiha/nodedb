// SPDX-License-Identifier: BUSL-1.1

//! Apply scope grant catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredScopeGrant, SystemCatalog, catalog_err};

pub fn put(stored: &StoredScopeGrant, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_scope_grant(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_scope_grant '{}' for {} '{}'",
                stored.scope_name, stored.grantee_type, stored.grantee_id
            ),
            e,
        )
    })
}

pub fn delete(
    scope_name: &str,
    grantee_type: &str,
    grantee_id: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_scope_grant(scope_name, grantee_type, grantee_id)
        .map_err(|e| {
            catalog_err(
                &format!("delete_scope_grant '{scope_name}' for {grantee_type} '{grantee_id}'"),
                e,
            )
        })
        .map(|_| ())
}
