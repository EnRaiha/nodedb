// SPDX-License-Identifier: BUSL-1.1

//! Apply permission grant catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredPermission, SystemCatalog, catalog_err};

pub fn put(stored: &StoredPermission, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_permission(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_permission '{}' on '{}' for '{}'",
                stored.permission, stored.target, stored.grantee
            ),
            e,
        )
    })
}

pub fn delete(
    target: &str,
    grantee: &str,
    permission: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_permission(target, grantee, permission)
        .map_err(|e| {
            catalog_err(
                &format!("delete_permission '{permission}' on '{target}' for '{grantee}'"),
                e,
            )
        })
}
