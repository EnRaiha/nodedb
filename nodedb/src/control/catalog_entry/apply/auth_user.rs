// SPDX-License-Identifier: BUSL-1.1

//! Apply auth-user catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredAuthUser, SystemCatalog, catalog_err};

pub fn put(stored: &StoredAuthUser, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_auth_user(stored).map_err(|e| {
        catalog_err(
            &format!("put_auth_user {} (status {})", stored.id, stored.status),
            e,
        )
    })
}
