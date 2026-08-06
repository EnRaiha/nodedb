// SPDX-License-Identifier: BUSL-1.1

//! Apply auth-user catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredAuthUser, SystemCatalog};

pub fn put(stored: &StoredAuthUser, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_auth_user(stored) {
        warn!(
            user_id = %stored.id,
            status = %stored.status,
            error = %e,
            "catalog_entry: put_auth_user failed"
        );
    }
}
