// SPDX-License-Identifier: BUSL-1.1

//! Apply User catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredUser, SystemCatalog, catalog_err};

pub fn put(stored: &StoredUser, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_user(stored)
        .map_err(|e| catalog_err(&format!("put_user '{}'", stored.username), e))
}

/// Fully remove the user record from redb. `delete_user` is idempotent — a
/// missing record on a fresh follower succeeds (redb `remove` tolerates it).
pub fn delete(username: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_user(username)
        .map_err(|e| catalog_err(&format!("delete_user '{username}'"), e))
}
