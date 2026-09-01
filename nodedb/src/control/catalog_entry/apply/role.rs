// SPDX-License-Identifier: BUSL-1.1

//! Apply Role catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredRole, SystemCatalog, catalog_err};

pub fn put(stored: &StoredRole, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_role(stored)
        .map_err(|e| catalog_err(&format!("put_role '{}'", stored.name), e))
}

pub fn delete(name: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_role(name)
        .map_err(|e| catalog_err(&format!("delete_role '{name}'"), e))
}
