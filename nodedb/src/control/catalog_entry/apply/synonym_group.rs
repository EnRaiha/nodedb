// SPDX-License-Identifier: BUSL-1.1

//! Apply synonym group catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::{StoredSynonymGroup, SystemCatalog, catalog_err};

pub fn put(stored: &StoredSynonymGroup, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_synonym_group(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_synonym_group '{}' (tenant {})",
                stored.name, stored.tenant_id
            ),
            e,
        )
    })
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_synonym_group(tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!("delete_synonym_group '{name}' (tenant {tenant_id})"),
                e,
            )
        })
        .map(|_| ())
}
