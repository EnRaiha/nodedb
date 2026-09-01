// SPDX-License-Identifier: BUSL-1.1

//! Apply the index-registry catalog entries.
//!
//! The registry is the identity spine of every index kind. A failed write here
//! would leave an index that no `SHOW INDEXES` lists and no `DROP INDEX` can
//! reach, so the error propagates and halts the metadata applier.

use crate::control::security::catalog::{StoredIndexRecord, SystemCatalog, catalog_err};

pub(super) fn put(record: &StoredIndexRecord, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_index_record(record).map_err(|e| {
        catalog_err(
            &format!(
                "put_index_record '{}' on '{}' (tenant {})",
                record.name, record.collection, record.tenant_id
            ),
            e,
        )
    })
}

pub(super) fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_index_record(database_id, tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "delete_index_record '{name}' (database {database_id}, tenant {tenant_id})"
                ),
                e,
            )
        })
        .map(|_| ())
}
