// SPDX-License-Identifier: BUSL-1.1

//! Apply the index-registry catalog entries.
//!
//! The registry is the identity spine of every index kind, so a write that
//! fails here leaves an index that no `SHOW INDEXES` lists and no
//! `DROP INDEX` can reach. Failures are logged at `warn` and the entry is
//! retried by startup replay, matching every other single-row apply in this
//! module.

use tracing::warn;

use crate::control::security::catalog::{StoredIndexRecord, SystemCatalog};

pub(super) fn put(record: &StoredIndexRecord, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_index_record(record) {
        warn!(
            index = %record.name,
            collection = %record.collection,
            tenant = record.tenant_id,
            error = %e,
            "catalog_entry: put_index_record failed"
        );
    }
}

pub(super) fn delete(database_id: u64, tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_index_record(database_id, tenant_id, name) {
        warn!(
            index = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_index_record failed"
        );
    }
}
