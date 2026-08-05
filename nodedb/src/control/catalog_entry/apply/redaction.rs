// SPDX-License-Identifier: BUSL-1.1

//! Apply redaction policy catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredRedactionPolicy, SystemCatalog};

pub fn put(stored: &StoredRedactionPolicy, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_redaction_policy(stored) {
        warn!(
            policy = %stored.name,
            collection = %stored.collection,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_redaction_policy failed"
        );
    }
}

pub fn delete(tenant_id: u64, collection: &str, for_role: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_redaction_policy(tenant_id, collection, for_role) {
        warn!(
            collection = %collection,
            for_role = %for_role,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_redaction_policy failed"
        );
    }
}
