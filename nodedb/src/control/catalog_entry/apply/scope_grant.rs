// SPDX-License-Identifier: BUSL-1.1

//! Apply scope grant catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredScopeGrant, SystemCatalog};

pub fn put(stored: &StoredScopeGrant, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_scope_grant(stored) {
        warn!(
            scope = %stored.scope_name,
            grantee_type = %stored.grantee_type,
            grantee_id = %stored.grantee_id,
            error = %e,
            "catalog_entry: put_scope_grant failed"
        );
    }
}

pub fn delete(scope_name: &str, grantee_type: &str, grantee_id: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_scope_grant(scope_name, grantee_type, grantee_id) {
        warn!(
            scope = %scope_name,
            grantee_type = %grantee_type,
            grantee_id = %grantee_id,
            error = %e,
            "catalog_entry: delete_scope_grant failed"
        );
    }
}
