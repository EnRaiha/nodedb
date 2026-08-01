// SPDX-License-Identifier: BUSL-1.1

//! Apply Procedure catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::procedure_types::StoredProcedure;

pub fn put(stored: &StoredProcedure, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_procedure(stored) {
        warn!(
            procedure = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_procedure failed"
        );
    }
    super::owner::put_parent_owner_in_database(
        object_type::PROCEDURE,
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) {
    if let Err(e) = catalog.delete_procedure_in_database(database_id, tenant_id, name) {
        warn!(
            procedure = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_procedure failed"
        );
    }
    super::owner::delete_parent_owner_in_database(
        object_type::PROCEDURE,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    );
}
