// SPDX-License-Identifier: BUSL-1.1

//! Apply Trigger catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::trigger_types::StoredTrigger;

pub fn put(stored: &StoredTrigger, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_trigger(stored) {
        warn!(
            trigger = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_trigger failed"
        );
    }
    super::owner::put_parent_owner_in_database(
        object_type::TRIGGER,
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
    if let Err(e) = catalog.delete_trigger_in_database(database_id, tenant_id, name) {
        warn!(
            trigger = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_trigger failed"
        );
    }
    super::owner::delete_parent_owner_in_database(
        object_type::TRIGGER,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    );
}
