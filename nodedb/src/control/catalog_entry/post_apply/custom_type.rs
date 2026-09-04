// SPDX-License-Identifier: BUSL-1.1

//! Custom type post-apply side effects — sync the in-memory registry.

use std::sync::Arc;

use crate::control::security::catalog::StoredCustomType;
use crate::control::state::SharedState;

pub fn put(stored: StoredCustomType, shared: Arc<SharedState>) {
    register_written(stored.database_id, stored.tenant_id, &stored.name, &shared);
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    shared
        .custom_type_registry
        .unregister(database_id, tenant_id, &name);
}

/// Register the record the apply step wrote, read back from the catalog.
///
/// The proposed entry carries no OID — the catalog assigns it. Registering the
/// entry's copy would let a pgwire client decode one type's values as
/// another's, so a row that cannot be read back is left unregistered and the
/// type stops resolving until the next reload.
pub fn register_written(database_id: u64, tenant_id: u64, name: &str, shared: &SharedState) {
    match shared
        .credentials
        .catalog()
        .get_custom_type(database_id, tenant_id, name)
    {
        Ok(Some(written)) => shared.custom_type_registry.register(written),
        Ok(None) => tracing::error!(
            database_id,
            tenant_id,
            name,
            "custom type is missing from the catalog after apply; it stays unresolvable"
        ),
        Err(e) => tracing::error!(
            database_id,
            tenant_id,
            name,
            error = %e,
            "reading back the applied custom type failed; it stays unresolvable"
        ),
    }
}
