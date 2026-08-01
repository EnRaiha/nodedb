// SPDX-License-Identifier: BUSL-1.1

//! Trigger post-apply side effects — sync the in-memory
//! `trigger_registry`.

use crate::control::security::catalog::trigger_types::StoredTrigger;
use crate::control::state::SharedState;

pub fn put(stored: StoredTrigger, shared: &SharedState) {
    // `register` is an upsert: inserts new triggers and replaces
    // on OR REPLACE / ALTER ENABLE/DISABLE.
    super::owner::install_from_parent_in_database(
        "trigger",
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        shared,
    );
    shared.trigger_registry.register(stored);
}

pub fn delete(
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    name: String,
    shared: &SharedState,
) {
    shared
        .trigger_registry
        .unregister(database_id, tenant_id, &name);
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            "trigger",
            database_id.as_u64(),
            tenant_id,
            &name,
        );
}
