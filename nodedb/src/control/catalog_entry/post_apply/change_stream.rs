// SPDX-License-Identifier: BUSL-1.1

//! ChangeStream post-apply side effects — sync the in-memory
//! `stream_registry`, tear down the CDC router buffer on drop, and
//! cascade consumer-group state cleanup.

use std::sync::Arc;

use tracing::warn;

use crate::control::state::SharedState;
use crate::event::cdc::stream_def::ChangeStreamDef;

pub fn put(stored: ChangeStreamDef, shared: Arc<SharedState>) {
    // The owner row must carry the same database the stream itself is keyed
    // by. Installing it under database 0 leaves an owner row that no
    // `get_change_stream` can resolve, which turns DROP USER reassignment
    // into a hard failure and shows up as an orphan row in catalog verify.
    super::owner::install_from_parent_in_database(
        "change_stream",
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    shared.stream_registry.register(stored);
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    // 1. Drop the stream def from the in-memory registry so no new
    //    events are routed to it.
    shared
        .stream_registry
        .unregister(crate::types::DatabaseId::new(database_id), tenant_id, &name);

    // 2. Drop the per-stream retention buffer from the CDC router.
    shared
        .cdc_router
        .remove_buffer(crate::types::DatabaseId::new(database_id), tenant_id, &name);

    // 3. Cascade consumer-group teardown. Every group scoped to this
    //    stream must have its in-memory registry entry dropped AND
    //    its persisted offset state wiped — otherwise a `CREATE
    //    CHANGE STREAM` with the same name after a drop would
    //    resume from a stale consumer-group offset and silently
    //    skip real events.
    let database_id = crate::types::DatabaseId::new(database_id);
    let groups = shared
        .group_registry
        .list_for_stream(database_id, tenant_id, &name);
    for def in &groups {
        shared
            .group_registry
            .unregister(database_id, tenant_id, &name, &def.name);
        if let Err(e) = shared
            .offset_store
            .delete_group(database_id, tenant_id, &name, &def.name)
        {
            warn!(
                tenant = tenant_id,
                stream = %name,
                group = %def.name,
                error = %e,
                "failed to delete persisted consumer-group offsets on stream drop"
            );
        }
    }

    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            "change_stream",
            database_id.as_u64(),
            tenant_id,
            &name,
        );
}
