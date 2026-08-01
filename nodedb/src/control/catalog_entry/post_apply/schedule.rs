// SPDX-License-Identifier: BUSL-1.1

//! Schedule post-apply side effects — sync the in-memory cron
//! registry so the scheduler executor picks up the change on its
//! next tick.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::scheduler::types::ScheduleDef;

pub fn put(stored: ScheduleDef, shared: Arc<SharedState>) {
    super::owner::install_from_parent_in_database(
        "schedule",
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    shared.schedule_registry.register(stored);
}

pub fn delete(
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    name: String,
    shared: Arc<SharedState>,
) {
    shared
        .schedule_registry
        .unregister(database_id, tenant_id, &name);
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            "schedule",
            database_id.as_u64(),
            tenant_id,
            &name,
        );
}
