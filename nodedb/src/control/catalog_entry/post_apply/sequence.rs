// SPDX-License-Identifier: BUSL-1.1

//! Sequence post-apply side effects — sync the in-memory
//! `sequence_registry` so `NEXTVAL` / `CURRVAL` on followers sees
//! the replicated definition immediately.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};
use crate::control::state::SharedState;

pub fn put(stored: StoredSequence, shared: Arc<SharedState>) {
    super::owner::install_from_parent(
        "sequence",
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    if let Err(e) = shared.sequence_registry.create(stored.clone()) {
        debug!(
            sequence = %stored.name,
            database = stored.database_id,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: sequence_registry create (ignored — already exists?)"
        );
    }
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    if let Err(e) = shared
        .sequence_registry
        .remove(database_id, tenant_id, &name)
    {
        debug!(
            sequence = %name,
            database = database_id,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: sequence_registry remove (ignored)"
        );
    }
    shared
        .permissions
        .install_replicated_remove_owner("sequence", database_id, tenant_id, &name);
}

pub fn put_state(state: SequenceState, shared: Arc<SharedState>) {
    // ALTER SEQUENCE RESTART ships a fresh `SequenceState`;
    // replicate the counter into the in-memory registry handle so
    // `NEXTVAL` on every node returns from the new value.
    if let Err(e) = shared.sequence_registry.restart(
        state.database_id,
        state.tenant_id,
        &state.name,
        state.current_value,
    ) {
        debug!(
            sequence = %state.name,
            database = state.database_id,
            tenant = state.tenant_id,
            error = %e,
            "catalog_entry: sequence_registry restart (ignored — sequence may be missing on fresh follower)"
        );
    }
}
