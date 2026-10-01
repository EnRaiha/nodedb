// SPDX-License-Identifier: BUSL-1.1

//! The one database-id allocation entry point for every DDL path.

use nodedb_types::DatabaseId;

use crate::control::metadata_proposer::propose_database_id_reserve;
use crate::control::state::SharedState;

/// Allocate a fresh database id.
///
/// The id comes from a replicated `DatabaseIdReserve` entry, so every node
/// agrees on it. The applier persists the new hwm before the id is
/// returned, and a persist error returns `Err`.
pub async fn allocate_database_id(state: &SharedState) -> crate::Result<DatabaseId> {
    let registry = &state.database_registry;
    let request_id = registry.begin_request();
    let proposed = propose_database_id_reserve(state, state.node_id, request_id).await;
    // Always clear the request so an error path leaves no pending slot.
    let id = registry.finish_request(request_id);
    let log_index = proposed?;
    id.ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "database id reservation at metadata log index {log_index} \
             (request {request_id}) applied without issuing an id to this node"
        ),
    })
}
