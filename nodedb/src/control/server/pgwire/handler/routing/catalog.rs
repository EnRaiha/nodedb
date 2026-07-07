// SPDX-License-Identifier: BUSL-1.1

//! Plan-cache freshness lookup against the local `SystemCatalog`.

use crate::control::state::SharedState;
use nodedb_types::DatabaseId;

/// Look up the current descriptor version for `id` against the
/// local `SystemCatalog`. Used by the plan cache's freshness
/// check — a cached plan is only returned when every recorded
/// `(id, version)` still matches the current catalog.
///
/// `database_id` scopes the lookup to the session's current database so
/// a plan compiled in one database is not incorrectly reused in another.
///
/// Returns `None` if the descriptor has been dropped, the
/// catalog is unavailable, or the descriptor kind is not
/// currently tracked (only `Collection` goes through this
/// path today; other kinds do not land in the plan cache).
pub(super) fn current_descriptor_version(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    id: &nodedb_cluster::DescriptorId,
) -> Option<u64> {
    if id.tenant_id != tenant_id {
        return None;
    }
    let catalog = state.credentials.catalog();
    match id.kind {
        nodedb_cluster::DescriptorKind::Collection => catalog
            .get_collection(database_id, tenant_id, &id.name)
            .ok()
            .flatten()
            .filter(|c| c.is_active)
            .map(|c| c.descriptor_version.max(1)),
        _ => None,
    }
}
