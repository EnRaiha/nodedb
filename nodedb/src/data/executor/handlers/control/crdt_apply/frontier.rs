// SPDX-License-Identifier: BUSL-1.1

//! Domain-bound CRDT frontier reads used by the apply fence.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Read the current domain-bound CRDT frontier without creating an engine.
    ///
    /// An absent engine has the canonical empty-domain digest, so a fenced
    /// write can be verified against immutable state before anything is
    /// allocated or advanced.
    pub(super) fn current_crdt_frontier_digest(
        &self,
        database_id: crate::types::DatabaseId,
        tenant_id: crate::types::TenantId,
        collection: &str,
    ) -> [u8; 32] {
        self.crdt_engines
            .get(&(database_id, tenant_id))
            .map(|engine| engine.frontier_digest(database_id, collection))
            .unwrap_or_else(|| {
                nodedb_crdt::state::frontier_digest::domain_frontier_digest(
                    tenant_id.as_u64(),
                    database_id.as_u64(),
                    collection,
                    None,
                )
            })
    }
}
