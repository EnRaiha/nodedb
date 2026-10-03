// SPDX-License-Identifier: BUSL-1.1

//! Install a snapshot's graph edges: every version, every TRUNCATE cut and
//! every applied ordinal, so each read resolves as it did on the snapshot's
//! source.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{TenantDataSnapshot, TenantId};

impl CoreLoop {
    /// Install the edge sections of `snap`, and return the number of edge
    /// versions installed. The plain sections install under `database_id`
    /// and `tenant_id`, the dispatch's own scope. The sections of a merged
    /// Raft snapshot carry their own database and tenant.
    pub(super) fn install_snapshot_edges(
        &self,
        database_id: u64,
        tenant_id: u64,
        snap: &TenantDataSnapshot,
    ) -> crate::Result<u64> {
        let tid = TenantId::new(tenant_id);
        let mut versions = 0u64;
        for (key, value) in snap.edges.iter().chain(&snap.edge_hidden) {
            self.edge_store.put_edge_raw(database_id, tid, key, value)?;
            versions += 1;
        }
        for (collection, cut) in &snap.edge_cuts {
            self.edge_store
                .put_edge_cut_raw(database_id, tid, collection, *cut)?;
        }
        for (key, applied) in &snap.edge_applied {
            self.edge_store
                .put_edge_applied_raw(database_id, tid, key, *applied)?;
        }
        for (db, edge_tid, key, value) in &snap.tenant_edges {
            self.edge_store
                .put_edge_raw(*db, TenantId::new(*edge_tid), key, value)?;
            versions += 1;
        }
        for (db, cut_tid, collection, cut) in &snap.tenant_edge_cuts {
            self.edge_store
                .put_edge_cut_raw(*db, TenantId::new(*cut_tid), collection, *cut)?;
        }
        for (db, applied_tid, key, applied) in &snap.tenant_edge_applied {
            self.edge_store.put_edge_applied_raw(
                *db,
                TenantId::new(*applied_tid),
                key,
                *applied,
            )?;
        }
        Ok(versions)
    }
}
