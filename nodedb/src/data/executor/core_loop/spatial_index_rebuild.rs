// SPDX-License-Identifier: BUSL-1.1

//! Crash-recovery backstop: rebuild every in-memory R-tree spatial index by
//! re-indexing all geometry documents from the durable redb `sparse` store.
//!
//! The in-memory R-tree is otherwise restored only from spatial checkpoints
//! plus WAL replay. Spatial checkpoints run only on a manual snapshot, and the
//! WAL is not crash-durable — a `kill -9` before the group-commit flush loses
//! the `Put` records, so on reopen the R-tree would be empty or incomplete even
//! though the geometry documents themselves survived in redb. This pass scans
//! the durable store and re-runs the live spatial-indexing side-effect for
//! every document, so spatial search survives a hard crash.
//!
//! It is idempotent — `apply_point_put_spatial` removes-then-inserts per
//! document — so it safely overlays whatever the spatial checkpoint plus WAL
//! replay already restored, never double-indexing a document.

use super::state::CoreLoop;

impl CoreLoop {
    /// Re-index every geometry document of each seeded spatial collection from
    /// the durable `sparse` store into the R-tree. Run after WAL replay so it
    /// overlays (rather than races) the replayed state.
    pub fn rebuild_spatial_indexes_from_store(&mut self, spatial_collections: &[(u64, String)]) {
        let db = crate::types::DatabaseId::DEFAULT.as_u64();

        for (tenant_id, collection) in spatial_collections {
            let tenant_id = *tenant_id;
            // Collect first (the scan borrows `&self.sparse`); re-index after
            // the borrow ends so `&mut self` is free for the R-tree insert.
            let mut docs: Vec<(String, Vec<u8>)> = Vec::new();
            let scan = self.sparse.scan_documents_for_each(
                db,
                tenant_id,
                collection,
                usize::MAX,
                |doc_id, value| {
                    docs.push((doc_id.to_string(), value.to_vec()));
                    Ok(())
                },
            );
            if let Err(e) = scan {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "spatial-index rebuild scan failed"
                );
                continue;
            }

            let mut rebuilt = 0usize;
            for (doc_id, value) in docs {
                let inserts =
                    self.apply_point_put_spatial(db, tenant_id, collection, &doc_id, &value);
                if !inserts.is_empty() {
                    rebuilt += 1;
                }
            }
            if rebuilt > 0 {
                tracing::info!(
                    core = self.core_id,
                    %collection,
                    rebuilt,
                    "rebuilt spatial index from durable store"
                );
            }
        }
    }
}
