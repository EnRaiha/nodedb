// SPDX-License-Identifier: BUSL-1.1

//! Crash-recovery backstop: rebuild every HNSW vector index by re-indexing
//! all documents from the durable redb `sparse` store.
//!
//! The only other rebuild source is WAL replay, and the WAL is not
//! crash-durable — a `kill -9` before the group-commit flush loses the
//! `VectorParams` and `Put` records, so on reopen the HNSW would be empty
//! even though the documents themselves survived in redb. This pass scans
//! the durable store and re-runs the live vector-indexing side-effect for
//! every document, so vector search survives a hard crash.
//!
//! It is idempotent — `apply_point_put_vector_indexes` removes-then-inserts
//! per surrogate — so it safely overlays whatever the vector checkpoint plus
//! WAL replay already restored, never double-indexing a surrogate.

use crate::data::executor::handlers::point::apply_put::VectorIndexPutParams;

use super::state::CoreLoop;

impl CoreLoop {
    /// Re-index every document of each seeded vector-index collection from
    /// the durable `sparse` store into the HNSW. Run after WAL replay so it
    /// overlays (rather than races) the replayed state.
    pub fn rebuild_vector_indexes_from_store(
        &mut self,
        entries: &[nodedb_types::StoredVectorIndexParams],
    ) {
        use std::collections::HashSet;
        let db = crate::types::DatabaseId::DEFAULT.as_u64();

        // One scan per (tenant, collection): `apply_point_put_vector_indexes`
        // re-indexes ALL of the document's vector fields, so multiple field
        // indexes on one collection need only a single scan.
        let mut seen: HashSet<(u64, String)> = HashSet::new();
        let mut targets: Vec<(u64, String)> = Vec::new();
        for e in entries {
            if seen.insert((e.tenant_id, e.collection.clone())) {
                targets.push((e.tenant_id, e.collection.clone()));
            }
        }

        for (tenant_id, collection) in targets {
            // Collect first (the scan borrows `&self.sparse`); re-index after
            // the borrow ends so `&mut self` is free for the HNSW insert.
            let mut docs: Vec<(nodedb_types::Surrogate, Vec<u8>)> = Vec::new();
            let scan = self.sparse.scan_documents_for_each(
                db,
                tenant_id,
                &collection,
                usize::MAX,
                |doc_id, value| {
                    if let Some(surrogate) =
                        crate::engine::document::store::doc_id_to_surrogate(doc_id)
                    {
                        docs.push((surrogate, value.to_vec()));
                    }
                    Ok(())
                },
            );
            if let Err(e) = scan {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "vector-index rebuild scan failed"
                );
                continue;
            }

            let mut rebuilt = 0usize;
            for (surrogate, value) in docs {
                let doc_id = crate::engine::document::store::surrogate_to_doc_id(surrogate);
                // Same as WAL replay: the document is already durable, so a
                // width mismatch from before the forward-path check existed is
                // reported and skipped rather than aborting the rebuild.
                let deltas = match self.apply_point_put_vector_indexes(VectorIndexPutParams {
                    database_id: db,
                    tid: tenant_id,
                    collection: &collection,
                    document_id: &doc_id,
                    surrogate,
                    value: &value,
                    wal_lsn: 0,
                }) {
                    Ok(deltas) => deltas,
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            error = %e,
                            "vector-index rebuild rejected this document; \
                             its embeddings will not be searchable"
                        );
                        continue;
                    }
                };
                if !deltas.is_empty() {
                    rebuilt += 1;
                }
            }
            if rebuilt > 0 {
                tracing::info!(
                    core = self.core_id,
                    %collection,
                    rebuilt,
                    "rebuilt vector index from durable store"
                );
            }
        }
    }
}
