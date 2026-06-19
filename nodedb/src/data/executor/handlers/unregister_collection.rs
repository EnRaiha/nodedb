// SPDX-License-Identifier: BUSL-1.1

//! Collection-scoped purge handler.
//!
//! Reclaims storage for a single `(tenant_id, collection)` pair
//! across every engine on this Data Plane core. Dispatched by the
//! Control Plane via `MetaOp::UnregisterCollection` after the
//! metadata-raft commit of `CatalogEntry::PurgeCollection`.
//!
//! Runs on **every node** (leader and followers) — each node's Data
//! Plane reclaims its own local storage symmetrically with the
//! metadata row removal.
//!
//! Idempotent: safe to re-run after partial completion. Missing
//! in-memory state is a no-op; missing files are a no-op.
//!
//! # Current coverage
//!
//! In-memory, tuple-keyed state is reclaimed here (retain filters on
//! maps keyed by `(TenantId, collection_name)` or
//! `(TenantId, collection_name, ...)`). This covers the vector, KV,
//! timeseries, spatial, columnar, CRDT, cache, doc-config, chain-hash,
//! and sparse-vector-index maps.
//!
//! Persistent, redb-backed engines (sparse documents, inverted index,
//! graph edges) are reclaimed here via collection-scoped purge methods
//! on each store.

use tracing::{info, warn};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Bounded retry wrapper for L1 reclaim ops.
///
/// Runs the op up to `MAX_ATTEMPTS` times; returns the first `Ok(T)`
/// it sees. On every failure it captures the error text for the final
/// warn. No sleep between attempts — the Data Plane is single-threaded
/// per core and a `sleep` here would stall every other request on the
/// shard; an immediate retry still recovers the vast majority of
/// transient fs-level errors (momentary lock, inflight fsync race).
const L1_RECLAIM_MAX_ATTEMPTS: u32 = 3;

fn retry_reclaim<T, E, F>(op_name: &str, tenant_id: u64, collection: &str, mut op: F) -> Option<T>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut last_err: Option<String> = None;
    for attempt in 1..=L1_RECLAIM_MAX_ATTEMPTS {
        match op() {
            Ok(v) => {
                if attempt > 1 {
                    info!(
                        tenant_id,
                        collection,
                        op = op_name,
                        attempt,
                        "L1 reclaim recovered after transient failure"
                    );
                }
                return Some(v);
            }
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
    }
    warn!(
        tenant_id,
        collection,
        op = op_name,
        attempts = L1_RECLAIM_MAX_ATTEMPTS,
        error = last_err.as_deref().unwrap_or("(no detail)"),
        "L1 reclaim failed after all retries; leaving partial state \
         for next purge attempt (idempotent)"
    );
    None
}

impl CoreLoop {
    /// Purge collection-scoped data on this core.
    pub(in crate::data::executor) fn execute_unregister_collection(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        purge_lsn: u64,
    ) -> Response {
        info!(
            core = self.core_id,
            tenant_id, collection, purge_lsn, "starting collection purge"
        );
        let tid = TenantId::new(tenant_id);
        let db = task.request.database_id;
        let coll = collection.to_string();

        // ── Persistent engines (redb-backed, collection-scoped range drop) ──

        // Sparse engine: documents + secondary indexes.
        let database_id = task.request.database_id.as_u64();
        let (docs_removed, idxs_removed) = retry_reclaim(
            "sparse.delete_all_for_collection",
            tenant_id,
            collection,
            || {
                self.sparse
                    .delete_all_for_collection(database_id, tenant_id, collection)
            },
        )
        .unwrap_or((0, 0));

        // Inverted index: postings + doc_lengths + stats + segments.
        let inv_removed = retry_reclaim("inverted.purge_collection", tenant_id, collection, || {
            self.inverted.purge_collection(tid, collection)
        })
        .unwrap_or(0);

        // Graph edge store: remove all edges scoped to this (database, collection).
        let edges_removed =
            retry_reclaim("edge_store.purge_collection", tenant_id, collection, || {
                self.edge_store
                    .purge_collection(database_id, tid, collection)
            })
            .unwrap_or(0);
        // The CSR in-memory index is collection-agnostic. Stale edges will
        // be absent from the next CSR rebuild (which reads from EdgeStore).
        self.csr.drop_collection(db, tid, collection);

        // ── In-memory, tuple-keyed state (reclaimable today) ─────────────────

        // Vector engine.
        let vec_removed = {
            let key = (db, tid, coll.clone());
            let mut r = 0;
            if self.vector_collections.remove(&key).is_some() {
                r += 1;
            }
            self.vector_params.remove(&key);
            self.index_configs.remove(&key);
            self.ivf_indexes.remove(&key);
            r
        };

        // Timeseries engine.
        let ts_removed = {
            let key = (db, tid, coll.clone());
            let mut r = 0;
            if self.columnar_memtables.remove(&key).is_some() {
                r += 1;
            }
            self.columnar_memtable_mem.remove(&key);
            self.ts_registries.remove(&key);
            self.ts_max_ingested_lsn.remove(&key);
            self.ts_last_value_caches.remove(&key);
            r
        };

        // Spatial indexes.
        let spatial_removed = {
            let before = self.spatial_indexes.len();
            self.spatial_indexes
                .retain(|(d, t, c, _), _| !(*d == db && *t == tid && c == &coll));
            self.spatial_doc_map
                .retain(|(d, t, c, _, _), _| !(*d == db && *t == tid && c == &coll));
            before - self.spatial_indexes.len()
        };

        // Columnar engine state (per-core, tuple-keyed).
        self.columnar_engines
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
        self.columnar_flushed_segments
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));

        // Sparse vector indexes (tuple key: database, tenant, collection, field).
        self.sparse_vector_indexes
            .retain(|(d, t, c, _), _| !(*d == db && *t == tid && c == &coll));

        // KV engine: drop this collection's hash table + indexes.
        let kv_removed = self.kv_engine.purge_collection(
            task.request.database_id.as_u64(),
            tenant_id,
            collection,
        );

        // CRDT engine: clear rows for this collection in the tenant state.
        let crdt_rows_removed = match self.crdt_engines.get_mut(&tid) {
            Some(engine) => retry_reclaim("crdt.purge_collection", tenant_id, collection, || {
                engine.purge_collection(collection)
            })
            .unwrap_or(0),
            None => 0,
        };

        // Doc cache: evict entries for this collection.
        self.doc_cache
            .evict_collection(task.request.database_id.as_u64(), tenant_id, collection);

        // ── Persistent on-disk unlinks (per-engine reclaim) ──────────────────
        //
        // Engines whose state is in shared redb (document, document-
        // strict, FTS, graph edges) already reclaimed above; engines
        // with no per-collection persistent file (KV hash index,
        // CRDT — per-tenant checkpoint) are N/A.
        use crate::data::executor::handlers::reclaim;
        let mut l1_stats = reclaim::ReclaimStats::default();
        l1_stats.merge(reclaim::vector::reclaim_vector_checkpoints(
            &self.data_dir,
            db.as_u64(),
            tenant_id,
            collection,
        ));
        l1_stats.merge(reclaim::spatial::reclaim_spatial_checkpoints(
            &self.data_dir,
            db.as_u64(),
            tenant_id,
            collection,
        ));
        l1_stats.merge(reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(
            &self.data_dir,
            db.as_u64(),
            tenant_id,
            collection,
        ));
        l1_stats.merge(reclaim::timeseries::reclaim_timeseries_partitions(
            &self.data_dir,
            db.as_u64(),
            tenant_id,
            collection,
        ));
        if let Some(metrics) = self.metrics.as_ref()
            && l1_stats.bytes_freed > 0
        {
            metrics
                .purge
                .add_bytes_reclaimed(tenant_id, "l1-mixed", "l1", l1_stats.bytes_freed);
        }

        // Doc configs + chain hashes + aggregate cache.
        self.doc_configs
            .retain(|(t, c), _| !(*t == tid && c == &coll));
        self.chain_hashes
            .retain(|(t, c), _| !(*t == tid && c == &coll));
        self.aggregate_cache
            .retain(|(t, c), _| !(*t == tid && c == &coll));

        info!(
            core = self.core_id,
            tenant_id,
            collection,
            purge_lsn,
            docs_removed,
            idxs_removed,
            inv_removed,
            kv_removed,
            crdt_rows_removed,
            vec_removed,
            ts_removed,
            spatial_removed,
            edges_removed,
            "collection purge reclaim complete"
        );

        let summary = serde_json::json!({
            "tenant_id": tenant_id,
            "collection": collection,
            "purge_lsn": purge_lsn,
            "documents_removed": docs_removed,
            "indexes_removed": idxs_removed,
            "inverted_entries_removed": inv_removed,
            "kv_tables_removed": kv_removed,
            "crdt_rows_removed": crdt_rows_removed,
            "vector_indexes_removed": vec_removed,
            "timeseries_removed": ts_removed,
            "spatial_removed": spatial_removed,
            "edges_removed": edges_removed,
            "l1_files_unlinked": l1_stats.files_unlinked,
            "l1_bytes_freed": l1_stats.bytes_freed,
        });

        match crate::data::executor::response_codec::encode_json(&summary) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(_) => self.response_ok(task),
        }
    }
}
