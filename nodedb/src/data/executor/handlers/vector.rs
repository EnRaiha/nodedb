// SPDX-License-Identifier: BUSL-1.1

//! Vector write handlers: VectorInsert, VectorBatchInsert, VectorDelete,
//! SetVectorParams.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::collection::VectorCollection;
use crate::types::TenantId;
use nodedb_types::DatabaseId;

/// Parameters for configuring vector index settings.
pub(in crate::data::executor) struct SetVectorParamsInput<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    /// Named vector field this config applies to. Empty = default field.
    pub field_name: &'a str,
    pub m: usize,
    pub ef_construction: usize,
    pub metric: &'a str,
    pub index_type: &'a str,
    pub pq_m: usize,
    pub ivf_cells: usize,
    pub ivf_nprobe: usize,
}

/// Parameters for a vector insert operation.
pub(in crate::data::executor) struct VectorInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub dim: usize,
    pub field_name: &'a str,
    pub surrogate: Surrogate,
    pub provenance: Option<&'a SyncProvenance>,
}

/// Parameters for the inner (non-gate) vector insert logic.
///
/// Bundles the operation fields passed from `execute_vector_insert` to
/// `execute_vector_insert_inner` on both the sync-apply and non-sync paths.
pub(in crate::data::executor) struct VectorInsertInner<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub dim: usize,
    pub field_name: &'a str,
    pub surrogate: Surrogate,
}

impl CoreLoop {
    /// Get or create a vector collection, validating dimension compatibility.
    pub(in crate::data::executor) fn get_or_create_vector_index(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        dim: usize,
        field_name: &str,
    ) -> Result<&mut VectorCollection, ErrorCode> {
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        if let Some(existing) = self.vector_collections.get(&index_key)
            && existing.dim() != dim
        {
            return Err(ErrorCode::RejectedConstraint {
                detail: String::new(),
                constraint: format!(
                    "dimension mismatch: index has {}, got {dim}",
                    existing.dim()
                ),
            });
        }
        let core_id = self.core_id;
        let params = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        Ok(self.vector_collections.entry(index_key).or_insert_with(|| {
            debug!(core = core_id, dim, m = params.m, ef = params.ef_construction, ?params.metric, "creating vector collection");
            VectorCollection::new(dim, params)
        }))
    }

    pub(in crate::data::executor) fn execute_vector_insert(
        &mut self,
        params: VectorInsertParams<'_>,
    ) -> Response {
        let VectorInsertParams {
            task,
            tid,
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            provenance,
        } = params;
        debug!(core = self.core_id, %collection, dim, "vector insert");

        // ── Sync idempotency gate (Data-Plane side) ──────────────────────────
        if let Some(prov) = provenance {
            // Copy all provenance fields before mutable borrows for engine apply.
            let producer_id = prov.producer_id;
            let epoch = prov.epoch;
            let stream_id = prov.stream_id;
            let seq = prov.seq;
            let admit = self.sync_admit(prov);
            match admit {
                SyncAdmit::Apply => {
                    // Fall through to the insert path below; sync_commit is
                    // called after the engine write succeeds.
                }
                non_apply @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let current_hwm = self.sync_hwm_value(producer_id, stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&non_apply),
                        current_hwm,
                    );
                }
            }
            // Apply branch: run the insert, then commit and return payload.
            let response = self.execute_vector_insert_inner(VectorInsertInner {
                task,
                tid,
                collection,
                vector,
                dim,
                field_name,
                surrogate,
            });
            if response.status == crate::bridge::envelope::Status::Ok {
                // Re-borrow prov by reconstructing from copied values; the borrow
                // on `self` for `execute_vector_insert_inner` has ended.
                let prov_copy = SyncProvenance {
                    producer_id,
                    epoch,
                    stream_id,
                    seq,
                };
                self.sync_commit(&prov_copy);
                let applied_seq = self.sync_hwm_value(producer_id, stream_id);
                return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
            }
            return response;
        }

        // Non-sync path: behave exactly as before.
        self.execute_vector_insert_inner(VectorInsertInner {
            task,
            tid,
            collection,
            vector,
            dim,
            field_name,
            surrogate,
        })
    }

    /// Inner insert logic shared by the sync and non-sync paths.
    fn execute_vector_insert_inner(&mut self, args: VectorInsertInner<'_>) -> Response {
        let VectorInsertInner {
            task,
            tid,
            collection,
            vector,
            dim,
            field_name,
            surrogate,
        } = args;
        if vector.len() != dim {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: format!(
                        "vector dimension mismatch: expected {dim}, got {}",
                        vector.len()
                    ),
                },
            );
        }
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);

        // Check if this collection uses IVF-PQ index.
        if let Some(cfg) = self.index_configs.get(&index_key)
            && cfg.index_type == crate::engine::vector::index_config::IndexType::IvfPq
        {
            let key = index_key.clone();
            return self.ivf_insert(task, &key, vector, dim, surrogate);
        }

        // Default: HNSW (with or without PQ).
        match self.get_or_create_vector_index(database_id, tid, collection, dim, field_name) {
            Ok(collection_ref) => {
                collection_ref.insert_with_surrogate(vector.to_vec(), surrogate);
                let seal_key = CoreLoop::vector_checkpoint_filename(&index_key);
                if collection_ref.needs_seal()
                    && let Some(req) = collection_ref.seal(&seal_key)
                    && let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request");
                }
                self.checkpoint_coordinator.mark_dirty("vector", 1);
                self.response_ok(task)
            }
            Err(err) => self.response_error(task, err),
        }
    }

    /// Insert into an IVF-PQ index, returning the assigned vector ID.
    fn ivf_insert(
        &mut self,
        task: &ExecutionTask,
        index_key: &(DatabaseId, TenantId, String),
        vector: &[f32],
        dim: usize,
        surrogate: Surrogate,
    ) -> Response {
        let ivf = self
            .ivf_indexes
            .entry(index_key.clone())
            .or_insert_with(|| {
                let cfg = self
                    .index_configs
                    .get(index_key)
                    .cloned()
                    .unwrap_or_default();
                let params = cfg.to_ivf_params();
                debug!(
                    core = self.core_id,
                    key = %index_key.2,
                    "creating IVF-PQ index"
                );
                crate::engine::vector::ivf::IvfPqIndex::new(dim, params)
            });

        // IVF-PQ requires training before the first insert.
        if ivf.n_cells() == 0 {
            let refs: Vec<&[f32]> = vec![vector];
            ivf.train(&refs);
        }

        let vector_id = ivf.add(vector);

        // Register surrogate mapping using the actual IVF-assigned vector ID.
        if surrogate != Surrogate::ZERO {
            let coll = self
                .vector_collections
                .entry(index_key.clone())
                .or_insert_with(|| VectorCollection::new(dim, Default::default()));
            coll.surrogate_map.insert(vector_id, surrogate);
            coll.surrogate_to_local.insert(surrogate, vector_id);
        }

        self.checkpoint_coordinator.mark_dirty("vector", 1);
        self.response_ok(task)
    }

    /// Delete a vector by surrogate (sync inbound path).
    ///
    /// Resolves `surrogate → HNSW node_id` via `surrogate_to_local`, then
    /// delegates to the standard delete path.  If the surrogate is not
    /// present in any index for `collection`, the op is a no-op (idempotent).
    ///
    /// When `provenance` is `Some`, the sync idempotency gate runs first:
    /// non-Apply outcomes return `SyncAckResult` via `response_with_payload`
    /// without touching engine state. Apply outcomes call `sync_commit` after
    /// a successful delete and return `SyncAckResult{Applied}` via payload.
    ///
    /// When `provenance` is `None`, behaves exactly as before (no gate, normal
    /// `response_ok` / `response_error` response shape).
    pub(in crate::data::executor) fn execute_vector_delete_by_surrogate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        field_name: &str,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // ── Sync idempotency gate (Data-Plane side) ──────────────────────────
        if let Some(prov) = provenance {
            let producer_id = prov.producer_id;
            let epoch = prov.epoch;
            let stream_id = prov.stream_id;
            let seq = prov.seq;
            let admit = self.sync_admit(prov);
            match admit {
                SyncAdmit::Apply => {
                    // Fall through to the delete path below.
                }
                non_apply @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let current_hwm = self.sync_hwm_value(producer_id, stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&non_apply),
                        current_hwm,
                    );
                }
            }
            // Apply branch: run the delete, then commit.
            let response = self.execute_vector_delete_by_surrogate_inner(
                task, tid, collection, surrogate, field_name,
            );
            if response.status == crate::bridge::envelope::Status::Ok {
                let prov_copy = SyncProvenance {
                    producer_id,
                    epoch,
                    stream_id,
                    seq,
                };
                self.sync_commit(&prov_copy);
                let applied_seq = self.sync_hwm_value(producer_id, stream_id);
                return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
            }
            return response;
        }

        // Non-sync path: behave exactly as before.
        self.execute_vector_delete_by_surrogate_inner(task, tid, collection, surrogate, field_name)
    }

    /// Inner delete-by-surrogate logic shared by the sync and non-sync paths.
    fn execute_vector_delete_by_surrogate_inner(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let tenant = TenantId::new(tid);
        let db = DatabaseId::new(database_id);
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        let fallback_key = (db, tenant, collection.to_string());

        let resolved_key = if self.vector_collections.contains_key(&index_key) {
            Some(index_key)
        } else if self.vector_collections.contains_key(&fallback_key) {
            Some(fallback_key)
        } else {
            None
        };

        let Some(key) = resolved_key else {
            // Collection not found — treat as idempotent success for sync.
            return self.response_ok(task);
        };

        let node_id = self
            .vector_collections
            .get(&key)
            .and_then(|c| c.surrogate_to_local.get(&surrogate).copied());

        match node_id {
            Some(vid) => self.execute_vector_delete(task, tid, collection, vid),
            None => {
                // Surrogate not present — idempotent.
                self.response_ok(task)
            }
        }
    }
}
