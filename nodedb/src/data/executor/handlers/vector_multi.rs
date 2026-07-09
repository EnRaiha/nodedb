// SPDX-License-Identifier: BUSL-1.1

//! Multi-vector document handlers: insert N vectors per doc, delete all,
//! and aggregated scoring search (MaxSim / AvgSim / SumSim).

use std::collections::HashMap;

use nodedb_types::Surrogate;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_multi_vector_insert`].
pub(in crate::data::executor) struct MultiVectorInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub document_surrogate: Surrogate,
    pub vectors_flat: &'a [f32],
    pub count: usize,
    pub dim: usize,
}

/// Parameters for [`CoreLoop::execute_multi_vector_score_search`].
pub(in crate::data::executor) struct MultiVectorScoreSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub mode_str: &'a str,
}

impl CoreLoop {
    /// Insert multiple vectors for a single document into the HNSW index.
    ///
    /// All vectors share the same `document_surrogate` in `surrogate_map`
    /// and are tracked in `multi_doc_map` for bulk deletion.
    pub(in crate::data::executor) fn execute_multi_vector_insert(
        &mut self,
        params: MultiVectorInsertParams<'_>,
    ) -> Response {
        let MultiVectorInsertParams {
            task,
            tid,
            collection,
            field_name,
            document_surrogate,
            vectors_flat,
            count,
            dim,
        } = params;
        debug!(
            core = self.core_id,
            %collection, %field_name, doc_surrogate = document_surrogate.as_u32(), count, dim,
            "multi-vector insert"
        );

        if count == 0 || dim == 0 {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: "multi-vector count and dim must be > 0".into(),
                },
            );
        }
        if vectors_flat.len() != count * dim {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: format!(
                        "data length mismatch: expected {} ({}×{}), got {}",
                        count * dim,
                        count,
                        dim,
                        vectors_flat.len()
                    ),
                },
            );
        }

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);

        // Validate dimension compatibility before taking mutable reference.
        if let Some(existing) = self.vector_collections.get(&index_key)
            && existing.dim() != dim
        {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: format!(
                        "dimension mismatch: index has {}, got {dim}",
                        existing.dim()
                    ),
                },
            );
        }

        // Get or create the vector collection.
        let core_id = self.core_id;
        let params = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        let coll = self
            .vector_collections
            .entry(index_key.clone())
            .or_insert_with(|| {
                debug!(
                    core = core_id,
                    dim, "creating vector collection for multi-vector"
                );
                crate::engine::vector::collection::VectorCollection::new(dim, params)
            });

        // Build vector slices from flat data.
        let vector_slices: Vec<&[f32]> = (0..count)
            .map(|i| &vectors_flat[i * dim..(i + 1) * dim])
            .collect();

        // Delete old multi-vector entries for this doc if they exist (upsert).
        coll.delete_multi_vector(document_surrogate);

        // Insert all vectors with shared surrogate.
        let ids = coll.insert_multi_vector(&vector_slices, document_surrogate);
        // Advance the checkpoint watermark to this write's WAL LSN so a later
        // checkpoint records these nodes as absorbed; startup replay then skips
        // the straddling WAL record instead of appending duplicate HNSW nodes.
        // `None` (unassigned LSN) leaves the watermark untouched.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }

        // Auto-seal if needed.
        let seal_key = CoreLoop::vector_checkpoint_filename(&index_key);
        if coll.needs_seal()
            && let Some(req) = coll.seal(&seal_key)
            && let Some(tx) = &self.build_tx
            && let Err(e) = tx.send(req)
        {
            warn!(core = self.core_id, error = %e, "failed to send HNSW build after multi-vector insert");
        }

        self.checkpoint_coordinator.mark_dirty("vector", ids.len());

        match super::super::response_codec::encode_count("inserted_vectors", ids.len()) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Delete all vectors for a multi-vector document.
    pub(in crate::data::executor) fn execute_multi_vector_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
        document_surrogate: Surrogate,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection, %field_name, doc_surrogate = document_surrogate.as_u32(),
            "multi-vector delete"
        );

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        let deleted = coll.delete_multi_vector(document_surrogate);
        // Advance the watermark to this delete's WAL LSN (single per-collection
        // value covering inserts and deletes), so a checkpoint records the
        // removal as absorbed and replay does not re-run it below the mark.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }
        if deleted > 0 {
            self.checkpoint_coordinator.mark_dirty("vector", deleted);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    /// Search with multi-vector aggregated scoring.
    ///
    /// 1. Over-fetch from HNSW: top_k × over_fetch_factor candidates
    /// 2. Group candidates by doc_id
    /// 3. For each document, collect all its candidate distances
    /// 4. Aggregate per-document using the specified mode (MaxSim/AvgSim/SumSim)
    /// 5. Sort by aggregated score, dedup, return top-K documents
    pub(in crate::data::executor) fn execute_multi_vector_score_search(
        &self,
        params: MultiVectorScoreSearchParams<'_>,
    ) -> Response {
        let MultiVectorScoreSearchParams {
            task,
            tid,
            collection,
            field_name,
            query_vector,
            top_k,
            ef_search,
            mode_str,
        } = params;
        debug!(
            core = self.core_id,
            %collection, %field_name, top_k, %mode_str,
            "multi-vector score search"
        );

        let mode = match nodedb_types::MultiVectorScoreMode::parse(mode_str) {
            Some(m) => m,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: format!(
                            "unknown score mode '{mode_str}'; supported: max_sim, avg_sim, sum_sim"
                        ),
                    },
                );
            }
        };

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        let Some(coll) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if coll.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        // Over-fetch: we need enough candidates so that after grouping by doc_id,
        // we still have top_k distinct documents. Factor of 10 is conservative
        // for typical multi-vector docs with 50-500 tokens.
        let over_fetch = (top_k * 10).clamp(100, 10_000);
        let ef = if ef_search > 0 {
            ef_search.max(over_fetch)
        } else {
            over_fetch.saturating_mul(2).max(64)
        };

        let candidates = coll.search(query_vector, over_fetch, ef);

        // Group by surrogate. For distance metrics where lower = better
        // (L2, cosine) we convert similarity = 1 / (1 + distance) so
        // higher = better. For inner product, distance is already a
        // similarity score. Candidates without a bound surrogate fall
        // back to the local node id wrapped as `Surrogate(local)` so
        // headless inserts still group.
        let mut doc_scores: HashMap<Surrogate, Vec<f32>> = HashMap::new();
        for result in &candidates {
            let key = coll
                .get_surrogate(result.id)
                .unwrap_or_else(|| Surrogate::new(result.id));
            let similarity = 1.0 / (1.0 + result.distance);
            doc_scores.entry(key).or_default().push(similarity);
        }

        let mut scored_docs: Vec<(Surrogate, f32)> = doc_scores
            .iter()
            .map(|(s, scores)| (*s, mode.aggregate(scores)))
            .collect();
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored_docs.truncate(top_k);

        // DP emits surrogate as `id`; CP translates to user PK at the response boundary.
        let hits: Vec<super::super::response_codec::VectorSearchHit> = scored_docs
            .iter()
            .map(|(s, score)| super::super::response_codec::VectorSearchHit {
                id: s.as_u32(),
                distance: *score,
                doc_id: None,
                body: None,
            })
            .collect();

        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "multi-vector search encode failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }
}
