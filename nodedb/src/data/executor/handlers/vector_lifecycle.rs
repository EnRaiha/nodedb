// SPDX-License-Identifier: BUSL-1.1

//! Vector index lifecycle handlers: stats query, seal, compact, rebuild.
//!
//! Separated from `vector.rs` (write handlers) by concern:
//! write handlers deal with inserts/deletes, these deal with index management.

use tracing::{debug, info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_vector_rebuild`].
pub(in crate::data::executor) struct VectorRebuildParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
}

impl CoreLoop {
    /// Return live `VectorIndexStats` for a collection/field.
    ///
    /// Serializes stats as MessagePack in the response payload.
    pub(in crate::data::executor) fn execute_vector_query_stats(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(
            core = self.core_id,
            key = &index_key.2,
            "vector query stats"
        );

        let Some(coll) = self.vector_collections.get(&index_key) else {
            // Check IVF index as fallback.
            if let Some(ivf) = self.ivf_indexes.get(&index_key) {
                let stats = nodedb_types::VectorIndexStats {
                    sealed_count: 0,
                    building_count: 0,
                    growing_vectors: ivf.len(),
                    sealed_vectors: 0,
                    live_count: ivf.len(),
                    tombstone_count: 0,
                    tombstone_ratio: 0.0,
                    quantization: nodedb_types::VectorIndexQuantization::Pq,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    build_in_progress: false,
                    index_type: nodedb_types::VectorIndexType::IvfPq,
                    hnsw_m: 0,
                    hnsw_m0: 0,
                    hnsw_ef_construction: 0,
                    metric: "l2".into(),
                    dimensions: ivf.dim(),
                    seal_threshold: 0,
                    mmap_segment_count: 0,
                    arena_bytes: None,
                };
                return match zerompk::to_msgpack_vec(&stats) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("serialize stats: {e}"),
                        },
                    ),
                };
            }
            return self.response_error(task, ErrorCode::NotFound);
        };

        let mut stats = coll.stats();
        // Populate arena_bytes from the per-collection arena registry.
        // Only set when the collection was assigned a dedicated arena
        // (vector-primary collections) and the registry is wired.
        if coll.arena_index.is_some()
            && let Some(ref reg) = self.collection_arena_registry
            && let Some(handle) = reg.get(tid, collection)
        {
            stats.arena_bytes = handle.resident_bytes();
        }
        match zerompk::to_msgpack_vec(&stats) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("serialize stats: {e}"),
                },
            ),
        }
    }

    /// Force-seal the growing segment, triggering background HNSW build.
    pub(in crate::data::executor) fn execute_vector_seal(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(core = self.core_id, key = &index_key.2, "vector seal");

        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if coll.growing_is_empty() {
            info!(
                core = self.core_id,
                key = &index_key.2,
                "seal: growing segment empty, nothing to seal"
            );
            return self.response_ok(task);
        }

        let seal_key = CoreLoop::vector_checkpoint_filename(&index_key);
        match coll.seal(&seal_key) {
            Some(req) => {
                if let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request after seal");
                }
                info!(core = self.core_id, key = %seal_key, "growing segment sealed, HNSW build dispatched");
                self.checkpoint_coordinator.mark_dirty("vector", 1);
                self.response_ok(task)
            }
            None => self.response_ok(task),
        }
    }

    /// Force tombstone compaction on a specific vector collection.
    pub(in crate::data::executor) fn execute_vector_compact_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(
            core = self.core_id,
            key = &index_key.2,
            "vector compact index"
        );

        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        let removed = coll.compact();
        info!(
            core = self.core_id,
            key = &index_key.2,
            removed,
            "vector index compaction complete"
        );
        if removed > 0 {
            self.checkpoint_coordinator.mark_dirty("vector", removed);
        }

        match super::super::response_codec::encode_count("compacted", removed) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Rebuild sealed segments with new HNSW parameters.
    ///
    /// Updates the params so future builds use them. Then re-seals and
    /// re-builds all sealed segments with the new params by extracting their
    /// vectors, creating new HNSW indexes, and swapping atomically.
    pub(in crate::data::executor) fn execute_vector_rebuild(
        &mut self,
        params: VectorRebuildParams<'_>,
    ) -> Response {
        use crate::engine::vector::hnsw::{HnswIndex, HnswParams};

        let VectorRebuildParams {
            task,
            tid,
            collection,
            field_name,
            m,
            m0,
            ef_construction,
        } = params;

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        debug!(
            core = self.core_id,
            key = &index_key.2,
            m,
            m0,
            ef_construction,
            "vector rebuild"
        );

        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Merge new params with current (0 = keep current).
        let current = coll.params().clone();
        let new_params = HnswParams {
            m: if m > 0 { m } else { current.m },
            m0: if m0 > 0 { m0 } else { current.m0 },
            ef_construction: if ef_construction > 0 {
                ef_construction
            } else {
                current.ef_construction
            },
            metric: current.metric,
            dtype: current.dtype,
        };

        coll.set_params(new_params.clone());

        // Rebuild each sealed segment in-place with the new params.
        // Each segment is rebuilt atomically: new index is constructed fully
        // before swapping, so a failure leaves the old segment intact.
        let mut rebuilt_count = 0usize;
        for seg in coll.sealed_segments_mut() {
            let vectors = match seg.index.export_vectors() {
                Ok(v) => v,
                Err(e) => {
                    // Leave the segment on the old params rather than rebuild
                    // it from vectors we could not read.
                    warn!(
                        core = self.core_id,
                        key = &index_key.2,
                        error = %e,
                        "rebuild: vector export failed, skipping segment"
                    );
                    continue;
                }
            };
            if vectors.is_empty() {
                continue;
            }
            let dim = seg.index.dim();
            let expected_count = vectors.len();
            let mut new_index = HnswIndex::new(dim, new_params.clone());
            for v in &vectors {
                new_index
                    .insert(v.clone())
                    .unwrap_or_else(|e| tracing::error!(error = %e, "HNSW rebuild insert failed"));
            }
            // Verify all vectors were inserted before swapping.
            if new_index.len() != expected_count {
                warn!(
                    core = self.core_id,
                    key = &index_key.2,
                    expected = expected_count,
                    actual = new_index.len(),
                    "rebuild: vector count mismatch, skipping segment"
                );
                continue;
            }
            seg.index = new_index;
            // Rebuild SQ8 for the new index.
            seg.sq8 = super::super::handlers::vector_lifecycle::rebuild_sq8(&seg.index);
            rebuilt_count += 1;
        }

        info!(
            core = self.core_id,
            key = &index_key.2,
            rebuilt_count,
            m = new_params.m,
            ef = new_params.ef_construction,
            "vector index rebuild complete"
        );

        self.checkpoint_coordinator
            .mark_dirty("vector", rebuilt_count);

        // Also update the stored params for this key.
        self.vector_params.insert(index_key, new_params);

        self.response_ok(task)
    }
}

/// Rebuild SQ8 quantized data for an HNSW index.
///
/// Public within the handler module so `execute_vector_rebuild` can call it.
pub(in crate::data::executor) fn rebuild_sq8(
    index: &crate::engine::vector::hnsw::HnswIndex,
) -> Option<(crate::engine::vector::quantize::sq8::Sq8Codec, Vec<u8>)> {
    crate::engine::vector::collection::VectorCollection::build_sq8_for_index(index)
}
