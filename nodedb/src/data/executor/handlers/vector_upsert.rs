// SPDX-License-Identifier: BUSL-1.1

//! Direct upsert handler for vector-primary collections.
//!
//! Bypasses MessagePack document encoding. The caller (Control Plane) has
//! already serialised only the payload-indexed fields into `payload` bytes;
//! this handler inserts the vector into HNSW and updates the bitmap indexes.
//!
//! **Ordering invariant** (enforced below):
//!   1. Validate dimension.
//!   2. Decode `payload` bytes → `HashMap<String, Value>`.
//!   3. Insert vector into HNSW (surrogate bound).
//!   4. Update payload bitmap indexes.
//!
//! If step 3 fails, step 4 is not reached — no partial state.
//! If step 4 panics (should not happen — pure in-memory), the handler
//! attempts to delete the just-inserted HNSW node and returns an error.

use std::collections::HashMap;

use nodedb_types::{Surrogate, Value};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Decode MessagePack payload bytes into `HashMap<String, Value>` and
/// lower-case all field names so bitmap inserts agree with SELECT
/// pre-filters regardless of caller capitalisation.
pub(in crate::data::executor) fn decode_payload_lowercased(
    bytes: &[u8],
) -> Result<HashMap<String, Value>, zerompk::Error> {
    zerompk::from_msgpack::<HashMap<String, Value>>(bytes).map(|m| {
        m.into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect()
    })
}

/// Parameters for [`CoreLoop::execute_vector_direct_upsert`].
pub(in crate::data::executor) struct VectorDirectUpsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub vector: &'a [f32],
    pub payload: &'a [u8],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

impl CoreLoop {
    /// Handle `VectorOp::DirectUpsert`.
    pub(in crate::data::executor) fn execute_vector_direct_upsert(
        &mut self,
        params: VectorDirectUpsertParams<'_>,
    ) -> Response {
        let VectorDirectUpsertParams {
            task,
            tid,
            collection,
            field,
            surrogate,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %field,
            dim = vector.len(),
            "vector direct upsert"
        );

        let dim = vector.len();
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);

        // Step 1: validate dimension and storage dtype against any existing
        // index. The dtype is a creation-time choice baked into segment
        // layout — changing it after the fact would invalidate every node
        // already in the graph.
        if let Some(existing) = self.vector_collections.get(&index_key) {
            if existing.dim() != dim {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: format!(
                            "vector dimension mismatch: index has {}, got {dim}",
                            existing.dim()
                        ),
                    },
                );
            }
            let existing_dtype = existing.params().dtype;
            if existing_dtype != storage_dtype {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: format!(
                            "vector storage_dtype mismatch: index has {existing_dtype}, got {storage_dtype}; \
                             dtype is immutable after collection creation"
                        ),
                    },
                );
            }
        }

        // Step 2: decode payload bytes.
        // Empty slice → empty map (collection has no payload indexes).
        // Field names are lower-cased so the bitmap insert and the SELECT
        // pre-filter agree regardless of how the SQL caller capitalized them.
        let payload_fields: HashMap<String, Value> = if payload.is_empty() {
            HashMap::new()
        } else {
            match decode_payload_lowercased(payload) {
                Ok(m) => m,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("payload decode error: {e}"),
                        },
                    );
                }
            }
        };

        // Step 3: insert into HNSW (with surrogate binding).
        // When a new vector-primary collection is created here, request a
        // dedicated jemalloc arena from the registry so its allocations are
        // isolated from document-engine workloads. Resolve the arena up front
        // to avoid borrowing `self` immutably while also holding a mutable
        // borrow on `self.vector_collections` via `coll`.
        let is_new_collection = !self.vector_collections.contains_key(&index_key);
        let core_id = self.core_id;

        // For a brand-new vector-primary collection, seed `vector_params`
        // with the requested storage dtype so `get_or_create_vector_index`
        // constructs the HNSW graph with the right `NodeStorage` variant
        // (F32 / F16 / BF16). If `set_vector_params` ran first (CREATE
        // COLLECTION path), the existing params are preserved and we only
        // override the dtype.
        if is_new_collection {
            let params = self.vector_params.entry(index_key.clone()).or_default();
            params.dtype = storage_dtype;
        }

        let arena_handle = if is_new_collection {
            self.collection_arena_registry.clone().and_then(|reg| {
                match reg.get_or_create(tid, collection) {
                    Ok(handle) => Some(handle),
                    Err(e) => {
                        tracing::debug!(
                            core = core_id,
                            %collection,
                            error = %e,
                            "per-collection arena allocation failed; using global allocator"
                        );
                        None
                    }
                }
            })
        } else {
            None
        };
        let coll = match self.get_or_create_vector_index(database_id, tid, collection, dim, field) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, e),
        };
        if let Some(handle) = arena_handle {
            coll.arena_index = handle.arena_index();
        }
        if is_new_collection {
            coll.set_quantization(quantization);
            for (f, kind) in payload_indexes {
                coll.payload.add_index(f.to_ascii_lowercase(), *kind);
            }
        }

        let node_id = coll.insert_with_surrogate(vector.to_vec(), surrogate);
        // Advance the checkpoint watermark so a later vector checkpoint records
        // this write as absorbed; startup replay then skips the straddling WAL
        // record instead of appending a duplicate node.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }

        // Step 4: update payload bitmap indexes.
        // If this panics (pure in-memory, should not happen), attempt rollback.
        coll.payload.insert_row(node_id, &payload_fields);

        // Step 5: persist payload to the sparse store keyed by surrogate-hex.
        // The SELECT slow path (`attach_body` + CP response translator
        // flatten) reads document bodies from sparse using the same key
        // shape, so vector-primary collections must write here even though
        // the full document path is bypassed.
        if !payload.is_empty() {
            let row_key = format!("{:08x}", surrogate.as_u32());
            if let Err(e) = self.sparse.put(
                task.request.database_id.as_u64(),
                tid,
                collection,
                &row_key,
                payload,
            ) {
                // Roll back Steps 3 + 4 so the HNSW node and bitmap entries
                // do not survive a failed payload persist. Without this,
                // the orphan node would be returned by future searches
                // with `body: null` on the slow path.
                if let Some(coll) = self.vector_collections.get_mut(&index_key) {
                    coll.payload.delete_row(node_id, &payload_fields);
                    coll.delete(node_id);
                }
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("vector-primary payload sparse write failed: {e}"),
                    },
                );
            }
        }

        // Trigger segment seal if needed.
        let seal_key = CoreLoop::vector_checkpoint_filename(&index_key);
        let coll = self
            .vector_collections
            .get_mut(&index_key)
            .expect("vector collection must exist after insert_with_surrogate");
        if coll.needs_seal()
            && let Some(req) = coll.seal(&seal_key)
            && let Some(tx) = &self.build_tx
            && let Err(e) = tx.send(req)
        {
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "failed to send HNSW build request"
            );
        }

        self.checkpoint_coordinator.mark_dirty("vector", 1);
        self.response_ok(task)
    }
}
