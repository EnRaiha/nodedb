// SPDX-License-Identifier: BUSL-1.1

//! Batch vector insert and node-id–based vector delete handlers.
//!
//! Extracted from `vector.rs` to keep file sizes within the 500-line limit.

use nodedb_types::Surrogate;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_upsert::decode_payload_lowercased;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// Execute batch vector insert (always to the default/unnamed field).
    pub(in crate::data::executor) fn execute_vector_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        vectors: &[Vec<f32>],
        dim: usize,
        surrogates: &[Surrogate],
    ) -> Response {
        debug!(core = self.core_id, %collection, dim, count = vectors.len(), "vector batch insert");
        let index_key = CoreLoop::vector_index_key(tid, collection, "");
        match self.get_or_create_vector_index(tid, collection, dim, "") {
            Ok(collection_ref) => {
                for (i, vector) in vectors.iter().enumerate() {
                    if vector.len() != dim {
                        return self.response_error(
                            task,
                            ErrorCode::RejectedConstraint {
                                detail: String::new(),
                                constraint: format!(
                                    "dimension mismatch in batch: expected {dim}, got {}",
                                    vector.len()
                                ),
                            },
                        );
                    }
                    let s = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
                    collection_ref.insert_with_surrogate(vector.clone(), s);
                }
                let seal_key = CoreLoop::vector_checkpoint_filename(&index_key);
                if collection_ref.needs_seal()
                    && let Some(req) = collection_ref.seal(&seal_key)
                    && let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request");
                }
                self.checkpoint_coordinator
                    .mark_dirty("vector", vectors.len());
                match super::super::response_codec::encode_count("inserted", vectors.len()) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(err) => self.response_error(task, err),
        }
    }

    pub(in crate::data::executor) fn execute_vector_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        vector_id: u32,
    ) -> Response {
        debug!(core = self.core_id, %collection, vector_id, "vector delete");
        // Resolve the actual index key. Legacy `CREATE VECTOR INDEX` uses
        // an empty field segment; vector-primary collections use
        // `"{collection}:{field}"`. Try the legacy key first, then scan
        // for any field-suffixed key under the same (tenant, collection).
        let tenant = TenantId::new(tid);
        let plain_key = (tenant, collection.to_string());
        let prefix = format!("{collection}:");
        let resolved_key = if self.vector_collections.contains_key(&plain_key) {
            Some(plain_key)
        } else {
            self.vector_collections
                .keys()
                .find(|(t, c)| *t == tenant && c.starts_with(&prefix))
                .cloned()
        };
        let Some(index_key) = resolved_key else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Capture the surrogate before deletion so we can fetch the
        // payload row from the sparse store and update the bitmap. The
        // bitmap stores node-id -> field-value membership; without the
        // original field values we cannot remove the entries cleanly.
        //
        // Asymmetric with the insert path (`vector_upsert`): insert
        // atomically rolls back the HNSW node if the sparse write fails,
        // because a phantom node would be returned by future searches.
        // Delete is best-effort cleanup — if the sparse read or decode
        // fails we still drop the HNSW node and skip bitmap cleanup.
        // Phantom bitmap entries are safe (the bitmap is filtered against
        // live node ids on read), so leaving them is preferable to
        // aborting the delete and leaking the vector.
        let surrogate_opt = self
            .vector_collections
            .get(&index_key)
            .and_then(|c| c.get_surrogate(vector_id));

        if let Some(surrogate) = surrogate_opt {
            let row_key = format!("{:08x}", surrogate.as_u32());
            let fields = match self.sparse.get(tid, collection, &row_key) {
                Ok(Some(bytes)) => decode_payload_lowercased(&bytes).ok(),
                _ => None,
            };
            if let Some(fields) = fields
                && let Some(coll) = self.vector_collections.get_mut(&index_key)
            {
                coll.payload.delete_row(vector_id, &fields);
            }
        }

        let Some(collection_ref) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if collection_ref.delete(vector_id) {
            self.checkpoint_coordinator.mark_dirty("vector", 1);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }
}
