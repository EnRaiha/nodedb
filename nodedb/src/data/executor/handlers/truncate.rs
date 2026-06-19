// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE and ESTIMATE_COUNT handlers.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// TRUNCATE: delete all documents in a collection without filter scanning.
    ///
    /// Iterates the DOCUMENTS table prefix and deletes every key. Cascades to
    /// inverted index, secondary indexes, graph edges, and document cache.
    /// Returns `{"truncated": N}` payload.
    pub(in crate::data::executor) fn execute_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "truncate");

        // Collect all document IDs in this collection.
        let all_ids = match self.scan_matching_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &[],
        ) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("scan for truncate: {e}"),
                    },
                );
            }
        };

        // Delete each document with full cascade.
        let database_id = task.request.database_id.as_u64();
        let mut truncated = 0u64;
        for doc_id in &all_ids {
            if self
                .sparse
                .delete(database_id, tid, collection, doc_id)
                .ok()
                .flatten()
                .is_some()
            {
                // doc_id is the hex-encoded surrogate (the redb storage key).
                // Parse back to Surrogate for FTS removal. Non-hex keys
                // (legacy non-surrogate docs) produce None and skip FTS.
                if let Some(surrogate) = crate::engine::document::store::doc_id_to_surrogate(doc_id)
                    && let Err(e) = self.inverted.remove_document(
                        crate::types::TenantId::new(tid),
                        collection,
                        surrogate,
                    )
                {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "truncate: inverted removal failed");
                }
                if let Err(e) =
                    self.sparse
                        .delete_indexes_for_document(database_id, tid, collection, doc_id)
                {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "truncate: index cascade failed");
                }
                let edges = self
                    .csr_partition_mut(database_id, tid)
                    .remove_node_edges(doc_id);
                let cascade_ord = self.hlc.next_ordinal();
                if edges > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(
                        database_id,
                        nodedb_types::TenantId::new(tid),
                        doc_id,
                        cascade_ord,
                    )
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "truncate: edge cascade failed");
                }
                self.doc_cache.invalidate(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    doc_id,
                );
                truncated += 1;
            }
        }

        // Clear aggregate cache for this collection.
        let tid_key = crate::types::TenantId::new(tid);
        let coll_prefix = format!("{collection}\0");
        self.aggregate_cache
            .retain(|(t, rest), _| !(*t == tid_key && rest.starts_with(&coll_prefix)));

        debug!(core = self.core_id, %collection, truncated, "truncate complete");
        let result = serde_json::json!({ "truncated": truncated });
        match response_codec::encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// ESTIMATE_COUNT: return approximate row count from HLL cardinality stats.
    pub(in crate::data::executor) fn execute_estimate_count(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        match self
            .stats_store
            .get(task.request.database_id.as_u64(), tid, collection, field)
        {
            Ok(Some(stats)) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": stats.distinct_count,
                    "row_count": stats.row_count,
                    "null_count": stats.null_count,
                });
                match response_codec::encode_json(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Ok(None) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": 0,
                    "row_count": 0,
                    "null_count": 0,
                });
                match response_codec::encode_json(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
