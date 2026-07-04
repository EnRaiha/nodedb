// SPDX-License-Identifier: BUSL-1.1

//! INSERT ... SELECT handler: copy documents from source to target collection.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// INSERT ... SELECT: scan source collection, insert each document into target.
    ///
    /// Returns `{"inserted": N}` payload.
    pub(in crate::data::executor) fn execute_insert_select(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        target_collection: &str,
        source_collection: &str,
        source_filter_bytes: &[u8],
        source_limit: usize,
    ) -> Response {
        debug!(core = self.core_id, %source_collection, %target_collection, "insert select");

        let filters: Vec<ScanFilter> = if source_filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(source_filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize source filters: {e}"),
                        },
                    );
                }
            }
        };

        let fetch_limit = source_limit.saturating_mul(10).max(1000);
        let mut source_docs = match self.scan_collection(
            task.request.database_id.as_u64(),
            tid,
            source_collection,
            fetch_limit,
        ) {
            Ok(docs) => docs,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("scan source: {e}"),
                    },
                );
            }
        };

        if !filters.is_empty() {
            source_docs.retain(|(_, data)| filters.iter().all(|f| f.matches_binary(data)));
        }
        source_docs.truncate(source_limit);

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("begin write: {e}"),
                    },
                );
            }
        };

        let mut inserted = 0usize;
        for (source_id, value) in &source_docs {
            // source_id is the hex-encoded surrogate from the source collection.
            // Parse it back; if it's not a valid surrogate key, fall back to ZERO
            // (FTS indexing will be skipped for that document).
            let source_surrogate = crate::engine::document::store::doc_id_to_surrogate(source_id)
                .unwrap_or(nodedb_types::Surrogate::ZERO);
            if let Err(e) = self.apply_point_put(
                &txn,
                PointPutParams {
                    database_id: task.request.database_id.as_u64(),
                    tid,
                    collection: target_collection,
                    document_id: source_id,
                    surrogate: source_surrogate,
                    value,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    enable_side_indexes: true,
                },
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
            inserted += 1;
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        if inserted > 0 {
            self.checkpoint_coordinator.mark_dirty("sparse", inserted);
        }

        debug!(core = self.core_id, %target_collection, inserted, "insert select complete");
        let result = serde_json::json!({ "inserted": inserted });
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
}
