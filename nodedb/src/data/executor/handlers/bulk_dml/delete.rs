// SPDX-License-Identifier: BUSL-1.1

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{OllpPredictedEdge, ReturningSpec};

/// OLLP prediction inputs threaded to `execute_bulk_delete`: the predicted
/// matched-doc surrogate set and the predicted implicit-edge set. Both are
/// verified against the actual scan at admission time, returning
/// [`ErrorCode::OllpRetryRequired`] on any divergence (predicate drift or
/// edge-content drift) before any write occurs. Bundled into one struct to keep
/// the handler signature within the argument-count budget.
pub(in crate::data::executor) struct OllpPrediction<'a> {
    pub surrogates: Option<&'a [u32]>,
    pub edges: Option<&'a [OllpPredictedEdge]>,
}

impl CoreLoop {
    /// Bulk delete: scan documents matching filters, delete all matches.
    ///
    /// Cascades to inverted index, secondary indexes, and graph edges.
    /// When `returning` is `None`, returns affected row count as JSON payload: `{"affected": N}`.
    /// When `returning` is `Some(spec)`, returns a `RowsPayload` with the pre-deletion documents.
    pub(in crate::data::executor) fn execute_bulk_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        filter_bytes: &[u8],
        returning: Option<&ReturningSpec>,
        ollp: OllpPrediction<'_>,
    ) -> Response {
        let ollp_predicted_surrogates = ollp.surrogates;
        let ollp_predicted_edges = ollp.edges;
        debug!(core = self.core_id, %collection, has_returning = returning.is_some(), "bulk delete");
        let database_id = task.request.database_id.as_u64();

        // Empty `filter_bytes` means "no WHERE clause" — match every row.
        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize filters: {e}"),
                        },
                    );
                }
            }
        };

        let matching_ids = match self.scan_matching_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &filters,
        ) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // OLLP verification: when predicted surrogates are provided, compare
        // against the actual matching set. On mismatch return OllpRetryRequired
        // WITHOUT writing. The set comparison is deterministic: both sides are
        // sorted before comparison.
        if let Some(predicted) = ollp_predicted_surrogates {
            let actual = super::scan::ollp_actual_surrogates(&matching_ids);
            let mut predicted_sorted: Vec<u32> = predicted.to_vec();
            predicted_sorted.sort_unstable();
            if actual != predicted_sorted {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // OLLP edge-content verification: implicit-edge DELETE derives
        // `EdgeDelete` tasks from the recon scan's `_from`/`_to`/`_type`. If a
        // matched doc's edge fields were concurrently changed (or an edge
        // appeared/disappeared among the matched docs) between recon and now,
        // the wrong edge would be deleted / a new edge would dangle. The
        // surrogate-set check above cannot see this — the surrogate set is
        // unchanged. Recompute the ACTUAL edge set from the matched docs and
        // compare it to the predicted set carried in the plan; on ANY
        // divergence return OllpRetryRequired WITHOUT writing. Both sides are
        // sorted the same way (the Control Plane sorts the injected predicted
        // set), so this is a plain sorted-slice equality check. The existing
        // retry loop re-scans and re-derives fresh edges.
        if let Some(predicted) = ollp_predicted_edges {
            let actual = self.ollp_actual_edges(database_id, tid, collection, &matching_ids);
            let mut predicted_sorted: Vec<OllpPredictedEdge> = predicted.to_vec();
            predicted_sorted.sort_unstable();
            if actual != predicted_sorted {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // Delete each matching document with full cascade.
        let mut affected = 0u64;
        let mut returned_docs: Vec<serde_json::Value> = if returning.is_some() {
            Vec::with_capacity(matching_ids.len())
        } else {
            Vec::new()
        };
        for doc_id in &matching_ids {
            // Capture pre-deletion snapshot if RETURNING was requested.
            let pre_delete_doc: Option<serde_json::Value> = if returning.is_some() {
                self.sparse
                    .get(task.request.database_id.as_u64(), tid, collection, doc_id)
                    .ok()
                    .flatten()
                    .and_then(|bytes| {
                        let with_id =
                            nodedb_query::msgpack_scan::inject_str_field(&bytes, "id", doc_id);
                        doc_format::decode_document(&with_id)
                    })
            } else {
                None
            };

            if self
                .sparse
                .delete(task.request.database_id.as_u64(), tid, collection, doc_id)
                .ok()
                .flatten()
                .is_some()
            {
                // Cascade: inverted index. doc_id is the hex-encoded surrogate
                // (the redb storage key). Parse back for FTS removal.
                match crate::engine::document::store::doc_id_to_surrogate(doc_id) {
                    Some(surrogate) => {
                        if let Err(e) = self.inverted.remove_document(
                            task.request.database_id.as_u64(),
                            crate::types::TenantId::new(tid),
                            collection,
                            surrogate,
                        ) {
                            warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: inverted index removal failed");
                        }
                    }
                    None => {
                        warn!(core = self.core_id, %collection, %doc_id, "bulk delete: doc_id is not a valid surrogate; FTS entry may be orphaned");
                    }
                }
                // Cascade: secondary indexes.
                if let Err(e) = self.sparse.delete_indexes_for_document(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    doc_id,
                ) {
                    warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: secondary index cascade failed");
                }
                // Cascade: graph edges.
                let edges_removed = self
                    .csr_partition_mut(database_id, tid)
                    .remove_node_edges(doc_id);
                let cascade_ord = self.hlc.next_ordinal();
                if edges_removed > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(
                        database_id,
                        nodedb_types::TenantId::new(tid),
                        doc_id,
                        cascade_ord,
                    )
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "bulk delete: edge cascade failed");
                }
                self.mark_node_deleted(database_id, tid, doc_id);
                self.doc_cache.invalidate(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    doc_id,
                );
                affected += 1;
                if let Some(doc) = pre_delete_doc {
                    returned_docs.push(doc);
                }
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk delete complete");

        if let Some(spec) = returning {
            match returning_rows::build_rows_payload(spec, &returned_docs) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            let result = serde_json::json!({ "affected": affected });
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

    /// Compute the sorted ACTUAL implicit-edge set for the matched docs.
    ///
    /// For each matched `doc_id`, parse its surrogate (same `len()==8` hex
    /// parse as [`ollp_actual_surrogates`]), fetch the stored doc bytes via the
    /// SAME `sparse.get` path the delete loop uses, decode it, and — only when
    /// it carries BOTH `_from` and `_to` as strings — record an
    /// [`OllpPredictedEdge`] with the raw `_type` as `label`. A matched doc
    /// without both endpoints is not an edge and is skipped; if it gained an
    /// edge after recon it appears here and forces a set mismatch (correct).
    ///
    /// The output is sorted via `OllpPredictedEdge`'s derived `Ord` so it
    /// compares as a plain sorted-slice equality against the Control-Plane-sorted
    /// predicted set. Edge docs are schemaless (`_from`/`_to`), so `decode_document`
    /// (msgpack→JSON) is the field-extraction primitive — no hand-rolled
    /// msgpack. Bytes that don't decode (e.g. a strict Binary Tuple) yield no
    /// edge, matching the schemaless-only scope of implicit edges.
    pub(in crate::data::executor) fn ollp_actual_edges(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        matching_ids: &[String],
    ) -> Vec<OllpPredictedEdge> {
        // `decode_document` returns `serde_json::Value`, whose `get`/`as_str`
        // are inherent methods — no extra trait import needed.
        let mut edges: Vec<OllpPredictedEdge> = Vec::new();
        for doc_id in matching_ids {
            let surrogate = if doc_id.len() == 8 {
                match u32::from_str_radix(doc_id, 16) {
                    Ok(s) => s,
                    Err(_) => continue,
                }
            } else {
                continue;
            };
            let Ok(Some(bytes)) = self.sparse.get(database_id, tid, collection, doc_id) else {
                continue;
            };
            let Some(doc) = doc_format::decode_document(&bytes) else {
                continue;
            };
            let from = doc.get("_from").and_then(|v| v.as_str());
            let to = doc.get("_to").and_then(|v| v.as_str());
            if let (Some(from), Some(to)) = (from, to) {
                let label = doc
                    .get("_type")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                edges.push(OllpPredictedEdge {
                    surrogate,
                    from: from.to_string(),
                    to: to.to_string(),
                    label,
                });
            }
        }
        edges.sort_unstable();
        edges
    }
}
