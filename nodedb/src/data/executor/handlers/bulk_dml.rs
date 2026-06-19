// SPDX-License-Identifier: BUSL-1.1

//! Bulk DML handlers: BulkUpdate, BulkDelete.
//!
//! These operate on document sets matching ScanFilter predicates,
//! unlike PointUpdate/PointDelete which require `WHERE id = 'x'`.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ReturningSpec;

impl CoreLoop {
    /// Scan documents in a collection matching the given filters.
    ///
    /// Returns document IDs of all matching documents.
    pub(in crate::data::executor) fn scan_matching_documents(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<String>> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        // Check if this is a strict (Binary Tuple) collection.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        let mut ids = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                let matches = if let Some(ref schema) = strict_schema {
                    // Strict: Binary Tuple → Value → MessagePack → matches_binary.
                    match super::super::strict_format::binary_tuple_to_json(value_bytes, schema) {
                        Some(doc) => {
                            let msgpack = doc_format::encode_to_msgpack(&doc);
                            filters.iter().all(|f| f.matches_binary(&msgpack))
                        }
                        None => false,
                    }
                } else {
                    filters.iter().all(|f| f.matches_binary(value_bytes))
                };
                if matches && let Some(doc_id) = key.strip_prefix(&prefix) {
                    ids.push(doc_id.to_string());
                }
            }
        }
        Ok(ids)
    }
}

/// Parameters for a bulk update operation.
pub(in crate::data::executor) struct BulkUpdateParams<'a> {
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub ollp_predicted_surrogates: Option<&'a [u32]>,
}

impl CoreLoop {
    /// Bulk update: scan documents matching filters, apply field updates.
    ///
    /// When `returning` is `None`, returns affected row count as JSON:
    /// `{"affected": N}`.
    ///
    /// When `returning` is `Some(spec)`, returns a `RowsPayload` with the
    /// post-update documents projected per spec. If 0 rows match, returns
    /// an empty `RowsPayload`.
    pub(in crate::data::executor) fn execute_bulk_update(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: BulkUpdateParams<'_>,
    ) -> Response {
        let BulkUpdateParams {
            collection,
            filter_bytes,
            updates,
            returning,
            ollp_predicted_surrogates,
        } = params;
        debug!(core = self.core_id, %collection, has_returning = returning.is_some(), "bulk update");

        // Reject direct updates to generated columns.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = super::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

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
            let actual = ollp_actual_surrogates(&matching_ids);
            let mut predicted_sorted: Vec<u32> = predicted.to_vec();
            predicted_sorted.sort_unstable();
            if actual != predicted_sorted {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // Check if this is a strict (Binary Tuple) collection.
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Apply updates to each matching document.
        let mut affected = 0u64;
        let mut returned_docs: Vec<serde_json::Value> = if returning.is_some() {
            Vec::with_capacity(matching_ids.len())
        } else {
            Vec::new()
        };

        for doc_id in &matching_ids {
            match self
                .sparse
                .get(task.request.database_id.as_u64(), tid, collection, doc_id)
            {
                Ok(Some(current_bytes)) => {
                    // Decode current value — format depends on storage mode.
                    let mut doc = if let Some(ref schema) = strict_schema {
                        match super::super::strict_format::binary_tuple_to_json(
                            &current_bytes,
                            schema,
                        ) {
                            Some(v) => v,
                            None => continue,
                        }
                    } else {
                        match doc_format::decode_document(&current_bytes) {
                            Some(v) => v,
                            None => continue,
                        }
                    };
                    // Snapshot the current row for expression evaluation. All
                    // expression assignments see the pre-update state — multiple
                    // assignments in the same UPDATE do not observe each other,
                    // matching PostgreSQL semantics.
                    let eval_doc: nodedb_types::Value = doc.clone().into();
                    if let Some(obj) = doc.as_object_mut() {
                        for (field, update_val) in updates {
                            let val: serde_json::Value = match update_val {
                                nodedb_physical::physical_plan::UpdateValue::Literal(bytes) => {
                                    match nodedb_types::json_from_msgpack(bytes) {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    }
                                }
                                nodedb_physical::physical_plan::UpdateValue::Expr(expr) => {
                                    let result: nodedb_types::Value = expr.eval(&eval_doc);
                                    result.into()
                                }
                            };
                            obj.insert(field.clone(), val);
                        }
                    }
                    // Recompute generated columns if any dependency changed.
                    if let Some(config) = self.doc_configs.get(&config_key)
                        && !config.enforcement.generated_columns.is_empty()
                        && super::generated::needs_recomputation(
                            updates,
                            &config.enforcement.generated_columns,
                        )
                        && let Err(e) = super::generated::evaluate_generated_columns(
                            &mut doc,
                            &config.enforcement.generated_columns,
                        )
                    {
                        tracing::warn!(
                            %doc_id,
                            error = ?e,
                            "generated column recomputation failed, skipping document"
                        );
                        continue;
                    }
                    // Re-encode — format depends on storage mode.
                    let updated_bytes = if let Some(ref schema) = strict_schema {
                        let ndb_val: nodedb_types::Value = doc.clone().into();
                        match super::super::strict_format::value_to_binary_tuple(&ndb_val, schema) {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                tracing::warn!(
                                    %doc_id,
                                    error = %e,
                                    "strict re-encode failed, skipping document"
                                );
                                continue;
                            }
                        }
                    } else {
                        doc_format::encode_to_msgpack(&doc)
                    };
                    if self
                        .sparse
                        .put(
                            task.request.database_id.as_u64(),
                            tid,
                            collection,
                            doc_id,
                            &updated_bytes,
                        )
                        .is_ok()
                    {
                        self.doc_cache.put(
                            task.request.database_id.as_u64(),
                            tid,
                            collection,
                            doc_id,
                            &updated_bytes,
                        );
                        affected += 1;
                        if returning.is_some() {
                            // Include document ID in the returned document.
                            if let Some(obj) = doc.as_object_mut() {
                                obj.insert(
                                    "id".to_string(),
                                    serde_json::Value::String(doc_id.clone()),
                                );
                            }
                            returned_docs.push(doc);
                        }
                    }
                }
                _ => continue,
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk update complete");

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
        ollp_predicted_surrogates: Option<&[u32]>,
    ) -> Response {
        debug!(core = self.core_id, %collection, has_returning = returning.is_some(), "bulk delete");

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
            let actual = ollp_actual_surrogates(&matching_ids);
            let mut predicted_sorted: Vec<u32> = predicted.to_vec();
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
                let edges_removed = self.csr_partition_mut(tid).remove_node_edges(doc_id);
                let cascade_ord = self.hlc.next_ordinal();
                if edges_removed > 0
                    && let Err(e) = self.edge_store.delete_edges_for_node(
                        nodedb_types::TenantId::new(tid),
                        doc_id,
                        cascade_ord,
                    )
                {
                    warn!(core = self.core_id, %doc_id, error = %e, "bulk delete: edge cascade failed");
                }
                self.mark_node_deleted(tid, doc_id);
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
}

/// Compute the sorted list of surrogates from scanned document IDs.
///
/// Document storage keys are 8-character hex-encoded u32 surrogates
/// (see `engine::document::store::key`). Ids that cannot be parsed are
/// silently skipped — they represent legacy non-surrogate documents that
/// do not participate in OLLP verification.
///
/// The output is sorted ascending, matching the contract expected by the
/// OLLP verification comparison on both sides (Data Plane and Control
/// Plane pre-exec).
fn ollp_actual_surrogates(doc_ids: &[String]) -> Vec<u32> {
    let mut surrogates: Vec<u32> = doc_ids
        .iter()
        .filter_map(|id| {
            if id.len() == 8 {
                u32::from_str_radix(id, 16).ok()
            } else {
                None
            }
        })
        .collect();
    surrogates.sort_unstable();
    surrogates
}
