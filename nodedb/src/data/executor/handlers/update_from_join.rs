// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::UpdateFromJoin`: implements the two-phase
//! `UPDATE target SET ... FROM src WHERE target.col = src.col` execution.
//!
//! Phase 1: scan the source collection to build a lookup map keyed by the
//!          equi-join value (`source[source_join_col]`).
//! Phase 2: scan the target collection; for each row whose join-column value
//!          matches a source row, build a merged document, evaluate the
//!          assignments, and write the updated row back.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::response_codec::encode_json;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{ReturningSpec, UpdateValue};

/// Parameters for `execute_update_from_join`.
pub(in crate::data::executor) struct UpdateFromJoinParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub target_filter_bytes: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
}

impl CoreLoop {
    /// Execute an `UPDATE target FROM source WHERE target.join_col = source.join_col` operation.
    pub(in crate::data::executor) fn execute_update_from_join(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: UpdateFromJoinParams<'_>,
    ) -> Response {
        let UpdateFromJoinParams {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            target_filter_bytes,
            returning,
        } = params;

        debug!(
            core = self.core_id,
            target = %target_collection,
            source = %source_collection,
            "update from join"
        );

        // Phase 1: Scan source collection, build join map:
        //   source_join_value (as string) → serde_json::Value (the source document).
        let source_map = match self.build_source_join_map(
            task.request.database_id.as_u64(),
            tid,
            source_collection,
            source_join_col,
        ) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        if source_map.is_empty() {
            // No source rows — nothing to update.
            let result = serde_json::json!({ "affected": 0u64 });
            return match encode_json(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            };
        }

        // Phase 2: Deserialize target filters.
        let target_filters: Vec<ScanFilter> = if target_filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(target_filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize target_filters: {e}"),
                        },
                    );
                }
            }
        };

        // Check for strict storage mode on the target.
        let config_key = (
            crate::types::TenantId::new(tid),
            target_collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Gate secondary-vector maintenance once for the whole statement so a
        // non-vector target collection pays nothing. When a vector field is
        // present, a joined UPDATE that rewrites an embedding must re-index the
        // row's HNSW vectors, or KNN search keeps scoring the stale embedding.
        let database_id = task.request.database_id.as_u64();
        let has_vectors = self.collection_has_vectors(database_id, tid, target_collection);

        // Scan target documents and apply updates for those that match.
        let prefix = crate::engine::sparse::btree::coll_prefix(
            task.request.database_id.as_u64(),
            tid,
            target_collection,
        );
        let end = format!("{prefix}\u{ffff}");

        let target_doc_ids: Vec<String> = {
            let read_txn = match self
                .sparse
                .db()
                .begin_read()
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("read txn: {e}"),
                }) {
                Ok(t) => t,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
            let table = match read_txn
                .open_table(crate::engine::sparse::btree::DOCUMENTS)
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("open table: {e}"),
                }) {
                Ok(t) => t,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };

            let mut ids = Vec::new();
            if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
                for entry in range.flatten() {
                    let key = entry.0.value();
                    let value_bytes = entry.1.value();
                    let matches = if let Some(ref schema) = strict_schema {
                        match super::super::strict_format::binary_tuple_to_json(value_bytes, schema)
                        {
                            Some(doc) => {
                                let msgpack = doc_format::encode_to_msgpack(&doc);
                                target_filters.iter().all(|f| f.matches_binary(&msgpack))
                            }
                            None => false,
                        }
                    } else {
                        target_filters.iter().all(|f| f.matches_binary(value_bytes))
                    };
                    if matches && let Some(doc_id) = key.strip_prefix(&prefix) {
                        ids.push(doc_id.to_string());
                    }
                }
            }
            ids
        };

        let mut affected = 0u64;
        let mut returned_docs: Vec<serde_json::Value> = if returning.is_some() {
            Vec::with_capacity(target_doc_ids.len())
        } else {
            Vec::new()
        };

        for doc_id in &target_doc_ids {
            let current_bytes = match self.sparse.get(
                task.request.database_id.as_u64(),
                tid,
                target_collection,
                doc_id,
            ) {
                Ok(Some(b)) => b,
                Ok(None) => continue,
                Err(_) => continue,
            };

            let mut target_doc = if let Some(ref schema) = strict_schema {
                match super::super::strict_format::binary_tuple_to_json(&current_bytes, schema) {
                    Some(v) => v,
                    None => continue,
                }
            } else {
                match doc_format::decode_document(&current_bytes) {
                    Some(v) => v,
                    None => continue,
                }
            };

            // Extract the join key from the target document.
            let join_val = target_doc
                .get(target_join_col)
                .map(json_value_to_string)
                .unwrap_or_default();

            // Look up the matching source row.
            let source_doc = match source_map.get(&join_val) {
                Some(s) => s,
                None => continue, // No matching source row — skip this target row.
            };

            // Build a merged document for expression evaluation:
            // target fields are bare; source fields are qualified as "alias.field".
            let mut merged = target_doc.clone();
            if let (Some(merged_obj), Some(src_obj)) =
                (merged.as_object_mut(), source_doc.as_object())
            {
                for (k, v) in src_obj {
                    merged_obj.insert(format!("{source_alias}.{k}"), v.clone());
                }
            }
            let merged_ndb: nodedb_types::Value = merged.clone().into();

            // Apply SET assignments evaluated against the merged document.
            if let Some(target_obj) = target_doc.as_object_mut() {
                for (field, update_val) in updates {
                    let val: serde_json::Value = match update_val {
                        UpdateValue::Literal(bytes) => {
                            match nodedb_types::json_from_msgpack(bytes) {
                                Ok(v) => v,
                                Err(_) => continue,
                            }
                        }
                        UpdateValue::Expr(expr) => expr.eval(&merged_ndb).into(),
                    };
                    target_obj.insert(field.clone(), val);
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
                    &mut target_doc,
                    &config.enforcement.generated_columns,
                )
            {
                tracing::warn!(
                    %doc_id,
                    error = ?e,
                    "generated column recomputation failed during UpdateFromJoin, skipping"
                );
                continue;
            }

            // Re-encode and write back.
            let updated_bytes = if let Some(ref schema) = strict_schema {
                let ndb_val: nodedb_types::Value = target_doc.clone().into();
                match super::super::strict_format::value_to_binary_tuple(&ndb_val, schema) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        tracing::warn!(
                            %doc_id,
                            error = %e,
                            "strict re-encode failed during UpdateFromJoin, skipping"
                        );
                        continue;
                    }
                }
            } else {
                doc_format::encode_to_msgpack(&target_doc)
            };

            if self
                .sparse
                .put(
                    task.request.database_id.as_u64(),
                    tid,
                    target_collection,
                    doc_id,
                    &updated_bytes,
                )
                .is_ok()
            {
                self.doc_cache.put(
                    task.request.database_id.as_u64(),
                    tid,
                    target_collection,
                    doc_id,
                    &updated_bytes,
                );
                // Re-index the row's vectors from the new body (soft-delete the
                // old HNSW node + insert the new one, keyed by the stable
                // surrogate). No-op unless the collection has a vector field.
                if has_vectors
                    && let Some(surrogate) =
                        crate::engine::document::store::doc_id_to_surrogate(doc_id)
                {
                    self.update_reindex_vector_indexes(UpdateVectorReindex {
                        database_id,
                        tid,
                        collection: target_collection,
                        row_key: doc_id,
                        surrogate,
                        new_body: &updated_bytes,
                        is_strict: strict_schema.is_some(),
                        has_vectors,
                    });
                }
                affected += 1;
                if returning.is_some() {
                    if let Some(obj) = target_doc.as_object_mut() {
                        obj.insert("id".to_string(), serde_json::Value::String(doc_id.clone()));
                    }
                    returned_docs.push(target_doc);
                }
            }
        }

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
            match encode_json(&result) {
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

    /// Scan the source collection and build a `HashMap<join_key_string, document>`.
    fn build_source_join_map(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        join_col: &str,
    ) -> crate::Result<std::collections::HashMap<String, serde_json::Value>> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn for source: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open source table: {e}"),
            })?;

        // Check if the source collection is strict-mode.
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

        let mut map = std::collections::HashMap::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let value_bytes = entry.1.value();
                let doc = if let Some(ref schema) = strict_schema {
                    match super::super::strict_format::binary_tuple_to_json(value_bytes, schema) {
                        Some(v) => v,
                        None => continue,
                    }
                } else {
                    match doc_format::decode_document(value_bytes) {
                        Some(v) => v,
                        None => continue,
                    }
                };
                let key = doc
                    .get(join_col)
                    .map(json_value_to_string)
                    .unwrap_or_default();
                if !key.is_empty() {
                    map.insert(key, doc);
                }
            }
        }
        Ok(map)
    }
}

/// Convert a `serde_json::Value` to a string for join-key comparison.
fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}
