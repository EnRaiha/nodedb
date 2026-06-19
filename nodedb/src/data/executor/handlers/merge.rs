// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::Merge`: implements the MERGE statement execution.
//!
//! Execution model (mirroring SQL MERGE semantics):
//!
//! Phase 1: Build a join map from the source collection:
//!   source_join_value → source_document
//!
//! Phase 2: Walk all target rows.  For each target row:
//!   - If the source map has a matching entry, evaluate WHEN MATCHED arms in
//!     order; apply the first arm whose extra_predicate is satisfied.
//!   - If no source row matches, evaluate WHEN NOT MATCHED BY SOURCE arms.
//!
//! Phase 3: Walk source rows that had no target match.  Evaluate WHEN NOT
//!   MATCHED arms in order; apply the first whose extra_predicate is satisfied.

use tracing::debug;

use super::merge_helpers::{
    apply_action, apply_insert_action, build_merged, find_arm, json_to_str,
};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::response_codec::encode_json;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeClauseKind as MergeClauseKindOp, MergeClauseOp,
};

/// Parameters for `execute_merge`.
pub(in crate::data::executor) struct MergeParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
}

impl CoreLoop {
    /// Execute a MERGE statement.
    pub(in crate::data::executor) fn execute_merge(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        let MergeParams {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            clauses,
        } = params;

        debug!(
            core = self.core_id,
            target = %target_collection,
            source = %source_collection,
            "merge"
        );

        // Phase 1: Build source join map.
        let source_map = match self.build_merge_source_map(
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

        // Check strict schema for target.
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

        // Collect all target doc IDs and their documents.
        let prefix = crate::engine::sparse::btree::coll_prefix(
            task.request.database_id.as_u64(),
            tid,
            target_collection,
        );
        let end = format!("{prefix}\u{ffff}");

        let target_docs: Vec<(String, Vec<u8>)> = {
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

            let mut docs = Vec::new();
            if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
                for entry in range.flatten() {
                    let key = entry.0.value();
                    let bytes = entry.1.value().to_vec();
                    if let Some(doc_id) = key.strip_prefix(&prefix) {
                        docs.push((doc_id.to_string(), bytes));
                    }
                }
            }
            docs
        };

        let mut affected = 0u64;
        // Track which source keys were matched against a target row.
        let mut matched_source_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Phase 2: process target rows.
        for (doc_id, bytes) in &target_docs {
            let target_doc = if let Some(ref schema) = strict_schema {
                match super::super::strict_format::binary_tuple_to_json(bytes, schema) {
                    Some(v) => v,
                    None => continue,
                }
            } else {
                match doc_format::decode_document(bytes) {
                    Some(v) => v,
                    None => continue,
                }
            };

            let join_val = target_doc
                .get(target_join_col)
                .map(json_to_str)
                .unwrap_or_default();

            if let Some(source_doc) = source_map.get(&join_val) {
                matched_source_keys.insert(join_val.clone());
                // Build merged document for predicate / expression evaluation.
                let merged = build_merged(&target_doc, source_doc, source_alias);
                // Find first MATCHED arm whose predicate is satisfied.
                if let Some(arm) = find_arm(clauses, MergeClauseKindOp::Matched, &merged) {
                    let db_id = task.request.database_id.as_u64();
                    match apply_action(
                        self,
                        db_id,
                        tid,
                        target_collection,
                        doc_id,
                        &target_doc,
                        source_doc,
                        source_alias,
                        arm,
                        &strict_schema,
                    ) {
                        Ok(true) => affected += 1,
                        Ok(false) => {}
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            );
                        }
                    }
                }
            } else {
                // No matching source row — check NOT MATCHED BY SOURCE arms.
                let merged = target_doc.clone();
                if let Some(arm) = find_arm(clauses, MergeClauseKindOp::NotMatchedBySource, &merged)
                {
                    let db_id = task.request.database_id.as_u64();
                    match apply_action(
                        self,
                        db_id,
                        tid,
                        target_collection,
                        doc_id,
                        &target_doc,
                        &serde_json::Value::Null,
                        source_alias,
                        arm,
                        &strict_schema,
                    ) {
                        Ok(true) => affected += 1,
                        Ok(false) => {}
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            );
                        }
                    }
                }
            }
        }

        // Phase 3: source rows without a matching target row.
        for (src_key, src_doc) in &source_map {
            if matched_source_keys.contains(src_key.as_str()) {
                continue;
            }
            if let Some(arm) = find_arm(clauses, MergeClauseKindOp::NotMatched, src_doc) {
                match apply_insert_action(
                    self,
                    task.request.database_id.as_u64(),
                    tid,
                    target_collection,
                    src_doc,
                    arm,
                    &strict_schema,
                ) {
                    Ok(true) => affected += 1,
                    Ok(false) => {}
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                }
            }
        }

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

    /// Scan source collection and build join map: `join_val → document`.
    fn build_merge_source_map(
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
                detail: format!("read txn for merge source: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open merge source table: {e}"),
            })?;

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
                let key = doc.get(join_col).map(json_to_str).unwrap_or_default();
                if !key.is_empty() {
                    map.insert(key, doc);
                }
            }
        }
        Ok(map)
    }
}
