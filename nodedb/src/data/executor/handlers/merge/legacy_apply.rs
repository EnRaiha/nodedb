// SPDX-License-Identifier: BUSL-1.1

//! The legacy per-row MERGE walk: classify and write one target row at a time.
//!
//! Isolated from the source-map and target-scan helpers it calls because it is
//! the only remaining consumer of the row-at-a-time write model — every live
//! MERGE now goes through the orchestrated RESOLVE/APPLY passes, which classify
//! into a plan first and apply the whole plan atomically. Keeping this walk in
//! its own file means the shared read helpers can be read (and changed) without
//! stepping through a fallback that no current statement reaches, and makes the
//! walk removable in one piece when the last caller is gone.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::response_codec::encode_json;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseKind as MergeClauseKindOp;

use super::super::merge_helpers::{
    ApplyActionParams, ApplyInsertActionParams, apply_action, apply_insert_action, build_merged,
    find_arm, json_to_str,
};
use super::dispatch::MergeParams;

impl CoreLoop {
    /// Legacy per-row MERGE apply, retained only as a fallback. In-transaction
    /// MERGE (which formerly reached this via buffered replay) is now expanded
    /// at COMMIT into concrete point ops before dispatch.
    pub(super) fn execute_merge_legacy(
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
            resolve_only: _,
            resolved_inserts: _,
            source_rows,
            // The legacy per-row path is reached only by in-transaction replay,
            // which never carries a RETURNING projection (the COMMIT expander
            // rewrites the statement into concrete point ops before dispatch).
            returning: _,
            rls_filters: _,
            rls_write_check,
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
            source_rows,
        ) {
            Ok(m) => m,
            Err(e) => return self.response_error(task, e),
        };

        // Check strict schema for target.
        let config_key = (
            task.request.database_id,
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
        // non-vector target collection pays nothing; the per-row UPDATE / DELETE
        // arms maintain the HNSW index only when this is set.
        let has_vectors =
            self.collection_has_vectors(task.request.database_id.as_u64(), tid, target_collection);

        // Collect all target doc IDs and their documents.
        let target_docs: Vec<(String, Vec<u8>)> = match self.collect_target_docs(
            task.request.database_id.as_u64(),
            tid,
            target_collection,
            task.request.txn_id,
        ) {
            Ok(docs) => docs,
            Err(e) => return self.response_error(task, e),
        };

        let mut affected = 0u64;
        // Track which source keys were matched against a target row.
        let mut matched_source_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Phase 2: process target rows.
        for (doc_id, bytes) in &target_docs {
            // A target row the classifier cannot read is not "absent": skipping
            // it makes the MERGE take its NOT MATCHED arm and insert a
            // duplicate of a row that already exists.
            let target_doc = if let Some(ref schema) = strict_schema {
                match super::super::super::strict_format::binary_tuple_to_json(bytes, schema) {
                    Some(v) => v,
                    None => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "MERGE target row '{doc_id}' ({} bytes) is not a Binary Tuple \
                                     readable under the collection's strict schema",
                                    bytes.len()
                                ),
                            },
                        );
                    }
                }
            } else {
                match doc_format::decode_document(bytes) {
                    Ok(v) => v,
                    Err(e) => return self.response_error(task, e),
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
                let arm = match find_arm(clauses, MergeClauseKindOp::Matched, &merged) {
                    Ok(arm) => arm,
                    Err(e) => return self.response_error(task, e),
                };
                if let Some(arm) = arm {
                    let db_id = task.request.database_id.as_u64();
                    match apply_action(
                        self,
                        ApplyActionParams {
                            database_id: db_id,
                            tid,
                            collection: target_collection,
                            doc_id,
                            target_doc: &target_doc,
                            source_doc,
                            source_alias,
                            clause: arm,
                            strict_schema: &strict_schema,
                            has_vectors,
                            rls_write_check,
                        },
                    ) {
                        Ok(true) => affected += 1,
                        Ok(false) => {}
                        Err(e) => return self.response_error(task, e),
                    }
                }
            } else {
                // No matching source row — check NOT MATCHED BY SOURCE arms.
                let merged = target_doc.clone();
                let arm = match find_arm(clauses, MergeClauseKindOp::NotMatchedBySource, &merged) {
                    Ok(arm) => arm,
                    Err(e) => return self.response_error(task, e),
                };
                if let Some(arm) = arm {
                    let db_id = task.request.database_id.as_u64();
                    match apply_action(
                        self,
                        ApplyActionParams {
                            database_id: db_id,
                            tid,
                            collection: target_collection,
                            doc_id,
                            target_doc: &target_doc,
                            source_doc: &serde_json::Value::Null,
                            source_alias,
                            clause: arm,
                            strict_schema: &strict_schema,
                            has_vectors,
                            rls_write_check,
                        },
                    ) {
                        Ok(true) => affected += 1,
                        Ok(false) => {}
                        Err(e) => return self.response_error(task, e),
                    }
                }
            }
        }

        // Phase 3: source rows without a matching target row.
        for (src_key, src_doc) in &source_map {
            if matched_source_keys.contains(src_key.as_str()) {
                continue;
            }
            let arm = match find_arm(clauses, MergeClauseKindOp::NotMatched, src_doc) {
                Ok(arm) => arm,
                Err(e) => return self.response_error(task, e),
            };
            if let Some(arm) = arm {
                match apply_insert_action(
                    self,
                    ApplyInsertActionParams {
                        database_id: task.request.database_id.as_u64(),
                        tid,
                        collection: target_collection,
                        source_doc: src_doc,
                        source_alias,
                        clause: arm,
                        strict_schema: &strict_schema,
                        rls_write_check,
                    },
                ) {
                    Ok(true) => affected += 1,
                    Ok(false) => {}
                    Err(e) => return self.response_error(task, e),
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
}
