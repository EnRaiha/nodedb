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
    ApplyActionParams, ApplyInsertActionParams, apply_action, apply_insert_action, build_merged,
    find_arm, json_to_str,
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
    /// RESOLVE-ONLY read pass (orchestrator phase 1): classify without writing
    /// and return the NOT-MATCHED insert rows.
    pub resolve_only: bool,
    /// Control-Plane-pre-assigned surrogates for the NOT-MATCHED insert rows,
    /// keyed by source join value (orchestrator phase 3). `Some` selects the
    /// atomic verify-and-apply path; `None` (with `resolve_only == false`)
    /// selects the legacy per-row path used by in-transaction buffered replay.
    pub resolved_inserts: Option<&'a [(String, u32)]>,
}

impl CoreLoop {
    /// Execute a MERGE statement.
    ///
    /// Three modes, selected by [`MergeParams`]:
    /// - `resolve_only` → [`Self::execute_merge_resolve`]: a read pass that
    ///   returns the NOT-MATCHED insert rows for Control-Plane surrogate
    ///   assignment (no writes).
    /// - `resolved_inserts.is_some()` → [`Self::execute_merge_apply`]: the
    ///   atomic apply with CP-assigned surrogates + resolve→apply drift verify.
    /// - otherwise → `execute_merge_legacy`: the per-row path retained for
    ///   in-transaction buffered replay.
    pub(in crate::data::executor) fn execute_merge(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        if params.resolve_only {
            return self.execute_merge_resolve(task, tid, params);
        }
        if params.resolved_inserts.is_some() {
            return self.execute_merge_apply(task, tid, params);
        }
        self.execute_merge_legacy(task, tid, params)
    }

    /// Legacy per-row MERGE execution (in-transaction buffered replay).
    fn execute_merge_legacy(
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
            Err(e) => return self.response_error(task, e),
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
                if let Some(arm) = find_arm(clauses, MergeClauseKindOp::NotMatchedBySource, &merged)
                {
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
            if let Some(arm) = find_arm(clauses, MergeClauseKindOp::NotMatched, src_doc) {
                match apply_insert_action(
                    self,
                    ApplyInsertActionParams {
                        database_id: task.request.database_id.as_u64(),
                        tid,
                        collection: target_collection,
                        source_doc: src_doc,
                        clause: arm,
                        strict_schema: &strict_schema,
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

    /// Resolve a collection's strict Binary-Tuple schema, if it is a strict
    /// document collection. `None` for schemaless collections.
    pub(in crate::data::executor) fn merge_strict_schema(
        &self,
        tid: u64,
        collection: &str,
    ) -> Option<nodedb_types::columnar::StrictSchema> {
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    /// Collect every target row as `(doc_id, stored_bytes)` from a consistent
    /// read snapshot. Shared by the legacy walk and the orchestrated
    /// resolve/apply classification so both see the same target set.
    pub(in crate::data::executor) fn collect_target_docs(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
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
        Ok(docs)
    }

    /// Scan source collection and build join map: `join_val → document`.
    pub(in crate::data::executor) fn build_merge_source_map(
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
