// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::UpdateFromJoin`: implements the two-phase
//! `UPDATE target SET ... FROM src WHERE target.col = src.col` execution.
//!
//! Phase 1: scan the source collection to build a lookup map keyed by the
//!          equi-join value (`source[source_join_col]`).
//! Phase 2: scan the target collection; for each row whose join-column value
//!          matches a source row, build a merged document and evaluate the
//!          assignments to produce the post-image (shared classifier in
//!          `update_from_join_collect::collect_update_from_join_rows`).
//! Phase 3: either write each post-image back (`resolve_only == false`,
//!          delegated to [`update_from_join_write`]) or, on the COMMIT-time
//!          RESOLVE pass (`resolve_only == true`), return the matched rows as
//!          `(doc_id, Option<surrogate>, post_image_body)` for the expander to
//!          rewrite into concrete `PointPut` ops — WITHOUT writing,
//!          re-indexing, accumulating a write-set, or emitting events.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::response_codec::encode_json_as_msgpack;
use crate::data::executor::task::ExecutionTask;

use super::update_from_join_write::WriteResolvedRowsCtx;

pub(in crate::data::executor) use super::update_from_join_types::ResolvedUpdateRow;
pub(in crate::data::executor) use super::update_from_join_types::UpdateFromJoinParams;

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
            resolve_only,
            source_rows,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = params;

        debug!(
            core = self.core_id,
            target = %target_collection,
            source = %source_collection,
            resolve_only,
            "update from join"
        );

        // Phase 1: Scan source collection, build join map:
        //   source_join_value (as string) → serde_json::Value (the source document).
        let source_map = match self.build_source_join_map(
            task.request.database_id.as_u64(),
            tid,
            source_collection,
            source_join_col,
            source_rows,
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

        // Check for strict storage mode on the target.
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

        if source_map.is_empty() {
            // No source rows — nothing matches. The RESOLVE pass returns an
            // empty match set; the write path reports zero affected.
            if resolve_only {
                return self.encode_resolved_update_rows(task, Vec::new());
            }
            let result = serde_json::json!({ "affected": 0u64 });
            return match encode_json_as_msgpack(&result) {
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

        // Phase 3: Scan the target, join each row against the source, evaluate
        // the SET assignments, and encode the post-image — WITHOUT writing. This
        // classification is shared verbatim by both the RESOLVE pass and the
        // write path so the two cannot diverge on match set or post-image.
        let rows = match self.collect_update_from_join_rows(
            super::update_from_join_collect::CollectUpdateRows {
                task,
                tid,
                target_collection,
                source_alias,
                target_join_col,
                updates,
                source_map: &source_map,
                target_filters: &target_filters,
                strict_schema: strict_schema.as_ref(),
                config_key: &config_key,
            },
        ) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // RESOLVE pass: hand the matched rows back for COMMIT-time expansion.
        // No `sparse.put`, no vector re-index, no write-set, no events.
        if resolve_only {
            return self.encode_resolved_update_rows(task, rows);
        }

        // Materialized-sum coverage verification (LEADER-ONLY, mirroring the
        // bulk-DML OLLP checks): the resolution carried in the plan was derived
        // by the Control-Plane orchestrator from a RESOLVE pass taken before this
        // write pass. The join map, the target scan, or a matched row's join key
        // can all have moved since. Both images of every matched row are handed
        // in — the stored pre-image and the post-image just classified — because a
        // rewritten join key debits the target the row leaves and credits the one
        // it joins, so a resolution covering one side only leaves the other's
        // total wrong. `updates` is deliberately empty here: the post-images are
        // supplied rather than re-derived, so the assignments must not be applied
        // a second time. On a shortfall this returns OllpRetryRequired WITHOUT
        // writing and the orchestrator re-resolves.
        let sum_check = SumTargetCheck {
            database_id: task.request.database_id.as_u64(),
            tid,
            collection: target_collection,
            updates: &[],
            resolved: resolved_sum_targets,
        };
        // Gated so a target collection declaring no binding — nearly every one —
        // never decodes a pre-image or clones a post-image for this.
        if self.declares_materialized_sums(&sum_check) {
            let mut sum_images: Vec<serde_json::Value> = Vec::with_capacity(rows.len() * 2);
            for row in &rows {
                if let Some(old_doc) = self.decode_source_row(&sum_check, &row.old_body) {
                    sum_images.push(old_doc);
                }
                sum_images.push(row.doc.clone());
            }
            if self.sum_targets_diverged(&sum_check, &sum_images) {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // Gate every matched target row on the TARGET's write policy before any
        // write, so a rejected row cannot leave the rows ahead of it rewritten.
        // `row.doc` carries the assignments and any regenerated columns already
        // applied, so it is the row that would exist afterwards. The RESOLVE
        // pass returns above without writing; the Control-Plane expander that
        // consumes it gates the point ops it emits.
        if !rls_write_check.is_empty() {
            for row in &rows {
                if let Err(e) =
                    rls_write_gate::admit_row(rls_write_check, &row.doc, tid, target_collection)
                {
                    return self.response_error(task, e);
                }
            }
        }

        // Gate secondary-vector maintenance once for the whole statement so a
        // non-vector target collection pays nothing. When a vector field is
        // present, a joined UPDATE that rewrites an embedding must re-index the
        // row's HNSW vectors, or KNN search keeps scoring the stale embedding.
        let database_id = task.request.database_id.as_u64();
        let has_vectors = self.collection_has_vectors(database_id, tid, target_collection);

        // BALANCED over the whole resolved set, BEFORE the first row is
        // written: each row below commits in its own transaction, so a check
        // after the loop could only report a violation the rows ahead of it had
        // already made durable. Both images are already carried on every
        // resolved row, so nothing is re-read.
        let balanced_entries = {
            let images: Vec<(&[u8], &[u8])> = rows
                .iter()
                .map(|row| (row.old_body.as_slice(), row.body.as_slice()))
                .collect();
            self.balanced_entries_for_stored_updates(database_id, tid, target_collection, &images)
        };
        match balanced_entries {
            Ok(entries) => {
                if let Err(e) =
                    self.settle_balanced_entries(database_id, tid, target_collection, entries)
                {
                    return self.response_error(task, e);
                }
            }
            Err(e) => return self.response_error(task, e),
        }

        let outcome = match self.write_resolved_update_from_join_rows(
            task,
            WriteResolvedRowsCtx {
                tid,
                target_collection,
                resolved_sum_targets,
                has_vectors,
                is_strict: strict_schema.is_some(),
                want_returning: returning.is_some(),
            },
            rows,
        ) {
            Ok(o) => o,
            Err(resp) => return resp,
        };

        let mut response = if let Some(spec) = returning {
            match super::returning_rows::build_rows_payload(
                spec,
                rls_filters,
                &outcome.returned_docs,
            ) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            let result = serde_json::json!({ "affected": outcome.affected });
            match encode_json_as_msgpack(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            }
        };
        if !outcome.write_set.is_empty() {
            response.write_set = outcome.write_set;
        }
        response
    }

    /// Encode the RESOLVE pass payload: a msgpack `Vec<(doc_id,
    /// Option<surrogate_u32>, post_image_body, pre_image_body)>` the
    /// statement-time expander decodes and rewrites into concrete `PointPut` ops
    /// (see
    /// `control::update_from_join_orchestrator::resolve_and_emit_update_from_join_ops`).
    ///
    /// The PRE-image travels alongside the post-image because the Control Plane
    /// resolves this statement's materialized-sum targets from it: a delta is
    /// the DIFFERENCE between the two images, and an assignment that rewrites
    /// the join column moves value between TWO targets, neither of which the
    /// post-image alone identifies.
    fn encode_resolved_update_rows(
        &self,
        task: &ExecutionTask,
        rows: Vec<ResolvedUpdateRow>,
    ) -> Response {
        let wire: Vec<crate::query::ResolvedUpdateRowWire> = rows
            .into_iter()
            .map(|r| {
                (
                    r.doc_id,
                    r.surrogate.map(|s| s.as_u32()),
                    r.body,
                    r.old_body,
                )
            })
            .collect();
        match zerompk::to_msgpack_vec(&wire) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("update-from-join resolve encode: {e}"),
                },
            ),
        }
    }
}
