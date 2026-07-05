// SPDX-License-Identifier: BUSL-1.1

//! `StageWrite` dispatch: route a point-write plan to the matching staging
//! path, compute its real affected-row count, and record it in the overlay.

use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, GraphOp, SpatialOp, UpdateValue};

use super::constraint::OverlayPk;
use super::context::StageCtx;
use super::{
    StageBulkDeleteParams, StageBulkUpdateParams, StageColumnarInsertParams,
    StageInsertSelectParams, StageSpatialInsertParams,
};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::generated;
use crate::data::executor::handlers::transaction::overlay::{MAX_TXN_OVERLAY_BYTES, Staged};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::TenantId;

impl CoreLoop {
    /// Execute a `MetaOp::StageWrite` for an in-transaction point write.
    ///
    /// Only point-write `DocumentOp`s are valid here (the Control Plane only
    /// builds `StageWrite` for those); anything else is an internal error.
    pub(in crate::data::executor) fn execute_stage_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
    ) -> Response {
        let Some(txn_id) = task.request.txn_id else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "StageWrite dispatched without a txn_id".into(),
                },
            );
        };

        let doc_op = match plan {
            PhysicalPlan::Document(op) => op,
            PhysicalPlan::Kv(op) => return self.execute_stage_kv(task, tid, txn_id, op),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                surrogates,
                schema_bytes,
                ..
            }) => {
                return self.stage_columnar_insert(StageColumnarInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    payload,
                    surrogates,
                    schema_bytes,
                });
            }
            PhysicalPlan::Columnar(
                ColumnarOp::Scan { .. }
                | ColumnarOp::Update { .. }
                | ColumnarOp::Delete { .. }
                | ColumnarOp::MaterializeScan { .. },
            ) => return self.stage_not_point_write(task),
            PhysicalPlan::Spatial(SpatialOp::Insert {
                collection,
                field,
                surrogate,
                geometry,
                provenance: _,
            }) => {
                return self.stage_spatial_insert(StageSpatialInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    field,
                    surrogate: *surrogate,
                    geometry,
                });
            }
            PhysicalPlan::Spatial(SpatialOp::Delete {
                collection,
                surrogate,
                field: _,
                provenance: _,
            }) => return self.stage_spatial_delete(task, tid, txn_id, collection, *surrogate),
            PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => {
                return self.stage_not_point_write(task);
            }
            PhysicalPlan::Graph(
                op @ (GraphOp::EdgePut { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::SetNodeLabels { .. }
                | GraphOp::RemoveNodeLabels { .. }),
            ) => return self.execute_stage_graph(task, tid, txn_id, op),
            PhysicalPlan::Graph(
                GraphOp::Hop { .. }
                | GraphOp::Neighbors { .. }
                | GraphOp::NeighborsMulti { .. }
                | GraphOp::Path { .. }
                | GraphOp::Subgraph { .. }
                | GraphOp::RagFusion { .. }
                | GraphOp::Algo { .. }
                | GraphOp::Match { .. }
                | GraphOp::MatchContinuation { .. }
                | GraphOp::MatchVarLenResume { .. }
                | GraphOp::BspSuperstep(_)
                | GraphOp::WccSuperstep(_)
                | GraphOp::TemporalNeighbors { .. }
                | GraphOp::TemporalAlgorithm { .. }
                | GraphOp::Stats { .. },
            ) => return self.stage_not_point_write(task),
            PhysicalPlan::Vector(_) => return self.stage_not_point_write(task),
            PhysicalPlan::Text(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_) => return self.stage_not_point_write(task),
        };

        match doc_op {
            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_insert(&ctx, value, *if_absent)
            }
            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_put(&ctx, value)
            }
            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_delete(&ctx)
            }
            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                updates,
                ..
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_update(&ctx, updates)
            }
            // Predicate UPDATE staged at statement time — same treatment as a
            // point update, resolved against the BASE ∪ OVERLAY matching set.
            // The Control Plane only builds `StageWrite` for the `returning:
            // None` variant (see `is_point_write`); a `RETURNING` bulk update
            // stays on the pre-existing buffer + "OK" deferral.
            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning: None,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
            } => self.stage_bulk_update(StageBulkUpdateParams {
                task,
                tid,
                txn_id,
                collection,
                filter_bytes: filters,
                updates,
            }),
            DocumentOp::BulkUpdate {
                returning: Some(_), ..
            } => self.stage_not_point_write(task),

            // Predicate DELETE staged at statement time — same treatment as a
            // point delete, resolved against the BASE ∪ OVERLAY matching set.
            DocumentOp::BulkDelete {
                collection,
                filters,
                returning: None,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
            } => self.stage_bulk_delete(StageBulkDeleteParams {
                task,
                tid,
                txn_id,
                collection,
                filter_bytes: filters,
            }),
            DocumentOp::BulkDelete {
                returning: Some(_), ..
            } => self.stage_not_point_write(task),

            // `INSERT ... SELECT ... WHERE <predicate>` staged at statement
            // time — resolve the source's BASE ∪ OVERLAY matching set and
            // copy each matched row into the target overlay under its own
            // surrogate/doc_id. `InsertSelect` has no `RETURNING` variant, so
            // it is always stageable (see `is_point_write`).
            DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_filters,
                source_limit,
            } => self.stage_insert_select(StageInsertSelectParams {
                task,
                tid,
                txn_id,
                target_collection,
                source_collection,
                filter_bytes: source_filters,
                source_limit: *source_limit,
            }),

            // `UPSERT INTO` staged at statement time -- resolve the current
            // body under BASE ∪ OVERLAY and either insert or merge/apply
            // `ON CONFLICT DO UPDATE SET`, mirroring the autocommit
            // `execute_upsert` handler exactly. `Upsert` has no `RETURNING`
            // variant, so it is always stageable (see `is_point_write`).
            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_document_upsert(&ctx, value, on_conflict_updates)
            }

            DocumentOp::PointGet { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::MaterializeScan { .. } => self.stage_not_point_write(task),
        }
    }

    pub(super) fn stage_not_point_write(&self, task: &ExecutionTask) -> Response {
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: "StageWrite is only valid for point-write document operations".into(),
            },
        )
    }

    fn stage_point_insert(
        &mut self,
        ctx: &StageCtx<'_>,
        value: &[u8],
        if_absent: bool,
    ) -> Response {
        let row_key = surrogate_to_doc_id(ctx.surrogate);
        let bitemporal = self.is_bitemporal(ctx.tid, ctx.collection);

        let overlay_pk = self.stage_overlay_pk(ctx);
        let present = match self.stage_pk_present(
            ctx.database_id,
            ctx.tid,
            ctx.collection,
            row_key.as_str(),
            bitemporal,
            overlay_pk,
        ) {
            Ok(p) => p,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if present {
            if if_absent {
                return self.stage_count_response(ctx.task, 0);
            }
            return self.response_error(
                ctx.task,
                crate::Error::RejectedConstraint {
                    collection: ctx.collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "duplicate key value '{}' violates primary-key uniqueness on '{}'",
                        ctx.document_id, ctx.collection
                    ),
                },
            );
        }

        if let Err(e) = self.stage_check_unique(ctx, value) {
            return self.response_error(ctx.task, e);
        }
        self.stage_encode_and_commit(ctx, value)
    }

    fn stage_point_put(&mut self, ctx: &StageCtx<'_>, value: &[u8]) -> Response {
        // Upsert semantics: no primary-key existence check (overwrite allowed);
        // UNIQUE indexes still apply against a DIFFERENT row.
        if let Err(e) = self.stage_check_unique(ctx, value) {
            return self.response_error(ctx.task, e);
        }
        self.stage_encode_and_commit(ctx, value)
    }

    fn stage_point_delete(&mut self, ctx: &StageCtx<'_>) -> Response {
        self.txn_overlays
            .entry(ctx.txn_id)
            .or_default()
            .insert_tombstone(ctx.coll_key.clone(), ctx.surrogate.0, &ctx.document_id);
        self.stage_count_response(ctx.task, 1)
    }

    fn stage_point_update(
        &mut self,
        ctx: &StageCtx<'_>,
        updates: &[(String, UpdateValue)],
    ) -> Response {
        let config_key = (TenantId::new(ctx.tid), ctx.collection.to_string());
        let row_key = surrogate_to_doc_id(ctx.surrogate);

        // Reject direct updates to generated columns (matches the durable path).
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) =
                generated::check_generated_readonly(updates, &config.enforcement.generated_columns)
        {
            return self.response_error(ctx.task, e);
        }

        // Current body: overlay wins over base; an in-transaction tombstone
        // means the row is gone (0 rows updated).
        let overlay_cur = self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
            .cloned();
        let current_bytes = match overlay_cur {
            Some(Staged::Put(body)) => body,
            Some(Staged::Tombstone) => return self.stage_count_response(ctx.task, 0),
            None => {
                let bitemporal = self.is_bitemporal(ctx.tid, ctx.collection);
                let read = if bitemporal {
                    self.sparse.versioned_get_current(
                        ctx.database_id,
                        ctx.tid,
                        ctx.collection,
                        row_key.as_str(),
                    )
                } else {
                    self.sparse
                        .get(ctx.database_id, ctx.tid, ctx.collection, row_key.as_str())
                };
                match read {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => return self.stage_count_response(ctx.task, 0),
                    Err(e) => return self.response_error(ctx.task, e),
                }
            }
        };

        let body = match self.stage_apply_update(ctx.tid, ctx.collection, &current_bytes, updates) {
            Ok(b) => b,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_put_capped(ctx, body) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    // ── Shared helpers ──────────────────────────────────────────────────────

    pub(super) fn stage_overlay_pk(&self, ctx: &StageCtx<'_>) -> OverlayPk {
        match self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
        {
            Some(Staged::Put(_)) => OverlayPk::Present,
            Some(Staged::Tombstone) => OverlayPk::Absent,
            None => OverlayPk::Unstaged,
        }
    }

    /// Run BASE ∪ OVERLAY UNIQUE checks for an incoming put/insert body.
    fn stage_check_unique(&self, ctx: &StageCtx<'_>, value: &[u8]) -> crate::Result<()> {
        let config_key = (TenantId::new(ctx.tid), ctx.collection.to_string());
        let Some(config) = self.doc_configs.get(&config_key).cloned() else {
            return Ok(());
        };
        if config.index_paths.iter().all(|p| !p.unique) {
            return Ok(());
        }
        let Some(incoming_doc) = doc_format::decode_document(value) else {
            return Ok(());
        };
        let staged_others: Vec<Vec<u8>> = self
            .txn_overlays
            .get(&ctx.txn_id)
            .map(|o| {
                o.iter_for_collection(&ctx.coll_key)
                    .filter_map(|(s, st)| match st {
                        Staged::Put(body) if s != ctx.surrogate.0 => Some(body.clone()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.stage_unique_check(ctx, &config, &incoming_doc, &staged_others)
    }

    fn stage_encode_and_commit(&mut self, ctx: &StageCtx<'_>, value: &[u8]) -> Response {
        let body = match self.stage_encode_put_body(ctx.tid, ctx.collection, ctx.surrogate, value) {
            Ok(b) => b,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_put_capped(ctx, body) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    /// Stage a put after enforcing the per-transaction overlay memory cap.
    pub(super) fn stage_put_capped(
        &mut self,
        ctx: &StageCtx<'_>,
        body: Vec<u8>,
    ) -> crate::Result<()> {
        let current = self
            .txn_overlays
            .get(&ctx.txn_id)
            .map(|o| o.memory_size_estimate())
            .unwrap_or(0);
        if current.saturating_add(body.len()) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        self.txn_overlays.entry(ctx.txn_id).or_default().insert_put(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
            body,
        );
        Ok(())
    }

    pub(super) fn stage_count_response(&self, task: &ExecutionTask, affected: usize) -> Response {
        match response_codec::encode_count("affected", affected) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}
