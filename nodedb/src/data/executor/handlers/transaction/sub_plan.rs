// SPDX-License-Identifier: BUSL-1.1

//! Per-sub-plan execution within a transaction batch.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, MetaOp, TimeseriesOp, VectorOp,
};

use super::sub_plan_doc::{TxPointDelete, TxPointPut};
use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a single sub-plan within a transaction, recording undo info.
    ///
    /// CRDT deltas are NOT applied immediately — they are buffered in
    /// `crdt_deltas` and only applied after all sub-plans succeed.
    ///
    /// The match is exhaustive: every `PhysicalPlan` variant is explicitly
    /// handled. Write-producing variants push an `UndoEntry`; read-only and
    /// DDL variants pass through without undo tracking.
    #[allow(clippy::too_many_lines)]
    pub(super) fn execute_tx_sub_plan(
        &mut self,
        tid: u64,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        // Temporary task used for sub-plan response construction.
        let dummy_task = ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: TenantId::new(tid),
            database_id: DatabaseId::DEFAULT,
            vshard_id: crate::types::VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: crate::types::RequestId::new(0),
            }),
            // no-determinism: sub-plan deadline is ephemeral, not written to WAL
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
            priority: crate::bridge::envelope::Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
        });

        match plan {
            // ── Document writes ──────────────────────────────────────────────
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            }) => self.tx_point_put(
                TxPointPut {
                    task: &dummy_task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    value,
                    user_roles,
                    insert_if_absent: None,
                },
                undo_log,
            ),

            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            }) => self.tx_point_put(
                TxPointPut {
                    task: &dummy_task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    value,
                    user_roles,
                    insert_if_absent: Some(*if_absent),
                },
                undo_log,
            ),

            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            }) => self.tx_point_delete(
                TxPointDelete {
                    task: &dummy_task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    user_roles,
                },
                undo_log,
            ),

            // ── Vector writes ────────────────────────────────────────────────
            PhysicalPlan::Vector(VectorOp::Insert {
                collection,
                vector,
                dim,
                field_name,
                surrogate,
                pk_bytes: _,
                provenance: _,
            }) => {
                let index_key = Self::vector_index_key(
                    dummy_task.request.database_id.as_u64(),
                    tid,
                    collection,
                    field_name,
                );
                let params = self
                    .vector_params
                    .get(&index_key)
                    .cloned()
                    .unwrap_or_default();
                let index = self
                    .vector_collections
                    .entry(index_key.clone())
                    .or_insert_with(|| {
                        crate::engine::vector::collection::VectorCollection::new(*dim, params)
                    });

                if vector.len() != index.dim() {
                    return Err(ErrorCode::Internal {
                        detail: format!(
                            "dimension mismatch: expected {}, got {}",
                            index.dim(),
                            vector.len()
                        ),
                    });
                }

                let vector_id = index.len() as u32;
                index.insert_with_surrogate(vector.clone(), *surrogate);
                undo_log.push(UndoEntry::InsertVector {
                    index_key,
                    vector_id,
                });
                Ok(self.response_ok(&dummy_task))
            }

            PhysicalPlan::Vector(VectorOp::Delete {
                collection,
                vector_id,
            }) => {
                let index_key = Self::vector_index_key(
                    dummy_task.request.database_id.as_u64(),
                    tid,
                    collection,
                    "",
                );
                if let Some(index) = self.vector_collections.get_mut(&index_key)
                    && index.delete(*vector_id)
                {
                    undo_log.push(UndoEntry::DeleteVector {
                        index_key,
                        vector_id: *vector_id,
                    });
                }
                Ok(self.response_ok(&dummy_task))
            }

            // ── Graph writes ─────────────────────────────────────────────────
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            }) => {
                let old_properties = self
                    .edge_store
                    .get_edge(
                        dummy_task.request.database_id.as_u64(),
                        nodedb_types::TenantId::new(tid),
                        collection,
                        src_id,
                        label,
                        dst_id,
                    )
                    .ok()
                    .flatten();

                let resp = self.execute_edge_put(
                    &dummy_task,
                    tid,
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    *src_surrogate,
                    *dst_surrogate,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge put failed".into(),
                    }));
                }

                undo_log.push(UndoEntry::PutEdge {
                    collection: collection.clone(),
                    src_id: src_id.clone(),
                    label: label.clone(),
                    dst_id: dst_id.clone(),
                    old_properties,
                });
                Ok(resp)
            }

            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                ..
            }) => {
                let old_properties = self
                    .edge_store
                    .get_edge(
                        dummy_task.request.database_id.as_u64(),
                        nodedb_types::TenantId::new(tid),
                        collection,
                        src_id,
                        label,
                        dst_id,
                    )
                    .ok()
                    .flatten();

                let resp =
                    self.execute_edge_delete(&dummy_task, tid, collection, src_id, label, dst_id);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge delete failed".into(),
                    }));
                }

                if let Some(props) = old_properties {
                    undo_log.push(UndoEntry::DeleteEdge {
                        collection: collection.clone(),
                        src_id: src_id.clone(),
                        label: label.clone(),
                        dst_id: dst_id.clone(),
                        old_properties: props,
                    });
                }
                Ok(resp)
            }

            // ── CRDT (buffered until commit) ──────────────────────────────────
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection,
                delta,
                peer_id,
                ..
            }) => {
                crdt_deltas.push((delta.clone(), *peer_id, collection.clone()));
                Ok(self.response_ok(&dummy_task))
            }

            // ── KV writes (tracked) and reads (passthrough) ──────────────────
            PhysicalPlan::Kv(kv_op) => self.execute_tx_kv(&dummy_task, tid, kv_op, undo_log),

            // ── Columnar insert (tracked) ────────────────────────────────────
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                provenance: _,
                wal_lsn: _,
            }) => self.execute_tx_columnar_insert(
                &dummy_task,
                collection,
                payload,
                format,
                *intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                undo_log,
            ),

            // ── Timeseries ingest (tracked) ──────────────────────────────────
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
                ..
            }) => self.execute_tx_timeseries_ingest(
                &dummy_task,
                TenantId::new(tid),
                collection,
                payload,
                format,
                *wal_lsn,
                undo_log,
            ),

            // ── Read-only / passthrough variants ─────────────────────────────
            // None of these mutate engine state, so no undo entry is needed.
            // They execute normally via the standard dispatch path.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_) => {
                let resp = self.execute(&ExecutionTask::new(crate::bridge::envelope::Request {
                    request_id: crate::types::RequestId::new(0),
                    tenant_id: TenantId::new(tid),
                    database_id: DatabaseId::DEFAULT,
                    vshard_id: crate::types::VShardId::new(0),
                    plan: plan.clone(),
                    // no-determinism: sub-plan deadline is ephemeral, not written to WAL
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
                    priority: crate::bridge::envelope::Priority::Normal,
                    trace_id: TraceId::ZERO,
                    consistency: crate::types::ReadConsistency::Strong,
                    idempotency_key: None,
                    event_source: crate::event::EventSource::User,
                    user_roles: Vec::new(),
                    user_id: None,
                    statement_digest: None,
                }));
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "sub-plan execution failed".into(),
                    }));
                }
                Ok(resp)
            }
        }
    }
}
