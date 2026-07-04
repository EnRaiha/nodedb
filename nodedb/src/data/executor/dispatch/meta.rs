// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for MetaOp variants (WAL, snapshots, retention, continuous aggregates).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::MetaOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_meta(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &MetaOp,
    ) -> Response {
        match op {
            MetaOp::WalAppend { payload } => self.execute_wal_append(task, payload),

            MetaOp::Cancel { target_request_id } => self.execute_cancel(task, *target_request_id),

            MetaOp::TransactionBatch { plans } => self.execute_transaction_batch(task, tid, plans),

            MetaOp::CreateSnapshot => self.execute_create_snapshot(task),
            MetaOp::Compact => self.execute_compact(task),
            MetaOp::Checkpoint => self.execute_checkpoint(task),

            MetaOp::RegisterContinuousAggregate { def } => {
                self.continuous_agg_mgr.register(def.clone());
                tracing::info!(
                    name = def.name,
                    source = def.source,
                    interval = def.bucket_interval,
                    "continuous aggregate registered"
                );
                self.response_ok(task)
            }

            MetaOp::UnregisterContinuousAggregate { name } => {
                self.continuous_agg_mgr
                    .unregister(task.request.database_id.as_u64(), name);
                tracing::info!(name, "continuous aggregate unregistered");
                self.response_ok(task)
            }

            MetaOp::ListContinuousAggregates => {
                let infos = self.continuous_agg_mgr.list_aggregates();
                match response_codec::encode_serde(&infos) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            MetaOp::CreateTenantSnapshot { tenant_id } => {
                self.execute_create_tenant_snapshot(task, *tenant_id)
            }

            MetaOp::RestoreTenantSnapshot {
                tenant_id,
                snapshot,
                replace_mode,
                clear_vshards,
                collections_to_clear,
            } => self.execute_restore_tenant_snapshot(
                task,
                *tenant_id,
                snapshot,
                *replace_mode,
                clear_vshards,
                collections_to_clear,
            ),

            MetaOp::ConvertCollection {
                collection,
                target_type,
                schema_json,
            } => self.execute_convert_collection(task, tid, collection, target_type, schema_json),

            MetaOp::PurgeTenant { tenant_id } => self.execute_purge_tenant(task, *tenant_id),

            MetaOp::UnregisterCollection {
                tenant_id,
                name,
                purge_lsn,
            } => self.execute_unregister_collection(task, *tenant_id, name, *purge_lsn),

            MetaOp::UnregisterMaterializedView { tenant_id, name } => {
                self.execute_unregister_materialized_view(task, *tenant_id, name)
            }

            MetaOp::QueryCollectionSize { tenant_id, name } => {
                self.execute_query_collection_size(task, *tenant_id, name)
            }

            // Retention / purge / continuous-agg / last-value bodies live in
            // `dispatch/meta_retention/`; the arms below are one-line delegations
            // so the Meta match stays exhaustive.
            MetaOp::EnforceTimeseriesRetention {
                collection,
                max_age_ms,
            } => self.meta_enforce_timeseries_retention(task, collection, *max_age_ms),
            MetaOp::ApplyContinuousAggRetention => self.meta_apply_continuous_agg_retention(task),
            MetaOp::QueryAggregateWatermark { aggregate_name } => {
                self.meta_query_aggregate_watermark(task, aggregate_name)
            }
            MetaOp::QueryLastValues { collection } => self.meta_query_last_values(task, collection),
            MetaOp::QueryLastValue {
                collection,
                series_id,
            } => self.meta_query_last_value(task, collection, *series_id),

            MetaOp::AlterArray {
                audit_retain_ms, ..
            } => {
                // All catalog + registry mutations are performed on the Control
                // Plane before this op is dispatched. The Data Plane simply echoes
                // an 8-byte LE u64 acknowledgement (the new audit_retain_ms, or 0
                // when set to NULL).
                let ack: u64 = (*audit_retain_ms)
                    .and_then(|inner| inner)
                    .map(|ms| ms as u64)
                    .unwrap_or(0);
                self.response_with_payload(task, ack.to_le_bytes().to_vec())
            }

            op @ (MetaOp::TemporalPurgeEdgeStore { .. }
            | MetaOp::TemporalPurgeDocumentStrict { .. }
            | MetaOp::TemporalPurgeColumnar { .. }
            | MetaOp::TemporalPurgeCrdt { .. }
            | MetaOp::TemporalPurgeArray { .. }) => self.dispatch_temporal_purge(task, op),

            MetaOp::CalvinExecuteStatic {
                epoch,
                position,
                tenant_id,
                plans,
                epoch_system_ms,
                is_group_leader,
            } => self.execute_calvin_execute_static(
                task,
                CalvinExecCtx {
                    epoch: *epoch,
                    position: *position,
                    epoch_system_ms: *epoch_system_ms,
                    is_group_leader: *is_group_leader,
                },
                tenant_id,
                plans,
            ),

            MetaOp::CalvinExecutePassive {
                epoch,
                position,
                tenant_id,
                keys_to_read,
            } => self.execute_calvin_execute_passive(
                task,
                *epoch,
                *position,
                tenant_id,
                keys_to_read,
            ),

            MetaOp::CalvinExecuteActive {
                epoch,
                position,
                tenant_id,
                plans,
                injected_reads,
                epoch_system_ms,
                is_group_leader,
            } => self.execute_calvin_execute_active(
                task,
                CalvinExecCtx {
                    epoch: *epoch,
                    position: *position,
                    epoch_system_ms: *epoch_system_ms,
                    is_group_leader: *is_group_leader,
                },
                tenant_id,
                plans,
                injected_reads,
            ),

            MetaOp::RebuildIndex {
                collection,
                index_name,
                concurrent,
            } => self.execute_rebuild_index(
                task,
                tid,
                collection,
                index_name.as_deref(),
                *concurrent,
            ),

            MetaOp::PutSynonymGroup {
                tenant_id,
                record_json,
            } => self.execute_put_synonym_group(task, *tenant_id, record_json),

            MetaOp::DeleteSynonymGroup { tenant_id, name } => {
                self.execute_delete_synonym_group(task, *tenant_id, name)
            }

            MetaOp::RenameCollection {
                tenant_id,
                old_database_id,
                new_database_id,
                old_collection,
                new_collection,
            } => self.execute_rename_collection(
                task,
                *tenant_id,
                *old_database_id,
                *new_database_id,
                old_collection,
                new_collection,
            ),

            // Pure scaffolding removal. Nothing populates the overlay yet,
            // so this is a no-op `HashMap::remove` on an absent key today;
            // wiring the call site (session commit/rollback) lands
            // separately.
            MetaOp::DropTxnOverlay { txn_id } => {
                self.txn_overlays.remove(txn_id);
                self.response_ok(task)
            }
        }
    }
}
