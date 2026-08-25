// SPDX-License-Identifier: BUSL-1.1

//! Columnar engine sub-plan dispatch within a transaction batch.
//!
//! Split out of `sub_plan.rs` to keep that file under the size limit; this
//! is still the columnar arm of the same per-sub-plan dispatcher.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ColumnarOp;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Columnar engine: insert / predicate update / predicate delete are
    /// undo-tracked; everything else passes through the standard dispatch
    /// path.
    ///
    /// Predicate update/delete are staged at statement time; this is the
    /// durable COMMIT replay. Undo is captured here so a sibling sub-plan
    /// failing later in the same COMMIT batch reverses this mutation —
    /// without it the columnar change would survive an atomic-rollback
    /// (partial commit).
    pub(super) fn exec_tx_columnar(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &ColumnarOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                provenance: _,
                wal_lsn: _,
                rls_write_check,
                // A row-returning write is refused before it can be staged into
                // a transaction, so neither the projection nor the read gate
                // that bounds it can be set on a plan reaching this path.
                returning: _,
                rls_filters: _,
            } => self.execute_tx_columnar_insert(
                dummy_task,
                super::sub_plan_kv::TxColumnarInsertParams {
                    collection,
                    payload,
                    format,
                    intent: *intent,
                    on_conflict_updates,
                    surrogates,
                    schema_bytes,
                    rls_write_check,
                },
                undo_log,
            ),

            ColumnarOp::Update {
                collection,
                filters,
                updates,
                rls_write_check,
            } => self.exec_tx_columnar_update(
                dummy_task,
                collection,
                filters,
                updates,
                rls_write_check,
                undo_log,
            ),

            ColumnarOp::Delete {
                collection,
                filters,
                rls_write_check,
            } => self.exec_tx_columnar_delete(
                dummy_task,
                collection,
                filters,
                rls_write_check,
                undo_log,
            ),

            // Resolved-row-set forms: the Control Plane already resolved the
            // predicate and decided the write policy against the exact rows,
            // so — unlike `Update`/`Delete` above — there is no filter scan
            // here either; the apply handler itself does the drift check
            // against the current PK index before mutating anything.
            ColumnarOp::ResolvedUpdate {
                collection,
                rows,
                rls_write_check,
            } => self.exec_tx_columnar_resolved_update(
                dummy_task,
                collection,
                rows,
                rls_write_check,
                undo_log,
            ),

            ColumnarOp::ResolvedDelete {
                collection,
                pks,
                rls_write_check,
            } => self.exec_tx_columnar_resolved_delete(
                dummy_task,
                collection,
                pks,
                rls_write_check,
                undo_log,
            ),

            ColumnarOp::Scan { .. }
            | ColumnarOp::MaterializeScan { .. }
            | ColumnarOp::ResolveDml { .. } => self.exec_tx_passthrough(tid, plan),
        }
    }
}
