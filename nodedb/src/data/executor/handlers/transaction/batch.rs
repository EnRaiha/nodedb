// SPDX-License-Identifier: BUSL-1.1

//! Transaction batch execution handler.
//!
//! Executes a `PhysicalPlan::TransactionBatch` atomically: all sub-plans
//! succeed or all are rolled back. Write operations (PointPut, PointDelete,
//! VectorInsert, EdgePut, EdgeDelete) are tracked for rollback on failure.
//! CRDT deltas are accumulated in a scratch buffer and only applied on success.

use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, error, warn};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a transaction batch atomically.
    ///
    /// All sub-plans are executed in order. If any sub-plan fails, all
    /// previous writes are rolled back. CRDT deltas are buffered and only
    /// applied to LoroDoc on full success.
    ///
    /// The Control Plane has already written a single `RecordType::Transaction`
    /// WAL record covering all operations before dispatching this batch.
    #[allow(clippy::too_many_lines)]
    pub(in crate::data::executor) fn execute_transaction_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) -> Response {
        debug!(
            core = self.core_id,
            plan_count = plans.len(),
            "transaction batch begin"
        );

        let mut undo_log: Vec<UndoEntry> = Vec::with_capacity(plans.len());
        let mut crdt_deltas: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut last_response = self.response_ok(task);

        for (i, plan) in plans.iter().enumerate() {
            // Wrap the sub-apply + post-apply fail-point in catch_unwind so a
            // panic (real or test-injected) routes through the typed-rollback
            // arm below instead of unwinding past `undo_log`. Without this,
            // a panic between sub-applies would drop the undo log without
            // running rollback and leave the shard half-committed.
            let user_roles = &task.request.user_roles;
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let r = self.execute_tx_sub_plan(
                    tid,
                    plan,
                    &mut undo_log,
                    &mut crdt_deltas,
                    user_roles,
                );
                crate::fail_point!("transaction_batch::between_subapply");
                r
            }));
            let result = match outcome {
                Ok(r) => r,
                Err(payload) => {
                    let detail = panic_payload_to_string(payload.as_ref());
                    error!(
                        core = self.core_id,
                        plan_index = i,
                        panic = %detail,
                        "transaction sub-apply panicked; routing through rollback path"
                    );
                    Err(ErrorCode::Internal {
                        detail: format!("panic in sub-apply at index {i}: {detail}"),
                    })
                }
            };

            match result {
                Ok(resp) => {
                    last_response = resp;
                }
                Err(error_code) => {
                    warn!(
                        core = self.core_id,
                        plan_index = i,
                        "transaction sub-plan failed, rolling back {} operations",
                        undo_log.len()
                    );

                    // Roll back all previous writes in reverse order.
                    // If rollback itself fails, the shard state is unknown —
                    // return RollbackFailed (never warn-and-continue).
                    let rollback_error_code = match self.rollback_undo_log(
                        crate::types::DatabaseId::DEFAULT.as_u64(),
                        tid,
                        undo_log,
                    ) {
                        Ok(()) => error_code,
                        Err((entry_index, detail)) => {
                            error!(
                                core = self.core_id,
                                plan_index = i,
                                entry_index,
                                detail = %detail,
                                "transaction rollback failed; shard state unknown — \
                                 restart required for WAL replay"
                            );
                            crate::bridge::envelope::ErrorCode::RollbackFailed {
                                entry_index,
                                detail,
                            }
                        }
                    };

                    // Discard CRDT scratch buffer (never applied).
                    drop(crdt_deltas);

                    return Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(rollback_error_code),
                    };
                }
            }
        }

        // Pre-commit: BALANCED constraint check across all inserts in this transaction.
        if let Err(error_code) = self.check_balanced_constraints(tid, &undo_log) {
            warn!(
                core = self.core_id,
                "BALANCED constraint violated, rolling back {} operations",
                undo_log.len()
            );
            let rollback_error_code = match self.rollback_undo_log(
                crate::types::DatabaseId::DEFAULT.as_u64(),
                tid,
                undo_log,
            ) {
                Ok(()) => error_code,
                Err((entry_index, detail)) => {
                    error!(
                        core = self.core_id,
                        entry_index,
                        detail = %detail,
                        "transaction rollback failed (BALANCED constraint path); \
                         shard state unknown — restart required for WAL replay"
                    );
                    crate::bridge::envelope::ErrorCode::RollbackFailed {
                        entry_index,
                        detail,
                    }
                }
            };
            return Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: crate::bridge::envelope::Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(rollback_error_code),
            };
        }

        // All sub-plans succeeded. Apply buffered CRDT deltas.
        // Failure here means the CRDT state is inconsistent with the already-committed
        // forward writes — return RollbackFailed so the client knows the shard needs
        // a restart to restore consistency via WAL replay. Never warn-and-continue.
        for (crdt_idx, (delta, peer_id)) in crdt_deltas.into_iter().enumerate() {
            // Second crash-injection point — between forward-write commit and
            // CRDT apply. WAL replay must roll the CRDT side forward (or roll
            // forward writes back) to restore consistency.
            crate::fail_point!("transaction_batch::between_crdt_delta");

            let tenant_id = crate::types::TenantId::new(tid);
            match self.get_crdt_engine(tenant_id) {
                Ok(engine) => {
                    let _ = peer_id; // peer_id used for dedup in future
                    if let Err(e) = engine.apply_committed_delta(&delta) {
                        error!(
                            core = self.core_id,
                            crdt_delta_index = crdt_idx,
                            error = %e,
                            "CRDT delta apply failed after forward writes committed; \
                             shard state unknown — restart required for WAL replay"
                        );
                        return Response {
                            request_id: task.request_id(),
                            status: Status::Error,
                            attempt: 1,
                            partial: false,
                            payload: crate::bridge::envelope::Payload::empty(),
                            watermark_lsn: self.watermark,
                            error_code: Some(crate::bridge::envelope::ErrorCode::RollbackFailed {
                                entry_index: crdt_idx,
                                detail: format!("CRDT delta apply failed: {e}"),
                            }),
                        };
                    }
                }
                Err(e) => {
                    error!(
                        core = self.core_id,
                        crdt_delta_index = crdt_idx,
                        error = %e,
                        "CRDT engine not found after forward writes committed; \
                         shard state unknown — restart required for WAL replay"
                    );
                    return Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(crate::bridge::envelope::ErrorCode::RollbackFailed {
                            entry_index: crdt_idx,
                            detail: format!("CRDT engine not available: {e}"),
                        }),
                    };
                }
            }
        }

        debug!(
            core = self.core_id,
            committed = plans.len(),
            "transaction batch committed"
        );

        // Emit deferred trigger events for all writes in the committed transaction.
        use crate::data::executor::core_loop::deferred::DeferredWrite;
        let deferred_writes: Vec<DeferredWrite> = undo_log
            .into_iter()
            .filter_map(|entry| match entry {
                UndoEntry::PutDocument {
                    collection,
                    document_id,
                    old_value,
                    surrogate: _,
                } => Some(DeferredWrite {
                    collection,
                    op: if old_value.is_some() {
                        crate::event::WriteOp::Update
                    } else {
                        crate::event::WriteOp::Insert
                    },
                    row_id: document_id,
                    new_value: None,
                    old_value,
                }),
                UndoEntry::DeleteDocument {
                    collection,
                    document_id,
                    old_value,
                } => Some(DeferredWrite {
                    collection,
                    op: crate::event::WriteOp::Delete,
                    row_id: document_id,
                    new_value: None,
                    old_value: Some(old_value),
                }),
                _ => None, // Vector and edge undo entries don't trigger deferred triggers.
            })
            .collect();

        if !deferred_writes.is_empty() {
            self.emit_deferred_events(
                deferred_writes,
                task.request.tenant_id,
                task.request.vshard_id,
            );
        }

        // Return the last sub-plan payload, but keyed to the outer transaction request.
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: last_response.payload,
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}

/// Best-effort conversion of a panic payload to a human-readable string.
/// Tries the two common payload types (`&'static str` and `String`); falls
/// back to `"<non-string panic payload>"` for anything else.
fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
