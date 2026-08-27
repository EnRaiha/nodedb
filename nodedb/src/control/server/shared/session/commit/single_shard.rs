// SPDX-License-Identifier: BUSL-1.1

//! Single-shard COMMIT: one `TransactionRedo` WAL record, then one atomic
//! `TransactionBatch` dispatch stamped with that record's LSN.

use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::gateway::RouteDecision;
use crate::control::state::SharedState;

use super::super::outcome::{AbortReason, TxnDataPlane};

/// Single-shard commit: resolve the transaction's staged post-images into one
/// replayable `TransactionRedo` WAL record, then dispatch the buffered plans as
/// one atomic `TransactionBatch` stamped with that record's LSN. The redo
/// record restores restart durability for in-transaction writes into in-memory
/// secondary indexes (vector HNSW, FTS) that the base storage engine cannot
/// rebuild on its own. Returns `Some(reason)` on failure.
pub(super) async fn dispatch_single_shard(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
) -> Option<AbortReason> {
    let plans: Vec<PhysicalPlan> = buffered.iter().map(|t| t.plan.clone()).collect();
    let database_id = buffered
        .first()
        .map_or(crate::types::DatabaseId::DEFAULT, |task| task.database_id);
    if buffered.iter().any(|task| task.database_id != database_id) {
        return Some(AbortReason::Dispatch(crate::Error::BadRequest {
            detail: "transaction spans multiple databases".to_owned(),
        }));
    }

    // txn_id is present for any staged commit (buffer_write stamps it).
    let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) else {
        return Some(AbortReason::Dispatch(crate::Error::Internal {
            detail: "single-shard commit: buffered task carries no txn_id".into(),
        }));
    };

    // 1. Resolve the transaction's staged post-images into ONE replayable
    //    RedoRecord. Read-only: reads `txn_overlays[txn_id]` on the owning
    //    core, writes nothing.
    let resolve_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: PhysicalPlan::Meta(MetaOp::ResolveTxn {
            txn_id,
            plans: plans.clone(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let resolve_resp = match dp.dispatch_no_wal(resolve_task, None).await {
        Ok(r) if r.status == Status::Ok => r,
        Ok(r) => {
            return Some(AbortReason::BatchRejected {
                code: r.error_code.as_deref().cloned(),
            });
        }
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    let redo = match crate::wal::RedoRecord::from_bytes(resolve_resp.payload.as_bytes()) {
        Ok(r) => r,
        Err(e) => {
            return Some(AbortReason::Dispatch(crate::Error::Internal {
                detail: format!("single-shard commit: resolve redo decode failed: {e}"),
            }));
        }
    };

    // Re-verify local vShard ownership immediately before the durable WAL
    // append. `run_commit` resolved this vShard as `Local`, but a leadership
    // handoff can land during the `ResolveTxn` await above. Without this
    // re-check the transaction redo would be appended to a WAL this node no
    // longer owns, and the batch dispatch below (which re-resolves leadership)
    // would then reject the now-non-local commit — leaving an orphaned durable
    // redo record behind while the client is told the commit aborted. Aborting
    // here, BEFORE any durable write, keeps the failure side-effect-free and
    // retryable: the client's retry re-enters `run_commit`, sees the vShard is
    // non-local, and routes the commit through Calvin's replicated barrier.
    if !matches!(
        crate::control::server::graph_dispatch::cluster_resolve::resolve_for_vshard(
            state,
            vshard_id.as_u32(),
        ),
        RouteDecision::Local
    ) {
        return Some(AbortReason::Serialization);
    }

    // 2. Write-ahead the transaction as ONE replayable `TransactionRedo` record
    //    (each sub-op keeps its real engine `record_type`). `None` when the txn
    //    has no durable writes (all reads / CRDT / text). Its LSN stamps the
    //    batch install so the Data Plane records the committed write version for
    //    every key in the batch.
    let wal_lsn = if redo.ops.is_empty() {
        None
    } else {
        match state
            .wal
            .append_transaction_redo(tenant_id, vshard_id, database_id, &redo)
        {
            Ok(lsn) => Some(lsn),
            Err(e) => {
                return Some(AbortReason::Dispatch(crate::Error::Internal {
                    detail: format!("single-shard commit: transaction redo WAL append failed: {e}"),
                }));
            }
        }
    };
    let batch_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans,
            // Reuse the resolve-time bitemporal stamps recorded in this
            // transaction's staging overlay so a `bitemporal=true` document put
            // installs on the same version key the redo (WAL-appended just
            // above) carries — otherwise a normal restart writes a second
            // version of the row.
            txn_id: Some(txn_id),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    classify_batch_dispatch(dp.dispatch_no_wal(batch_task, wal_lsn).await)
}

/// Convert a transaction-batch dispatch result into a commit abort reason, if
/// any. `dispatch_no_wal` returns `Ok(Response { status: Error, .. })` for a
/// failed batch rather than a Rust `Err` — the status must be checked
/// explicitly or a failed sub-plan reports as COMMIT success.
fn classify_batch_dispatch(result: crate::Result<Response>) -> Option<AbortReason> {
    match result {
        Err(e) => {
            tracing::warn!(error = %e, "transaction batch dispatch failed");
            Some(AbortReason::Dispatch(e))
        }
        Ok(resp) if resp.status != Status::Ok => Some(AbortReason::BatchRejected {
            code: resp.error_code.as_deref().cloned(),
        }),
        Ok(_) => None,
    }
}
