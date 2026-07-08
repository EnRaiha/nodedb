// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard arm of the neutral COMMIT orchestrator.
//!
//! Routes a transaction whose buffered writes span two or more vShards through
//! the Calvin sequencer (strict mode) or an independent per-vShard best-effort
//! fan-out, awaiting assignment + completion via the completion registry.
//! Split out of `commit.rs` to keep the orchestrator's top-level flow short.

use std::net::SocketAddr;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{DispatchOutcome, dispatch_calvin_or_fast};
use crate::control::state::SharedState;
use nodedb_cluster::calvin::{AttemptOutcome, TxnId as CalvinTxnId};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::commit::classify_batch_dispatch;
use super::outcome::{AbortReason, TxnDataPlane};
use super::store::SessionStore;

/// Dispatch a multi-shard transaction batch through Calvin (or best-effort
/// per-vShard). Returns `Some(reason)` on failure, `None` on success.
pub(super) async fn run_commit_calvin(
    sessions: &SessionStore,
    addr: &SocketAddr,
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
) -> Option<AbortReason> {
    let cross_shard_mode = sessions.cross_shard_txn_mode(addr);
    let tx_state = sessions.transaction_state(addr);

    let inbox = state.sequencer_inbox.get();
    let orchestrator = state.ollp_orchestrator.get();
    let registry = match state.calvin_completion_registry.get() {
        Some(r) => r,
        None => return Some(AbortReason::Dispatch(crate::Error::SequencerUnavailable)),
    };

    let dispatch = match dispatch_calvin_or_fast(
        buffered,
        cross_shard_mode,
        tx_state,
        inbox,
        orchestrator,
        tenant_id,
    )
    .await
    {
        Ok(d) => d,
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };

    match dispatch {
        DispatchOutcome::CalvinStatic { inbox_seq }
        | DispatchOutcome::CalvinDependent { inbox_seq } => {
            let timeout =
                std::time::Duration::from_secs(state.tuning.network.default_deadline_secs);
            let assignment_rx = registry.register_submission(inbox_seq);
            let (epoch, position, participants) =
                match tokio::time::timeout(timeout, assignment_rx).await {
                    Ok(Ok(assignment)) => assignment,
                    Ok(Err(_)) => return Some(AbortReason::CalvinCancelled),
                    Err(_) => return Some(AbortReason::CalvinTimeout),
                };

            let completion_rx =
                registry.register_completion(CalvinTxnId::new(epoch, position), participants);
            let outcome = match tokio::time::timeout(timeout, completion_rx).await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(_)) => return Some(AbortReason::CalvinCancelled),
                Err(_) => return Some(AbortReason::CalvinTimeout),
            };
            // The static (non-dependent) Calvin path never produces an OLLP
            // mismatch — `note_ollp_mismatch` only fires on the
            // dependent-predicate retry path — so this branch is unreachable at
            // runtime today. It is kept as a typed abort (never a panic) so any
            // future mismatch signal surfaces deterministically instead of
            // crashing.
            if outcome == AttemptOutcome::Mismatch {
                return Some(AbortReason::Dispatch(crate::Error::Internal {
                    detail: "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
                }));
            }
            None
        }
        DispatchOutcome::SingleShard | DispatchOutcome::BestEffortNonAtomic => {
            // BestEffortNonAtomic: dispatch each vShard's sub-batch
            // independently. Group buffered tasks by vShard and dispatch
            // per-vShard TransactionBatches.
            let mut by_vshard: std::collections::BTreeMap<u32, Vec<PhysicalPlan>> =
                std::collections::BTreeMap::new();
            for task in buffered {
                by_vshard
                    .entry(task.vshard_id.as_u32())
                    .or_default()
                    .push(task.plan.clone());
            }
            for (vshard_u32, plans) in by_vshard {
                let vshard_id = nodedb_types::id::VShardId::new(vshard_u32);
                let batch_task = PhysicalTask {
                    tenant_id,
                    vshard_id,
                    database_id: crate::types::DatabaseId::DEFAULT,
                    plan: PhysicalPlan::Meta(MetaOp::TransactionBatch { plans }),
                    post_set_op: PostSetOp::None,
                    txn_id: None,
                };
                // Calvin owns durability + write-version recording on its apply
                // path (the scheduler's `append_calvin_applied` LSN is stamped
                // there); no session-level WAL record exists here to stamp.
                if let Some(reason) =
                    classify_batch_dispatch(dp.dispatch_no_wal(batch_task, None).await)
                {
                    return Some(reason);
                }
            }
            None
        }
    }
}
