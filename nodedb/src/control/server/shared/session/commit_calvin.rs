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
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use nodedb_cluster::calvin::{AttemptOutcome, TxnId as CalvinTxnId};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::commit::classify_batch_dispatch;
use super::outcome::{AbortReason, TxnDataPlane};
use super::store::SessionStore;

/// Map a Calvin completion-registry outcome that is neither the happy path
/// (`Completed`) nor a fresh-attempt retry decision into a terminal abort, or
/// `None` if the caller should proceed with `Completed`.
///
/// `Aborted` (the global cross-shard OCC verdict was ABORT — a read-set
/// validation failure) surfaces as `AbortReason::Serialization`, which both
/// transports map to SQLSTATE `40001`. `Failed` (a scheduler-side routing
/// rejection, never retried) and `Mismatch` (an OLLP predicate-drift signal,
/// unreachable on this non-dependent path today but kept as a typed abort
/// rather than a panic) both surface here as `AbortReason::Dispatch`. Split out
/// from `run_commit_calvin` so the mapping is unit-testable without a live
/// `SharedState` / sequencer / registry.
fn calvin_outcome_to_abort(outcome: &AttemptOutcome) -> Option<AbortReason> {
    match outcome {
        AttemptOutcome::Completed => None,
        AttemptOutcome::Aborted => Some(AbortReason::Serialization),
        AttemptOutcome::Failed { detail } => Some(AbortReason::Dispatch(crate::Error::Internal {
            detail: format!("calvin transaction routing failed: {detail}"),
        })),
        AttemptOutcome::Mismatch => Some(AbortReason::Dispatch(crate::Error::Internal {
            detail: "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
        })),
    }
}

/// Dispatch a multi-shard transaction batch through Calvin (or best-effort
/// per-vShard). Returns `Some(reason)` on failure, `None` on success.
pub(super) async fn run_commit_calvin(
    sessions: &SessionStore,
    addr: &SocketAddr,
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    reads: &[ReadSetEntry],
) -> Option<AbortReason> {
    let cross_shard_mode = sessions.cross_shard_txn_mode(addr);
    let tx_state = sessions.transaction_state(addr);

    // Cross-shard writes inside an explicit transaction block are a capability
    // gap that is independent of deployment. Reject here — BEFORE the Calvin
    // infrastructure availability check below — so an embedded/local node
    // returns the same `CrossShardInExplicitTransaction` a cluster does for the
    // identical query, instead of a deployment-specific "sequencer unavailable"
    // error. `run_commit_calvin` is only ever the multi-shard arm, so being
    // `InBlock` here is already the rejected shape (mirrors the classification
    // reject in `dispatch_calvin_or_fast`).
    if tx_state == super::TransactionState::InBlock {
        return Some(AbortReason::Dispatch(
            crate::Error::CrossShardInExplicitTransaction,
        ));
    }

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
        reads,
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
            // A terminal routing failure (`Failed`) is never retried on this
            // path — the scheduler broadcast `TxnRoutingFailed` via the
            // sequencer Raft so every replica's completion waiter wakes
            // immediately with the reason, instead of burning the full
            // deadline and reporting a generic `CalvinTimeout`. A `Mismatch`
            // is unreachable here today (OLLP mismatch only fires on the
            // dependent-predicate retry path) but is kept as a typed abort
            // rather than a panic. See `calvin_outcome_to_abort`.
            if let Some(reason) = calvin_outcome_to_abort(&outcome) {
                return Some(reason);
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
                    // Calvin threads its bitemporal stamps in on the Data Plane
                    // (`execute_calvin_flush`), not via a session overlay.
                    plan: PhysicalPlan::Meta(MetaOp::TransactionBatch {
                        plans,
                        txn_id: None,
                    }),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_outcome_maps_to_no_abort() {
        assert!(calvin_outcome_to_abort(&AttemptOutcome::Completed).is_none());
    }

    #[test]
    fn failed_outcome_maps_to_typed_dispatch_abort_carrying_detail() {
        let outcome = AttemptOutcome::Failed {
            detail: "calvin txn 7/2 for vshard 3 contains an unroutable plan".to_owned(),
        };
        let reason =
            calvin_outcome_to_abort(&outcome).expect("Failed must map to a terminal abort");
        match reason {
            AbortReason::Dispatch(err) => {
                let message = err.to_string();
                assert!(
                    message.contains("calvin txn 7/2 for vshard 3 contains an unroutable plan"),
                    "abort message must carry the routing-failure detail verbatim, got: {message}"
                );
            }
            _ => panic!("Failed outcome must map to AbortReason::Dispatch"),
        }
    }

    #[test]
    fn aborted_outcome_maps_to_serialization_abort() {
        let reason = calvin_outcome_to_abort(&AttemptOutcome::Aborted)
            .expect("Aborted must map to a terminal abort");
        assert!(
            matches!(reason, AbortReason::Serialization),
            "a global ABORT verdict must map to AbortReason::Serialization (SQLSTATE 40001)"
        );
    }

    #[test]
    fn mismatch_outcome_maps_to_typed_dispatch_abort() {
        let reason = calvin_outcome_to_abort(&AttemptOutcome::Mismatch)
            .expect("Mismatch must map to a terminal abort");
        assert!(matches!(reason, AbortReason::Dispatch(_)));
    }
}
