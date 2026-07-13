// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard arm of the neutral COMMIT orchestrator.
//!
//! Routes a transaction whose buffered writes span two or more vShards through
//! the Calvin sequencer. Strict mode routes the whole buffered batch to the
//! sequencer-group leader via `dispatch_tasks_to_calvin` — the same routed
//! submit-and-await the autocommit cross-shard path uses — so a non-leader
//! coordinator's interactive COMMIT still completes. Best-effort mode fans the
//! batch out per-vShard independently. Split out of `commit.rs` to keep the
//! orchestrator's top-level flow short.

use std::net::SocketAddr;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{
    CrossShardTxnMode, TxnDispatchPosition, dispatch_tasks_to_calvin,
};
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::commit::classify_batch_dispatch;
use super::outcome::{AbortReason, TxnDataPlane};
use super::store::SessionStore;

/// Dispatch a multi-shard transaction batch through Calvin (or best-effort
/// per-vShard). Returns `Some(reason)` on failure, `None` on success.
///
/// This is the COMMIT flush of a buffered explicit block: the interactive
/// `BEGIN; <cross-shard writes/reads>; COMMIT` sequence lands here with its
/// whole batch, which commits atomically through the durable Vote/Verdict
/// barrier. Strict mode routes to the sequencer-group leader so the commit
/// completes even when this coordinator is not the leader.
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

    match cross_shard_mode {
        CrossShardTxnMode::Strict => {
            // The sequencer inbox must be wired for the strict cross-shard path;
            // surface a deployment-neutral `SequencerUnavailable` if it is not.
            if state.sequencer_inbox.get().is_none() {
                return Some(AbortReason::Dispatch(crate::Error::SequencerUnavailable));
            }
            // Route the buffered cross-shard batch through the sequencer-group
            // leader via the SAME routed submit-and-await the autocommit path
            // uses. This is the COMMIT flush of a buffered explicit block — the
            // whole batch commits atomically — NOT a mid-block single statement,
            // so the mid-block cross-shard guard must not fire (hence
            // `CommitFlush`, not `MidBlockStatement`).
            match dispatch_tasks_to_calvin(
                state,
                buffered,
                tenant_id,
                cross_shard_mode,
                TxnDispatchPosition::CommitFlush,
                reads,
            )
            .await
            {
                // Success: the durable, replicated commit was acknowledged by
                // the routed submit-and-await. COMMIT returns no rows, so the
                // applied Response (with any RETURNING payload) is unused here.
                Ok(_) => None,
                // The global cross-shard OCC verdict was ABORT (read-set
                // validation failed); the routed await surfaces this as
                // `CalvinSerializationConflict`. Map it to the serialization
                // abort both transports render as SQLSTATE `40001` so the client
                // retries the whole transaction.
                Err(crate::Error::CalvinSerializationConflict) => Some(AbortReason::Serialization),
                // Every other hard error (sequencer unavailable, a scheduler
                // routing failure, an assignment/completion timeout mapped to
                // `Internal`, or a TxClass encode failure) is a dispatch abort;
                // adapters map the carried error per their existing rules.
                Err(e) => Some(AbortReason::Dispatch(e)),
            }
        }
        CrossShardTxnMode::BestEffortNonAtomic => {
            // Dispatch each vShard's sub-batch independently. Group buffered
            // tasks by vShard and dispatch per-vShard TransactionBatches.
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
