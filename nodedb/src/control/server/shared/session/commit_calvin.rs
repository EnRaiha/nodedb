// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard arm of the neutral COMMIT orchestrator.
//!
//! Routes a transaction's buffered writes through the Calvin sequencer. Strict
//! mode routes the whole buffered batch to the sequencer-group leader through
//! the universal strict atomic task-set entry point — one transaction bound by
//! the durable Vote/Verdict barrier. Best-effort mode groups writes by vShard
//! and submits each group
//! as an INDEPENDENT single-vShard Calvin transaction (via
//! `build_single_vshard_tx_class` then `submit_calvin_routed`) — the SAME
//! deterministic sequencer funnel, so each vShard gets an epoch-anchored
//! bitemporal stamp and a `TransactionRedo` WAL record, while remaining
//! non-atomic ACROSS vShards (no global vote binds them; a failure on one vShard
//! does not roll back another).

use crate::control::planner::calvin::{
    CrossShardTxnMode, TxnDispatchPosition, build_single_vshard_tx_class,
    dispatch_strict_atomic_tasks_to_calvin, submit_calvin_routed,
};
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

use super::connection::SessionId;
use super::outcome::AbortReason;
use super::store::SessionStore;

/// Dispatch a multi-shard transaction batch through Calvin. Strict commits the
/// whole batch atomically through the leader-routed Vote/Verdict barrier;
/// best-effort submits one independent single-vShard Calvin transaction per
/// vShard. Returns `Some(reason)` on failure, `None` on success.
pub(super) async fn run_commit_calvin(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    reads: &[ReadSetEntry],
) -> Option<AbortReason> {
    let cross_shard_mode = sessions.cross_shard_txn_mode(session_id);
    // The session's read-reservation owner `R`, taken at read time. Fetched once
    // and stamped onto every Calvin submit below so each commit batch acquires
    // its keys as `R` and self-upgrades the shared reservations — never
    // recomputed at commit.
    let reservation_owner = sessions.current_reservation_owner(session_id);

    match cross_shard_mode {
        CrossShardTxnMode::Strict => {
            // One universal strict admission path builds either a single- or
            // multi-participant class and submits exactly once to the sequencer.
            let result = dispatch_strict_atomic_tasks_to_calvin(
                state,
                buffered,
                tenant_id,
                TxnDispatchPosition::CommitFlush,
                reads,
                reservation_owner,
            )
            .await;
            match result {
                Ok(_) => None,
                Err(crate::Error::CalvinSerializationConflict) => {
                    super::hot_key::record_read_set_aborts(state, reads);
                    Some(AbortReason::Serialization)
                }
                Err(e) => Some(AbortReason::Dispatch(e)),
            }
        }
        CrossShardTxnMode::BestEffortNonAtomic => {
            // The sequencer funnel each per-vShard submit uses requires the inbox
            // (or, in cluster mode, a routable sequencer leader); fail fast and
            // deployment-neutral if it is not wired, mirroring the strict arm.
            if state.sequencer_inbox.get().is_none() {
                return Some(AbortReason::Dispatch(crate::Error::SequencerUnavailable));
            }
            // Group the buffered writes by vShard. Each group becomes ONE
            // independent single-vShard Calvin transaction, sequenced through the
            // SAME deterministic funnel the contended point-write path uses
            // (`build_single_vshard_tx_class` + `submit_calvin_routed`): the
            // scheduler resolves it into a `TransactionRedo`, WAL-appends it, and
            // the Calvin flush sets `epoch_system_ms` so every engine's bitemporal
            // stamp is epoch-anchored and byte-identical on replay.
            //
            // Non-atomic ACROSS vShards is preserved by construction: each group
            // is a separate submit-and-await with its own single-participant
            // verdict (no cross-shard vote barrier). On the FIRST failure we
            // surface the reason and stop — we do NOT roll back vShards that have
            // already committed, exactly as the mode's contract requires.
            let mut by_vshard: std::collections::BTreeMap<u32, Vec<PhysicalTask>> =
                std::collections::BTreeMap::new();
            for task in buffered {
                by_vshard
                    .entry(task.vshard_id.as_u32())
                    .or_default()
                    .push(task.clone());
            }
            for (_vshard_u32, tasks) in by_vshard {
                // Empty read-set: best-effort performs no cross-shard OCC (the
                // multi-shard COMMIT path never ran `si_conflict_abort`), so each
                // group carries no versioned reads — matching the single-vShard
                // submit `route_write_to_calvin` uses.
                let mut tx_class = match build_single_vshard_tx_class(&tasks, tenant_id, &[]) {
                    Ok(tc) => tc,
                    Err(e) => return Some(AbortReason::Dispatch(e)),
                };
                // Each per-vShard group acquires under `R` too, so it self-upgrades
                // its slice of the session's shared reservations.
                tx_class.set_lock_owner(reservation_owner);
                match submit_calvin_routed(state, tx_class).await {
                    Ok(_) => {}
                    Err(crate::Error::CalvinSerializationConflict) => {
                        super::hot_key::record_read_set_aborts(state, reads);
                        return Some(AbortReason::Serialization);
                    }
                    Err(e) => return Some(AbortReason::Dispatch(e)),
                }
            }
            None
        }
    }
}
