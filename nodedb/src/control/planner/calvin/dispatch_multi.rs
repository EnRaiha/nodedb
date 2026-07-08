// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral Calvin multi-shard dispatch (static path).
//!
//! This is the session-UNAWARE core extracted from the pgwire
//! `dispatch_calvin_multishard` static (non-OLLP) branch: classify the task
//! set, reject cross-shard writes inside an explicit transaction block, build
//! the static `TxClass`, and route the single submit-and-await to the
//! sequencer-group leader via `submit_calvin_routed`.
//!
//! It takes the two session-derived inputs the core needs — the cross-shard
//! transaction mode and whether the caller is inside an explicit transaction
//! block — as plain parameters, so both the pgwire and native protocol paths
//! can supply them and share one implementation. The function returns a raw
//! `crate::Result<()>`: Calvin's static path produces no Data-Plane payload —
//! its success is the durable, replicated commit acknowledged by
//! `submit_calvin_routed` — so the per-task command tags are synthesised by
//! each protocol from the original task list AFTER this returns `Ok`.
//!
//! The OLLP (dependent-predicate) variant is intentionally NOT handled here: it
//! is still tied to the local `OllpOrchestrator` and completion registry and is
//! not yet leader-routed (a declared follow-up). Callers that may carry a
//! dependent predicate must route that case through their own OLLP path; this
//! helper is the static cross-shard write path only.

use crate::bridge::envelope::Response;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, build_static_tx_class, classify_dispatch,
    submit_calvin_routed,
};
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

/// Drive the static Calvin multi-shard path for `tasks`.
///
/// - `cross_shard_mode`: the session's effective cross-shard transaction mode.
///   Only [`CrossShardTxnMode::Strict`] routes through Calvin here; callers are
///   expected to have already gated on this, but it is re-checked defensively.
/// - `in_txn_block`: `true` if the caller is inside an explicit transaction
///   block. Cross-shard writes in an explicit block are rejected with
///   [`crate::Error::CrossShardInExplicitTransaction`], matching pgwire.
///
/// On success the Calvin transaction has been submitted and acknowledged by the
/// sequencer leader. Returns the applied Data-Plane [`Response`] when the write
/// carried a RETURNING clause (so the caller can emit its rows), or `None` for a
/// plain write — where the caller synthesises one command tag per task.
pub async fn dispatch_tasks_to_calvin(
    state: &SharedState,
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    cross_shard_mode: CrossShardTxnMode,
    in_txn_block: bool,
) -> crate::Result<Option<Response>> {
    match classify_dispatch(tasks) {
        DispatchClass::MultiShard { .. } => {
            if in_txn_block {
                return Err(crate::Error::CrossShardInExplicitTransaction);
            }
            match cross_shard_mode {
                CrossShardTxnMode::Strict => {
                    // The sequencer inbox must be wired for the strict path.
                    // A non-leader local submit is silently discarded, so
                    // route the single submit-and-await to the leader.
                    if state.sequencer_inbox.get().is_none() {
                        return Err(crate::Error::SequencerUnavailable);
                    }
                    let tx_class = build_static_tx_class(tasks, tenant_id)?;
                    submit_calvin_routed(state, tx_class).await
                }
                CrossShardTxnMode::BestEffortNonAtomic => {
                    // Best-effort never reaches this strict multi-shard entry
                    // point; surface a typed internal error rather than
                    // silently doing nothing.
                    Err(crate::Error::Internal {
                        detail: "unexpected non-Calvin dispatch outcome for strict \
                                 multi-shard query"
                            .to_owned(),
                    })
                }
            }
        }
        DispatchClass::SingleShard { .. } => Err(crate::Error::Internal {
            detail: "unexpected single-shard classification on the strict \
                     multi-shard Calvin path"
                .to_owned(),
        }),
    }
}
