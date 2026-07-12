// SPDX-License-Identifier: BUSL-1.1

//! Phase 4 of `start_raft`: install the sync/async Raft proposer and
//! compactor closures onto `SharedState`, and spawn the background apply
//! loop that drains `DistributedApplier::apply_committed` into the Data
//! Plane and notifies propose waiters.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{self, Sender};

use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::distributed_applier::{ApplyBatch, ProposeTracker, run_apply_loop};
use crate::control::state::SharedState;

use super::loop_build::RaftLoopType;

/// Install the sync `raft_proposer` / `raft_compactor`, the async
/// `async_raft_proposer`, and spawn the apply loop.
pub(super) fn wire_proposers(
    shared: &Arc<SharedState>,
    raft_loop: &Arc<RaftLoopType>,
    tracker: Arc<ProposeTracker>,
    apply_rx: mpsc::Receiver<ApplyBatch>,
    calvin_read_result_senders: Arc<Mutex<BTreeMap<u32, Sender<ReadResultEvent>>>>,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
) {
    // Wire the Raft proposer into SharedState so CP dispatch paths
    // (pgwire, HTTP, array inbound) can route writes through Raft.
    // Hold `raft_loop` weakly: `SharedState` owns this closure, and the
    // closure must NOT keep `raft_loop` alive or the two form a strong
    // reference cycle that pins `SharedState` forever. During normal
    // operation the loop's spawned tasks keep it alive so `upgrade`
    // always succeeds; `None` only occurs once those tasks have stopped
    // on shutdown, where a clean "cluster not running" error is correct.
    let raft_loop_for_propose = Arc::downgrade(raft_loop);
    let proposer: Arc<crate::control::wal_replication::RaftProposer> =
        Arc::new(move |vshard_id, data| {
            let rl = raft_loop_for_propose
                .upgrade()
                .ok_or_else(|| crate::Error::Internal {
                    detail: "raft propose: cluster not running".into(),
                })?;
            rl.propose(vshard_id, data)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft propose: {e}"),
                })
        });
    if shared.raft_proposer.set(proposer).is_err() {
        tracing::warn!("raft_proposer already set — start_raft appears to have run twice");
    }

    // Wire the Raft log-compaction trigger. `run_apply_loop` invokes this
    // after a committed entry has been durably applied to the Data Plane,
    // so compaction is gated on the data-plane applied watermark — never
    // raft's commit index. A no-op for groups whose
    // `log_compaction_threshold` is `None`.
    // Weak for the same cycle-breaking reason as `raft_proposer` above.
    let raft_loop_for_compact = Arc::downgrade(raft_loop);
    let compactor: Arc<crate::control::wal_replication::RaftCompactor> =
        Arc::new(move |group_id, applied_index| {
            let rl = raft_loop_for_compact
                .upgrade()
                .ok_or_else(|| crate::Error::Internal {
                    detail: "raft log compaction: cluster not running".into(),
                })?;
            rl.maybe_compact_group(group_id, applied_index)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft log compaction: {e}"),
                })
        });
    if shared.raft_compactor.set(compactor).is_err() {
        tracing::warn!("raft_compactor already set — start_raft appears to have run twice");
    }

    // Install the async proposer with transparent leader forwarding.
    //
    // Proposes via the data group leader (forwarding to a remote leader if
    // needed), then registers a ProposeTracker waiter and awaits apply.
    //
    // The ProposeTracker is race-safe: if `run_apply_loop` calls complete()
    // before register() is called (possible on fast clusters where the entry
    // commits and applies on this node before the proposer returns), the
    // result is stored and register() picks it up immediately with no timeout.
    // Weak for the same cycle-breaking reason as `raft_proposer` above.
    let raft_loop_async = Arc::downgrade(raft_loop);
    let tracker_for_proposer = tracker.clone();
    let deadline_secs = shared.tuning.network.default_deadline_secs;
    let async_proposer: Arc<crate::control::wal_replication::AsyncRaftProposer> =
        Arc::new(move |vshard_id, idempotency_key, data| {
            let rl_weak = raft_loop_async.clone();
            let tk = tracker_for_proposer.clone();
            Box::pin(async move {
                let rl = rl_weak.upgrade().ok_or_else(|| crate::Error::Internal {
                    detail: "raft propose (async): cluster not running".into(),
                })?;
                let (group_id, log_index) = rl
                    .propose_via_data_leader(vshard_id, data)
                    .await
                    .map_err(|e| crate::Error::Internal {
                        detail: format!("raft propose (async): {e}"),
                    })?;

                // Register the waiter with the proposer's idempotency
                // key. The apply path compares against the committed
                // entry's key so a leader-change overwrite at the same
                // (group_id, log_index) — by either an empty no-op or a
                // different proposer's real entry — surfaces as
                // `RetryableLeaderChange` instead of leaking a
                // not-our-payload back to the caller.
                let rx = tk.register(group_id, log_index, idempotency_key);
                tokio::time::timeout(std::time::Duration::from_secs(deadline_secs), rx)
                    .await
                    .map_err(|_| crate::Error::Dispatch {
                        detail: format!(
                            "raft commit timeout for group {group_id} index {log_index}"
                        ),
                    })?
                    .map_err(|_| crate::Error::Dispatch {
                        detail: "propose waiter channel closed".into(),
                    })?
                    // Preserve `RetryableLeaderChange` so the gateway
                    // retry loop can re-propose against the new leader
                    // — wrapping it in `Dispatch` would hide the
                    // retryable signal and surface as silent INSERT
                    // success. Other errors stay wrapped for
                    // diagnostics.
                    .map_err(|e| match e {
                        crate::Error::RetryableLeaderChange { .. } => e,
                        other => crate::Error::Dispatch {
                            detail: format!("apply error: {other}"),
                        },
                    })
            })
        });
    if shared.async_raft_proposer.set(async_proposer).is_err() {
        tracing::warn!("async_raft_proposer already set — start_raft appears to have run twice");
    }

    // Spawn the background apply loop. It reads from the mpsc channel
    // pushed by `DistributedApplier::apply_committed`, dispatches to the
    // Data Plane, and notifies propose waiters.
    let apply_state = shared.clone();
    let apply_tracker = tracker.clone();
    let apply_calvin_read_result_senders = Arc::clone(&calvin_read_result_senders);
    let sr_apply = shutdown_rx.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = run_apply_loop(
                apply_rx,
                apply_state,
                apply_tracker,
                apply_calvin_read_result_senders,
            ) => {}
            _ = async {
                let mut rx = sr_apply;
                let _ = rx.changed().await;
            } => {}
        }
    });
}
