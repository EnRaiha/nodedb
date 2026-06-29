// SPDX-License-Identifier: BUSL-1.1

//! Single tick of the Raft event loop.
//!
//! Ordering (each phase uses a short-lived `MultiRaft` lock acquisition —
//! the lock is never held across an `.await`):
//!
//! 1. **Tick Raft groups**: drive election timeouts / heartbeats and pull
//!    `MultiRaftReady` output (messages, vote requests, committed
//!    entries, snapshot-needed peers).
//! 2. **Dispatch AppendEntries**: one background task per peer, batching
//!    all messages targeting the same peer.
//! 3. **Dispatch RequestVote**: same batching strategy.
//! 4. **Apply committed entries**: feed them to the user-supplied
//!    `CommitApplier`. Conf-change entries are detected and applied to
//!    `MultiRaft` before the user applier sees them.
//! 5. **Install snapshots**: send `InstallSnapshot` RPCs to peers that
//!    have fallen behind the leader's snapshot boundary.
//! 6. **Converge entering learners**: for each group this node leads,
//!    propose `AddLearner` for placement nodes not yet present as
//!    voters or learners. Re-proposals while a conf-change is pending
//!    are harmlessly rejected by Raft and retried next tick.
//! 7. **Promote caught-up learners**: for every group where this node is
//!    leader, query learners whose `match_index >= commit_index` and
//!    propose `PromoteLearner` for each. Idempotent by design — after
//!    the first promotion the peer has moved from `learners` to
//!    `members` and won't be returned again.

use std::collections::HashMap as BatchMap;

use tracing::{debug, warn};

use nodedb_raft::transport::RaftTransport;

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};

/// Ticks between placement-reconcile passes (100 ticks × ~10ms ≈ 1s).
///
/// `SetPlacement` is a normal metadata entry, not a conf-change, so Raft does
/// not dedup per-tick re-proposals before they commit. Running reconcile ~1s
/// apart is long enough that a proposed `SetPlacement` commits+applies before
/// the next diff, so an unchanged target never re-proposes.
const PLACEMENT_RECONCILE_TICK_INTERVAL: u64 = 100;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Execute a single tick: drive Raft, dispatch outbound messages,
    /// apply commits, promote caught-up learners.
    pub(super) fn do_tick(&self) {
        // Tick under lock and extract Ready.
        let ready = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.tick()
        };

        // Dispatch outgoing messages and persist log/HardState first (even if
        // ready looks "empty" we still want to run the learner-promotion step
        // each tick so a just-caught-up learner is promoted promptly).
        if !ready.is_empty() {
            let mut ae_batches: BatchMap<u64, Vec<(u64, nodedb_raft::AppendEntriesRequest)>> =
                BatchMap::new();
            let mut vote_batches: BatchMap<u64, Vec<(u64, nodedb_raft::RequestVoteRequest)>> =
                BatchMap::new();

            for (group_id, group_ready) in &ready.groups {
                for (peer, req) in &group_ready.messages {
                    ae_batches
                        .entry(*peer)
                        .or_default()
                        .push((*group_id, req.clone()));
                }
                for (peer, req) in &group_ready.vote_requests {
                    vote_batches
                        .entry(*peer)
                        .or_default()
                        .push((*group_id, req.clone()));
                }
            }

            // Dispatch batched AppendEntries — one task per peer.
            //
            // Each detached task subscribes to the shutdown watch
            // and wraps its RPC awaits in `tokio::select!` so a
            // `RaftLoop::begin_shutdown` signal (or the `run` loop
            // propagating an external shutdown) cancels the
            // in-flight QUIC call at the next await point. This
            // is what lets graceful shutdown drop the
            // `Arc<Mutex<MultiRaft>>` clone promptly and release
            // per-group redb locks for an in-process restart.
            for (peer, messages) in ae_batches {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                let mut shutdown_rx = self.shutdown_watch.subscribe();
                tokio::spawn(async move {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    for (group_id, req) in messages {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => return,
                            rpc = transport.append_entries(peer, req) => {
                                match rpc {
                                    Ok(resp) => {
                                        let mut mr =
                                            mr.lock().unwrap_or_else(|p| p.into_inner());
                                        if let Err(e) = mr
                                            .handle_append_entries_response(group_id, peer, &resp)
                                        {
                                            debug!(group_id, peer, error = %e, "handle ae response");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(group_id, peer, error = %e, "append_entries RPC failed");
                                        break; // Peer is down — skip remaining groups.
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Dispatch batched RequestVote — one task per peer.
            for (peer, votes) in vote_batches {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                let mut shutdown_rx = self.shutdown_watch.subscribe();
                tokio::spawn(async move {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    for (group_id, req) in votes {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => return,
                            rpc = transport.request_vote(peer, req) => {
                                match rpc {
                                    Ok(resp) => {
                                        let mut mr =
                                            mr.lock().unwrap_or_else(|p| p.into_inner());
                                        if let Err(e) = mr
                                            .handle_request_vote_response(group_id, peer, &resp)
                                        {
                                            debug!(group_id, peer, error = %e, "handle vote response");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(group_id, peer, error = %e, "request_vote RPC failed");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Apply committed entries and conf-changes.
            for (group_id, group_ready) in ready.groups {
                if !group_ready.committed_entries.is_empty() {
                    for entry in &group_ready.committed_entries {
                        if let Some(cc) = ConfChange::from_entry_data(&entry.data) {
                            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                            if let Err(e) = mr.apply_conf_change(group_id, &cc) {
                                warn!(group_id, error = %e, "failed to apply conf change");
                            }
                        }
                    }

                    let last_applied = if group_id == crate::metadata_group::METADATA_GROUP_ID {
                        // Metadata group (0): dispatch to the metadata applier.
                        // Raft no-op entries and conf-changes are already
                        // handled above; data entries carry a serialized
                        // `MetadataEntry` and are decoded by the applier.
                        let pairs: Vec<(u64, Vec<u8>)> = group_ready
                            .committed_entries
                            .iter()
                            .filter(|e| ConfChange::from_entry_data(&e.data).is_none())
                            .map(|e| (e.index, e.data.clone()))
                            .collect();
                        self.metadata_applier.apply(&pairs)
                    } else {
                        self.applier
                            .apply_committed(group_id, &group_ready.committed_entries)
                    };
                    if last_applied > 0 {
                        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        if let Err(e) = mr.advance_applied(group_id, last_applied) {
                            warn!(group_id, error = %e, "failed to advance applied index");
                        } else if group_id == crate::metadata_group::METADATA_GROUP_ID {
                            // Metadata group: the metadata applier
                            // applied entries synchronously to redb
                            // before returning, so the apply
                            // watermark is data-visible at this
                            // point. Bump the watcher.
                            //
                            // Data groups are NOT bumped here — for
                            // them `applier.apply_committed` only
                            // enqueues entries onto the
                            // `DistributedApplier` channel; the
                            // actual data lands in storage when
                            // `run_apply_loop` finishes the
                            // SPSC round-trip to the Data Plane.
                            // The host crate bumps the watcher
                            // there, so the watermark always means
                            // "data visible on this node up to
                            // index N" regardless of which group.
                            //
                            // Snapshot-install path also bumps
                            // (in `super::handle_rpc`) — covers
                            // jump-on-snapshot for both group
                            // kinds.
                            self.group_watchers.bump(group_id, last_applied);
                        }
                    }

                    // Boot-time readiness: the first time the metadata
                    // group (0) applies any entry on this node — which
                    // is the leader-election no-op or a replayed entry
                    // — flip the ready watch. The host crate's
                    // `start_raft` returns the receiver; `main.rs`
                    // awaits it before binding client-facing
                    // listeners. Idempotent: subsequent ticks are a
                    // no-op once the latch is set.
                    if group_id == crate::metadata_group::METADATA_GROUP_ID
                        && !*self.ready_watch.borrow()
                    {
                        let _ = self.ready_watch.send(true);
                    }

                    // Detect false→true transitions on metadata-group
                    // leadership and bump the cluster epoch exactly once
                    // per acquisition. The fence token rides on every
                    // outbound RPC after this point (see
                    // `cluster_epoch.rs`).
                    if group_id == crate::metadata_group::METADATA_GROUP_ID {
                        let is_leader = self
                            .multi_raft
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .group_role_is_leader(group_id);
                        let was_leader = self
                            .prev_metadata_leader
                            .swap(is_leader, std::sync::atomic::Ordering::AcqRel);
                        if is_leader && !was_leader {
                            if let Some(catalog) = self.catalog.as_ref() {
                                match crate::cluster_epoch::bump_local_cluster_epoch(catalog) {
                                    Ok(new_epoch) => tracing::info!(
                                        node = self.node_id,
                                        new_epoch,
                                        "bumped cluster epoch on metadata-group leadership acquisition"
                                    ),
                                    Err(e) => tracing::warn!(
                                        node = self.node_id,
                                        error = %e,
                                        "failed to persist bumped cluster epoch (in-memory value advanced anyway)"
                                    ),
                                }
                            } else {
                                // No catalog → in-memory only (test path).
                                let _ = crate::cluster_epoch::observe_peer_cluster_epoch(
                                    crate::cluster_epoch::current_local_cluster_epoch() + 1,
                                );
                            }
                        }
                    }
                }

                // Install-snapshot dispatch for lagging peers.
                if !group_ready.snapshots_needed.is_empty() {
                    let snapshot_meta = {
                        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        mr.snapshot_metadata(group_id).ok()
                    };

                    if let Some((term, snap_index, snap_term)) = snapshot_meta {
                        for peer in group_ready.snapshots_needed {
                            let transport = self.transport.clone();
                            let mr = self.multi_raft.clone();
                            let mut shutdown_rx = self.shutdown_watch.subscribe();
                            let node_id = self.node_id;
                            let chunk_bytes = self.snapshot_chunk_bytes;
                            let snapshot_builder = self.snapshot_builder.clone();
                            tokio::spawn(async move {
                                if *shutdown_rx.borrow() {
                                    return;
                                }
                                // Build the real per-group snapshot payload on the
                                // leader before framing the chunked RPC. A `None`
                                // builder (cluster-only tests) or a build failure
                                // falls back to the stub (empty) chunk, which the
                                // sender already handles.
                                let snapshot_bytes: Vec<u8> = match &snapshot_builder {
                                    Some(b) => b
                                        .build_group_snapshot(group_id, snap_index, snap_term)
                                        .await
                                        .unwrap_or_else(|e| {
                                            warn!(
                                                group_id, peer, error = %e,
                                                "snapshot build failed; sending stub"
                                            );
                                            Vec::new()
                                        }),
                                    None => Vec::new(),
                                };
                                tokio::select! {
                                    biased;
                                    _ = shutdown_rx.changed() => {}
                                    result = crate::install_snapshot::sender::send_chunked(
                                        &transport,
                                        crate::install_snapshot::sender::SendChunkedParams {
                                            peer,
                                            group_id,
                                            term,
                                            leader_id: node_id,
                                            last_included_index: snap_index,
                                            last_included_term: snap_term,
                                            snapshot_bytes: &snapshot_bytes,
                                            chunk_bytes,
                                        },
                                    ) => {
                                        match result {
                                            Ok(resp_term) => {
                                                if resp_term > term {
                                                    let mut mr =
                                                        mr.lock().unwrap_or_else(|p| p.into_inner());
                                                    // Higher term — let the tick loop handle step-down.
                                                    let _ = mr.handle_append_entries_response(
                                                        group_id,
                                                        peer,
                                                        &nodedb_raft::AppendEntriesResponse {
                                                            term: resp_term,
                                                            success: false,
                                                            last_log_index: 0,
                                                        },
                                                    );
                                                }
                                                debug!(group_id, peer, "install_snapshot sent");
                                            }
                                            Err(e) => {
                                                warn!(
                                                    group_id, peer, error = %e,
                                                    "install_snapshot RPC failed"
                                                );
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }

        // Add placement nodes as learners so `promote_ready_learners` can
        // pick them up once they catch up. Re-proposals while a conf-change
        // is already pending are harmlessly rejected by Raft. Runs every tick.
        self.converge_entering_learners();

        // Promote caught-up learners. Runs every tick so a
        // just-caught-up learner is promoted within one tick interval of
        // catching up. Idempotent: once promoted, the peer is in
        // `members` and `ready_learners` no longer returns it.
        self.promote_ready_learners();

        // Placement reconcile is throttled well above the tick rate: SetPlacement
        // is a normal metadata entry (not a conf-change), so Raft would not dedup
        // per-tick re-proposals before they commit. Running ~1s apart lets each
        // proposal apply before the next diff.
        let tick = self
            .tick_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if tick.is_multiple_of(PLACEMENT_RECONCILE_TICK_INTERVAL) {
            self.reconcile_placement();
        }
    }

    /// For every group where this node is leader, propose
    /// `PromoteLearner` for every learner whose `match_index` has
    /// reached the current `commit_index` and whose node-id appears in
    /// the group's placement set (when one is configured).
    ///
    /// This is the automated second phase of the two-step Raft single-
    /// server add. The first phase (`AddLearner`) is proposed by the
    /// join flow; the second phase is proposed here, asynchronously,
    /// once the learner has caught up.
    fn promote_ready_learners(&self) {
        // Snapshot the work list under one lock acquisition.
        let promotions: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            group_ids
                .into_iter()
                .flat_map(|gid| {
                    // Placement set for this group (None = promote all — unchanged behavior).
                    let placement: Option<Vec<u64>> = mr
                        .routing()
                        .read()
                        .unwrap_or_else(|p| p.into_inner())
                        .group_info(gid)
                        .and_then(|info| info.placement.clone());
                    mr.ready_learners(gid)
                        .into_iter()
                        .filter(move |&learner| {
                            should_promote_learner(placement.as_deref(), learner)
                        })
                        .map(move |learner| (gid, learner))
                })
                .collect()
        };

        // Propose each promotion in its own lock acquisition. If any fails
        // (e.g., `NotLeader` after a step-down between phases), log and
        // move on — the next tick will retry any still-pending learner.
        for (group_id, learner_id) in promotions {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let change = ConfChange {
                change_type: ConfChangeType::PromoteLearner,
                node_id: learner_id,
            };
            match mr.propose_conf_change(group_id, &change) {
                Ok((_gid, idx)) => {
                    debug!(
                        group_id,
                        learner_id,
                        log_index = idx,
                        "proposed learner promotion"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        learner_id,
                        error = %e,
                        "learner promotion proposal deferred"
                    );
                }
            }
        }
    }
}

/// Decide whether a caught-up learner may be promoted to voter.
///
/// `None` placement means no explicit placement set for the group —
/// all caught-up learners are promoted (unchanged behavior).
/// `Some(set)` means only learners whose node-id appears in the
/// intended voter set are promoted; others remain learners until the
/// membership reconciler acts on them.
fn should_promote_learner(placement: Option<&[u64]>, learner_id: u64) -> bool {
    match placement {
        None => true,
        Some(set) => set.contains(&learner_id),
    }
}

#[cfg(test)]
mod tests {
    use super::should_promote_learner;

    #[test]
    fn no_placement_always_promotes() {
        assert!(should_promote_learner(None, 1));
        assert!(should_promote_learner(None, 42));
    }

    #[test]
    fn placement_contains_learner_promotes() {
        assert!(should_promote_learner(Some(&[1, 2, 3]), 2));
    }

    #[test]
    fn placement_missing_learner_holds() {
        assert!(!should_promote_learner(Some(&[1, 2, 3]), 99));
    }

    #[test]
    fn empty_placement_promotes_nobody() {
        assert!(!should_promote_learner(Some(&[]), 1));
        assert!(!should_promote_learner(Some(&[]), 42));
    }
}
