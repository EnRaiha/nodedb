// SPDX-License-Identifier: BUSL-1.1

//! Apply a group's committed entries: detect + apply conf-changes, dispatch
//! to the metadata or data applier, advance the applied watermark, flip the
//! boot-time readiness watch, and bump the cluster epoch on metadata-group
//! leadership acquisition.

use tracing::warn;

use crate::conf_change::ConfChange;
use crate::forward::PlanExecutor;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Apply one group's committed entries from this tick's `Ready` output.
    /// Called only when `!group_ready.committed_entries.is_empty()`.
    pub(super) fn apply_group_commits(&self, group_id: u64, group_ready: &nodedb_raft::Ready) {
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
        if group_id == crate::metadata_group::METADATA_GROUP_ID && !*self.ready_watch.borrow() {
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
}
