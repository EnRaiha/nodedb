// SPDX-License-Identifier: BUSL-1.1

//! Placement reconciler for the metadata-group leader.
//!
//! Periodically (throttled in [`super::tick::do_tick`]) the metadata-group
//! leader diffs the deterministically computed target placement against the
//! routing table's current effective placement and proposes `SetPlacement`
//! for every group that differs. This authors/proposes placement only; the
//! membership add/remove execution that acts on a committed placement lives
//! elsewhere.

use tracing::{debug, warn};

use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// On the metadata-group leader, diff the computed target placement against
    /// the routing table's current effective placement and propose
    /// `SetPlacement` for every group that differs.
    ///
    /// Two phases under distinct lock acquisitions: phase 1 snapshots inputs and
    /// computes the diff under the `mr` lock (taking `topology.read()` and
    /// `routing().read()` nested — mirrors `promote_ready_learners`' mr→routing
    /// discipline); phase 2 proposes after every guard is dropped. Authoring is
    /// gated on metadata-group leadership so there is a single writer.
    pub(super) fn reconcile_placement(&self) {
        let changes: Vec<(u64, Vec<u64>)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if !mr.group_role_is_leader(crate::metadata_group::METADATA_GROUP_ID) {
                return;
            }
            let active_nodes: Vec<u64> = {
                let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
                topo.active_nodes().iter().map(|n| n.node_id).collect()
            };
            if active_nodes.is_empty() {
                return;
            }
            let data_group_ids: Vec<u64> = mr
                .group_ids()
                .into_iter()
                .filter(|g| {
                    *g != crate::metadata_group::METADATA_GROUP_ID
                        && *g != crate::calvin::sequencer::SEQUENCER_GROUP_ID
                })
                .collect();
            let target = crate::rebalancer::placement::compute_placement(
                &active_nodes,
                &data_group_ids,
                self.replication_factor(),
            );
            let routing = mr.routing();
            let routing = routing.read().unwrap_or_else(|p| p.into_inner());
            crate::rebalancer::placement::compute_placement_changes(&routing, &target)
        };

        for (group_id, placement) in changes {
            let entry = crate::metadata_group::entry::MetadataEntry::RoutingChange(
                crate::metadata_group::entry::RoutingChange::SetPlacement {
                    group_id,
                    placement,
                },
            );
            let bytes = match crate::metadata_group::codec::encode_entry(&entry) {
                Ok(b) => b,
                Err(e) => {
                    warn!(group_id, error = %e, "placement: encode SetPlacement failed");
                    continue;
                }
            };
            match self.propose_to_metadata_group(bytes) {
                Ok(idx) => {
                    debug!(
                        group_id,
                        log_index = idx,
                        "placement: proposed SetPlacement"
                    )
                }
                Err(e) => {
                    debug!(group_id, error = %e, "placement: SetPlacement proposal deferred")
                }
            }
        }
    }
}
