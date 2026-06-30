// SPDX-License-Identifier: BUSL-1.1

//! Leader step-aside: when the voter a group must shed (per placement) is the
//! group leader itself, the leader cannot remove itself directly. This step
//! transfers leadership to an in-placement voter so that, on a later tick, the
//! ex-leader is no longer the leader and the leaving-voter removal proceeds.
//!
//! Pairs with `converge_leaving_voters`, which defers removing a leaving voter
//! while that voter is still the group leader. This step is what eventually
//! clears that deferral by moving leadership off the leaving node.

use tracing::debug;

use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};
use super::membership_convergence::plan_leaving_voters;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// For each group this node leads whose placement says this node (the
    /// leader) must leave, transfer leadership to an in-placement node that is
    /// currently a voter (so it can win the election immediately).
    ///
    /// Fire-and-forget and idempotent: a failed transfer is retried on the next
    /// tick. If no eligible target exists yet (e.g. placement has no other
    /// current voter), the group is skipped this tick — entering learners are
    /// promoted by the earlier steps and a target appears on a later tick.
    pub(super) fn transfer_leadership_for_leaving_voters(&self) {
        let rf = self.replication_factor() as usize;

        // Phase 1: snapshot at most one transfer per group under one lock,
        // taking the routing read-lock nested inside the multi_raft lock (same
        // lock order as `converge_leaving_voters`).
        let transfers: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            let mut out = Vec::new();
            for gid in group_ids {
                if gid == crate::metadata_group::METADATA_GROUP_ID
                    || gid == crate::calvin::sequencer::SEQUENCER_GROUP_ID
                {
                    continue;
                }
                // Can only transfer leadership from a group we lead.
                if !mr.group_role_is_leader(gid) {
                    continue;
                }
                let placement: Option<Vec<u64>> = mr
                    .routing()
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_info(gid)
                    .and_then(|info| info.placement.clone());
                let Some(placement) = placement else {
                    continue;
                };
                let Some(m) = mr.group_membership(gid) else {
                    continue;
                };
                // Is SELF (the leader) a leaving voter? We lead this group, so
                // `m.leader_id` is this node. If it is in the leaving set, this
                // node must step aside before it can be removed.
                let self_is_leaving =
                    plan_leaving_voters(&m.voters, &placement, rf).contains(&m.leader_id);
                if !self_is_leaving {
                    continue;
                }
                // Pick an in-placement node that is currently a voter (so it can
                // win the election) and is not this node.
                let target = placement
                    .iter()
                    .copied()
                    .find(|t| *t != m.leader_id && m.voters.contains(t));
                if let Some(target) = target {
                    out.push((gid, target));
                } else {
                    debug!(
                        group_id = gid,
                        "step-aside: leader is a leaving voter but no in-placement \
                         voter target yet; deferring transfer"
                    );
                }
            }
            out
        };

        // Phase 2: initiate each transfer in its own lock acquisition.
        for (group_id, target) in transfers {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.transfer_leadership(group_id, target) {
                Ok(()) => {
                    debug!(
                        group_id,
                        target, "step-aside: initiated leadership transfer"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        target,
                        error = %e,
                        "step-aside: leadership transfer deferred"
                    );
                }
            }
        }
    }
}
