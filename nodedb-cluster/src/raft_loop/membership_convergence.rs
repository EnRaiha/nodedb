// SPDX-License-Identifier: BUSL-1.1

//! Membership convergence: drive each group toward its authored placement set.
//!
//! **Entering path** — proposes `AddLearner` for placement nodes not yet
//! present as voters or learners; promotion to voter is handled by
//! `promote_ready_learners` once the learner catches up.
//!
//! **Leaving voter path** — proposes `RemoveNode` for committed voters no
//! longer in the placement set, capped so the group never drops below RF
//! committed voters (ensuring a replacement is promoted before the old node is
//! removed), one removal per group per pass, never removing the group leader.
//!
//! **Leaving learner path** — proposes `RemoveLearner` for non-voting learners
//! that are NOT in the group's placement set. This is the steady-state cleanup
//! for over-replication: when N > RF a joining node is admitted as a learner
//! to all groups (so it can catch up and bootstrap correctly), but only RF
//! nodes appear in each group's placement. Once placement is authored, this
//! step removes learners that placement excludes. It is deliberately inert
//! while placement is `None` (the bootstrap window) — the `let Some` guard
//! ensures the bootstrap invariant is never violated.

use tracing::debug;

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};

/// Nodes that should be added as learners to converge a group toward its
/// placement set: placement members that are neither current voters nor
/// current learners. Returned sorted and deduplicated. Pure and
/// deterministic.
pub(super) fn plan_entering_learners(
    actual_voters: &[u64],
    actual_learners: &[u64],
    placement: &[u64],
) -> Vec<u64> {
    let mut out: Vec<u64> = placement
        .iter()
        .copied()
        .filter(|n| !actual_voters.contains(n) && !actual_learners.contains(n))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// Learners that should be REMOVED to converge a group toward its placement
/// set: learners present in membership whose node-id does NOT appear in
/// placement. Returned sorted ascending. Pure and deterministic.
///
/// No RF-floor needed — learners are non-voting and their removal never affects
/// quorum, commit, or election outcomes.
pub(super) fn plan_leaving_learners(actual_learners: &[u64], placement: &[u64]) -> Vec<u64> {
    let mut leaving: Vec<u64> = actual_learners
        .iter()
        .copied()
        .filter(|n| !placement.contains(n))
        .collect();
    leaving.sort_unstable();
    leaving
}

/// Voters that should be REMOVED to converge a group toward its placement set:
/// committed voters not present in the placement. Capped so the group never
/// drops below `rf` committed voters — at most `voters.len() - rf` may be
/// removed. Because `actual_voters` are COMMITTED voters (learners excluded),
/// this cap also guarantees we never remove a node before its replacement has
/// actually been promoted to voter. Returned sorted ascending.
///
/// Caller is responsible for: (a) only acting as the group leader, (b) NOT
/// removing the leader itself, and (c) pacing removals (one at a time).
pub(super) fn plan_leaving_voters(actual_voters: &[u64], placement: &[u64], rf: usize) -> Vec<u64> {
    let removable = actual_voters.len().saturating_sub(rf);
    if removable == 0 {
        return Vec::new();
    }
    let mut leaving: Vec<u64> = actual_voters
        .iter()
        .copied()
        .filter(|n| !placement.contains(n))
        .collect();
    leaving.sort_unstable();
    leaving.truncate(removable);
    leaving
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// For each group this node leads that has an authored placement set,
    /// propose `AddLearner` for placement nodes not yet voters or learners.
    ///
    /// Promotion to voter is handled by `promote_ready_learners` once the
    /// learner catches up. Re-proposals while a conf-change is pending are
    /// rejected by Raft and simply retried next tick — no throttle needed.
    pub(super) fn converge_entering_learners(&self) {
        // Phase 1: snapshot additions under one lock acquisition.
        let additions: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            let mut out = Vec::new();
            for gid in group_ids {
                if gid == crate::metadata_group::METADATA_GROUP_ID
                    || gid == crate::calvin::sequencer::SEQUENCER_GROUP_ID
                {
                    continue;
                }
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
                for node_id in plan_entering_learners(&m.voters, &m.learners, &placement) {
                    out.push((gid, node_id));
                }
            }
            out
        };

        // Phase 2: propose each addition in its own lock acquisition.
        // If any fails (e.g., a conf-change is already pending, or this node
        // stepped down between phases) log and move on — the next tick retries.
        for (group_id, node_id) in additions {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let change = ConfChange {
                change_type: ConfChangeType::AddLearner,
                node_id,
            };
            match mr.propose_conf_change(group_id, &change) {
                Ok((_gid, idx)) => {
                    debug!(
                        group_id,
                        node_id,
                        log_index = idx,
                        "convergence: proposed AddLearner"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        node_id,
                        error = %e,
                        "convergence: AddLearner deferred"
                    );
                }
            }
        }
    }

    /// For each group this node leads that has an authored placement set,
    /// propose `RemoveNode` for committed voters not in the placement —
    /// safely. At most one removal is proposed per group per pass
    /// (one-at-a-time pacing) and the group leader is never removed
    /// (self-removal needs a leadership-transfer primitive we don't have).
    ///
    /// The RF floor in `plan_leaving_voters` guarantees the group keeps
    /// `>= rf` committed voters after the removal — and because those are
    /// committed voters (learners excluded), a leaving voter is never
    /// removed before its replacement has actually been promoted to voter.
    /// Re-proposals while a conf-change is pending are rejected by Raft and
    /// simply retried next tick — no throttle needed.
    pub(super) fn converge_leaving_voters(&self) {
        let rf = self.replication_factor() as usize;

        // Phase 1: snapshot at most one removal per group under one lock.
        let removals: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            let mut out = Vec::new();
            for gid in group_ids {
                if gid == crate::metadata_group::METADATA_GROUP_ID
                    || gid == crate::calvin::sequencer::SEQUENCER_GROUP_ID
                {
                    continue;
                }
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
                // Pick the first leaving voter that is NOT the group leader.
                // A candidate equal to the leader is this node (self-removal);
                // skip it — leader step-aside needs leadership transfer.
                for node_id in plan_leaving_voters(&m.voters, &placement, rf) {
                    if node_id == m.leader_id {
                        debug!(
                            group_id = gid,
                            node_id,
                            "convergence: leaving voter is group leader; \
                             deferring removal (needs leadership transfer)"
                        );
                        continue;
                    }
                    out.push((gid, node_id));
                    break;
                }
            }
            out
        };

        // Phase 2: propose each removal in its own lock acquisition.
        for (group_id, node_id) in removals {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let change = ConfChange {
                change_type: ConfChangeType::RemoveNode,
                node_id,
            };
            match mr.propose_conf_change(group_id, &change) {
                Ok((_gid, idx)) => {
                    debug!(
                        group_id,
                        node_id,
                        log_index = idx,
                        "convergence: proposed RemoveNode"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        node_id,
                        error = %e,
                        "convergence: RemoveNode deferred"
                    );
                }
            }
        }
    }

    /// For each group this node leads that has an authored placement set,
    /// propose `RemoveLearner` for non-voting learners not in the placement.
    ///
    /// This is the steady-state cleanup for the over-replication artifact
    /// produced when N > RF: joining admits a node as a learner to all groups,
    /// but only RF groups include it in their placement. Once placement is
    /// authored, non-placement learners are removed within a few ticks.
    ///
    /// **Bootstrap guard:** the `let Some(placement) else { continue }` below
    /// is load-bearing. During the bootstrap window, data-group placement is
    /// `None` (not yet authored by reconcile). Skipping on `None` makes this
    /// step inert in that window — it cannot strip formation-time learners, and
    /// the formation invariant is preserved. This is identical to the guard used
    /// by `converge_entering_learners` and `converge_leaving_voters`.
    ///
    /// Re-proposals while a conf-change is pending are rejected by Raft and
    /// retried next tick — no throttle needed.
    pub(super) fn converge_leaving_learners(&self) {
        // Phase 1: snapshot removals under one lock acquisition.
        let removals: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            let mut out = Vec::new();
            for gid in group_ids {
                if gid == crate::metadata_group::METADATA_GROUP_ID
                    || gid == crate::calvin::sequencer::SEQUENCER_GROUP_ID
                {
                    continue;
                }
                if !mr.group_role_is_leader(gid) {
                    continue;
                }
                let placement: Option<Vec<u64>> = mr
                    .routing()
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_info(gid)
                    .and_then(|info| info.placement.clone());
                // Bootstrap guard: while placement is None the group is in its
                // formation window. Skip unconditionally — this step must be
                // inert until placement is authored (Some).
                let Some(placement) = placement else {
                    continue;
                };
                let Some(m) = mr.group_membership(gid) else {
                    continue;
                };
                for node_id in plan_leaving_learners(&m.learners, &placement) {
                    out.push((gid, node_id));
                }
            }
            out
        };

        // Phase 2: propose each removal in its own lock acquisition.
        for (group_id, node_id) in removals {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let change = ConfChange {
                change_type: ConfChangeType::RemoveLearner,
                node_id,
            };
            match mr.propose_conf_change(group_id, &change) {
                Ok((_gid, idx)) => {
                    debug!(
                        group_id,
                        node_id,
                        log_index = idx,
                        "convergence: proposed RemoveLearner"
                    );
                }
                Err(e) => {
                    debug!(
                        group_id,
                        node_id,
                        error = %e,
                        "convergence: RemoveLearner deferred"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{plan_entering_learners, plan_leaving_learners, plan_leaving_voters};

    #[test]
    fn entering_nodes_not_yet_in_membership() {
        assert_eq!(
            plan_entering_learners(&[1, 2], &[], &[1, 2, 3, 4]),
            vec![3, 4]
        );
    }

    #[test]
    fn existing_learner_not_re_added() {
        assert_eq!(
            plan_entering_learners(&[1, 2], &[3], &[1, 2, 3]),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn no_entering_when_placement_subset_of_members() {
        assert_eq!(
            plan_entering_learners(&[1, 2, 3], &[4], &[1, 2, 3, 4]),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn result_is_sorted_and_deduped() {
        // placement has duplicate and unsorted entries
        assert_eq!(
            plan_entering_learners(&[1], &[], &[4, 2, 4, 3]),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn empty_placement_returns_empty() {
        assert_eq!(
            plan_entering_learners(&[1, 2], &[3], &[]),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn leaving_never_below_rf_cap() {
        // 4 voters, RF=3 → removable=1; node 4 is not in placement.
        assert_eq!(plan_leaving_voters(&[1, 2, 3, 4], &[1, 2, 3], 3), vec![4]);
    }

    #[test]
    fn leaving_blocked_when_at_rf() {
        // voters == rf → removable=0; the leaving voter (3) cannot be removed
        // until voters grows (replacement promoted), so nothing is returned.
        assert_eq!(
            plan_leaving_voters(&[1, 2, 3], &[1, 2], 3),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn leaving_empty_when_placement_superset() {
        assert_eq!(
            plan_leaving_voters(&[1, 2, 3], &[1, 2, 3, 4], 3),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn leaving_multiple_capped_by_rf() {
        // 5 voters, RF=3 → removable=2; leaving candidates {4,5} → [4,5].
        assert_eq!(
            plan_leaving_voters(&[1, 2, 3, 4, 5], &[1, 2, 3], 3),
            vec![4, 5]
        );
    }

    #[test]
    fn leaving_only_returns_voters_not_in_placement() {
        // leaving candidates {3,4,5}, removable=2 → [3,4].
        assert_eq!(
            plan_leaving_voters(&[1, 2, 3, 4, 5], &[1, 2], 3),
            vec![3, 4]
        );
    }

    #[test]
    fn leaving_output_is_sorted() {
        // unsorted voters, RF=1 → removable high; leaving {9,2,7,4} sorted.
        assert_eq!(plan_leaving_voters(&[9, 2, 7, 4], &[], 0), vec![2, 4, 7, 9]);
    }

    // --- plan_leaving_learners ---

    #[test]
    fn learner_not_in_placement_is_removed() {
        // Learner 4 is not in placement {1,2,3} → returned for removal.
        assert_eq!(plan_leaving_learners(&[4], &[1, 2, 3]), vec![4]);
    }

    #[test]
    fn learner_in_placement_is_kept() {
        // Learner 3 is in placement {1,2,3} → nothing to remove.
        assert_eq!(plan_leaving_learners(&[3], &[1, 2, 3]), Vec::<u64>::new());
    }

    #[test]
    fn mixed_learners_only_out_of_placement_removed() {
        // Learners 3 (in placement) and 4 (not in placement).
        assert_eq!(plan_leaving_learners(&[3, 4], &[1, 2, 3]), vec![4]);
    }

    #[test]
    fn empty_placement_superset_removes_nothing() {
        // Placement includes all learners → nothing to remove.
        assert_eq!(
            plan_leaving_learners(&[2, 3], &[1, 2, 3]),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn leaving_learners_output_is_sorted() {
        // Learners in reverse order, all out-of-placement → sorted output.
        assert_eq!(plan_leaving_learners(&[9, 2, 7], &[]), vec![2, 7, 9]);
    }

    #[test]
    fn no_learners_returns_empty() {
        assert_eq!(plan_leaving_learners(&[], &[1, 2, 3]), Vec::<u64>::new());
    }
}
