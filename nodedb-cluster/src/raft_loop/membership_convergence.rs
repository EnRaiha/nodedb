// SPDX-License-Identifier: BUSL-1.1

//! Membership convergence: drive each group toward its authored placement
//! set by proposing `AddLearner` for placement nodes not yet present as
//! voters or learners. Promotion to voter is handled by
//! `promote_ready_learners` once the learner has caught up.

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
}

#[cfg(test)]
mod tests {
    use super::plan_entering_learners;

    #[test]
    fn entering_nodes_not_yet_in_membership() {
        assert_eq!(
            plan_entering_learners(&[1, 2], &[], &[1, 2, 3, 4]),
            vec![3, 4]
        );
    }

    #[test]
    fn existing_learner_not_re_added() {
        assert_eq!(plan_entering_learners(&[1, 2], &[3], &[1, 2, 3]), Vec::<u64>::new());
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
        assert_eq!(plan_entering_learners(&[1, 2], &[3], &[]), Vec::<u64>::new());
    }
}
