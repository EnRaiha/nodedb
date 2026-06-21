// SPDX-License-Identifier: BUSL-1.1

//! Group-level membership helpers consumed by the tick loop's join /
//! promotion phases.
//!
//! - `commit_index_for(group)`: used by the join flow to wait until a
//!   proposed `AddLearner` conf-change commits before replying to the
//!   joining node.
//! - `ready_learners(group)`: used by the tick loop's "promote
//!   caught-up learners" phase — returns every learner in the group
//!   whose `match_index` on this (leader) node is at least the current
//!   `commit_index`, i.e. learners that have replicated enough log to be
//!   safely promoted.
//! - `group_leader(group)`: leader id observed by this node's local
//!   RaftNode state, used by the join flow to decide redirect vs admit.
//! - `group_role_is_leader(group)`: cheap leader-check helper.

use nodedb_raft::NodeRole;

use super::core::MultiRaft;

impl MultiRaft {
    /// Whether a node is already admitted as a voter or learner.
    pub fn group_contains_node(&self, group_id: u64, node_id: u64) -> Option<bool> {
        let membership = self.group_membership(group_id)?;
        Some(membership.voters.contains(&node_id) || membership.learners.contains(&node_id))
    }

    /// Current commit index for a group, or `None` if the group is not
    /// hosted on this node.
    pub fn commit_index_for(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id).map(|n| n.commit_index())
    }

    /// Learners in `group_id` whose `match_index` on this leader has
    /// caught up to the current `commit_index` — safe to promote.
    ///
    /// Returns an empty vec if this node is not the leader of the group
    /// or the group is not hosted here.
    pub fn ready_learners(&self, group_id: u64) -> Vec<u64> {
        let Some(node) = self.groups.get(&group_id) else {
            return Vec::new();
        };
        if node.role() != NodeRole::Leader {
            return Vec::new();
        }
        let commit = node.commit_index();
        node.learners()
            .iter()
            .copied()
            .filter(|&learner| node.match_index_for(learner).unwrap_or(0) >= commit)
            .collect()
    }

    /// Observed leader id for a group (0 = unknown / no election yet).
    pub fn group_leader(&self, group_id: u64) -> u64 {
        self.groups
            .get(&group_id)
            .map(|n| n.leader_id())
            .unwrap_or(0)
    }

    /// Whether this node is currently the leader of `group_id`.
    pub fn group_role_is_leader(&self, group_id: u64) -> bool {
        self.groups
            .get(&group_id)
            .map(|n| n.role() == NodeRole::Leader)
            .unwrap_or(false)
    }
}
