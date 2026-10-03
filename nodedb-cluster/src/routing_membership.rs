// SPDX-License-Identifier: BUSL-1.1

//! A group's membership as its leader answers it, adopted into this node's
//! routing view.
//!
//! A node's routing view of a group's voters and learners moves when the node
//! applies the group's conf changes. Two kinds of node never apply them:
//! - a node that hosts no replica of the group;
//! - a node removed from the group before it learned of its removal.
//!
//! Such a node asks a placement node for the group's leader status (see
//! [`crate::rpc_codec::LeaderStatusResponse`]). A leader answers its Raft
//! voters and learners. A leader changes them only by applying a committed
//! conf change, so the answer holds only committed changes.
//!
//! The same view answers whether a node is a replica of a group. A leader
//! hint that names a node the view does not list is stale.

use crate::routing::RoutingTable;

impl RoutingTable {
    /// Whether this routing view lists `node_id` as a voter or learner of the
    /// group `vshard_id` maps to.
    ///
    /// A leader hint that names `node_id` is stale when this is false: the
    /// node left the group, so it cannot serve it.
    pub fn is_replica_of_vshard(&self, vshard_id: u32, node_id: u64) -> bool {
        self.group_for_vshard(vshard_id)
            .ok()
            .and_then(|group_id| self.group_info(group_id))
            .is_some_and(|info| info.members.contains(&node_id) || info.learners.contains(&node_id))
    }

    /// Set `group_id`'s voters and learners to the ones `leader` answered at
    /// `term`. Returns whether the view changed.
    ///
    /// Applies only while the leader hint names `leader` at `term`, so the
    /// caller confirms the leader first (see [`RoutingTable::confirm_leader`]).
    /// An answer from a leader of an older term never passes this check once
    /// the hint moved to a newer term.
    pub fn adopt_leader_membership(
        &mut self,
        group_id: u64,
        leader: u64,
        term: u64,
        voters: &[u64],
        learners: &[u64],
    ) -> bool {
        let Some(info) = self.group_info(group_id) else {
            return false;
        };
        if leader == 0 || term == 0 || info.leader != leader || info.leader_term != term {
            return false;
        }
        let mut voters = voters.to_vec();
        voters.sort_unstable();
        voters.dedup();
        let mut learners = learners.to_vec();
        learners.sort_unstable();
        learners.dedup();
        let mut members_now = info.members.clone();
        members_now.sort_unstable();
        let mut learners_now = info.learners.clone();
        learners_now.sort_unstable();
        if members_now == voters && learners_now == learners {
            return false;
        }
        self.set_group_members(group_id, voters);
        self.set_group_learners(group_id, learners);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_confirmed_leaders_membership_replaces_the_view() {
        let mut rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        assert!(rt.confirm_leader(1, 2, 5));
        assert!(rt.adopt_leader_membership(1, 2, 5, &[2, 1], &[4]));
        let info = rt.group_info(1).expect("group 1");
        assert_eq!(info.members, vec![1, 2]);
        assert_eq!(info.learners, vec![4]);
        // The same answer again changes nothing.
        assert!(!rt.adopt_leader_membership(1, 2, 5, &[1, 2], &[4]));
    }

    #[test]
    fn an_answer_the_hint_does_not_name_is_ignored() {
        let mut rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        assert!(rt.confirm_leader(1, 2, 5));
        // Another leader, or the same leader at another term.
        assert!(!rt.adopt_leader_membership(1, 3, 5, &[3], &[]));
        assert!(!rt.adopt_leader_membership(1, 2, 4, &[2], &[]));
        // A deposed leader's answer after the hint moved on.
        assert!(rt.confirm_leader(1, 3, 6));
        assert!(!rt.adopt_leader_membership(1, 2, 5, &[2], &[]));
        let mut members = rt.group_info(1).expect("group 1").members.clone();
        members.sort_unstable();
        assert_eq!(members, vec![1, 2, 3]);
        // An unknown group.
        assert!(!rt.adopt_leader_membership(9, 2, 5, &[2], &[]));
    }
}
