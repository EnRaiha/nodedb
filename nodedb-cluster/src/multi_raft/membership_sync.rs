// SPDX-License-Identifier: BUSL-1.1

//! Bring a group's Raft membership in line with the routing table.
//!
//! A snapshot install covers conf changes this node never applied. The
//! routing table the install wrote holds their outcome, so the group's voter
//! and learner sets are set from it.

use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

impl MultiRaft {
    /// Set `group_id`'s voters and learners to its routing entry.
    ///
    /// A voter the routing lists and this node holds as a learner is
    /// promoted. This node's own membership is never removed here: a node
    /// the routing drops learns that from the next committed conf change.
    pub fn sync_group_membership_from_routing(&mut self, group_id: u64) -> Result<()> {
        let self_id = self.node_id;
        let (members, learners) = {
            let routing = self.routing.read().unwrap_or_else(|p| p.into_inner());
            let Some(info) = routing.group_info(group_id) else {
                return Ok(());
            };
            (info.members.clone(), info.learners.clone())
        };
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;

        if members.contains(&self_id) {
            node.promote_self_to_voter();
        }
        for &member in members.iter().filter(|&&id| id != self_id) {
            if node.learners().contains(&member) {
                node.promote_learner(member);
            } else {
                node.add_peer(member);
            }
        }
        let stale_voters: Vec<u64> = node
            .peers()
            .iter()
            .copied()
            .filter(|id| !members.contains(id))
            .collect();
        for voter in stale_voters {
            node.remove_peer(voter);
        }
        for &learner in learners.iter().filter(|&&id| id != self_id) {
            node.add_learner(learner);
        }
        let stale_learners: Vec<u64> = node
            .learners()
            .iter()
            .copied()
            .filter(|id| !learners.contains(id) && !members.contains(id))
            .collect();
        for learner in stale_learners {
            node.remove_learner(learner);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::routing::RoutingTable;

    fn multi_raft(dir: &Path) -> MultiRaft {
        let mut mr = MultiRaft::new(1, RoutingTable::uniform(1, &[1, 2], 2), dir.to_path_buf());
        mr.add_group(0, vec![2]).unwrap();
        mr
    }

    #[test]
    fn voters_and_learners_follow_the_routing_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut mr = multi_raft(dir.path());
        {
            let routing = mr.routing();
            let mut rt = routing.write().unwrap();
            rt.set_group_members(0, vec![1, 3]);
            rt.set_group_learners(0, vec![4]);
        }
        mr.sync_group_membership_from_routing(0).unwrap();
        let node = mr.groups_mut().get(&0).unwrap();
        assert_eq!(node.peers(), &[3]);
        assert_eq!(node.learners(), &[4]);
    }
}
