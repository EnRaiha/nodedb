// SPDX-License-Identifier: BUSL-1.1

//! Unmount the replica of a data group this node has left.
//!
//! Placement convergence removes a node from a group it is no longer placed
//! in: `RemoveNode` for a voter, `RemoveLearner` for a learner. The leader
//! tells the departing node its removal committed, so the node applies it
//! and its routing view stops listing it. When that one message is lost, the
//! leader probe learns the group's membership from its leader and the
//! routing view drops the node the same way (see the leader probe in
//! [`super::tick`]). Its replica then only holds a log the group no longer sends to. This step drops that replica, so a node
//! outside a group's placement hosts no replica of it.
//!
//! A replica is dropped only once all of these hold:
//! - the group's placement is authored and names other nodes only;
//! - this node's routing view lists it as neither voter nor learner;
//! - this node does not lead the group.
//!
//! `mount_entering_groups` mounts only a group whose placement names this
//! node, so the two steps never undo each other. The metadata group and
//! the sequencer group are never unmounted.

use std::collections::HashSet;

use tracing::debug;

use crate::forward::PlanExecutor;
use crate::routing::RoutingTable;

use super::loop_core::{CommitApplier, RaftLoop};

/// Hosted data groups this node has left, per the rules in the module docs.
/// `leading` holds the hosted groups this node leads. Returned sorted
/// ascending. Pure and deterministic.
pub(super) fn plan_unmounts(
    self_id: u64,
    hosted: &HashSet<u64>,
    leading: &HashSet<u64>,
    routing: &RoutingTable,
) -> Vec<u64> {
    let mut out: Vec<u64> = hosted
        .iter()
        .copied()
        .filter(|gid| {
            *gid != crate::metadata_group::METADATA_GROUP_ID
                && *gid != crate::calvin::sequencer::SEQUENCER_GROUP_ID
                && !leading.contains(gid)
        })
        .filter(|gid| {
            routing.group_info(*gid).is_some_and(|info| {
                info.placement
                    .as_ref()
                    .is_some_and(|placement| !placement.contains(&self_id))
                    && !info.members.contains(&self_id)
                    && !info.learners.contains(&self_id)
            })
        })
        .collect();
    out.sort_unstable();
    out
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Drop the replica of every data group this node has left.
    ///
    /// Never waits on disk. A dropped group's Raft storage closes on a
    /// blocking thread. The routing table is saved by the routing persister,
    /// so a restart mounts only the groups the table still lists this node
    /// in. The persister retries a failed save.
    pub(super) fn unmount_left_groups(&self) {
        let dropped = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            let hosted: HashSet<u64> = group_ids.iter().copied().collect();
            let leading: HashSet<u64> = group_ids
                .iter()
                .copied()
                .filter(|gid| mr.group_role_is_leader(*gid))
                .collect();
            let left = {
                let routing = mr.routing();
                let table = routing.read().unwrap_or_else(|p| p.into_inner());
                plan_unmounts(self.node_id, &hosted, &leading, &table)
            };
            let mut dropped = Vec::new();
            for group_id in left {
                if let Some(node) = mr.unmount_group(group_id) {
                    self.tick_state.clear_conf_save(group_id);
                    debug!(
                        group_id,
                        node_id = self.node_id,
                        "unmount: dropped the replica of a group this node left"
                    );
                    dropped.push(node);
                }
            }
            dropped
        };
        if dropped.is_empty() {
            return;
        }
        // Closing a group's log storage can write to disk.
        tokio::task::spawn_blocking(move || drop(dropped));
        if let Some(persister) = self.routing_persister.as_ref() {
            persister.request();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> RoutingTable {
        // Data groups 1 and 2 over nodes 1..=3, metadata group 0.
        RoutingTable::uniform(2, &[1, 2, 3], 3)
    }

    fn set(ids: &[u64]) -> HashSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn a_left_group_outside_the_placement_unmounts() {
        let mut rt = table();
        rt.set_placement(1, vec![2, 3]);
        rt.set_group_members(1, vec![2, 3]);
        assert_eq!(plan_unmounts(1, &set(&[0, 1, 2]), &set(&[]), &rt), vec![1]);
    }

    #[test]
    fn a_group_that_still_lists_this_node_stays() {
        let mut rt = table();
        // Placement excludes node 1, but its removal has not applied yet.
        rt.set_placement(1, vec![2, 3]);
        assert!(plan_unmounts(1, &set(&[1]), &set(&[]), &rt).is_empty());
        // A learner entry keeps it too.
        rt.set_group_members(1, vec![2, 3]);
        rt.set_group_learners(1, vec![1]);
        assert!(plan_unmounts(1, &set(&[1]), &set(&[]), &rt).is_empty());
    }

    #[test]
    fn an_unauthored_or_including_placement_keeps_the_group() {
        let mut rt = table();
        rt.set_group_members(1, vec![2, 3]);
        // No placement authored yet.
        assert!(plan_unmounts(1, &set(&[1]), &set(&[]), &rt).is_empty());
        // The placement names this node: it is entering, not leaving.
        rt.set_placement(1, vec![1, 2]);
        assert!(plan_unmounts(1, &set(&[1]), &set(&[]), &rt).is_empty());
    }

    #[test]
    fn a_led_metadata_or_sequencer_group_is_never_unmounted() {
        let mut rt = table();
        rt.set_placement(1, vec![2, 3]);
        rt.set_group_members(1, vec![2, 3]);
        assert!(plan_unmounts(1, &set(&[1]), &set(&[1]), &rt).is_empty());
        rt.set_placement(0, vec![2, 3]);
        rt.set_group_members(0, vec![2, 3]);
        assert!(plan_unmounts(1, &set(&[0]), &set(&[]), &rt).is_empty());
        let seq = crate::calvin::sequencer::SEQUENCER_GROUP_ID;
        assert!(plan_unmounts(1, &set(&[seq]), &set(&[]), &rt).is_empty());
    }
}
