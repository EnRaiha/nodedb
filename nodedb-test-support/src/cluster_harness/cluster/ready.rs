// SPDX-License-Identifier: BUSL-1.1

//! The convergence barriers a cluster passes before a test issues anything:
//! topology size, metadata-group leader stability, and data groups settled
//! on their placement and leader. A fresh bringup and an in-place restart
//! both wait here.

use std::collections::HashMap;
use std::time::Duration;

use super::TestCluster;
use crate::cluster_harness::TestClusterNode;
use crate::cluster_harness::wait::{wait_for, wait_for_report};

/// Which leader a settled data group must have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LeaderBar {
    /// The group's preferred leader. The leader balance moves every group's
    /// leader there as the cluster forms.
    Preferred,
    /// Any leader. A restart elects its leaders against other voters, and
    /// the leader balance holds such a win for ten election timeouts.
    Elected,
}

impl TestCluster {
    /// Wait until every node agrees on the topology, the metadata group has
    /// one leader every node sees, and every data group has settled with a
    /// leader that meets `bar`.
    pub(super) async fn await_ready(&self, bar: LeaderBar) {
        let node_count = self.nodes.len();
        wait_for(
            "every node reports the full topology",
            Duration::from_secs(30),
            Duration::from_millis(50),
            || self.nodes.iter().all(|n| n.topology_size() == node_count),
        )
        .await;

        // CRITICAL: wait for the metadata Raft group to elect a leader
        // and for every node's local view to agree on the same leader id.
        //
        // Topology convergence only guarantees membership is agreed; it
        // says nothing about election state. Under heavy host load (e.g. running this test
        // immediately after another full-suite cluster test exits and
        // the unit-test pool ramps back up), the initial Raft heartbeat
        // window can be missed and the first `acquire`/`propose` issued
        // by the test races a re-election — surfacing as
        // `raft error: not leader (leader hint: None)` from a
        // descriptor-lease or DDL call.
        //
        // Waiting until every node reports the same non-zero leader id
        // closes the window deterministically: no retries, no flakes, no
        // wasted CI minutes on cleanup of a doomed cluster bringup.
        wait_for(
            "metadata group has stable leader visible on every node",
            Duration::from_secs(30),
            Duration::from_millis(20),
            || {
                let leaders: Vec<u64> = self
                    .nodes
                    .iter()
                    .map(|n| n.metadata_group_leader())
                    .collect();
                let first = leaders[0];
                first != 0 && leaders.iter().all(|&l| l == first)
            },
        )
        .await;

        // CRITICAL: wait for EVERY data Raft group to settle before the
        // test issues anything. Without this barrier, the first data-group
        // write can race a still-electing group: a proposer that thinks it
        // leads gets a `log_index` that never commits, and an unrelated
        // entry at that index wakes its waiter.
        //
        // A data group has settled once:
        // - its replicas are exactly its placement, as every replica's
        //   routing view records it, and no other node hosts a replica.
        //   With a replication factor below the node count, the nodes
        //   outside a group's placement host none of it;
        // - every replica's Raft names one leader that meets `bar`;
        // - every replica's routing hint names that leader, and every
        //   other node's hint names a replica;
        // - every node's routing view lists the replicas as the group's
        //   voters, with no learners. A node outside the group learns its
        //   membership from the group leader's answer to its leader probe.
        //
        // The Calvin sequencer group is not part of the routing topology.
        // Calvin tests gate on it separately (`wait_for_sequencer_leader`).
        wait_for_report(
            "every data group has settled on its placement and leader",
            Duration::from_secs(30),
            Duration::from_millis(20),
            || self.data_groups_settled(bar),
        )
        .await;
    }

    /// Every data group's leader, as a replica's Raft reports it:
    /// `group_id → leader`. A group no replica knows a leader of is absent.
    ///
    /// No single node answers this. With a replication factor below the
    /// node count, a node hosts only the groups placed on it.
    pub fn data_group_leaders(&self) -> HashMap<u64, u64> {
        let mut leaders = HashMap::new();
        for node in &self.nodes {
            for (group_id, leader) in node.all_group_leaders() {
                if group_id == nodedb_cluster::METADATA_GROUP_ID
                    || group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID
                    || leader == 0
                    || !node.replicates_data_group(group_id)
                {
                    continue;
                }
                leaders.entry(group_id).or_insert(leader);
            }
        }
        leaders
    }

    /// `Ok` once every data group has settled, as [`Self::await_ready`]
    /// describes. `Err` names each group and node that has not, and why.
    fn data_groups_settled(&self, bar: LeaderBar) -> Result<(), String> {
        let routing = self
            .nodes
            .first()
            .and_then(|n| n.shared.cluster_routing.as_ref())
            .ok_or("node 1 has no routing table")?;
        let group_ids: Vec<u64> = routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_ids()
            .into_iter()
            .filter(|gid| {
                *gid != nodedb_cluster::METADATA_GROUP_ID
                    && *gid != nodedb_cluster::calvin::SEQUENCER_GROUP_ID
            })
            .collect();
        if group_ids.is_empty() {
            return Err("node 1's routing table holds no data group".into());
        }
        let reasons: Vec<String> = group_ids
            .into_iter()
            .filter_map(|gid| {
                self.data_group_settled(gid, bar)
                    .err()
                    .map(|why| format!("group {gid}: {why}"))
            })
            .collect();
        if reasons.is_empty() {
            Ok(())
        } else {
            Err(reasons.join("; "))
        }
    }

    fn data_group_settled(&self, group_id: u64, bar: LeaderBar) -> Result<(), String> {
        let replicas: Vec<&TestClusterNode> = self
            .nodes
            .iter()
            .filter(|n| n.replicates_data_group(group_id))
            .collect();
        if replicas.is_empty() {
            return Err("no node replicates it".into());
        }
        let stale: Vec<u64> = self
            .nodes
            .iter()
            .filter(|n| n.hosts_data_group(group_id) && !n.replicates_data_group(group_id))
            .map(|n| n.node_id)
            .collect();
        if !stale.is_empty() {
            return Err(format!("nodes {stale:?} host a replica they left"));
        }
        let mut replica_ids: Vec<u64> = replicas.iter().map(|n| n.node_id).collect();
        replica_ids.sort_unstable();

        let leader = raft_leader(replicas[0], group_id);
        if leader == 0 {
            return Err(format!(
                "replica {} knows no leader; replicas {replica_ids:?}",
                replicas[0].node_id
            ));
        }
        for replica in &replicas {
            let node = replica.node_id;
            // Raft status locks `MultiRaft`, which reads routing under that lock.
            // It must be read before this routing guard is taken.
            let raft = raft_leader(replica, group_id);
            let routing = replica
                .shared
                .cluster_routing
                .as_ref()
                .ok_or(format!("node {node} has no routing table"))?;
            let routing = routing.read().unwrap_or_else(|p| p.into_inner());
            let info = routing
                .group_info(group_id)
                .ok_or(format!("node {node} has no routing entry"))?;
            let mut placement = routing.effective_placement(group_id);
            placement.sort_unstable();
            let mut members = info.members.clone();
            members.sort_unstable();
            if placement != replica_ids || members != replica_ids || !info.learners.is_empty() {
                return Err(format!(
                    "node {node}: replicas {replica_ids:?}, placement {placement:?}, members \
                     {members:?}, learners {:?}",
                    info.learners
                ));
            }
            if raft != leader || info.leader != leader {
                return Err(format!(
                    "node {node}: raft leader {raft}, hint ({}, term {}), replica {} names {leader}",
                    info.leader, info.leader_term, replicas[0].node_id
                ));
            }
            if bar == LeaderBar::Preferred {
                let preferred = nodedb_cluster::rebalancer::preferred_leaders(&routing)
                    .get(&group_id)
                    .copied();
                if preferred != Some(leader) {
                    return Err(format!(
                        "node {node}: leader {leader}, preferred leader {preferred:?}"
                    ));
                }
            }
        }
        for node in &self.nodes {
            let view = node.shared.cluster_routing.as_ref().and_then(|routing| {
                routing
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_info(group_id)
                    .map(|info| {
                        let mut members = info.members.clone();
                        members.sort_unstable();
                        (
                            info.leader,
                            info.leader_term,
                            members,
                            info.learners.clone(),
                        )
                    })
            });
            let Some((leader, term, members, learners)) = view else {
                return Err(format!("node {}: no routing entry", node.node_id));
            };
            if !replica_ids.contains(&leader) {
                return Err(format!(
                    "node {}: hint ({leader}, term {term}) names no replica of {replica_ids:?}",
                    node.node_id
                ));
            }
            if members != replica_ids || !learners.is_empty() {
                return Err(format!(
                    "node {}: members {members:?}, learners {learners:?}, replicas \
                     {replica_ids:?}",
                    node.node_id
                ));
            }
        }
        Ok(())
    }
}

/// The leader of `group_id` as `node`'s Raft reports it, `0` when none.
fn raft_leader(node: &TestClusterNode, group_id: u64) -> u64 {
    node.all_group_leaders()
        .into_iter()
        .find(|&(group, _)| group == group_id)
        .map_or(0, |(_, leader)| leader)
}
