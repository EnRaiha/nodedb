// SPDX-License-Identifier: BUSL-1.1

//! A node that hosts no replica of a group follows leader redirects by term.
//!
//! With a replication factor of 2, one of the three nodes, the outsider,
//! hosts no replica of a data group. Its Raft never observes that group's
//! leader, so its routing hint moves by redirects. After the group elects a
//! new leader, the outsider's leader probe asks the old leader its hint
//! names, which redirects it with the new leader and the new term. The hint
//! then names the new leader at the new term, and the next read-index
//! request reaches it. A later redirect at the old term, through the
//! gateway's retry path, does not move the hint back.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::time::Duration;

use nodedb::control::gateway::retry::retry_not_leader;
use nodedb::types::VShardId;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(20);
const READ_INDEX_TIMEOUT: Duration = Duration::from_secs(5);
/// Each bounce elects either replica. Ten bounces that all re-elect the
/// same replica mean the election is not moving at all.
const MAX_BOUNCES: usize = 10;

/// The routing hint of `group_id` on `node`: `(leader, leader_term)`.
fn hint(node: &TestClusterNode, group_id: u64) -> (u64, u64) {
    node.shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_info(group_id)
        .map_or((0, 0), |info| (info.leader, info.leader_term))
}

/// The leader of `group_id` as `node`'s Raft reports it, `0` when none.
fn raft_leader(node: &TestClusterNode, group_id: u64) -> u64 {
    node.all_group_leaders()
        .into_iter()
        .find(|&(group, _)| group == group_id)
        .map_or(0, |(_, leader)| leader)
}

fn node_by_id(cluster: &TestCluster, node_id: u64) -> &TestClusterNode {
    cluster
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .expect("a cluster member")
}

/// Wait until both replicas' Raft agree on a leader of `group_id`, and both
/// replicas' routing hints name it at the same term. Returns that
/// `(leader, term)`.
async fn settled_leader(cluster: &TestCluster, group_id: u64, replicas: &[u64]) -> (u64, u64) {
    wait_for("both replicas settle on one leader", CONVERGE, STEP, || {
        let hints: Vec<(u64, u64)> = replicas
            .iter()
            .map(|&id| hint(node_by_id(cluster, id), group_id))
            .collect();
        let (leader, term) = hints[0];
        leader != 0
            && term != 0
            && hints.iter().all(|&h| h == (leader, term))
            && replicas
                .iter()
                .all(|&id| raft_leader(node_by_id(cluster, id), group_id) == leader)
    })
    .await;
    hint(node_by_id(cluster, replicas[0]), group_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_outsider_follows_a_redirect_to_the_new_leader_and_ignores_a_stale_one() {
    let mut cluster = TestCluster::spawn_three_with_replication_factor(2)
        .await
        .expect("spawn 3-node cluster with replication factor 2");

    // A vShard-owning data group with two replicas, and the node outside it.
    let (group_id, replicas, vshard_id) = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing
            .group_ids()
            .into_iter()
            .filter(|&group| group != nodedb_cluster::METADATA_GROUP_ID)
            .find_map(|group| {
                let info = routing.group_info(group)?;
                let vshard = routing.vshards_for_group(group).first().copied()?;
                (info.members.len() == 2).then(|| (group, info.members.clone(), vshard))
            })
            .expect("a data group with two replicas")
    };
    let outsider_id = cluster
        .nodes
        .iter()
        .map(|node| node.node_id)
        .find(|id| !replicas.contains(id))
        .expect("a node outside the group");
    assert!(
        !node_by_id(&cluster, outsider_id).hosts_data_group(group_id),
        "node {outsider_id} must host no replica of group {group_id}"
    );

    // Bounce the follower until the group elects the other replica. With its
    // follower down, the leader loses quorum contact and steps down, so the
    // follower's return starts a new election at a higher term.
    let (mut old_leader, mut old_term) = settled_leader(&cluster, group_id, &replicas).await;
    let mut bounces = 0;
    let (new_leader, new_term) = loop {
        bounces += 1;
        assert!(
            bounces <= MAX_BOUNCES,
            "group {group_id} re-elected node {old_leader} {MAX_BOUNCES} times"
        );
        let follower = replicas
            .iter()
            .copied()
            .find(|&id| id != old_leader)
            .expect("the group's other replica");
        let index = cluster
            .nodes
            .iter()
            .position(|node| node.node_id == follower)
            .expect("the follower is a member");
        let stopped = cluster.stop_member(index).await.expect("stop the follower");
        {
            let leader_node = node_by_id(&cluster, old_leader);
            wait_for(
                "the leader steps down without its quorum",
                CONVERGE,
                STEP,
                || raft_leader(leader_node, group_id) != old_leader,
            )
            .await;
        }
        cluster
            .restart_member(stopped)
            .await
            .expect("restart the follower");
        let (leader, term) = settled_leader(&cluster, group_id, &replicas).await;
        assert!(term > old_term, "the new election runs at a higher term");
        if leader != old_leader {
            break (leader, term);
        }
        old_term = term;
        old_leader = leader;
    };

    let outsider = node_by_id(&cluster, outsider_id);
    wait_for(
        "the outsider sees every node active",
        CONVERGE,
        STEP,
        || outsider.active_topology_size() == 3,
    )
    .await;
    let routing = outsider
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing");

    // The outsider's leader probe asks the node its hint names. The old
    // leader redirects it, so the hint moves to the new leader at the new
    // term.
    wait_for(
        "the outsider's hint follows the new leader",
        CONVERGE,
        STEP,
        || hint(outsider, group_id) == (new_leader, new_term),
    )
    .await;

    // The next request reaches the new leader.
    let gate = outsider
        .shared
        .raft_read_gate
        .get()
        .expect("raft read gate")
        .clone();
    let read = gate.read_index(group_id, READ_INDEX_TIMEOUT).await;
    assert!(
        read.is_ok(),
        "the new leader {new_leader} answers the read index: {read:?}"
    );

    // A stale redirect naming the old leader at the old term, through the
    // gateway's retry path, leaves the hint on the new leader.
    let _ = retry_not_leader(Some(&**routing), |attempt| async move {
        if attempt == 0 {
            Err(nodedb::Error::NotLeader {
                vshard_id: VShardId::new(vshard_id),
                leader_node: old_leader,
                leader_addr: String::new(),
                leader_term: old_term,
            })
        } else {
            Ok::<(), nodedb::Error>(())
        }
    })
    .await;
    assert_eq!(
        hint(outsider, group_id),
        (new_leader, new_term),
        "a redirect at the old term never moves the hint back"
    );

    cluster.shutdown().await;
}
