// SPDX-License-Identifier: BUSL-1.1

//! Every node's routing leader hint follows its own Raft's elections.
//!
//! Killing a group leader makes SWIM clear the hint, and nothing in the
//! metadata log names the new leader. Each surviving node's Raft tick
//! writes the leader it observes, at its term, into its routing table. So
//! within one election timeout of the new leader's election, every survivor's
//! hint names it. Backup source selection, change-bus forwarding and the
//! gateway read that hint.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_report};

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(20);
/// The harness's fast election tuning: `election_timeout_max_ms`.
const ELECTION_TIMEOUT: Duration = Duration::from_millis(1_000);

/// Every hosted group's leader as `node`'s Raft reports it.
fn raft_leaders(node: &TestClusterNode) -> BTreeMap<u64, u64> {
    node.all_group_leaders().into_iter().collect()
}

/// The routing hint of `group_id` on `node`.
fn hinted_leader(node: &TestClusterNode, group_id: u64) -> u64 {
    node.shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_info(group_id)
        .map_or(0, |info| info.leader)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_survivor_hints_the_new_leader_within_an_election_timeout() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // A data group, and the node leading it.
    let (group_id, leader) = {
        let leaders = raft_leaders(&cluster.nodes[0]);
        leaders
            .into_iter()
            .find(|&(group_id, leader)| group_id != 0 && leader != 0)
            .expect("a data group with a leader")
    };
    wait_for(
        "every node hints the current leader",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| hinted_leader(node, group_id) == leader)
        },
    )
    .await;

    let idx = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .expect("the leader is a member");
    let dead = cluster.nodes.remove(idx);
    dead.shutdown().await;

    // Wait for the survivors' Raft to agree on a new leader.
    let nodes = &cluster.nodes;
    wait_for("the survivors elect a new leader", CONVERGE, STEP, || {
        let first = raft_leaders(&nodes[0]).get(&group_id).copied().unwrap_or(0);
        first != 0
            && first != leader
            && nodes
                .iter()
                .all(|node| raft_leaders(node).get(&group_id).copied() == Some(first))
    })
    .await;
    let elected = raft_leaders(&nodes[0])[&group_id];

    // Within one election timeout, every survivor's hint names it.
    let deadline = Instant::now() + ELECTION_TIMEOUT;
    loop {
        let hints: Vec<u64> = nodes
            .iter()
            .map(|node| hinted_leader(node, group_id))
            .collect();
        if hints.iter().all(|&hint| hint == elected) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "group {group_id}: survivors hint {hints:?}, the new leader is {elected}"
        );
        tokio::time::sleep(STEP).await;
    }

    // Every other routed group the survivors host hints its Raft leader too.
    // The Calvin sequencer group is not part of the routing topology, so no
    // node holds a hint for it.
    for node in nodes {
        for (group, raft_leader) in raft_leaders(node) {
            if group == nodedb_cluster::calvin::SEQUENCER_GROUP_ID
                || raft_leader == 0
                || raft_leader == leader
            {
                continue;
            }
            wait_for_report(
                "each hint follows its group's Raft leader",
                CONVERGE,
                STEP,
                || {
                    let current = raft_leaders(node).get(&group).copied();
                    let hint = hinted_leader(node, group);
                    if current != Some(raft_leader) || hint == raft_leader {
                        Ok(())
                    } else {
                        Err(format!(
                            "node {}: group {group}: raft leader {raft_leader}, hint {hint}",
                            node.node_id
                        ))
                    }
                },
            )
            .await;
        }
    }

    cluster.shutdown().await;
}
