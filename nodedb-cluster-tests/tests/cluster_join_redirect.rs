// SPDX-License-Identifier: BUSL-1.1

//! Integration test: a joining node that contacts a follower
//! first gets a `not leader; retry at <addr>` response, follows
//! the hint, and joins via the real group-0 leader.
//!
//! Exercises the redirect path in
//! `bootstrap::join::parse_leader_hint` + `try_join_once`.

mod cluster_common;

use std::time::Duration;

use cluster_common::{TestNode, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_follows_leader_redirect_from_follower() {
    // Start 3 nodes — node 1 is the bootstrap seed and therefore
    // leader of every group. Nodes 2 and 3 become voters via the
    // learner → promote pipeline.
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let seeds_for_2_3 = vec![node1.listen_addr()];
    let node2 = TestNode::spawn(2, seeds_for_2_3.clone())
        .await
        .expect("node 2 join");
    let node3 = TestNode::spawn(3, seeds_for_2_3)
        .await
        .expect("node 3 join");

    // Wait for 3-node convergence so we know nodes 2 and 3 are
    // followers with node 1 recorded as the leader of group 0.
    // Their `handle_join` on a fresh incoming `JoinRequest` will
    // therefore emit `not leader; retry at <node1_addr>`.
    let existing = [&node1, &node2, &node3];
    wait_for(
        "all 3 nodes converge on topology_size == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || existing.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    // Now spawn a 4th node. Seed list ordering matters: the join
    // loop pops the work list from the back, so the last entry is
    // tried first. Putting `node3_addr` last means node 4 hits the
    // follower first and must follow its redirect to node 1.
    let seeds_for_4 = vec![node1.listen_addr(), node3.listen_addr()];
    let node4 = TestNode::spawn(4, seeds_for_4)
        .await
        .expect("node 4 redirect-driven join");

    // Within 10 s every node must see a 4-member topology.
    let all = [&node1, &node2, &node3, &node4];
    wait_for(
        "all 4 nodes converge on topology_size == 4",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || all.iter().all(|n| n.topology_size() == 4),
    )
    .await;

    assert_eq!(node4.topology_ids(), vec![1, 2, 3, 4]);

    node4.shutdown().await;
    node3.shutdown().await;
    node2.shutdown().await;
    node1.shutdown().await;
}
