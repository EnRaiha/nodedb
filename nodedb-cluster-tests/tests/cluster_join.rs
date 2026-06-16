// SPDX-License-Identifier: BUSL-1.1

//! Integration test: 3 nodes, one bootstraps, two join over QUIC.
//!
//! Drives the production server-side code path: joining nodes send
//! `RaftRpc::JoinRequest` to the bootstrap leader, whose `RaftLoop` is
//! serving the transport. Asserts that all three nodes converge on a
//! 3-member topology within 10 seconds.

mod cluster_common;

use std::time::Duration;

use nodedb_cluster::ClusterLifecycleState;

use cluster_common::{TestNode, wait_for};

/// Spawns 3 in-process cluster nodes on loopback. Node 1 bootstraps. Nodes
/// 2 and 3 join via node 1 using the production `RaftLoop` RPC handler.
/// Within 10 seconds every node's topology must contain all 3 members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_join_over_quic() {
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");

    // Give node 1's serve loop a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let seeds = vec![node1.listen_addr()];

    let node2 = TestNode::spawn(2, seeds.clone())
        .await
        .expect("node 2 join");
    let node3 = TestNode::spawn(3, seeds).await.expect("node 3 join");

    let nodes = [&node1, &node2, &node3];
    wait_for(
        "all 3 nodes report topology_size == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || nodes.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    assert_eq!(node1.node_id, 1);
    assert_eq!(node2.node_id, 2);
    assert_eq!(node3.node_id, 3);

    for n in nodes {
        assert!(
            matches!(n.lifecycle_state(), ClusterLifecycleState::Ready { .. }),
            "node {} should be Ready, got {:?}",
            n.node_id,
            n.lifecycle_state()
        );
    }

    node3.shutdown().await;
    node2.shutdown().await;
    node1.shutdown().await;
}

/// Minimal smoke variant — a single node still bootstraps cleanly.
///
/// Guards against the "fix the join path and accidentally break the
/// single-node path" regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_bootstrap_still_works() {
    let node = TestNode::spawn(1, vec![])
        .await
        .expect("single-node bootstrap");
    assert_eq!(node.topology_size(), 1);
    node.shutdown().await;
}
