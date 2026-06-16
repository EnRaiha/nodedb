// SPDX-License-Identifier: BUSL-1.1

//! Integration test: 5 nodes start simultaneously, all with the
//! full 5-address seed list. Thanks to the deterministic
//! elected-bootstrapper rule (lexicographically smallest seed →
//! bootstrap, everyone else → join with retry-and-backoff), exactly
//! one cluster forms with all 5 members. No disjoint clusters
//! regardless of scheduler ordering.
//!
//! Exercises `probe::designated_bootstrapper` + the retry loop in
//! `bootstrap::join::join`.

mod cluster_common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::NexarTransport;

use cluster_common::{TestNode, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn five_nodes_race_on_full_seed_list_form_one_cluster() {
    // Pre-bind every transport so we can learn every ephemeral
    // address before any `start_cluster` call. This is what lets us
    // hand every node the full 5-address seed list at spawn time —
    // which is the whole point of the race scenario.
    const NODE_COUNT: u64 = 5;
    let mut transports: Vec<Arc<NexarTransport>> = Vec::with_capacity(NODE_COUNT as usize);
    for id in 1..=NODE_COUNT {
        transports.push(Arc::new(
            cluster_common::test_transport(id).expect("bind transport"),
        ));
    }
    let seeds: Vec<SocketAddr> = transports.iter().map(|t| t.local_addr()).collect();

    // Spawn all 5 nodes concurrently. The order in which the
    // `TestNode::spawn_with_transport` futures complete is up to
    // the tokio scheduler — whichever node runs its
    // `should_bootstrap` probe first might observe the others as
    // unreachable, but the single-elected-bootstrapper rule
    // ensures only the one with the lex-smallest seed actually
    // bootstraps. The other four enter the join loop and retry
    // with exponential backoff until the bootstrapper is up.
    let mut join_set: tokio::task::JoinSet<
        Result<TestNode, Box<dyn std::error::Error + Send + Sync>>,
    > = tokio::task::JoinSet::new();
    for (idx, transport) in transports.into_iter().enumerate() {
        let node_id = (idx as u64) + 1;
        let seeds_for_node = seeds.clone();
        join_set.spawn(async move {
            TestNode::spawn_with_transport(node_id, transport, seeds_for_node).await
        });
    }

    let mut nodes: Vec<TestNode> = Vec::with_capacity(NODE_COUNT as usize);
    while let Some(result) = join_set.join_next().await {
        let node = result
            .expect("spawn task did not panic")
            .expect("TestNode::spawn_with_transport succeeded");
        nodes.push(node);
    }
    assert_eq!(nodes.len(), NODE_COUNT as usize);

    // Wait for convergence. Allow a generous 30 s window — the
    // join loop's retry schedule goes up to 16 s per attempt, and
    // any node that loses its first probe might sleep a full
    // backoff cycle before the bootstrapper is up.
    {
        let nodes_ref = &nodes;
        wait_for(
            "all 5 nodes converge on topology_size == 5",
            Duration::from_secs(30),
            Duration::from_millis(200),
            || nodes_ref.iter().all(|n| n.topology_size() == 5),
        )
        .await;
    }

    // Assert exactly one cluster: every node's topology must
    // contain the same id set. Disjoint clusters would show up as
    // two nodes disagreeing on the member list.
    let reference_ids = nodes[0].topology_ids();
    assert_eq!(reference_ids.len(), NODE_COUNT as usize);
    assert_eq!(reference_ids, vec![1, 2, 3, 4, 5]);
    for n in &nodes {
        assert_eq!(
            n.topology_ids(),
            reference_ids,
            "node {} disagrees on cluster membership",
            n.node_id
        );
    }

    for n in nodes {
        n.shutdown().await;
    }
}
