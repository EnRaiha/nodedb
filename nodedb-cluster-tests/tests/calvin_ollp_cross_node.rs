// SPDX-License-Identifier: BUSL-1.1

//! Cross-node OLLP dependent-write test.
//!
//! Validates that a dependent (value-predicate ⇒ `BulkDelete`) cross-shard Calvin
//! write submitted on a NON-sequencer-leader coordinator completes. The
//! coordinator owns the OLLP retry loop (`run_dependent_with_retry`); its `submit`
//! step routes the inbox submit to the sequencer-group leader via
//! `submit_calvin_routed_assign` (the only node whose sequencer service assigns)
//! and awaits completion on its local registry (which receives the replicated
//! completion ack on every sequencer-group member), while still passing through
//! the coordinator's circuit-breaker / tenant-budget gate.
//!
//! Steps:
//! 1. Bring up the standard 3-node cluster (`TestCluster::spawn_three`).
//! 2. Create a collection and wait for convergence on every node plus a stable
//!    sequencer-group leader.
//! 3. INSERT 3 rows; wait for full apply convergence.
//! 4. Pick a coordinator node that is NOT the sequencer leader.
//! 5. From that non-leader coordinator, run
//!    `DELETE FROM <coll> WHERE status = 'inactive'` — a non-PK predicate ⇒
//!    `BulkDelete` ⇒ the dependent/OLLP path. Assert it SUCCEEDS.
//! 6. Assert only the 2 'active' rows remain (the delete actually committed).
//! 7. Shut down.
//!
//! This validates the HAPPY path only. It does NOT force a predicate-drift
//! mismatch — mismatch / re-scan / exhaustion is already covered by the
//! coordinator-loop unit tests (`retry_loop_tests.rs`).
//!
//! `spawn_three` gives RF=3 (every shard on every node), so the coordinator's
//! local pre-exec reconnaissance scan sees the inserted rows without any special
//! scan routing.
//!
//! File name contains "cluster" via the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0` if
/// no leader is known yet.
fn sequencer_leader(node: &common::cluster_harness::TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ollp_dependent_delete_from_non_leader_coordinator_completes() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let coll = "ollp_xnode_delete";

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, status TEXT)"
        ))
        .await
        .expect("CREATE COLLECTION");

    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    // Wait for a stable sequencer-group leader visible cluster-wide.
    wait_for(
        "sequencer-group leader elected and visible on every node",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| sequencer_leader(n) != 0)
                && cluster
                    .nodes
                    .iter()
                    .all(|n| sequencer_leader(n) == sequencer_leader(&cluster.nodes[0]))
        },
    )
    .await;

    let leader = sequencer_leader(&cluster.nodes[0]);
    assert_ne!(leader, 0, "sequencer leader must be elected");

    // Insert 3 rows from node 0; two 'active', one 'inactive'.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {coll} (id, status) VALUES \
             ('a', 'active'), ('b', 'inactive'), ('c', 'active')"
        ))
        .await
        .expect("INSERT 3 rows");

    // Deterministic barrier: every Raft group has fully propagated the inserts.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Pick a coordinator node that is NOT the sequencer leader — this is the case
    // the routed OLLP submit must handle: a non-leader coordinator drives the
    // dependent cross-shard write to completion.
    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.shared.node_id != leader)
        .expect("a non-sequencer-leader coordinator must exist in a 3-node cluster");
    assert_ne!(
        coordinator.shared.node_id, leader,
        "coordinator must not be the sequencer leader for this test to be meaningful"
    );

    // Enable strict cross-shard mode so the predicate DELETE routes through Calvin.
    coordinator
        .client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // The dependent write: a non-PK predicate ⇒ `BulkDelete` ⇒ the OLLP path. On a
    // non-leader coordinator this must complete (route to leader for assignment,
    // complete via the replicated ack on the local registry).
    coordinator
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE status = 'inactive'"))
        .await
        .expect(
            "dependent (BulkDelete) cross-shard write from a non-leader coordinator must complete",
        );

    // Prove the delete committed: only the 2 'active' rows remain.
    let count_rows = |msgs: &[tokio_postgres::SimpleQueryMessage]| -> usize {
        msgs.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()
    };

    let rows = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT all rows");
    assert_eq!(
        count_rows(&rows),
        2,
        "only the 2 'active' rows must remain after the routed OLLP delete"
    );

    cluster.shutdown().await;
}
