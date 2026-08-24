// SPDX-License-Identifier: BUSL-1.1

//! A node that has missed a topology transition stands down from coordinating
//! queries until it catches up.
//!
//! The cluster epoch names the generation of the cluster's topology. A node
//! advances its own generation only by applying a committed
//! `ClusterEpochBump`; a higher number arriving on an inbound frame is evidence
//! about the SENDER, and tells the receiver only that it is behind.
//!
//! That gap is what these tests force. A node whose observed epoch has moved
//! past its applied one is planning work against a routing table the cluster
//! has already replaced, so it refuses to coordinate rather than answer from a
//! superseded map. It fences itself: it knows its own applied generation
//! exactly, and only ever guesses where its peers are.
//!
//! Forcing this at all depends on the epoch being per-node. While it lived in a
//! process-global atomic, every node in this test process shared one value and
//! could never disagree — the condition under test was unrepresentable.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

/// A node's epoch state, or a panic naming which node was not clustered — an
/// unset handle means `start_raft` never published it, which would make every
/// assertion below vacuous.
fn epoch_of(
    cluster: &TestCluster,
    index: usize,
) -> &std::sync::Arc<nodedb_cluster::ClusterEpochState> {
    cluster.nodes[index]
        .shared
        .cluster_epoch
        .get()
        .unwrap_or_else(|| panic!("node at index {index} has no published cluster epoch"))
}

/// Whether `err` is the refusal a node gives while its topology view is behind.
///
/// The SQLSTATE alone is not enough to identify it: "no leader is currently
/// serving this range" is retriable for a different reason and shares the code.
/// Matching the message keeps this test honest about WHICH refusal it saw.
fn is_superseded_view_refusal(err: &tokio_postgres::Error) -> bool {
    err.as_db_error().is_some_and(|db| {
        db.code().code() == nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER
            && db.message().contains("cluster generation")
    })
}

/// Nodes in one process hold their own generations. If this fails, every other
/// assertion here is meaningless: the nodes would be sharing one epoch and
/// could never disagree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_nodes_hold_independent_epochs() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    let first = epoch_of(&cluster, 0);
    let second = epoch_of(&cluster, 1);
    assert!(
        !std::sync::Arc::ptr_eq(first, second),
        "each node must own its epoch state, not share one process-wide value"
    );

    first.observe(first.applied() + 5);
    assert!(first.is_behind());
    assert!(
        !second.is_behind(),
        "one node falling behind must not drag its neighbours with it"
    );

    cluster.shutdown().await;
}

/// The fence fires: a node that has only OBSERVED a newer generation refuses to
/// coordinate a query, and says so with a retriable error rather than answering
/// from a routing table the cluster has moved past.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_behind_the_cluster_epoch_refuses_to_coordinate() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION fenced (id TEXT PRIMARY KEY, payload TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "collection visible on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    let node = &cluster.nodes[0];
    let epoch = epoch_of(&cluster, 0);
    let applied_before = epoch.applied();

    // Queries are served normally while the node is current.
    node.client
        .simple_query("SELECT id FROM fenced")
        .await
        .expect("a current node serves queries");

    // A peer's frame carries a generation this node has not applied.
    epoch.observe(applied_before + 1);
    assert!(epoch.is_behind(), "the node must now know it is behind");

    let err = node
        .client
        .simple_query("SELECT id FROM fenced")
        .await
        .expect_err("a node behind the cluster epoch must refuse to coordinate");
    assert!(
        is_superseded_view_refusal(&err),
        "the refusal must name the superseded topology view, not some other \
         retriable condition sharing the same SQLSTATE, got: {err}"
    );

    // Observing did not move what this node claims to have applied.
    assert_eq!(
        epoch.applied(),
        applied_before,
        "overhearing a peer must never advance this node's own generation"
    );

    cluster.shutdown().await;
}

/// The fence clears itself. Applying the bump this node had only overheard
/// closes the gap, and queries resume with no operator action.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn applying_the_missed_generation_lifts_the_fence() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION unfenced (id TEXT PRIMARY KEY, payload TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "collection visible on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    let node = &cluster.nodes[0];
    let epoch = epoch_of(&cluster, 0);
    let missed = epoch.applied() + 1;

    epoch.observe(missed);
    node.client
        .simple_query("SELECT id FROM unfenced")
        .await
        .expect_err("fenced while behind");

    // The metadata group delivers the bump this node had only heard about.
    epoch.advance_applied(missed);
    assert!(!epoch.is_behind());

    node.client
        .simple_query("SELECT id FROM unfenced")
        .await
        .expect("queries resume once the missed generation is applied");

    cluster.shutdown().await;
}

/// An epoch OLDER than this node's own moves nothing. A lagging peer is the
/// peer's problem; it must not fence the node that is ahead of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_peer_does_not_fence_the_node_ahead_of_it() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION ahead (id TEXT PRIMARY KEY, payload TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "collection visible on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    let node = &cluster.nodes[0];
    let epoch = epoch_of(&cluster, 0);
    epoch.advance_applied(9);

    // A peer still on an older generation reports it.
    epoch.observe(4);
    assert!(!epoch.is_behind(), "an older peer stamp must not fence us");

    node.client
        .simple_query("SELECT id FROM ahead")
        .await
        .expect("a node ahead of a lagging peer keeps serving");

    cluster.shutdown().await;
}
