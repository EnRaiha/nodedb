// SPDX-License-Identifier: BUSL-1.1
//! A linearizable read is served only by a node that can prove it leads.
//!
//! Believing you are the leader is not the same as being one. A partition
//! does not notify the node it cut off, so a deposed leader keeps answering
//! from a log the rest of the cluster has already moved past. Every read
//! below asks the same question: does this node answer from a belief, or
//! from a quorum?
//!
//! The consistency level is what decides it. `strong` — the default — must
//! reach a confirmed leader. `eventual` explicitly accepts a local replica,
//! so it keeps being served when no quorum is available. Both are asserted:
//! one alone cannot tell a working guarantee apart from a broken read path.

mod common;
use common::cluster_harness::{TestCluster, wait::wait_for};

use std::time::Duration;

const COLLECTION: &str = "linread";

/// Bring up three nodes holding one row.
async fn seeded_cluster() -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (id TEXT PRIMARY KEY, val TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create collection");

    let insert = format!("INSERT INTO {COLLECTION} (id, val) VALUES ('a', 'v')");
    wait_for(
        "seed row accepted",
        Duration::from_secs(15),
        Duration::from_millis(200),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[0].client.simple_query(&insert))
            })
            .is_ok()
        },
    )
    .await;

    cluster
}

/// The baseline: with every node up, the default read is served.
///
/// Without this, a test that only asserts refusal cannot tell leadership
/// confirmation from a read path that is simply broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_linearizable_read_is_served_while_the_quorum_is_healthy() {
    let cluster = seeded_cluster().await;

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    wait_for(
        "linearizable read served",
        Duration::from_secs(15),
        Duration::from_millis(200),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[0].client.simple_query(&select))
            })
            .is_ok()
        },
    )
    .await;

    cluster.shutdown().await;
}

/// The case the confirmation exists for: one node left, no quorum to ask.
///
/// The survivor's routing table still names a leader for a while, so the
/// read has everything it needs to be answered from local state — and must
/// not be.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_without_a_quorum_is_refused_rather_than_answered_locally() {
    let mut cluster = seeded_cluster().await;

    let survivor = cluster.nodes.remove(0);
    for node in cluster.nodes.drain(..) {
        node.shutdown().await;
    }

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    wait_for(
        "read refused once the quorum is gone",
        Duration::from_secs(20),
        Duration::from_millis(250),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(survivor.client.simple_query(&select))
            })
            .is_err()
        },
    )
    .await;

    survivor.shutdown().await;
}

/// `eventual` is the caller saying a local replica is acceptable, so the
/// same query on the same quorum-less node keeps being served.
///
/// This is what pins the refusal above to the consistency level rather than
/// to the node having become unable to answer anything at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_eventual_read_is_still_served_without_a_quorum() {
    let mut cluster = seeded_cluster().await;

    let survivor = cluster.nodes.remove(0);
    for node in cluster.nodes.drain(..) {
        node.shutdown().await;
    }

    survivor
        .client
        .simple_query("SET default_read_consistency = 'eventual'")
        .await
        .expect("set session consistency");

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    survivor
        .client
        .simple_query(&select)
        .await
        .expect("an eventual read accepts the local replica");

    survivor.shutdown().await;
}
