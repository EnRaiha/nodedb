// SPDX-License-Identifier: BUSL-1.1

//! A cross-collection SQL schedule fires only on the vShard 0 lease holder.
//!
//! A schedule whose body names no collection runs on the `_system`
//! coordinator: the node that leads vShard 0's group under a leader lease
//! valid now. The schedule fires every minute on the real scheduler, and
//! each node records its own runs in its job history.
//!
//! - While one node holds the lease, only that node records runs.
//! - Once that node is cut off from both peers, its lease lapses and it
//!   records no further run. A peer takes the lease and records runs.
//!
//! The test waits on minute boundaries, so it runs for a few minutes.

use crate::common;
use common::cluster_harness::shared_steps::holds_vshard0_lease;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::sync::Arc;
use std::time::Duration;

const SCHEDULE: &str = "slg_every_minute";
const CONVERGE: Duration = Duration::from_secs(30);
/// Long enough for at least one minute boundary and its fire.
const FIRE_WAIT: Duration = Duration::from_secs(150);
const STEP: Duration = Duration::from_millis(250);

/// Runs `node` recorded for the schedule.
fn runs(node: &TestClusterNode) -> usize {
    let Some(def) = node
        .shared
        .schedule_registry
        .list_all()
        .into_iter()
        .find(|def| def.name == SCHEDULE)
    else {
        return 0;
    };
    node.shared
        .job_history
        .last_runs(def.database_id, def.tenant_id, SCHEDULE, 100)
        .len()
}

/// Cut `node_id` off from every other node, both ways.
fn isolate(cluster: &TestCluster, node_id: u64) {
    let transport = |node: &TestClusterNode| {
        Arc::clone(
            node.shared
                .cluster_transport
                .as_ref()
                .expect("cluster transport"),
        )
    };
    let cut = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(transport)
        .expect("the node is a member");
    for node in cluster.nodes.iter().filter(|node| node.node_id != node_id) {
        transport(node).sever(node_id);
        cut.sever(node.node_id);
    }
}

fn holder(cluster: &TestCluster) -> Option<u64> {
    let holders: Vec<u64> = cluster
        .nodes
        .iter()
        .filter(|node| holds_vshard0_lease(node))
        .map(|node| node.node_id)
        .collect();
    match holders.as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_collection_schedule_fires_only_on_the_lease_holder() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    wait_for("one node holds the vShard 0 lease", CONVERGE, STEP, || {
        holder(&cluster).is_some()
    })
    .await;
    let first = holder(&cluster).expect("a lease holder");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE SCHEDULE {SCHEDULE} CRON '* * * * *' AS BEGIN RETURN 1; END"
        ))
        .await
        .expect("create schedule");

    // Only the holder fires.
    wait_for(
        "the lease holder fires the schedule",
        FIRE_WAIT,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .any(|node| node.node_id == first && runs(node) > 0)
        },
    )
    .await;
    assert_eq!(holder(&cluster), Some(first), "the lease stayed put");
    for node in cluster.nodes.iter().filter(|node| node.node_id != first) {
        assert_eq!(
            runs(node),
            0,
            "node {} holds no lease and fires nothing",
            node.node_id
        );
    }

    // Cut the holder off. Its lease lapses, and a peer takes it.
    isolate(&cluster, first);
    let cut_off = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == first)
        .expect("the cut-off node");
    wait_for("the cut-off node's lease lapses", CONVERGE, STEP, || {
        !holds_vshard0_lease(cut_off)
    })
    .await;
    let lapsed_runs = runs(cut_off);
    let peers: Vec<&TestClusterNode> = cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != first)
        .collect();
    wait_for("a peer takes the vShard 0 lease", CONVERGE, STEP, || {
        peers
            .iter()
            .filter(|node| holds_vshard0_lease(node))
            .count()
            == 1
    })
    .await;
    let second = peers
        .iter()
        .find(|node| holds_vshard0_lease(node))
        .map(|node| node.node_id)
        .expect("the new holder");

    // The new holder fires. The cut-off node, through the same minute
    // boundaries, fires nothing more.
    wait_for(
        "the new lease holder fires the schedule",
        FIRE_WAIT,
        STEP,
        || {
            peers
                .iter()
                .any(|node| node.node_id == second && runs(node) > 0)
        },
    )
    .await;
    assert_eq!(
        runs(cut_off),
        lapsed_runs,
        "a node without its lease fires nothing"
    );
    for node in peers.iter().filter(|node| node.node_id != second) {
        assert_eq!(
            runs(node),
            0,
            "node {} holds no lease and fires nothing",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
