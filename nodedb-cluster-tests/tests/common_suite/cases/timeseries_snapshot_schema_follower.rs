// SPDX-License-Identifier: BUSL-1.1

//! A replica that catches up by a real Raft `InstallSnapshot` holds the
//! leader's timeseries schema, before and after it restarts, and rejects a
//! conflicting row exactly as every other replica does.
//!
//! ## Test shape
//!
//! Bring up 3 nodes with a low log-compaction threshold and a replication
//! factor of 4, so a fourth node lands on every data group. Give a
//! timeseries collection that declares only its `ts` time key a column
//! `extra` of floats, and write enough rows that the data group's log
//! compacts past the start. Every row is a raw ILP line through the native
//! `TimeseriesIngest` opcode: a fresh column comes only from a raw ILP line
//! (see `ts_native_ingest`). Add a
//! fresh fourth node: only an `InstallSnapshot` can make it whole. Once it
//! mounted the group from a snapshot, restart it, so its schema comes from
//! its own disk. Then send a row that gives `extra` a string through the
//! restarted node, the node that resolves it:
//!
//! - its client reports the row rejected;
//! - every replica holds the same rows, none of them the rejected one;
//! - no core fail-stopped.
//!
//! A replica that lost the schema resolves the row whole and stores it
//! while the others reject it.

use crate::common;
use common::cluster_harness::shared_steps::{fail_stopped, local_timeseries_rows};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::time::{Duration, Instant};

use super::ts_native_ingest::{
    assert_native_ok, ingest_native, ingest_until_accepted, native_session, rejection_warnings,
};

const COMPACTION_THRESHOLD: u64 = 4;
const ROW_COUNT: usize = 30;
const COLLECTION: &str = "ts_snapshot_schema";
const TENANT: u64 = 1;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// Poll every replica until each holds `expected` rows, and return them.
async fn converged_rows(cluster: &TestCluster, expected: usize) -> Vec<(u64, Vec<String>)> {
    let deadline = Instant::now() + CONVERGE;
    loop {
        let mut per_node = Vec::new();
        for node in &cluster.nodes {
            per_node.push((
                node.node_id,
                local_timeseries_rows(node, TENANT, COLLECTION).await,
            ));
        }
        if per_node.iter().all(|(_, rows)| rows.len() == expected) || Instant::now() >= deadline {
            return per_node;
        }
        tokio::time::sleep(STEP).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_follower_restored_by_install_snapshot_rejects_as_the_leader() {
    let mut cluster =
        TestCluster::spawn_three_with_compaction_threshold_and_rf(COMPACTION_THRESHOLD, 4)
            .await
            .expect("3-node cluster with low compaction threshold and rf=4");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (ts BIGINT TIME_KEY) WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");

    // Each ingest is one data-group entry; together they compact the log.
    for i in 0..ROW_COUNT {
        ingest_until_accepted(
            &cluster.nodes[0],
            COLLECTION,
            &format!(
                "{COLLECTION} value={i}.0,extra={i}.5 {ts_ns}",
                ts_ns = 1_000_000_000 * (i as u64 + 1)
            ),
        )
        .await;
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    let compacted = cluster
        .nodes
        .iter()
        .map(TestClusterNode::max_data_group_snapshot_index)
        .max()
        .unwrap_or(0);
    assert!(
        compacted > 0,
        "the data group's log compacted before the new node joins"
    );
    let gid = cluster.nodes[0]
        .group_id_for_collection(COLLECTION)
        .expect("the collection maps to a data group");

    // A fresh node is made whole only by an InstallSnapshot.
    let learner_id = cluster
        .add_learner_node()
        .await
        .expect("add a fourth node")
        .node_id;
    let learner_index = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == learner_id)
        .expect("the fourth node is a member");
    wait_for(
        "the fourth node mounts the data group from a snapshot",
        CONVERGE,
        STEP,
        || {
            let learner = &cluster.nodes[learner_index];
            learner.hosts_data_group(gid) && learner.local_snapshot_index_for_group(gid) > 0
        },
    )
    .await;
    let before_restart = converged_rows(&cluster, ROW_COUNT).await;
    for (node_id, rows) in &before_restart {
        assert_eq!(
            rows.len(),
            ROW_COUNT,
            "node {node_id} stores {} rows",
            rows.len()
        );
    }

    // After the restart its schema comes from its own disk.
    let stopped = cluster
        .stop_member(learner_index)
        .await
        .expect("stop the fourth node");
    cluster
        .restart_member(stopped)
        .await
        .expect("restart the fourth node");

    let mut session = native_session(&cluster.nodes[learner_index]).await;
    let reply = ingest_native(
        &mut session,
        1,
        COLLECTION,
        &format!("{COLLECTION} value=1.0,extra=\"text\" 999000000000"),
    )
    .await;
    assert_native_ok(&reply, "the conflicting ingest");
    let notices = rejection_warnings(&reply, COLLECTION);
    assert!(
        notices.iter().any(|notice| notice.contains("1 line(s)")),
        "the restarted node reports the conflicting row rejected, got {notices:?}"
    );

    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    let after = converged_rows(&cluster, ROW_COUNT).await;
    let (reference_node, reference) = &after[0];
    for (node_id, rows) in &after {
        assert_eq!(
            rows.len(),
            ROW_COUNT,
            "node {node_id} stores {} rows",
            rows.len()
        );
        assert_eq!(
            rows, reference,
            "node {node_id} stores other rows than node {reference_node}"
        );
        assert!(
            rows.iter().all(|row| !row.contains("\"text\"")),
            "node {node_id} stored the rejected row"
        );
    }
    for node in &cluster.nodes {
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
