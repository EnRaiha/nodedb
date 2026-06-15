// SPDX-License-Identifier: BUSL-1.1

//! Regression test for the "gather local-only" cross-node correctness bug.
//!
//! Before the fix, `resolve_exchange` called `gather_all_cores`, which fanned
//! the plan to LOCAL Data-Plane cores only.  On a multi-node cluster a sharded
//! SELECT therefore returned only the coordinator node's rows — a silent
//! partial-result bug.
//!
//! After the fix, `resolve_exchange` calls `gather_all_vshards`, which
//! delegates to the gateway on multi-node clusters so every vShard on every
//! node is queried and the full result set is returned.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Helper: run a simple query and return the number of data rows returned.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// A SELECT from a single coordinator node must return ALL rows inserted across
/// the cluster, not just the rows that happen to live on the coordinator's
/// local Data-Plane cores.
///
/// 20 rows are inserted with diverse string keys so the key-hash routing
/// distributes them across multiple vShards spanning more than one node.
/// (Placement is by key hash, not by client connection; no routing hint is
/// given so the planner distributes naturally.)
///
/// Before the fix this test would return a partial count (rows on the
/// coordinator only).  After the fix it must return exactly 20.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn select_returns_all_rows_across_cluster() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION gather_test \
             (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION gather_test");

    // Wait for all nodes to see the new collection before inserting.
    wait_for(
        "all 3 nodes see gather_test",
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

    // Insert 20 rows with diverse keys.  Key names are chosen to spread across
    // the hash space; placement is by key hash, not client connection.
    const ROW_COUNT: usize = 20;
    let values: Vec<String> = (0..ROW_COUNT)
        .map(|i| format!("('gather_key_{i:03}', {i})"))
        .collect();
    let insert_sql = format!(
        "INSERT INTO gather_test (id, val) VALUES {}",
        values.join(", ")
    );

    // Insert via node 0 (arbitrary coordinator choice).
    cluster.nodes[0]
        .client
        .simple_query(&insert_sql)
        .await
        .expect("batched insert");

    // Wait until every node reports the full row count via a local scan, so
    // we know replication completed before we assert the cross-node gather.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {ROW_COUNT} rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM gather_test"))
                });
                n >= ROW_COUNT
            },
        )
        .await;
    }

    // Assert: a SELECT from node 0 must return the complete set of rows.
    // Before the gather_all_vshards fix this would silently return fewer rows.
    let returned = count_rows(&cluster.nodes[0].client, "SELECT id FROM gather_test").await;
    assert_eq!(
        returned, ROW_COUNT,
        "cross-node gather must return all {ROW_COUNT} rows; got {returned}"
    );

    cluster.shutdown().await;
}
