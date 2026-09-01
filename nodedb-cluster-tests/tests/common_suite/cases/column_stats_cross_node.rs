// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of `ANALYZE` column statistics.
//!
//! `ANALYZE` scans every vShard leader, so its numbers describe the whole
//! collection, not one node's slice. It proposes
//! `CatalogEntry::PutColumnStats` through the metadata raft group and every
//! node writes the rows from the apply path. The planner cost models read
//! the statistics from the local catalog of whichever node received the
//! query, so statistics that reach only the executing node leave every other
//! node costing joins and aggregates blind. This test asserts the follower's
//! own persisted rows — their column names and their computed values.

use crate::common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::cluster_harness::node::lifecycle::HARNESS_SUPERUSER;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

use nodedb::control::security::catalog::column_stats::StoredColumnStats;
use nodedb_types::DatabaseId;

/// Collection the test analyzes.
const COLLECTION: &str = "stats_metrics";
/// Rows inserted before `ANALYZE` runs.
const ROWS: usize = 12;
/// Columns the collection declares.
const COLUMNS: [&str; 3] = ["id", "k", "v"];

/// Index of the metadata-group leader.
fn pick_leader_index(cluster: &TestCluster) -> usize {
    let leader_id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id == leader_id)
        .expect("the metadata leader must be one of the spawned nodes")
}

/// Index of a node that is not the metadata-group leader.
fn pick_follower_index(cluster: &TestCluster) -> usize {
    let leader_id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id != leader_id)
        .expect("a 3-node cluster must have a follower")
}

/// Node id every node agrees is the metadata-group leader.
fn leader_id(cluster: &TestCluster) -> u64 {
    cluster
        .nodes
        .iter()
        .map(|n| n.metadata_group_leader())
        .find(|&id| id != 0)
        .expect("at least one node must report a non-zero leader id")
}

/// Tenant the harness pgwire client authenticates as.
///
/// `ANALYZE` keys its rows by the executing identity's tenant, so a catalog
/// read must use the same tenant the harness client carries.
fn harness_tenant_id(node: &TestClusterNode) -> u64 {
    node.shared
        .credentials
        .get_user(HARNESS_SUPERUSER)
        .expect("the harness superuser must exist on every node")
        .tenant_id
        .as_u64()
}

/// Column statistics this node persisted for the collection.
fn stored_column_stats(node: &TestClusterNode) -> Vec<StoredColumnStats> {
    node.shared
        .credentials
        .catalog()
        .load_column_stats(
            DatabaseId::DEFAULT.as_u64(),
            harness_tenant_id(node),
            COLLECTION,
        )
        .expect("read column stats")
}

/// Data rows this node's client returns for `sql`.
async fn count_rows(node: &TestClusterNode, sql: &str) -> usize {
    node.client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn analyze_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             (id TEXT PRIMARY KEY, k TEXT, v BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    wait_for(
        "all 3 nodes see the collection",
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

    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    let follower = &cluster.nodes[pick_follower_index(&cluster)];

    // Every row carries a distinct `k`, so `distinct_count` for `k` equals
    // the row count once the statistics hold real values.
    let mut values = String::new();
    for i in 0..ROWS {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("('r{i}', 'k{i}', {})", (i as i64 + 1) * 10));
    }
    leader
        .client
        .simple_query(&format!(
            "INSERT INTO {COLLECTION} (id, k, v) VALUES {values}"
        ))
        .await
        .expect("insert rows on the metadata leader");

    let scan = format!("SELECT id FROM {COLLECTION}");
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for_async(
            &format!("node {idx} sees all {ROWS} rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async { count_rows(node, &scan).await >= ROWS },
        )
        .await;
    }

    // Baseline: the follower carries no statistics row.
    assert_eq!(
        stored_column_stats(follower).len(),
        0,
        "follower node {} must carry no column statistics before ANALYZE runs",
        follower.node_id,
    );

    leader
        .client
        .simple_query(&format!("ANALYZE {COLLECTION}"))
        .await
        .expect("ANALYZE on the metadata leader");

    wait_for(
        "follower persists the replicated column statistics",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || !stored_column_stats(follower).is_empty(),
    )
    .await;

    let stats = stored_column_stats(follower);
    let names: BTreeSet<&str> = stats.iter().map(|s| s.column.as_str()).collect();
    for column in COLUMNS {
        assert!(
            names.contains(column),
            "follower node {} must hold statistics for column {column}; got {names:?}",
            follower.node_id,
        );
    }

    for row in &stats {
        assert_eq!(
            row.row_count, ROWS as u64,
            "column {} on follower node {} must report the {ROWS} scanned rows",
            row.column, follower.node_id,
        );
    }

    let k_stats = stats
        .iter()
        .find(|s| s.column == "k")
        .expect("statistics for column k");
    assert_eq!(
        k_stats.distinct_count, ROWS as u64,
        "column k holds {ROWS} distinct values, so the follower's replicated \
         row must count them, not a placeholder",
    );
    assert_eq!(
        k_stats.null_count, 0,
        "every inserted row carries a k value, so the follower's row must \
         count no nulls",
    );

    cluster.shutdown().await;
}
