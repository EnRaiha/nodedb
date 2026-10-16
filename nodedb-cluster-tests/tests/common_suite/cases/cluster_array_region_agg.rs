// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster tests for region-bounded `ARRAY_AGG`.
//!
//! Every test matches `binary(/cluster/)` in nextest.toml, so it runs in the
//! serial `cluster` test group. The arrays are created on one node and
//! written through another. Every node answers each query: the coordinator
//! sends the region to every shard it fans out to and merges the partials.

use crate::common;

use common::cluster_harness::TestCluster;

/// Spawn a 3-node cluster holding `ddl` with `insert` written and flushed
/// through a node other than the one that ran the DDL.
async fn spawn_with_array(ddl: &str, insert: &str, name: &str) -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");
    let ddl_idx = cluster
        .exec_ddl_on_any_leader(ddl)
        .await
        .expect("CREATE ARRAY");
    let writer = &cluster.nodes[(ddl_idx + 1) % cluster.nodes.len()];
    writer.exec(insert).await.expect("INSERT INTO ARRAY");
    writer
        .exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
        .await
        .expect("ARRAY_FLUSH");
    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;
    cluster
}

/// The genome fixture of `cluster_array.rs`: qual 1/2/3 on chr 0,
/// 10/20/30 on chr 1, 100/200/300 on chr 2, each at pos 10/20/30.
async fn spawn_genome() -> TestCluster {
    spawn_with_array(
        "CREATE ARRAY genome \
         DIMS (chr INT64 [0..9], pos INT64 [0..99]) \
         ATTRS (qual FLOAT64) \
         TILE_EXTENTS (1, 100) \
         CELL_ORDER HILBERT",
        "INSERT INTO ARRAY genome \
         COORDS (0, 10) VALUES (1.0), COORDS (0, 20) VALUES (2.0), \
         COORDS (0, 30) VALUES (3.0), COORDS (1, 10) VALUES (10.0), \
         COORDS (1, 20) VALUES (20.0), COORDS (1, 30) VALUES (30.0), \
         COORDS (2, 10) VALUES (100.0), COORDS (2, 20) VALUES (200.0), \
         COORDS (2, 30) VALUES (300.0)",
        "genome",
    )
    .await
}

fn number(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("{text:?} is not a number: {e}"))
}

/// The scalar `result` every node answers for `sql`.
async fn scalar_on_every_node(cluster: &TestCluster, sql: &str) -> Vec<f64> {
    let mut out = Vec::with_capacity(cluster.nodes.len());
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = node
            .query_named_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
        assert_eq!(rows.len(), 1, "node {idx}: one scalar row: {rows:?}");
        out.push(number(&rows[0]["result"]));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_box_sums_only_its_rows() {
    let cluster = spawn_genome().await;
    let sums = scalar_on_every_node(
        &cluster,
        "SELECT * FROM ARRAY_AGG('genome', 'qual', 'sum', NULL, '{chr: [1, 2]}')",
    )
    .await;
    for (idx, sum) in sums.iter().enumerate() {
        assert!((sum - 660.0).abs() < 1e-9, "node {idx}: sum {sum}");
    }
    cluster.shutdown().await;
}

/// A zone drawn over `pos` then `chr`: pos in `[15, 35)`, chr in `[0, 1.5)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_zone_sums_the_cells_inside_it() {
    let cluster = spawn_genome().await;
    let sums = scalar_on_every_node(
        &cluster,
        "SELECT * FROM ARRAY_AGG('genome', 'qual', 'sum', NULL, NULL, \
         'POLYGON((15 0, 35 0, 35 1.5, 15 1.5, 15 0))', ARRAY['pos', 'chr'])",
    )
    .await;
    for (idx, sum) in sums.iter().enumerate() {
        assert!((sum - 55.0).abs() < 1e-9, "node {idx}: sum {sum}");
    }
    cluster.shutdown().await;
}

/// A group-by on a FLOAT64 dim reports each group under its dim value on
/// every node, across shards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_groups_by_a_float_dim_value() {
    let cluster = spawn_with_array(
        "CREATE ARRAY scene \
         DIMS (lon FLOAT64 [100..106], lat FLOAT64 [0..8]) \
         ATTRS (ndvi FLOAT64) \
         TILE_EXTENTS (1, 1) \
         CELL_ORDER HILBERT",
        "INSERT INTO ARRAY scene \
         COORDS (101.5, 0.5) VALUES (1.0), COORDS (101.5, 5.5) VALUES (2.0), \
         COORDS (103.25, 2.5) VALUES (4.0), COORDS (103.25, 7.5) VALUES (8.0), \
         COORDS (105.5, 1.5) VALUES (16.0)",
        "scene",
    )
    .await;
    let sql = "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', 'lon', '{lon: [100, 104]}')";
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = node
            .query_named_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
        let mut groups: Vec<(f64, f64)> = rows
            .iter()
            .map(|row| (number(&row["group"]), number(&row["result"])))
            .collect();
        groups.sort_by(|a, b| a.0.total_cmp(&b.0));
        assert_eq!(
            groups,
            vec![(101.5, 3.0), (103.25, 12.0)],
            "node {idx}: one group per lon value"
        );
    }
    cluster.shutdown().await;
}
