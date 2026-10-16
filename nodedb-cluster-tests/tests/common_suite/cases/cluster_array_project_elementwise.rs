// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster tests for `ARRAY_PROJECT`, `ARRAY_ELEMENTWISE`,
//! `ARRAY_FLUSH` and `ARRAY_COMPACT`.
//!
//! Every test matches `binary(/cluster/)` in nextest.toml, so it runs in the
//! serial `cluster` test group. The arrays are created on one node and
//! written through another. Their 32 one-cell tiles spread over the shards of
//! every node, and every node answers each query with every cell.

use std::collections::BTreeMap;

use crate::common;

use common::cluster_harness::TestCluster;

const CELLS: i64 = 32;

/// The DDL of a 1-D array `name` with one FLOAT64 attr `v`.
fn ddl(name: &str) -> String {
    format!(
        "CREATE ARRAY {name} \
         DIMS (k INT64 [0..31]) \
         ATTRS (v FLOAT64) \
         TILE_EXTENTS (1)"
    )
}

/// The INSERT that writes `v = scale * k` for every cell of `name`.
fn insert(name: &str, scale: f64) -> String {
    let cells: Vec<String> = (0..CELLS)
        .map(|k| format!("COORDS ({k}) VALUES ({})", scale * k as f64))
        .collect();
    format!("INSERT INTO ARRAY {name} {}", cells.join(", "))
}

/// Spawn a 3-node cluster holding every `(name, scale)` array, written and
/// flushed through a node other than the one that ran the DDL.
async fn spawn_with_arrays(arrays: &[(&str, f64)]) -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");
    for &(name, scale) in arrays {
        let ddl_idx = cluster
            .exec_ddl_on_any_leader(&ddl(name))
            .await
            .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
        let writer = &cluster.nodes[(ddl_idx + 1) % cluster.nodes.len()];
        writer
            .exec(&insert(name, scale))
            .await
            .unwrap_or_else(|e| panic!("INSERT INTO ARRAY {name}: {e}"));
        writer
            .exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
            .await
            .unwrap_or_else(|e| panic!("ARRAY_FLUSH {name}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;
    cluster
}

fn int(text: &str) -> i64 {
    text.parse()
        .unwrap_or_else(|e| panic!("{text:?} is not an integer: {e}"))
}

fn number(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("{text:?} is not a number: {e}"))
}

/// The first element of a JSON array column.
fn first(column: &str) -> serde_json::Value {
    let value: serde_json::Value = serde_json::from_str(column)
        .unwrap_or_else(|e| panic!("column {column:?} is not JSON: {e}"));
    value
        .as_array()
        .and_then(|items| items.first().cloned())
        .unwrap_or_else(|| panic!("column {column:?} is not a non-empty JSON array"))
}

/// The `(k, v)` cells of `sql` on node `idx`.
async fn cells_on(cluster: &TestCluster, idx: usize, sql: &str) -> BTreeMap<i64, f64> {
    let rows = cluster.nodes[idx]
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
    let map: BTreeMap<i64, f64> = rows
        .iter()
        .map(|row| (int(&row["k"]), number(&row["v"])))
        .collect();
    assert_eq!(
        map.len(),
        rows.len(),
        "node {idx}: {sql}: a cell came twice"
    );
    map
}

fn scaled(range: std::ops::Range<i64>, scale: f64) -> BTreeMap<i64, f64> {
    range.map(|k| (k, scale * k as f64)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_project_returns_every_cell_on_every_node() {
    let cluster = spawn_with_arrays(&[("g", 1.0)]).await;
    for idx in 0..cluster.nodes.len() {
        let got = cells_on(&cluster, idx, "SELECT k, v FROM ARRAY_PROJECT('g', ['v'])").await;
        assert_eq!(got, scaled(0..CELLS, 1.0), "node {idx}");
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_project_feeds_a_relational_tail() {
    let cluster = spawn_with_arrays(&[("g", 1.0)]).await;
    for idx in 0..cluster.nodes.len() {
        let got = cells_on(
            &cluster,
            idx,
            "SELECT k, v FROM ARRAY_PROJECT('g', ['v']) WHERE v >= 20 ORDER BY k",
        )
        .await;
        assert_eq!(got, scaled(20..CELLS, 1.0), "node {idx}");

        let sql = "SELECT count(*) AS n FROM ARRAY_PROJECT('g', ['v'])";
        let rows = cluster.nodes[idx]
            .query_named_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
        assert_eq!(rows.len(), 1, "node {idx}: one count row: {rows:?}");
        assert_eq!(int(&rows[0]["n"]), CELLS, "node {idx}");
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_elementwise_pairs_every_cell_on_every_node() {
    let cluster = spawn_with_arrays(&[("a", 1.0), ("b", 100.0)]).await;
    let sql = "SELECT coords, attrs FROM ARRAY_ELEMENTWISE('a', 'b', 'add', 'v')";
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = node
            .query_named_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
        let got: BTreeMap<i64, f64> = rows
            .iter()
            .map(|row| {
                let k = first(&row["coords"])
                    .as_i64()
                    .unwrap_or_else(|| panic!("coords {:?} holds no integer", row["coords"]));
                let v = first(&row["attrs"])
                    .as_f64()
                    .unwrap_or_else(|| panic!("attrs {:?} holds no number", row["attrs"]));
                (k, v)
            })
            .collect();
        assert_eq!(got.len(), rows.len(), "node {idx}: a cell paired twice");
        assert_eq!(got, scaled(0..CELLS, 101.0), "node {idx}");
    }
    cluster.shutdown().await;
}

/// Three more flushed rounds give every shard four segments. The compaction
/// merges them on every shard that holds cells, and every node reads the
/// newest values afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_flush_and_compact_reach_every_shard() {
    let cluster = spawn_with_arrays(&[("m", 1.0)]).await;
    for round in 2..=4 {
        let node = &cluster.nodes[round % cluster.nodes.len()];
        node.exec(&insert("m", round as f64))
            .await
            .unwrap_or_else(|e| panic!("INSERT round {round}: {e}"));
        let rows = node
            .query_named_rows("SELECT ARRAY_FLUSH('m')")
            .await
            .unwrap_or_else(|e| panic!("ARRAY_FLUSH round {round}: {e}"));
        assert_eq!(int(&rows[0]["flushed"]), 1, "round {round}");
    }

    let rows = cluster.nodes[0]
        .query_named_rows("SELECT ARRAY_COMPACT('m')")
        .await
        .expect("ARRAY_COMPACT");
    assert!(
        int(&rows[0]["compacted"]) >= 1,
        "the shards that hold cells merge: {rows:?}"
    );
    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;

    for idx in 0..cluster.nodes.len() {
        let got = cells_on(&cluster, idx, "SELECT k, v FROM ARRAY_PROJECT('m', ['v'])").await;
        assert_eq!(got, scaled(0..CELLS, 4.0), "node {idx}");
    }
    cluster.shutdown().await;
}
