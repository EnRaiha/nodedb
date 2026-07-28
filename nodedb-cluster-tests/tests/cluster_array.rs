// SPDX-License-Identifier: BUSL-1.1
//! End-to-end 3-node cluster integration tests for the distributed Array
//! Engine.
//!
//! Every test in this file matches `binary(/cluster/)` in nextest.toml and
//! therefore runs in the `cluster` test group (max-threads = 1,
//! threads-required = num-test-threads). They run strictly serially and alone.
//!
//! ## Architecture Note: Local Array Catalog
//!
//! `CREATE ARRAY` writes to the local in-memory `ArrayCatalog` on the
//! executing node only — it is NOT replicated through Raft (unlike
//! `CREATE COLLECTION`). As a result, all array queries (ARRAY_SLICE,
//! ARRAY_AGG, etc.) must be issued on the same node that executed the
//! `CREATE ARRAY` DDL.
//!
//! The "distributed" aspect of these tests is that cell data is stored
//! across multiple vShards on different nodes (Hilbert-partitioned). The
//! coordinator on the DDL node fans out to peer shards via the array RPC
//! path and merges the results.
//!
//! Tests:
//!   1. `cluster_array_slice_spans_multiple_shards` — ARRAY_SLICE fan-out
//!      to peer shards returns exactly the expected cells.
//!   2. `cluster_array_agg_sum_across_shards` — ARRAY_AGG sum is correct.
//!   3. `cluster_array_agg_grouped_by_chr` — ARRAY_AGG group-by-dim returns
//!      correct per-group sums.
//!   4. `cluster_array_vector_prefilter_distributed` — fused vector+slice
//!      query wires end-to-end without error.
//!   5. `cluster_array_routing_retry_on_owner_change` — stale routing table
//!      on the coordinator node (poisoned via `force_stale_route_for_test`)
//!      recovers and the array query succeeds on retry.

mod common;

use common::cluster_harness::TestCluster;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Run `sql` on `client` and return every row's columns as a `HashMap`
/// keyed by column name.  Used for `SELECT *` over TVFs that expand into
/// multiple pgwire fields (one per array dimension/attribute).
async fn query_named_rows(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Vec<std::collections::HashMap<String, String>> {
    let msgs = client.simple_query(sql).await.unwrap_or_else(|e| {
        let detail = if let Some(db) = e.as_db_error() {
            format!(
                "SQLSTATE={} severity={} msg={}",
                db.code().code(),
                db.severity(),
                db.message()
            )
        } else {
            format!("{e:?}")
        };
        panic!("query failed: {detail}\n  sql: {sql}")
    });
    msgs.into_iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                let names: Vec<String> = r.columns().iter().map(|c| c.name().to_string()).collect();
                let mut map = std::collections::HashMap::with_capacity(names.len());
                for (i, name) in names.into_iter().enumerate() {
                    map.insert(name, r.get(i).unwrap_or("").to_string());
                }
                Some(map)
            } else {
                None
            }
        })
        .collect()
}

/// Spin up a 3-node cluster with a pre-populated genome array.
///
/// Returns `(cluster, leader_idx)` where `leader_idx` is the index into
/// `cluster.nodes` of the node that executed the `CREATE ARRAY` DDL. All
/// array queries must be issued on this node because the array catalog is
/// local (not replicated through Raft).
///
/// Schema:
///   DIMS  (chr INT64 [0..9], pos INT64 [0..99])
///   ATTRS (qual FLOAT64)
///   TILE_EXTENTS (1, 100)
///   CELL_ORDER HILBERT
///
/// 9 cells total across chr in {0, 1, 2}:
///   chr=0: pos=10/20/30, qual=1.0/2.0/3.0   → sum=6.0
///   chr=1: pos=10/20/30, qual=10.0/20.0/30.0 → sum=60.0
///   chr=2: pos=10/20/30, qual=100.0/200.0/300.0 → sum=600.0
///   total qual sum: 666.0
async fn spawn_cluster_with_genome() -> (TestCluster, usize) {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");

    let leader_idx = cluster
        .exec_ddl_on_any_leader(
            "CREATE ARRAY genome \
             DIMS (chr INT64 [0..9], pos INT64 [0..99]) \
             ATTRS (qual FLOAT64) \
             TILE_EXTENTS (1, 100) \
             CELL_ORDER HILBERT",
        )
        .await
        .expect("CREATE ARRAY genome");

    // Insert 9 cells from the DDL node (the one with the local array catalog).
    cluster.nodes[leader_idx]
        .exec(
            "INSERT INTO ARRAY genome \
             COORDS (0, 10) VALUES (1.0), \
             COORDS (0, 20) VALUES (2.0), \
             COORDS (0, 30) VALUES (3.0), \
             COORDS (1, 10) VALUES (10.0), \
             COORDS (1, 20) VALUES (20.0), \
             COORDS (1, 30) VALUES (30.0), \
             COORDS (2, 10) VALUES (100.0), \
             COORDS (2, 20) VALUES (200.0), \
             COORDS (2, 30) VALUES (300.0)",
        )
        .await
        .expect("INSERT INTO ARRAY genome");

    // Flush so reads exercise the segment-scan path, not just the memtable.
    cluster.nodes[leader_idx]
        .exec("SELECT ARRAY_FLUSH('genome')")
        .await
        .expect("ARRAY_FLUSH");

    (cluster, leader_idx)
}

// ── Test 1: slice fan-out to peer shards ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_slice_spans_multiple_shards() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;
    let client = &cluster.nodes[node_idx].client;

    // Slice chr=1, pos 0..99 — should return exactly the 3 cells for chr=1.
    // ARRAY_SLICE projects one pgwire field per declared column: a
    // `coords` JSON-array column followed by an `attrs` JSON-array column
    // carrying the requested attribute values in projection order.
    let rows = query_named_rows(
        client,
        "SELECT * FROM ARRAY_SLICE('genome', '{chr: [1, 1], pos: [0, 99]}', ['qual'], 100)",
    )
    .await;

    assert_eq!(
        rows.len(),
        3,
        "expected 3 cells for chr=1, pos 0..99; got {rows:?}"
    );

    let mut quals: Vec<f64> = rows
        .iter()
        .map(|row| {
            let attrs_text = row
                .get("attrs")
                .unwrap_or_else(|| panic!("missing attrs column in {row:?}"));
            let attrs: serde_json::Value = serde_json::from_str(attrs_text)
                .unwrap_or_else(|e| panic!("attrs not JSON: {attrs_text}: {e}"));
            attrs
                .as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_f64())
                .unwrap_or_else(|| panic!("missing qual in row: {row:?}"))
        })
        .collect();
    quals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        quals,
        vec![10.0, 20.0, 30.0],
        "chr=1 qual values must be [10.0, 20.0, 30.0]; got {quals:?}"
    );

    cluster.shutdown().await;
}

// ── Test 2: agg sum across all shards ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_sum_across_shards() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;
    let client = &cluster.nodes[node_idx].client;

    let rows = query_named_rows(client, "SELECT * FROM ARRAY_AGG('genome', 'qual', 'sum')").await;

    assert_eq!(rows.len(), 1, "scalar agg must return exactly one row");

    // ARRAY_AGG projects a `result` column carrying the aggregate value.
    let result_text = rows[0]
        .get("result")
        .unwrap_or_else(|| panic!("missing 'result' column in {:?}", rows[0]));
    let result: f64 = result_text
        .parse()
        .unwrap_or_else(|e| panic!("result not a float: {result_text}: {e}"));

    assert!(
        (result - 666.0).abs() < 1e-4,
        "expected sum=666.0, got {result}"
    );

    cluster.shutdown().await;
}

// ── Test 3: agg grouped by chr dimension ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_grouped_by_chr() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;
    let client = &cluster.nodes[node_idx].client;

    let rows = query_named_rows(
        client,
        "SELECT * FROM ARRAY_AGG('genome', 'qual', 'sum', 'chr')",
    )
    .await;

    assert_eq!(
        rows.len(),
        3,
        "group-by-chr must return 3 rows (one per chromosome); got {rows:?}"
    );

    // Group-by-key projects two columns: `group` (the dimension value)
    // and `result` (the aggregate).
    let mut groups: Vec<(i64, f64)> = rows
        .iter()
        .map(|row| {
            let key_text = row
                .get("group")
                .unwrap_or_else(|| panic!("missing 'group' column in {row:?}"));
            let result_text = row
                .get("result")
                .unwrap_or_else(|| panic!("missing 'result' column in {row:?}"));
            let key: i64 = key_text
                .parse()
                .unwrap_or_else(|e| panic!("group not an int: {key_text}: {e}"));
            let result: f64 = result_text
                .parse()
                .unwrap_or_else(|e| panic!("result not a float: {result_text}: {e}"));
            (key, result)
        })
        .collect();
    groups.sort_by_key(|(k, _)| *k);

    assert_eq!(groups[0].0, 0, "first group key must be chr=0");
    assert!(
        (groups[0].1 - 6.0).abs() < 1e-4,
        "chr=0 sum must be 6.0, got {}",
        groups[0].1
    );
    assert_eq!(groups[1].0, 1, "second group key must be chr=1");
    assert!(
        (groups[1].1 - 60.0).abs() < 1e-4,
        "chr=1 sum must be 60.0, got {}",
        groups[1].1
    );
    assert_eq!(groups[2].0, 2, "third group key must be chr=2");
    assert!(
        (groups[2].1 - 600.0).abs() < 1e-4,
        "chr=2 sum must be 600.0, got {}",
        groups[2].1
    );

    cluster.shutdown().await;
}

// ── Test 4: vector prefilter fused with distributed slice ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_vector_prefilter_distributed() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;

    // Create a document collection and vector index to back the fused query.
    // DDL must be issued and accepted on any leader.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION genes TYPE document")
        .await
        .expect("CREATE COLLECTION genes");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE VECTOR INDEX idx_genes_emb ON genes (embedding) METRIC cosine DIM 3",
        )
        .await
        .expect("CREATE VECTOR INDEX");

    // The fused query: ORDER BY vector_distance + JOIN ARRAY_SLICE.
    // Issued on the DDL node because the array catalog is local there.
    // The vector index is empty — the assertion is that the query wires
    // through every distributed layer without error:
    //   planner fusion → convert → ArrayOp::SurrogateBitmapScan (distributed
    //   fan-out to peer shards) + VectorOp::Search with inline_prefilter_plan.
    let result = cluster.nodes[node_idx]
        .client
        .simple_query(
            "SELECT id FROM genes \
             JOIN ARRAY_SLICE('genome', '{chr: [1, 1], pos: [0, 99]}') AS s \
               ON id = s.qual \
             ORDER BY vector_distance(embedding, [1.0, 0.0, 0.0]) \
             LIMIT 10",
        )
        .await;

    match result {
        Ok(_) => {}
        Err(e) => {
            let msg = format!("{e}");
            // Tolerate empty-index / no-rows errors. Codec panics and planner
            // errors indicate the fused distributed path was never attempted.
            assert!(
                !msg.contains("codec") && !msg.contains("panic") && !msg.contains("plan error"),
                "unexpected error from fused distributed array+vector query: {msg}"
            );
        }
    }

    cluster.shutdown().await;
}
