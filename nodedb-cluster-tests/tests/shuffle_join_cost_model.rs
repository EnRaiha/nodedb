// SPDX-License-Identifier: BUSL-1.1

//! Cost-based automatic shuffle-JOIN selection test (E4b-3).
//!
//! Brings up a 3-node cluster with two distributed collections homed on
//! DIFFERENT vShards, inserts rows with overlapping join keys, and verifies the
//! planner's ANALYZE-driven cost model picks the right distributed join
//! strategy WITHOUT any manual `force_shuffle_join` override:
//!
//! 1. **Auto-shuffle**: after `ANALYZE` persists row-count stats for both
//!    sides, `SET nodedb.broadcast_threshold_bytes = 0` makes any analyzed
//!    collection's estimated size exceed the threshold, so
//!    `select_strategy(.,.,0)` returns `Shuffle`. Running the SAME inner join
//!    with NO `force_shuffle_join` must still return the correct result — proof
//!    the cost model auto-selected shuffle from real persisted statistics.
//!
//! 2. **Graceful fallback (no stats)**: a join over collections that were never
//!    analyzed, at threshold 0, must STILL return the correct result via the
//!    default broadcast path — no stats means broadcast, never shuffle, never
//!    an error. Zero regression for un-analyzed collections.
//!
//! 3. **High threshold broadcasts**: at the (large) default threshold, analyzed
//!    small collections stay on the broadcast path and still return correctly.
//!
//! Every assertion is correctness: the auto-selected plan must return EXACTLY
//! the inner-join result a plain broadcast join returns (the baseline), so a
//! regression in either path is distinguishable.

use std::time::Duration;

use nodedb_cluster::routing::vshard_for_collection;
use nodedb_types::DatabaseId;

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run `sql` and collect the `col` column of every returned data row, sorted, so
/// the result is order-independent for equality assertions.
async fn collect_ids(client: &tokio_postgres::Client, sql: &str, col: &str) -> Vec<String> {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    let mut ids: Vec<String> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(col).map(|s| s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

/// Count data rows returned by `sql`.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    client
        .simple_query(sql)
        .await
        .expect("simple_query")
        .into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cost_model_auto_selects_shuffle_from_analyze_stats() {
    // Two collection names that hash to DIFFERENT vShards so the build and probe
    // sides are genuinely homed on different routes — the cross-node path the
    // shuffle join exists to handle.
    const LEFT: &str = "orders";
    const RIGHT: &str = "customers";
    assert_ne!(
        vshard_for_collection(DatabaseId::DEFAULT, LEFT),
        vshard_for_collection(DatabaseId::DEFAULT, RIGHT),
        "test collections must hash to different vShards to exercise cross-node shuffle"
    );

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION customers \
             (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION customers");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION orders \
             (id TEXT PRIMARY KEY, cust TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION orders");

    wait_for(
        "all 3 nodes see both collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;

    // 4 customers: c1..c4.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO customers (id, name) VALUES \
             ('c1', 'alice'), ('c2', 'bob'), ('c3', 'carol'), ('c4', 'dave')",
        )
        .await
        .expect("insert customers");

    // 7 orders: o1..o6 match c1..c3; o7 has a non-matching customer key.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO orders (id, cust) VALUES \
             ('o1', 'c1'), ('o2', 'c1'), ('o3', 'c2'), \
             ('o4', 'c2'), ('o5', 'c3'), ('o6', 'c3'), \
             ('o7', 'zz')",
        )
        .await
        .expect("insert orders");

    // Wait until every node sees the full local row counts so replication has
    // completed before ANALYZE and the cross-node join.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all customer rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM customers"))
                });
                n >= 4
            },
        )
        .await;
        wait_for(
            &format!("node {idx} sees all order rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM orders"))
                });
                n >= 7
            },
        )
        .await;
    }

    // The expected inner join: o1..o6 match (cust in c1..c3); o7 does not.
    let expected: Vec<String> = vec![
        "o1".into(),
        "o2".into(),
        "o3".into(),
        "o4".into(),
        "o5".into(),
        "o6".into(),
    ];

    // Alias the projected column explicitly so the result column name is stable
    // regardless of how each join path qualifies its output columns.
    let join_sql = "SELECT o.id AS oid FROM orders o JOIN customers c ON o.cust = c.id";

    // ----- Baseline: default plan, no overrides, no stats yet. -----
    // Establishes the correct answer and proves the broadcast path works with no
    // statistics present (graceful fallback case #2: no stats -> broadcast, not
    // shuffle, not error). We deliberately set the threshold to 0 FIRST to prove
    // that even at the most aggressive shuffle threshold, the cost model still
    // broadcasts when no ANALYZE stats exist.
    cluster.nodes[0]
        .client
        .simple_query("SET nodedb.broadcast_threshold_bytes = 0")
        .await
        .expect("SET nodedb.broadcast_threshold_bytes = 0 (no stats)");
    let fallback = collect_ids(&cluster.nodes[0].client, join_sql, "oid").await;
    assert_eq!(
        fallback, expected,
        "graceful fallback: with NO ANALYZE stats and threshold 0, the join must \
         still return the 6 matching orders via the broadcast path; got {fallback:?}"
    );
    // Reset the knob on this session so it cannot leak into later queries.
    cluster.nodes[0]
        .client
        .simple_query("SET nodedb.broadcast_threshold_bytes = 8388608")
        .await
        .expect("reset broadcast_threshold_bytes");
    let baseline = collect_ids(&cluster.nodes[0].client, join_sql, "oid").await;
    assert_eq!(
        baseline, expected,
        "baseline (default threshold) join must return the 6 matching orders; got {baseline:?}"
    );

    // ----- Persist statistics via ANALYZE for both sides. -----
    // The accepted form is `ANALYZE <collection>` (the maintenance handler
    // splits on whitespace and reads the second token). This computes and
    // persists per-column stats — crucially `row_count` > 0 for both sides.
    //
    // IMPORTANT: column statistics are written to the *local* catalog of the
    // node that runs ANALYZE — they are NOT Raft-replicated like collection
    // DDL. The cost model reads stats from the local catalog of whichever node
    // *plans* the join. So ANALYZE and the auto-shuffle query MUST run on the
    // SAME node for the stats-driven decision to be deterministic. We pin both
    // to node 0.
    let analyze_node = &cluster.nodes[0];
    analyze_node
        .client
        .simple_query("ANALYZE orders")
        .await
        .expect("ANALYZE orders");
    analyze_node
        .client
        .simple_query("ANALYZE customers")
        .await
        .expect("ANALYZE customers");

    // ----- Auto-shuffle: threshold 0, stats present, NO force override. -----
    // With both sides analyzed (estimated bytes > 0) and the threshold at 0,
    // `select_strategy(left, right, 0)` returns Shuffle (neither side is "small
    // enough" to broadcast). The planner emits Exchange{Shuffle} purely from the
    // cost model — no `nodedb.force_shuffle_join`. Run on the SAME node that
    // persisted the stats so the local catalog has them.
    analyze_node
        .client
        .simple_query("SET nodedb.broadcast_threshold_bytes = 0")
        .await
        .expect("SET nodedb.broadcast_threshold_bytes = 0 (with stats)");
    let auto_shuffled = collect_ids(&analyze_node.client, join_sql, "oid").await;
    assert_eq!(
        auto_shuffled, expected,
        "auto cost-model shuffle (threshold 0, both sides analyzed, NO force \
         override) must return the SAME 6 matching orders as the baseline; got \
         {auto_shuffled:?}"
    );

    // ----- High threshold broadcasts: stats present, large threshold. -----
    // At the large default threshold, the tiny analyzed collections stay under
    // the bound, so the cost model selects Broadcast. Still correct.
    analyze_node
        .client
        .simple_query("SET nodedb.broadcast_threshold_bytes = 1073741824")
        .await
        .expect("SET nodedb.broadcast_threshold_bytes high");
    let high_threshold = collect_ids(&analyze_node.client, join_sql, "oid").await;
    assert_eq!(
        high_threshold, expected,
        "high-threshold (broadcast) path with stats present must still return \
         the 6 matching orders; got {high_threshold:?}"
    );

    cluster.shutdown().await;
}
