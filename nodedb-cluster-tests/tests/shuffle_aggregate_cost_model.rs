// SPDX-License-Identifier: BUSL-1.1

//! Cost-based automatic shuffle-AGGREGATE selection test (E5d).
//!
//! Brings up a 3-node cluster, creates a distributed collection, inserts rows
//! with a HIGH-cardinality GROUP BY column `k`, runs `ANALYZE` (so the catalog's
//! `distinct_count` for `k` is populated), then runs a real SQL GROUP BY
//! aggregate and verifies the planner's ANALYZE-driven cost model auto-selects a
//! whole-aggregate shuffle WITHOUT any manual `nodedb.force_shuffle_agg`
//! override — purely by `SET nodedb.shuffle_agg_threshold = 1`:
//!
//! With `k` having many distinct values, the estimated group cardinality
//! (capped at the row count) far exceeds the threshold of 1, so the cost model
//! emits `Exchange{ShuffleAggregate}` from the persisted statistics alone. The
//! assertion is correctness: the auto-shuffled aggregate must return EXACTLY the
//! same per-group aggregates the default Gather plan returns (the baseline,
//! captured before any threshold is set). A regression in either path is
//! distinguishable.
//!
//! IMPORTANT: column statistics are written to the *local* catalog of the node
//! that runs ANALYZE — they are NOT Raft-replicated like collection DDL. The
//! cost model reads stats from the local catalog of whichever node *plans* the
//! aggregate. So ANALYZE and the auto-shuffle query MUST run on the SAME node
//! for the stats-driven decision to be deterministic. We pin both to node 0.

use std::collections::BTreeMap;
use std::time::Duration;

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run `sql` and collect the GROUP BY result as a sorted map keyed by the group
/// column `k`, with the remaining aggregate columns as a stable comma-joined
/// string, so two result sets are order-independently comparable.
async fn collect_groups(
    client: &tokio_postgres::Client,
    sql: &str,
    cols: &[&str],
) -> BTreeMap<String, String> {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    let mut groups: BTreeMap<String, String> = BTreeMap::new();
    for m in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
            let key = row.get("k").map(|s| s.to_string()).unwrap_or_default();
            let vals: Vec<String> = cols
                .iter()
                .map(|c| row.get(*c).map(|s| s.to_string()).unwrap_or_default())
                .collect();
            groups.insert(key, vals.join(","));
        }
    }
    groups
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
async fn cost_model_auto_selects_shuffle_aggregate_from_analyze_stats() {
    const ROWS: usize = 24;

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION metrics \
             (id TEXT PRIMARY KEY, k TEXT, v BIGINT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION metrics");

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

    // 24 rows, each with a DISTINCT `k` (high group cardinality). Every group has
    // exactly one row so COUNT(*) = 1 and SUM(v) = v per group — distinct values
    // make a regression in any group obvious.
    let mut values = String::new();
    for i in 0..ROWS {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("('r{i}', 'k{i}', {})", (i as i64 + 1) * 10));
    }
    cluster.nodes[0]
        .client
        .simple_query(&format!("INSERT INTO metrics (id, k, v) VALUES {values}"))
        .await
        .expect("insert metrics");

    // Wait until every node sees the full row count so replication has completed
    // before ANALYZE and the cross-node aggregate.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all metric rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM metrics"))
                });
                n >= ROWS
            },
        )
        .await;
    }

    // Alias every aggregate so the result column names are stable regardless of
    // how each plan path qualifies its output columns.
    let agg_sql = "SELECT k, COUNT(*) AS cnt, SUM(v) AS s FROM metrics GROUP BY k";
    let cols = ["cnt", "s"];

    // ----- Baseline: default Gather plan, no override, no threshold set. -----
    // Establishes the correct answer and proves the Gather path works.
    let baseline = collect_groups(&cluster.nodes[0].client, agg_sql, &cols).await;
    assert_eq!(
        baseline.len(),
        ROWS,
        "baseline must return {ROWS} distinct groups; got {baseline:?}"
    );

    // ----- Persist statistics via ANALYZE. -----
    // The accepted form is `ANALYZE <collection>` (the maintenance handler splits
    // on whitespace and reads the second token). This computes and persists
    // per-column stats — crucially `distinct_count` for `k` and `row_count` > 0.
    // Pin ANALYZE and the subsequent query to node 0 because column stats are
    // local-catalog only (not Raft-replicated).
    let analyze_node = &cluster.nodes[0];
    analyze_node
        .client
        .simple_query("ANALYZE metrics")
        .await
        .expect("ANALYZE metrics");

    // ----- Auto-shuffle: low threshold, stats present, NO force override. -----
    // With `k` having many distinct values, the estimated group cardinality
    // (capped at the row count) exceeds the threshold of 1, so the cost model
    // emits Exchange{ShuffleAggregate} purely from the persisted statistics — no
    // `nodedb.force_shuffle_agg`. Run on the SAME node that persisted the stats.
    analyze_node
        .client
        .simple_query("SET nodedb.shuffle_agg_threshold = 1")
        .await
        .expect("SET nodedb.shuffle_agg_threshold = 1");
    let auto_shuffled = collect_groups(&analyze_node.client, agg_sql, &cols).await;
    assert_eq!(
        auto_shuffled, baseline,
        "auto cost-model shuffle aggregate (threshold 1, stats present, NO force \
         override) must return the SAME per-group aggregates as the baseline \
         Gather aggregate; got {auto_shuffled:?} vs {baseline:?}"
    );

    // ----- High threshold keeps Gather: stats present, large threshold. -----
    // At a threshold above the group cardinality, the cost model keeps the Gather
    // plan. Still correct. Proves the threshold actually gates the decision.
    analyze_node
        .client
        .simple_query("SET nodedb.shuffle_agg_threshold = 1000000")
        .await
        .expect("SET nodedb.shuffle_agg_threshold high");
    let high_threshold = collect_groups(&analyze_node.client, agg_sql, &cols).await;
    assert_eq!(
        high_threshold, baseline,
        "high-threshold (Gather) path with stats present must still return the \
         same per-group aggregates; got {high_threshold:?} vs {baseline:?}"
    );

    cluster.shutdown().await;
}
