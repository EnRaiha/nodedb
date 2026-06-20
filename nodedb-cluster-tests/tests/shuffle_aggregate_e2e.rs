// SPDX-License-Identifier: BUSL-1.1

//! End-to-end distributed shuffle-AGGREGATE test (E5c, FORCE path).
//!
//! Brings up a 3-node cluster, creates a distributed collection, inserts rows
//! with a low-cardinality GROUP BY column, then runs a real SQL GROUP BY
//! aggregate TWICE: once with the default Gather plan and once with the permanent
//! force-shuffle-aggregate override engaged via the session var
//! `SET nodedb.force_shuffle_agg = on`. The override makes the planner emit a
//! whole-aggregate `Exchange{ShuffleAggregate}` instead of the default
//! Gather-merge plan; the coordinator resolver fans partial-state producers to
//! the collection's owner, repartitions per-group partial states on the GROUP BY
//! key to the part-owners, finalizes each part, and merges the results.
//!
//! The assertion is correctness: the shuffle aggregate must return EXACTLY the
//! same per-group aggregates the default Gather path returns. A baseline
//! (no-override) aggregate is run first so a regression in either path is
//! distinguishable.

use std::collections::BTreeMap;
use std::time::Duration;

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run `sql` and collect the GROUP BY result as a sorted map keyed by the group
/// column `k`, with the remaining numeric aggregate columns as a stable
/// comma-joined string, so two result sets are order-independently comparable.
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
async fn distributed_shuffle_aggregate_matches_gather() {
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

    // 9 rows across 3 low-cardinality groups (k in {a, b, c}). Values chosen so
    // each aggregate (COUNT/SUM/AVG/MIN/MAX) is distinct per group.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO metrics (id, k, v) VALUES \
             ('r1', 'a', 10), ('r2', 'a', 20), ('r3', 'a', 30), \
             ('r4', 'b', 5),  ('r5', 'b', 15), ('r6', 'b', 25), \
             ('r7', 'c', 100),('r8', 'c', 200),('r9', 'c', 300)",
        )
        .await
        .expect("insert metrics");

    // Wait until every node sees the full row count so replication has completed
    // before asserting the cross-node aggregate.
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
                n >= 9
            },
        )
        .await;
    }

    // Alias every aggregate so the result column names are stable regardless of
    // how each plan path qualifies its output columns.
    let agg_sql = "SELECT k, \
                   COUNT(*) AS cnt, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx \
                   FROM metrics GROUP BY k";
    let cols = ["cnt", "s", "a", "mn", "mx"];

    // Baseline (default Gather path, no override) — establishes the correct
    // answer and proves the non-shuffle path still works.
    let baseline = collect_groups(&cluster.nodes[0].client, agg_sql, &cols).await;
    assert_eq!(
        baseline.len(),
        3,
        "baseline must return 3 groups; got {baseline:?}"
    );

    // Engage the permanent force-shuffle-aggregate override on node 1's session,
    // then run the SAME aggregate. The plan cache is bypassed while the override
    // is set, so this re-plans into an Exchange{ShuffleAggregate} and drives the
    // cross-node distributed GROUP BY. Use an explicit small partition count to
    // exercise multi-part fan-out and merge.
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.force_shuffle_agg = on")
        .await
        .expect("SET nodedb.force_shuffle_agg");
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.shuffle_agg_num_parts = 2")
        .await
        .expect("SET nodedb.shuffle_agg_num_parts");

    let shuffled = collect_groups(&cluster.nodes[1].client, agg_sql, &cols).await;
    assert_eq!(
        shuffled, baseline,
        "distributed shuffle aggregate must return the SAME per-group aggregates as the \
         baseline Gather aggregate; got {shuffled:?} vs {baseline:?}"
    );

    // Turning the override off again must fall back to the default plan and still
    // return the correct result (the cache was not poisoned with the shuffle
    // plan).
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.force_shuffle_agg = off")
        .await
        .expect("SET nodedb.force_shuffle_agg = off");
    let after = collect_groups(&cluster.nodes[1].client, agg_sql, &cols).await;
    assert_eq!(
        after, baseline,
        "aggregate after disabling the override must still match the baseline; got {after:?}"
    );

    cluster.shutdown().await;
}
