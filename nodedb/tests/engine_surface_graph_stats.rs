// SPDX-License-Identifier: BUSL-1.1

//! End-to-end pgwire tests for `SHOW GRAPH STATS`.
//!
//! Covers the compact and VERBOSE response forms, error handling for
//! unknown collections, and the persistence-rooted invariant that
//! counts come from the durable edge store, not from
//! the in-memory CSR cache.

mod common;

use common::pgwire_harness::TestServer;

/// Find the row matching `collection` in a compact `SHOW GRAPH STATS` rowset.
/// Returns `(node_count, edge_count, distinct_label_count, labels_json)`.
fn find_row<'a>(rows: &'a [Vec<String>], collection: &str) -> Option<(i64, i64, i64, &'a str)> {
    rows.iter().find(|r| r[0] == collection).map(|r| {
        (
            r[1].parse::<i64>().expect("node_count is integer"),
            r[2].parse::<i64>().expect("edge_count is integer"),
            r[3].parse::<i64>()
                .expect("distinct_label_count is integer"),
            r[4].as_str(),
        )
    })
}

#[tokio::test]
async fn empty_collection_returns_zeros() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_empty").await.unwrap();

    let rows = srv.query_rows("SHOW GRAPH STATS 'g_empty'").await.unwrap();
    // With no edges, the broadcast aggregates to an empty result set.
    // That is the correct "zero state" — no row is emitted because the
    // counter table has no summary row for the collection yet.
    assert!(
        rows.is_empty() || {
            let (nc, ec, lc, _) = find_row(&rows, "g_empty").expect("row for g_empty");
            nc == 0 && ec == 0 && lc == 0
        },
        "expected empty or zero-counter row, got {rows:?}"
    );
}

#[tokio::test]
async fn single_edge_collection_counts() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_single").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_single' FROM 'a' TO 'b' TYPE 'knows'")
        .await
        .unwrap();

    let rows = srv.query_rows("SHOW GRAPH STATS 'g_single'").await.unwrap();
    let (nc, ec, lc, labels) = find_row(&rows, "g_single").expect("row for g_single");
    assert_eq!(nc, 2, "node_count");
    assert_eq!(ec, 1, "edge_count");
    assert_eq!(lc, 1, "distinct_label_count");
    assert!(
        labels.contains("knows"),
        "labels JSON must include 'knows': {labels}"
    );
}

#[tokio::test]
async fn strict_collection_edge_counts_once_at_every_stats_surface() {
    let srv = TestServer::start_multicores(2).await;
    srv.exec(
        "CREATE COLLECTION g_strict_counts (id TEXT PRIMARY KEY, name TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();
    srv.exec(
        "GRAPH INSERT EDGE IN g_strict_counts FROM 'a' TO 'b' \
         TYPE 'knows' PROPERTIES '{}'",
    )
    .await
    .unwrap();

    let compact = srv
        .query_rows("SHOW GRAPH STATS 'g_strict_counts'")
        .await
        .unwrap();
    let (nodes, edges, labels, labels_json) =
        find_row(&compact, "g_strict_counts").expect("compact stats row");
    assert_eq!(nodes, 2, "each endpoint must be counted once");
    assert_eq!(edges, 1, "one logical insert must count as one edge");
    assert_eq!(labels, 1, "one label must be distinct");
    assert!(
        labels_json.contains("\"count\":1"),
        "per-label count must also be one: {labels_json}"
    );

    let verbose = srv
        .query_rows("SHOW GRAPH STATS 'g_strict_counts' VERBOSE")
        .await
        .unwrap();
    assert_eq!(verbose.len(), 1, "one label row expected: {verbose:?}");
    assert_eq!(verbose[0][1], "knows");
    assert_eq!(verbose[0][2], "1", "verbose label count");
}

#[tokio::test]
async fn multi_label_distinct_count() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_multi").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_multi' FROM 'a' TO 'b' TYPE 'knows'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_multi' FROM 'a' TO 'c' TYPE 'follows'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_multi' FROM 'b' TO 'c' TYPE 'reports_to'")
        .await
        .unwrap();

    let rows = srv.query_rows("SHOW GRAPH STATS 'g_multi'").await.unwrap();
    let (nc, ec, lc, labels) = find_row(&rows, "g_multi").expect("row for g_multi");
    assert_eq!(nc, 3, "3 distinct nodes (a, b, c)");
    assert_eq!(ec, 3, "3 edges");
    assert_eq!(lc, 3, "3 distinct labels");
    for label in ["knows", "follows", "reports_to"] {
        assert!(
            labels.contains(label),
            "labels JSON missing '{label}': {labels}"
        );
    }
}

#[tokio::test]
async fn self_loop_counts_one_node() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_loop").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_loop' FROM 'x' TO 'x' TYPE 'self_ref'")
        .await
        .unwrap();

    let rows = srv.query_rows("SHOW GRAPH STATS 'g_loop'").await.unwrap();
    let (nc, ec, lc, _) = find_row(&rows, "g_loop").expect("row for g_loop");
    assert_eq!(nc, 1, "self-loop counts as one distinct node");
    assert_eq!(ec, 1);
    assert_eq!(lc, 1);
}

#[tokio::test]
async fn non_existent_collection_errors() {
    let srv = TestServer::start().await;
    let err = srv
        .query_rows("SHOW GRAPH STATS 'never_created'")
        .await
        .expect_err("missing collection must error");
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("never_created"),
        "expected collection-not-found error, got: {msg}"
    );
}

#[tokio::test]
async fn verbose_returns_one_row_per_label() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_verbose").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_verbose' FROM 'a' TO 'b' TYPE 'k'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_verbose' FROM 'a' TO 'c' TYPE 'k'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_verbose' FROM 'a' TO 'd' TYPE 'f'")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SHOW GRAPH STATS 'g_verbose' VERBOSE")
        .await
        .unwrap();
    // Verbose schema: (collection, label, edge_count) — one row per
    // (collection, label) pair.
    let by_label: std::collections::BTreeMap<String, i64> = rows
        .iter()
        .filter(|r| r[0] == "g_verbose")
        .map(|r| (r[1].clone(), r[2].parse::<i64>().expect("count is integer")))
        .collect();
    assert_eq!(by_label.get("k").copied(), Some(2));
    assert_eq!(by_label.get("f").copied(), Some(1));
}

#[tokio::test]
async fn tenant_aggregate_with_no_arg() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_agg_a").await.unwrap();
    srv.exec("CREATE COLLECTION g_agg_b").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_agg_a' FROM 'a' TO 'b' TYPE 'k'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_agg_b' FROM 'x' TO 'y' TYPE 'f'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_agg_b' FROM 'y' TO 'z' TYPE 'f'")
        .await
        .unwrap();

    let rows = srv.query_rows("SHOW GRAPH STATS").await.unwrap();
    let (_, ec_a, _, _) = find_row(&rows, "g_agg_a").expect("g_agg_a present");
    let (_, ec_b, _, _) = find_row(&rows, "g_agg_b").expect("g_agg_b present");
    assert_eq!(ec_a, 1);
    assert_eq!(ec_b, 2);
}

#[tokio::test]
async fn repeated_edges_in_multigraph() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_multi_edge").await.unwrap();
    // Two edges between the same pair with different labels — both count.
    srv.exec("GRAPH INSERT EDGE IN 'g_multi_edge' FROM 'a' TO 'b' TYPE 'l1'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_multi_edge' FROM 'a' TO 'b' TYPE 'l2'")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SHOW GRAPH STATS 'g_multi_edge'")
        .await
        .unwrap();
    let (nc, ec, lc, _) = find_row(&rows, "g_multi_edge").expect("row");
    assert_eq!(nc, 2, "still 2 distinct nodes");
    assert_eq!(ec, 2, "2 edges, distinct by label");
    assert_eq!(lc, 2);
}

#[tokio::test]
async fn deleted_edge_drops_from_live_count() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_del").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_del' FROM 'a' TO 'b' TYPE 'k'")
        .await
        .unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_del' FROM 'a' TO 'c' TYPE 'k'")
        .await
        .unwrap();

    // Sanity: both edges present.
    {
        let rows = srv.query_rows("SHOW GRAPH STATS 'g_del'").await.unwrap();
        let (_, ec, _, _) = find_row(&rows, "g_del").expect("pre-delete row");
        assert_eq!(ec, 2);
    }

    srv.exec("GRAPH DELETE EDGE IN 'g_del' FROM 'a' TO 'b' TYPE 'k'")
        .await
        .unwrap();

    let rows = srv.query_rows("SHOW GRAPH STATS 'g_del'").await.unwrap();
    let (_, ec, lc, _) = find_row(&rows, "g_del").expect("post-delete row");
    assert_eq!(ec, 1, "edge count decremented after delete");
    assert_eq!(lc, 1, "label count unchanged (label still in use)");
}
