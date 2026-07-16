// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill WAL-durability regressions for the cross-engine
//! overlays: Graph (edges + node labels) and Full-Text Search.
//!
//! Both overlays sit on top of a base document collection rather than
//! owning their own primary storage, so a `kill -9` must not just
//! preserve the underlying document rows — it must also leave the
//! overlay's own index (CSR adjacency for Graph, the inverted index for
//! FTS) queryable again once the process reopens the same data
//! directory and replays the WAL.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn graph_edges_survive_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION crash_graph_edges").await;
    h.exec("GRAPH INSERT EDGE IN 'crash_graph_edges' FROM 'a' TO 'b' TYPE 'knows'")
        .await;

    // Live sanity BEFORE the crash: the edge is traversable pre-restart, so
    // any post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "MATCH (x)-[:knows]->(y) IN 'crash_graph_edges' RETURN x, y",
            "y",
        )
        .await;
    assert_eq!(
        live,
        vec!["b".to_string()],
        "graph edge must be traversable BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "MATCH (x)-[:knows]->(y) IN 'crash_graph_edges' RETURN x, y",
            "y",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["b".to_string()],
        "graph edge did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_node_labels_survive_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION crash_graph_labels").await;
    h.exec("INSERT INTO crash_graph_labels { id: 'alice', name: 'Alice' }")
        .await;
    h.exec("INSERT INTO crash_graph_labels { id: 'bob', name: 'Bob' }")
        .await;
    h.exec("GRAPH INSERT EDGE IN 'crash_graph_labels' FROM 'alice' TO 'bob' TYPE 'knows'")
        .await;
    h.exec("GRAPH LABEL 'alice' AS 'Person'").await;
    h.exec("GRAPH LABEL 'bob' AS 'Person'").await;

    // Live sanity BEFORE the crash: the labeled MATCH works pre-restart, so
    // any post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col("MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b", "b")
        .await;
    assert_eq!(
        live,
        vec!["bob".to_string()],
        "labeled MATCH must work BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b", "b")
        .await;
    assert_eq!(
        recovered,
        vec!["bob".to_string()],
        "graph node labels did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fts_index_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION crash_fts WITH (engine='document_schemaless')")
        .await;
    h.exec("INSERT INTO crash_fts { id: 'd1', body: 'The quick brown fox' }")
        .await;

    // Live sanity BEFORE the crash: the FTS match works pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "SELECT id FROM crash_fts WHERE text_match(body, 'fox')",
            "id",
        )
        .await;
    assert_eq!(
        live,
        vec!["d1".to_string()],
        "FTS match must work BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT id FROM crash_fts WHERE text_match(body, 'fox')",
            "id",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["d1".to_string()],
        "FTS index did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}
