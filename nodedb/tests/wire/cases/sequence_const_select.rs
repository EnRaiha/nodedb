// SPDX-License-Identifier: BUSL-1.1

//! Sequence accessors in constant (row-less) contexts: `SELECT nextval('s')`
//! without a FROM clause and explicit `VALUES (nextval('s'))` cells.
//!
//! These fold at plan time inside nodedb-sql, which has no registry; the
//! control plane installs a registry hook for the duration of planning so
//! each constant accessor advances exactly once, in expression order.
//! Row-scope contexts (over a table) keep raising 0A000 — only constant
//! contexts gain real evaluation.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fromless_nextval_advances_and_currval_reads_back() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE csel_seq").await.unwrap();

    let rows = server
        .query_named_rows("SELECT nextval('csel_seq') AS n, currval('csel_seq') AS c")
        .await
        .expect("rows");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].get("n").map(|s| s.as_str()), Some("1"), "{rows:?}");
    assert_eq!(rows[0].get("c").map(|s| s.as_str()), Some("1"), "{rows:?}");

    // A second statement continues the same sequence.
    let rows = server
        .query_named_rows("SELECT nextval('csel_seq') AS n")
        .await
        .expect("rows");
    assert_eq!(rows[0].get("n").map(|s| s.as_str()), Some("2"), "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_cells_advance_per_row() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE csel_vseq").await.unwrap();
    server
        .exec("CREATE COLLECTION csel_t (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO csel_t (id, v) VALUES (nextval('csel_vseq'), 'a'), (nextval('csel_vseq'), 'b')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT id FROM csel_t ORDER BY id")
        .await
        .expect("rows");
    let ids: Vec<_> = rows
        .iter()
        .map(|r| r.get("id").map(|s| s.as_str()))
        .collect();
    assert_eq!(ids, vec![Some("1"), Some("2")], "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setval_const_expr_returns_value() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE csel_sseq").await.unwrap();
    let rows = server
        .query_named_rows("SELECT setval('csel_sseq', 41) AS v")
        .await
        .expect("rows");
    assert_eq!(rows[0].get("v").map(|s| s.as_str()), Some("41"), "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_sequence_in_const_context_raises() {
    let server = TestServer::start().await;
    // Registry miss surfaces as a plan error naming the sequence (same class
    // as the DEFAULT path), never a silent NULL and never 0A000.
    let err = server
        .exec("SELECT nextval('csel_missing')")
        .await
        .expect_err("must raise");
    assert!(
        err.to_string().contains("csel_missing"),
        "error must name the sequence: {err}"
    );
}
