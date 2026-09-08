// SPDX-License-Identifier: BUSL-1.1

//! Sequence accessors in SQL *expression* contexts must fail loudly with
//! SQLSTATE 0A000 (`feature_not_supported`) — never silently NULL — while
//! `DEFAULT nextval(...)` keeps working end-to-end.
//!
//! Grand plan (issue #294) matrix row F: accessors are stateful and
//! CP-side only; the DEFAULT path is the sole legal evaluation site.
//! Registered (A1) + dispatch guard (A3) + fold classification (A5) make
//! every escape loud:
//!
//! - Constant contexts (`SELECT nextval('s')` without a FROM clause,
//!   VALUES cells) evaluate through the CP registry (sequence_const_select);
//!   a missing sequence raises a plan error naming it.
//! - SELECT list / WHERE / ORDER BY over a table — row-scope eval.
//! - VALUES / INSERT..SELECT — expression eval on the write path.
//! - Derived-table constant expressions — same row-scope evaluator
//!   (mirrors issue #295's fold-silent class: `mod(5, 0)` must also stay
//!   loud 22012 here).

use crate::harness::TestServer;

async fn setup_kv(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION seqctx (id BIGINT PRIMARY KEY, grp BIGINT, denom BIGINT) \
             WITH (engine = 'kv')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO seqctx (id, grp, denom) VALUES (1, 1, 0), (2, 1, 1)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fromless_missing_sequence_raises_plan_error() {
    // Constant contexts (no FROM clause) gain real registry evaluation
    // (see sequence_const_select): nextval('existing') advances and a
    // missing sequence raises the registry-miss plan error naming it —
    // never a silent NULL. 0A000 remains reserved for row-scope contexts.
    let server = TestServer::start().await;
    server.expect_error("SELECT nextval('nope')", "42601").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_list_nextval_raises_0a000() {
    let server = TestServer::start().await;
    setup_kv(&server).await;
    server
        .exec("CREATE COLLECTION seqctx_col (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'columnar')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO seqctx_col (id, v) VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION seqctx_doc (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO seqctx_doc (id, v) VALUES (1, 'a')")
        .await
        .unwrap();

    // NOTE: kv-engine SELECT *projection* expressions are not evaluated at
    // all (pre-existing gap: `SELECT 1 + 1 FROM kv` returns an empty column)
    // so the loud-0A000 assertion is pinned on the engines that evaluate
    // row projections: columnar and document. kv WHERE / ORDER BY / VALUES
    // contexts are covered by the other tests here.
    for sql in [
        "SELECT nextval('nope') FROM seqctx_col",
        "SELECT nextval('nope') FROM seqctx_doc",
    ] {
        server.expect_error(sql, "0A000").await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn where_nextval_raises_0a000() {
    let server = TestServer::start().await;
    setup_kv(&server).await;
    server
        .expect_error("SELECT id FROM seqctx WHERE nextval('nope') > 0", "0A000")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_nextval_raises_0a000() {
    let server = TestServer::start().await;
    setup_kv(&server).await;
    server
        .expect_error("SELECT id FROM seqctx ORDER BY nextval('nope')", "0A000")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_missing_sequence_raises_plan_error() {
    let server = TestServer::start().await;
    setup_kv(&server).await;
    server
        .expect_error("INSERT INTO seqctx (id) VALUES (nextval('nope'))", "42601")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_nextval_raises_0a000() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION seqctx_is (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO seqctx_is (id, v) VALUES (1, 'a')")
        .await
        .unwrap();
    server
        .expect_error(
            "INSERT INTO seqctx_is (id) SELECT nextval('nope') FROM seqctx_is",
            "0A000",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_constant_errors_stay_loud() {
    let server = TestServer::start().await;
    // Issue #295 class: constant expressions over a derived table must not
    // fold silently — division stays 22012, sequence accessor stays 0A000.
    server
        .expect_error("SELECT * FROM (SELECT mod(5, 0) AS v) d", "22012")
        .await;
    // A derived-table constant cell is a constant context too: the accessor
    // now evaluates through the CP registry, so a missing sequence raises
    // the registry-miss plan error instead of 0A000.
    server
        .expect_error("SELECT * FROM (SELECT nextval('nope') AS v) d", "42601")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_nextval_still_fills_rows_after_guard() {
    // A1+A3 must not disturb the legal DEFAULT path: the CP-side sequence
    // registry still materializes values per row.
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE seqctx_default").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION seqctx_d (k BIGINT DEFAULT nextval('seqctx_default') PRIMARY KEY, v TEXT) \
             WITH (engine = 'kv')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO seqctx_d (v) VALUES ('one'), ('two')")
        .await
        .unwrap();
    let rows = server
        .query_named_rows("SELECT k FROM seqctx_d ORDER BY k")
        .await
        .expect("rows readable");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows.iter().map(|r| r.get("k")).collect::<Vec<_>>(),
        vec![Some(&"1".to_string()), Some(&"2".to_string())],
        "DEFAULT nextval must keep materializing 1,2: {rows:?}"
    );
}
