// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for row-bound validation: a `LIMIT` or `OFFSET` value
//! outside the `usize` domain must fail the statement with SQLSTATE `2201W`
//! (`invalid_limit_value`), the code PostgreSQL raises for the same input.
//!
//! The planner reads both clauses through one extractor that answers
//! `Option<usize>`, and every consumer treats `None` as a permissive default
//! — no limit, offset zero. An unreadable bound therefore widens the query
//! rather than failing it, so `LIMIT -1` returns the whole collection. On a
//! large collection that is a full scan the caller explicitly asked to avoid.
//!
//! Each rejection test is paired with a row-count guard on a seeded
//! collection, so a regression that drops the bound again is caught as
//! "returned every row" rather than only as "no error raised".

use crate::harness::TestServer;

/// Five rows, so a dropped bound is visible as a row count rather than an
/// empty result that any bound would also produce.
async fn seed(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY) WITH (engine='document_strict')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id) VALUES ('r1'), ('r2'), ('r3'), ('r4'), ('r5')"
    ))
    .await
    .unwrap();
}

/// A query whose row bound is out of domain must fail, and must never come
/// back with rows — naming the count when it does.
async fn expect_no_rows_returned(srv: &TestServer, sql: &str) {
    match srv.query_text(sql).await {
        Err(_) => {}
        Ok(rows) => panic!(
            "an out-of-domain row bound must not widen the scan, got {} row(s) from: {sql}",
            rows.len()
        ),
    }
}

/// The reported symptom: a negative LIMIT literal.
#[tokio::test]
async fn negative_limit_errors_2201w() {
    let srv = TestServer::start().await;
    seed(&srv, "limit_bounds_neg").await;

    srv.expect_error("SELECT id FROM limit_bounds_neg LIMIT -1", "2201W")
        .await;
    expect_no_rows_returned(&srv, "SELECT id FROM limit_bounds_neg LIMIT -1").await;
}

/// The untyped-parameter spelling. A pgwire driver that sends
/// `Type::UNKNOWN` binds `-1` as text, so the planner sees `LIMIT '-1'`.
#[tokio::test]
async fn negative_limit_as_text_errors_2201w() {
    let srv = TestServer::start().await;
    seed(&srv, "limit_bounds_text").await;

    srv.expect_error("SELECT id FROM limit_bounds_text LIMIT '-1'", "2201W")
        .await;
    expect_no_rows_returned(&srv, "SELECT id FROM limit_bounds_text LIMIT '-1'").await;
}

/// A negative OFFSET collapses to "skip nothing" today.
#[tokio::test]
async fn negative_offset_errors_2201w() {
    let srv = TestServer::start().await;
    seed(&srv, "offset_bounds_neg").await;

    srv.expect_error("SELECT id FROM offset_bounds_neg OFFSET -2", "2201W")
        .await;
    expect_no_rows_returned(&srv, "SELECT id FROM offset_bounds_neg OFFSET -2").await;
}

/// Non-numeric text is not a row bound at all.
#[tokio::test]
async fn non_numeric_limit_errors_2201w() {
    let srv = TestServer::start().await;
    seed(&srv, "limit_bounds_text_abc").await;

    srv.expect_error("SELECT id FROM limit_bounds_text_abc LIMIT 'abc'", "2201W")
        .await;
    expect_no_rows_returned(&srv, "SELECT id FROM limit_bounds_text_abc LIMIT 'abc'").await;
}

/// A bound wider than `usize` cannot be applied and must not be dropped.
#[tokio::test]
async fn overflowing_limit_errors_2201w() {
    let srv = TestServer::start().await;
    seed(&srv, "limit_bounds_overflow").await;

    let sql = "SELECT id FROM limit_bounds_overflow LIMIT 99999999999999999999999999";
    srv.expect_error(sql, "2201W").await;
    expect_no_rows_returned(&srv, sql).await;
}

/// Control: valid bounds still work, including the `LIMIT 0` edge that must
/// stay distinct from a missing bound.
#[tokio::test]
async fn valid_bounds_are_honored() {
    let srv = TestServer::start().await;
    seed(&srv, "limit_bounds_valid").await;

    let none = srv
        .query_text("SELECT id FROM limit_bounds_valid LIMIT 0")
        .await
        .expect("LIMIT 0 must plan");
    assert_eq!(none.len(), 0, "LIMIT 0 must return no rows");

    let two = srv
        .query_text("SELECT id FROM limit_bounds_valid LIMIT 2")
        .await
        .expect("LIMIT 2 must plan");
    assert_eq!(two.len(), 2, "LIMIT 2 must return two rows");

    let skipped = srv
        .query_text("SELECT id FROM limit_bounds_valid ORDER BY id OFFSET 3")
        .await
        .expect("OFFSET 3 must plan");
    assert_eq!(skipped.len(), 2, "OFFSET 3 of five rows must return two");

    let all = srv
        .query_text("SELECT id FROM limit_bounds_valid")
        .await
        .expect("an unbounded query must plan");
    assert_eq!(all.len(), 5, "no clause means every row");
}
