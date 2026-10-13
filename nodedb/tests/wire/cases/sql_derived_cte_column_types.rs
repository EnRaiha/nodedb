// SPDX-License-Identifier: BUSL-1.1

//! A column read through a CTE or a derived JOIN side carries the type its
//! body gives it. The RowDescription OIDs match a plain `SELECT` of the same
//! columns, and a driver decodes each cell as that type.

use crate::harness::TestServer;

/// `n`, `f`, `t`, `b`, `ts` as RowDescription OIDs: int8, float8, text,
/// bool, timestamp.
const EXPECTED_OIDS: [(&str, u32); 5] = [("n", 20), ("f", 701), ("t", 25), ("b", 16), ("ts", 1114)];

/// One strict row carrying each checked type.
async fn seed(server: &TestServer, collection: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, n BIGINT, f FLOAT8, t TEXT, b BOOL, ts TIMESTAMP) \
             WITH (engine='document_strict')"
        ),
        format!(
            "INSERT INTO {collection} (id, n, f, t, b, ts) \
             VALUES ('r1', 7, 2.5, 'alpha', true, '2020-03-05 10:00:00')"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// The server's SQLSTATE and message when `e` carries them, else its
/// `Display`, which prints only `db error` for a server error.
fn db_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{} {}", db.code().code(), db.message()),
        None => e.to_string(),
    }
}

/// Runs `sql` through the simple protocol and checks its `n` cell, then
/// through the extended protocol and checks the column OIDs and the decoded
/// `n`, `f`, `t`, `b` cells.
async fn assert_typed(server: &TestServer, sql: &str) {
    // The simple protocol plans the same statement and reads the same row.
    let simple = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("simple protocol: {sql}: {e}"));
    assert_eq!(simple.len(), 1, "simple protocol: {sql}: expected one row");
    assert_eq!(simple[0][0], "7", "simple protocol: {sql}: n");

    let rows = server
        .client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("extended protocol: {sql}: {}", db_error(&e)));
    assert_eq!(rows.len(), 1, "{sql}: expected one row");
    let row = &rows[0];
    let announced: Vec<(String, u32)> = row
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_().oid()))
        .collect();
    let expected: Vec<(String, u32)> = EXPECTED_OIDS
        .iter()
        .map(|(name, oid)| ((*name).to_string(), *oid))
        .collect();
    assert_eq!(announced, expected, "{sql}: RowDescription OIDs");

    let n: i64 = row.get("n");
    let f: f64 = row.get("f");
    let t: &str = row.get("t");
    let b: bool = row.get("b");
    assert_eq!(
        (n, f, t, b),
        (7, 2.5, "alpha", true),
        "{sql}: decoded cells"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_select_announces_the_declared_types() {
    let server = TestServer::start().await;
    seed(&server, "ct_plain").await;
    assert_typed(&server, "SELECT n, f, t, b, ts FROM ct_plain").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_reference_announces_the_body_types() {
    let server = TestServer::start().await;
    seed(&server, "ct_cte").await;
    assert_typed(
        &server,
        "WITH c AS (SELECT id, n, f, t, b, ts FROM ct_cte) SELECT n, f, t, b, ts FROM c",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chained_cte_reference_announces_the_body_types() {
    let server = TestServer::start().await;
    seed(&server, "ct_chain").await;
    assert_typed(
        &server,
        "WITH c AS (SELECT id, n, f, t, b, ts FROM ct_chain), \
              d AS (SELECT n, f, t, b, ts FROM c) \
         SELECT n, f, t, b, ts FROM d",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_derived_from_table_announces_the_body_types() {
    let server = TestServer::start().await;
    seed(&server, "ct_from").await;
    assert_typed(
        &server,
        "SELECT d.n, d.f, d.t, d.b, d.ts FROM (SELECT id, n, f, t, b, ts FROM ct_from) AS d",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_derived_join_side_announces_the_body_types() {
    let server = TestServer::start().await;
    seed(&server, "ct_derived").await;
    assert_typed(
        &server,
        "SELECT d.n, d.f, d.t, d.b, d.ts FROM ct_derived AS k \
         JOIN (SELECT id, n, f, t, b, ts FROM ct_derived) AS d ON k.id = d.id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_join_side_announces_the_body_types() {
    let server = TestServer::start().await;
    seed(&server, "ct_cte_join").await;
    assert_typed(
        &server,
        "WITH c AS (SELECT id, n, f, t, b, ts FROM ct_cte_join) \
         SELECT c.n, c.f, c.t, c.b, c.ts FROM ct_cte_join AS k JOIN c ON k.id = c.id",
    )
    .await;
}
