// SPDX-License-Identifier: BUSL-1.1

//! A Parse message reports a parse-analysis error with its SQLSTATE, as
//! PostgreSQL does. A driver's prepare fails. An error a bound value can
//! change, or one PostgreSQL raises while planning or executing, surfaces at
//! Execute.

use crate::harness::TestServer;
use tokio_postgres::types::Type;

/// The SQLSTATE a server error carries, or a panic naming `sql`.
fn sqlstate(sql: &str, error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .unwrap_or_else(|| panic!("{sql}: no server SQLSTATE: {error}"))
        .code()
        .code()
        .to_string()
}

async fn create_t(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create t");
    server
        .exec("INSERT INTO t (id, n) VALUES ('a', 1), ('b', 2)")
        .await
        .expect("seed t");
}

/// Asserts preparing `sql` fails with `state`.
async fn assert_prepare_fails(server: &TestServer, sql: &str, types: &[Type], state: &str) {
    let error = server
        .client
        .prepare_typed(sql, types)
        .await
        .expect_err(sql);
    assert_eq!(sqlstate(sql, &error), state, "{sql}");
}

#[tokio::test]
async fn prepare_reports_an_unknown_column_with_42703() {
    let server = TestServer::start().await;
    create_t(&server).await;
    assert_prepare_fails(&server, "SELECT nosuchcol FROM t", &[], "42703").await;
}

/// Name resolution does not depend on a bound value, so a statement with
/// placeholders reports it at Parse too.
#[tokio::test]
async fn prepare_reports_name_errors_with_placeholders() {
    let server = TestServer::start().await;
    create_t(&server).await;
    assert_prepare_fails(
        &server,
        "SELECT nosuchcol FROM t WHERE id = $1",
        &[Type::TEXT],
        "42703",
    )
    .await;
    assert_prepare_fails(
        &server,
        "SELECT id FROM no_such_collection WHERE id = $1",
        &[Type::TEXT],
        "42P01",
    )
    .await;
}

#[tokio::test]
async fn prepare_reports_an_unknown_table_and_a_grouping_error() {
    let server = TestServer::start().await;
    create_t(&server).await;
    assert_prepare_fails(&server, "SELECT id FROM no_such_collection", &[], "42P01").await;
    assert_prepare_fails(
        &server,
        "SELECT id, COUNT(*) FROM t GROUP BY n",
        &[],
        "42803",
    )
    .await;
}

/// A constant division by zero is a planning error in PostgreSQL. Parse
/// succeeds and Execute reports `22012`.
#[tokio::test]
async fn a_planning_data_error_reports_at_execute() {
    let server = TestServer::start().await;
    let sql = "SELECT 1 / 0 AS q";
    let statement = server
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: Parse must succeed: {}", sqlstate(sql, &e)));
    let error = server
        .client
        .query(&statement, &[])
        .await
        .expect_err("Execute must report the division by zero");
    assert_eq!(sqlstate(sql, &error), "22012");
}

/// A placeholder bound at Execute plans with its value, so a statement that
/// plans only with that value prepares and runs.
#[tokio::test]
async fn a_bound_limit_prepares_and_runs() {
    let server = TestServer::start().await;
    create_t(&server).await;
    let sql = "SELECT id FROM t ORDER BY id LIMIT $1";
    let statement = server
        .client
        .prepare_typed(sql, &[Type::INT8])
        .await
        .unwrap_or_else(|e| panic!("{sql}: Parse must succeed: {}", sqlstate(sql, &e)));
    let rows = server
        .client
        .query(&statement, &[&1_i64])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", sqlstate(sql, &e)));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("id"), "a");
}
