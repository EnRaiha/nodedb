// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for C1: a table-qualified WHERE column
//! (`u.name` / `users.name`) on a single-table query must not silently
//! match zero rows. Before the fix the qualifier was retained into the
//! scan filter's field name (`"u.name"`), which no stored document ever
//! has, so every row was silently filtered out with no error.

mod common;

use common::pgwire_harness::TestServer;

async fn seed(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION t (\
                id STRING PRIMARY KEY, \
                name STRING, \
                age INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name, age) VALUES ('t1', 'bob', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name, age) VALUES ('t2', 'alice', 25)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_name_qualified_where_matches_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE t.name = 'bob'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "table-qualified WHERE must not return 0 rows"
    );
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alias_qualified_where_matches_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT alias_t.id FROM t alias_t WHERE alias_t.name = 'bob'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "alias-qualified WHERE must not return 0 rows"
    );
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_pk_qualified_column_matches() {
    let server = TestServer::start().await;
    seed(&server).await;

    // `age` is not the primary key, so this proves the fix isn't only
    // covering the TEXT-PK-equality fast path.
    let rows = server
        .query_text("SELECT id FROM t WHERE t.age = 30")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compound_qualified_predicate_matches() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE t.name = 'bob' AND t.age = 30")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unqualified_where_still_works() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE name = 'bob'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn qualified_order_by_sorts_single_table() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE t.age > 20 ORDER BY t.age")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "both rows have age > 20");
    // alice (25) then bob (30) — ascending by the qualified sort key.
    assert!(rows[0].contains("t2"), "ascending order: t2 (age 25) first");
    assert!(
        rows[1].contains("t1"),
        "ascending order: t1 (age 30) second"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatched_qualifier_errors_instead_of_zero_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let result = server
        .query_text("SELECT id FROM t WHERE wrong.name = 'bob'")
        .await;
    assert!(
        result.is_err(),
        "a qualifier that matches neither the table name nor its alias must be a typed error, \
         not a silent 0-row result"
    );
}
