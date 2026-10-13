// SPDX-License-Identifier: BUSL-1.1

//! Recognition of the temporal / audit query functions.
//!
//! `VERIFY_AUDIT_CHAIN`, `VERIFY_HASH_CHAIN`, `BALANCE_AS_OF`,
//! `TEMPORAL_LOOKUP`, `VERIFY_BALANCE` and `CONVERT_CURRENCY_LOOKUP` run only
//! as a whole statement, `SELECT <FUNC>(...)`. The server recognizes them
//! from the parsed statement. A function name inside a string literal or a
//! comment is ordinary text, so the statement runs as written.

use crate::harness::TestServer;

const FUNCTIONS: [&str; 6] = [
    "verify_audit_chain",
    "verify_hash_chain",
    "balance_as_of",
    "temporal_lookup",
    "verify_balance",
    "convert_currency_lookup",
];

/// An INSERT whose id and value spell a query-function name stores the row.
/// Each name is tried in both positions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_naming_a_query_function_is_an_ordinary_insert() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION qfr_lit").await.unwrap();

    for name in FUNCTIONS {
        let id = format!("tmp_{name}_x");
        server
            .exec(&format!(
                "INSERT INTO qfr_lit (id, name) VALUES ('{id}', '{name}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert naming {name} must succeed: {e}"));
        let rows = server
            .query_rows(&format!("SELECT name FROM qfr_lit WHERE id = '{id}'"))
            .await
            .unwrap();
        assert_eq!(rows, vec![vec![name.to_string()]], "{name}");
    }
}

/// A comment that names a query function does not change the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn comment_naming_a_query_function_does_not_route() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION qfr_cmt").await.unwrap();

    server
        .exec("INSERT INTO qfr_cmt (id, name) VALUES ('a', 'x') -- verify_balance note")
        .await
        .expect("a comment naming a query function must not hijack the INSERT");
    let rows = server
        .query_rows("SELECT name FROM qfr_cmt WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["x".to_string()]]);
}

/// The whole-statement call still runs the function.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_call_still_routes() {
    let server = TestServer::start().await;

    let rows = server
        .query_text("SELECT VERIFY_AUDIT_CHAIN(1, 100)")
        .await
        .expect("the harness superuser may verify the audit chain");
    assert!(
        rows.iter().any(|row| row.contains("valid")),
        "the verdict must come back as the result row: {rows:?}"
    );
}

/// A query function used as one item of a wider SELECT is refused. Running
/// the function and dropping the rest of the statement would answer a
/// different question.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn misplaced_call_is_refused() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION qfr_misplaced")
        .await
        .unwrap();

    server
        .expect_error(
            "SELECT id, VERIFY_BALANCE('qfr_misplaced', 'v') FROM qfr_misplaced",
            "whole statement",
        )
        .await;
}

/// A call with the wrong number of arguments names the signature.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arity_error_names_the_signature() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "SELECT VERIFY_BALANCE('qfr_arity')",
            "VERIFY_BALANCE requires (collection, column)",
        )
        .await;
}
