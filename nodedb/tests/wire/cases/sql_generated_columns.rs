// SPDX-License-Identifier: BUSL-1.1

//! A write that supplies a generated column is refused with SQLSTATE `428C9`.
//!
//! The engine computes a `GENERATED ALWAYS AS` column on every write, so a
//! supplied value would never be stored. INSERT, UPSERT and INSERT … SELECT
//! refuse it, as an UPDATE of the column is refused.

use crate::harness::TestServer;

const GENERATED_ALWAYS: &str = "428C9";

async fn generated(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (\
                 id TEXT PRIMARY KEY, price FLOAT, \
                 tax FLOAT GENERATED ALWAYS AS (price * 0.1)) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_naming_a_generated_column_is_refused() {
    let server = TestServer::start().await;
    generated(&server, "gen_ins").await;

    server
        .expect_error(
            "INSERT INTO gen_ins (id, price, tax) VALUES ('a', 10, 99)",
            GENERATED_ALWAYS,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_naming_a_generated_column_is_refused() {
    let server = TestServer::start().await;
    generated(&server, "gen_ups").await;

    server
        .expect_error(
            "UPSERT INTO gen_ups (id, price, tax) VALUES ('a', 10, 99)",
            GENERATED_ALWAYS,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_naming_a_generated_column_is_refused() {
    let server = TestServer::start().await;
    generated(&server, "gen_sel").await;
    server
        .exec("CREATE COLLECTION gen_src (id TEXT PRIMARY KEY, price FLOAT, tax FLOAT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO gen_src (id, price, tax) VALUES ('s', 10, 99)")
        .await
        .unwrap();

    server
        .expect_error(
            "INSERT INTO gen_sel (id, price, tax) SELECT id, price, tax FROM gen_src",
            GENERATED_ALWAYS,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_omitted_generated_column_is_computed() {
    let server = TestServer::start().await;
    generated(&server, "gen_ok").await;

    server
        .exec("INSERT INTO gen_ok (id, price) VALUES ('a', 10)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT tax FROM gen_ok WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(rows[0].starts_with('1'), "tax must be computed: {rows:?}");
}
