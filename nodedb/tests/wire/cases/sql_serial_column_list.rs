// SPDX-License-Identifier: BUSL-1.1

//! `SERIAL` and `BIGSERIAL` in the parenthesised column list.
//!
//! `CREATE COLLECTION c (n SERIAL, v TEXT)` and
//! `CREATE COLLECTION c FIELDS (n SERIAL, v TEXT)` expand the type
//! identically. An insert that omits the column takes 1, 2, 3 in order.

use crate::harness::TestServer;

/// Insert three rows that omit `n`, then assert `n` reads back 1, 2, 3.
async fn assert_serial_allocates_in_order(server: &TestServer, collection: &str) {
    for value in ["a", "b", "c"] {
        server
            .exec(&format!("INSERT INTO {collection} (v) VALUES ('{value}')"))
            .await
            .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
    }
    let rows = server
        .query_text(&format!("SELECT n FROM {collection} ORDER BY n"))
        .await
        .unwrap();
    let keys: Vec<&str> = rows.iter().map(|row| row.trim()).collect();
    assert_eq!(
        keys,
        vec!["1", "2", "3"],
        "{collection}: n must allocate 1, 2, 3 in order"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_in_column_list_schemaless() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION serial_cl_schemaless (n SERIAL, v TEXT)")
        .await
        .unwrap();
    assert_serial_allocates_in_order(&server, "serial_cl_schemaless").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_in_column_list_document_strict() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION serial_cl_strict (n SERIAL PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    assert_serial_allocates_in_order(&server, "serial_cl_strict").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bigserial_in_column_list_schemaless() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION bigserial_cl_schemaless (n BIGSERIAL, v TEXT)")
        .await
        .unwrap();
    assert_serial_allocates_in_order(&server, "bigserial_cl_schemaless").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bigserial_in_column_list_document_strict() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION bigserial_cl_strict (n BIGSERIAL PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    assert_serial_allocates_in_order(&server, "bigserial_cl_strict").await;
}

/// The column-list spelling creates the same implicit sequence as `FIELDS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_in_column_list_creates_implicit_sequence() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION serial_cl_seq (n SERIAL, v TEXT)")
        .await
        .unwrap();
    let rows = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        !rows.is_empty(),
        "SERIAL in the column list must create an implicit sequence"
    );
}
