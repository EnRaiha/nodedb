// SPDX-License-Identifier: BUSL-1.1

//! A multi-row `INSERT` / `UPSERT` into a key-value collection is one write.
//!
//! The statement lowers to one batch put, so its rows commit together. Each
//! row is judged as the single-row form judges it: a stored or repeated key
//! refuses a plain `INSERT` with `23505` and nothing is written, `ON CONFLICT
//! DO NOTHING` skips that row, and `UPSERT` overwrites it.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn kv_collection(server: &TestServer) {
    server
        .exec("CREATE COLLECTION c (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
}

/// The row count `sql` reports in its command tag.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql} reported no command tag"))
}

/// The SQLSTATE `sql` fails with.
async fn sqlstate(server: &TestServer, sql: &str) -> String {
    match server.client.simple_query(sql).await {
        Ok(_) => panic!("{sql}: expected an error, got success"),
        Err(e) => e
            .as_db_error()
            .expect("expected DbError")
            .code()
            .code()
            .to_owned(),
    }
}

/// `key` and `n` of every row joined by a tab, ordered by `key`.
async fn rows(server: &TestServer) -> Vec<String> {
    server
        .query_text_joined("SELECT key, n FROM c ORDER BY key")
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_over_a_stored_key_writes_no_row() {
    let server = TestServer::start().await;
    kv_collection(&server).await;
    server
        .exec("INSERT INTO c (key, n) VALUES ('b', 1)")
        .await
        .unwrap();

    let code = sqlstate(&server, "INSERT INTO c (key, n) VALUES ('a', 2), ('b', 3)").await;
    assert_eq!(code, "23505");
    assert_eq!(rows(&server).await, vec!["b\t1".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_repeating_a_key_writes_no_row() {
    let server = TestServer::start().await;
    kv_collection(&server).await;

    let code = sqlstate(&server, "INSERT INTO c (key, n) VALUES ('a', 1), ('a', 2)").await;
    assert_eq!(code, "23505");
    assert!(rows(&server).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_reports_every_row() {
    let server = TestServer::start().await;
    kv_collection(&server).await;

    let n = affected(
        &server,
        "INSERT INTO c (key, n) VALUES ('a', 1), ('b', 2), ('c', 3)",
    )
    .await;
    assert_eq!(n, 3);
    assert_eq!(
        rows(&server).await,
        vec!["a\t1".to_string(), "b\t2".to_string(), "c\t3".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_on_conflict_do_nothing_skips_stored_and_repeated_keys() {
    let server = TestServer::start().await;
    kv_collection(&server).await;
    server
        .exec("INSERT INTO c (key, n) VALUES ('b', 1)")
        .await
        .unwrap();

    let n = affected(
        &server,
        "INSERT INTO c (key, n) VALUES ('a', 2), ('b', 3), ('a', 4), ('c', 5) \
         ON CONFLICT DO NOTHING",
    )
    .await;
    assert_eq!(n, 2);
    assert_eq!(
        rows(&server).await,
        vec!["a\t2".to_string(), "b\t1".to_string(), "c\t5".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_upsert_overwrites_stored_keys() {
    let server = TestServer::start().await;
    kv_collection(&server).await;
    server
        .exec("INSERT INTO c (key, n) VALUES ('b', 1)")
        .await
        .unwrap();

    let n = affected(&server, "UPSERT INTO c (key, n) VALUES ('a', 2), ('b', 3)").await;
    assert_eq!(n, 2);
    assert_eq!(
        rows(&server).await,
        vec!["a\t2".to_string(), "b\t3".to_string()]
    );
}

/// A repeated key merges into the body its earlier row produced, as two
/// statements in a row would.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_on_conflict_do_update_merges_stored_and_repeated_keys() {
    let server = TestServer::start().await;
    kv_collection(&server).await;
    server
        .exec("INSERT INTO c (key, n) VALUES ('b', 1)")
        .await
        .unwrap();

    let n = affected(
        &server,
        "INSERT INTO c (key, n) VALUES ('a', 2), ('b', 3), ('a', 4) \
         ON CONFLICT (key) DO UPDATE SET n = n + EXCLUDED.n",
    )
    .await;
    assert_eq!(n, 3);
    assert_eq!(
        rows(&server).await,
        vec!["a\t6".to_string(), "b\t4".to_string()]
    );
}

/// A row the conflict clause cannot write fails the statement, and no row
/// of the statement lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_row_of_an_on_conflict_do_update_writes_no_row() {
    let server = TestServer::start().await;
    kv_collection(&server).await;
    server
        .exec("INSERT INTO c (key, n) VALUES ('b', 1)")
        .await
        .unwrap();

    let result = server
        .client
        .simple_query(
            "INSERT INTO c (key, n) VALUES ('a', 2), ('b', 3) \
             ON CONFLICT (key) DO UPDATE SET key = 'z'",
        )
        .await;
    assert!(result.is_err(), "an assignment to the key must fail");
    assert_eq!(rows(&server).await, vec!["b\t1".to_string()]);
}
