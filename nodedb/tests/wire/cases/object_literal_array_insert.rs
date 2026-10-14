// SPDX-License-Identifier: BUSL-1.1

//! `INSERT INTO c [{ ... }, { ... }]` writes every row as one statement.
//!
//! Each row runs its pre-write work in row order, so a sequence-backed field
//! the row leaves out draws one value per row. The rows then go out as one
//! multi-row INSERT: a stored or repeated key fails the statement with
//! `23505`, and no row of it lands.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

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
            .unwrap_or_else(|| panic!("expected a DbError from: {sql}; got {e:?}"))
            .code()
            .code()
            .to_owned(),
    }
}

/// `id` and `v` of every row joined by a tab, ordered by `id`.
async fn rows(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text_joined(&format!("SELECT id, v FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_array_insert_writes_every_row() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION objarr_all").await.unwrap();

    let n = affected(
        &server,
        "INSERT INTO objarr_all [{ id: 'a', v: 'x' }, { id: 'b', v: 'y' }, { id: 'c', v: 'z' }]",
    )
    .await;
    assert_eq!(n, 3);
    assert_eq!(
        rows(&server, "objarr_all").await,
        vec!["a\tx".to_string(), "b\ty".to_string(), "c\tz".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_array_insert_repeating_a_key_writes_no_row() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION objarr_rep").await.unwrap();

    let code = sqlstate(
        &server,
        "INSERT INTO objarr_rep [{ id: 'a', v: 'x' }, { id: 'b', v: 'y' }, { id: 'a', v: 'z' }]",
    )
    .await;
    assert_eq!(code, "23505");
    assert!(rows(&server, "objarr_rep").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_array_insert_over_a_stored_key_writes_no_row() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION objarr_dup").await.unwrap();
    server
        .exec("INSERT INTO objarr_dup { id: 'b', v: 'kept' }")
        .await
        .unwrap();

    let code = sqlstate(
        &server,
        "INSERT INTO objarr_dup [{ id: 'a', v: 'x' }, { id: 'b', v: 'y' }]",
    )
    .await;
    assert_eq!(code, "23505");
    assert_eq!(
        rows(&server, "objarr_dup").await,
        vec!["b\tkept".to_string()]
    );
}

/// Each row draws its own sequence value, so the next statement continues
/// after the last row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_array_insert_draws_one_sequence_value_per_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objarr_seq (n SERIAL, v TEXT)")
        .await
        .unwrap();

    let n = affected(
        &server,
        "INSERT INTO objarr_seq [{ id: 'a', v: 'x' }, { id: 'b', v: 'y' }, { id: 'c', v: 'z' }]",
    )
    .await;
    assert_eq!(n, 3);
    server
        .exec("INSERT INTO objarr_seq { id: 'd', v: 'w' }")
        .await
        .unwrap();

    let drawn = server
        .query_text_joined("SELECT id, n FROM objarr_seq ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        drawn,
        vec![
            "a\t1".to_string(),
            "b\t2".to_string(),
            "c\t3".to_string(),
            "d\t4".to_string(),
        ]
    );
}
