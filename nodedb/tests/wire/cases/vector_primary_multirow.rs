// SPDX-License-Identifier: BUSL-1.1

//! A multi-row write into a vector-primary collection is one write.
//!
//! The statement lowers to one page, so its rows commit together, as one
//! log record. Each row is judged as the single-row form judges it, in
//! statement order: a stored or repeated key refuses a plain `INSERT` with
//! `23505` and nothing is written, `ON CONFLICT DO NOTHING` skips the row,
//! and `UPSERT` / `ON CONFLICT DO UPDATE` replace or patch it. A row the page
//! skipped stays skipped after a restart replays the log.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
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
            .unwrap_or_else(|| panic!("expected a DbError from: {sql}; got {e:?}"))
            .code()
            .code()
            .to_owned(),
    }
}

/// `id` and `owner` of every row joined by a tab, ordered by `id`.
async fn rows(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text_joined(&format!("SELECT id, owner FROM {name} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {name}: {e}"))
}

async fn seed(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "INSERT INTO {name} (id, vec, owner) VALUES ('b', ARRAY[0.0, 1.0, 0.0], 'kept')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_over_a_stored_key_writes_no_row() {
    let server = TestServer::start().await;
    create(&server, "vpm_dup").await;
    seed(&server, "vpm_dup").await;

    let code = sqlstate(
        &server,
        "INSERT INTO vpm_dup (id, vec, owner) VALUES \
         ('a', ARRAY[1.0, 0.0, 0.0], 'new'), ('b', ARRAY[0.0, 0.0, 1.0], 'other')",
    )
    .await;
    assert_eq!(code, "23505");
    assert_eq!(rows(&server, "vpm_dup").await, vec!["b\tkept".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_repeating_a_key_writes_no_row() {
    let server = TestServer::start().await;
    create(&server, "vpm_rep").await;

    let code = sqlstate(
        &server,
        "INSERT INTO vpm_rep (id, vec, owner) VALUES \
         ('a', ARRAY[1.0, 0.0, 0.0], 'one'), ('a', ARRAY[0.0, 1.0, 0.0], 'two')",
    )
    .await;
    assert_eq!(code, "23505");
    assert!(rows(&server, "vpm_rep").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_upsert_replaces_stored_and_repeated_keys() {
    let server = TestServer::start().await;
    create(&server, "vpm_up").await;
    seed(&server, "vpm_up").await;

    let n = affected(
        &server,
        "UPSERT INTO vpm_up (id, vec, owner) VALUES \
         ('a', ARRAY[1.0, 0.0, 0.0], 'first'), ('b', ARRAY[0.0, 0.0, 1.0], 'replaced'), \
         ('a', ARRAY[1.0, 1.0, 0.0], 'last')",
    )
    .await;
    assert_eq!(n, 3);
    assert_eq!(
        rows(&server, "vpm_up").await,
        vec!["a\tlast".to_string(), "b\treplaced".to_string()]
    );
}

/// A repeated key patches the body its earlier row produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_on_conflict_do_update_patches_stored_and_repeated_keys() {
    let server = TestServer::start().await;
    create(&server, "vpm_patch").await;
    seed(&server, "vpm_patch").await;

    let n = affected(
        &server,
        "INSERT INTO vpm_patch (id, vec, owner) VALUES \
         ('a', ARRAY[1.0, 0.0, 0.0], 'first'), ('b', ARRAY[0.0, 0.0, 1.0], 'patched'), \
         ('a', ARRAY[1.0, 1.0, 0.0], 'second') \
         ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner",
    )
    .await;
    assert_eq!(n, 3);
    assert_eq!(
        rows(&server, "vpm_patch").await,
        vec!["a\tsecond".to_string(), "b\tpatched".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skipped_row_of_a_page_stays_skipped_after_a_restart() {
    let srv = TestServer::start().await;
    create(&srv, "vpm_skip").await;
    seed(&srv, "vpm_skip").await;

    let n = affected(
        &srv,
        "INSERT INTO vpm_skip (id, vec, owner) VALUES \
         ('a', ARRAY[1.0, 0.0, 0.0], 'new'), ('b', ARRAY[0.0, 0.0, 1.0], 'skipped'), \
         ('a', ARRAY[0.0, 1.0, 1.0], 'skipped') ON CONFLICT DO NOTHING",
    )
    .await;
    assert_eq!(n, 1);
    let expected = vec!["a\tnew".to_string(), "b\tkept".to_string()];
    assert_eq!(rows(&srv, "vpm_skip").await, expected);

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        rows(&srv2, "vpm_skip").await,
        expected,
        "WAL replay stored a body ON CONFLICT DO NOTHING skipped"
    );
}
