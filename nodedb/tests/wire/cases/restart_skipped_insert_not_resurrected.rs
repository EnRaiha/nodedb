// SPDX-License-Identifier: BUSL-1.1

//! A row `INSERT ... ON CONFLICT DO NOTHING` skipped must not come back after
//! a restart.
//!
//! A skipped row is no refusal: the statement succeeds and writes nothing for
//! that key. The log must not hold the skipped body, or WAL replay stores it
//! over the row the statement kept. Key-value rows live only in an in-memory
//! hash table, so WAL replay is their sole recovery path.
//!
//! Each engine is checked for the point and the multi-row form. The restart is
//! a real one: `graceful_shutdown` releases every WAL and redb handle, and
//! `open_on_path` reopens the same directory.

use crate::harness::TestServer;

async fn values(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text_joined(&format!("SELECT id, v FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// Seed `('a','kept')`, then insert a different body for `a` with
/// `ON CONFLICT DO NOTHING`, once alone and once beside a new key.
async fn skip_existing_row(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, v) VALUES ('a', 'kept')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, v) VALUES ('a', 'skipped') ON CONFLICT DO NOTHING"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, v) VALUES ('a', 'skipped'), ('b', 'new') \
             ON CONFLICT DO NOTHING"
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skipped_kv_row_stays_skipped_after_a_restart() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION skip_kv (id TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    skip_existing_row(&srv, "skip_kv").await;
    let expected = vec!["a\tkept".to_string(), "b\tnew".to_string()];
    assert_eq!(values(&srv, "skip_kv").await, expected);

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        values(&srv2, "skip_kv").await,
        expected,
        "WAL replay stored the body ON CONFLICT DO NOTHING skipped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skipped_document_row_stays_skipped_after_a_restart() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION skip_doc (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();
    skip_existing_row(&srv, "skip_doc").await;
    let expected = vec!["a\tkept".to_string(), "b\tnew".to_string()];
    assert_eq!(values(&srv, "skip_doc").await, expected);

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        values(&srv2, "skip_doc").await,
        expected,
        "WAL replay stored the body ON CONFLICT DO NOTHING skipped"
    );
}
