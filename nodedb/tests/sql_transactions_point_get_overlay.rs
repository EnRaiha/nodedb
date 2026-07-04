// SPDX-License-Identifier: BUSL-1.1

//! In-transaction point-get reads must observe the transaction's own staged
//! writes (read-your-own-writes) by consulting the per-transaction staging
//! overlay before falling back to base storage. Scoped to point-get only —
//! scans and index lookups are not overlay-aware.

mod common;

use common::pgwire_harness::TestServer;

async fn setup(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION pg_ov \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO pg_ov (id, n) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO pg_ov (id, n) VALUES ('unrelated', 100)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_point_get_sees_own_insert() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO pg_ov (id, n) VALUES ('c', 3)")
        .await
        .unwrap();

    let c = server
        .query_text("SELECT n FROM pg_ov WHERE id = 'c'")
        .await
        .unwrap();
    assert_eq!(
        c,
        vec!["3"],
        "in-tx point-get must see the transaction's own staged insert"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let c_after = server
        .query_text("SELECT n FROM pg_ov WHERE id = 'c'")
        .await
        .unwrap();
    assert_eq!(c_after, vec!["3"], "committed insert must persist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_point_get_sees_own_update() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE pg_ov SET n = 20 WHERE id = 'a'")
        .await
        .unwrap();

    let a = server
        .query_text("SELECT n FROM pg_ov WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        a,
        vec!["20"],
        "in-tx point-get must see the transaction's own staged update"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let a_after = server
        .query_text("SELECT n FROM pg_ov WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(a_after, vec!["20"], "committed update must persist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_point_get_sees_own_delete() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM pg_ov WHERE id = 'a'")
        .await
        .unwrap();

    let a = server
        .query_rows("SELECT id, n FROM pg_ov WHERE id = 'a'")
        .await
        .unwrap();
    assert!(
        a.is_empty(),
        "in-tx point-get must see the transaction's own staged delete, got {a:?}"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let a_after = server
        .query_rows("SELECT id, n FROM pg_ov WHERE id = 'a'")
        .await
        .unwrap();
    assert!(a_after.is_empty(), "committed delete must persist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_point_get_of_unrelated_row_falls_through_to_base() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE pg_ov SET n = 20 WHERE id = 'a'")
        .await
        .unwrap();

    // A point-get of a row with no overlay entry for this txn must still
    // read the base row (overlay miss falls through unchanged).
    let unrelated = server
        .query_text("SELECT n FROM pg_ov WHERE id = 'unrelated'")
        .await
        .unwrap();
    assert_eq!(
        unrelated,
        vec!["100"],
        "point-get of an unstaged row must return the base value"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let a_after_rollback = server
        .query_text("SELECT n FROM pg_ov WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        a_after_rollback,
        vec!["1"],
        "ROLLBACK must restore the original base value; overlay must not leak past the txn"
    );
}

async fn create_bitemporal(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless', bitemporal=true)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} (id, n) VALUES ('a', 1)"))
        .await
        .unwrap();
}

const FUTURE_MS: i64 = 99_999_999_999_999;

/// On a bitemporal collection, a plain (non-AS-OF) in-tx point-get must see
/// the staged row, but `AS OF SYSTEM TIME` must skip the overlay entirely
/// and read only durable versioned state — staged bodies are current-version
/// only and carry no historical system-time placement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_in_tx_plain_point_get_sees_overlay_but_as_of_does_not() {
    let server = TestServer::start().await;
    create_bitemporal(&server, "pg_ov_bt").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE pg_ov_bt SET n = 20 WHERE id = 'a'")
        .await
        .unwrap();

    let plain = server
        .query_text("SELECT n FROM pg_ov_bt WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        plain,
        vec!["20"],
        "plain in-tx point-get on a bitemporal collection must see the staged update"
    );

    let as_of = server
        .query_text(&format!(
            "SELECT n FROM pg_ov_bt AS OF SYSTEM TIME {FUTURE_MS} WHERE id = 'a'"
        ))
        .await
        .unwrap();
    assert_eq!(
        as_of,
        vec!["1"],
        "AS OF SYSTEM TIME must skip the overlay and read only durable versioned state, \
         got {as_of:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}
