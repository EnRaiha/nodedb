// SPDX-License-Identifier: BUSL-1.1

//! UPSERT INTO routes through the protocol-neutral DSL dispatch path
//! (`dispatch` -> `try_dispatch` -> `upsert_document` -> `plan_and_dispatch`),
//! which historically had no transaction awareness at all: inside
//! BEGIN..COMMIT it dispatched writes to the Data Plane immediately, so an
//! UPSERT was durable and visible to other connections before COMMIT and was
//! never rolled back.
//!
//! `plan_and_dispatch` now routes every task through the same protocol-
//! neutral in-transaction staging gate pgwire-SQL and native already use:
//! outside a transaction block it is byte-identical to the old immediate
//! dispatch; inside one, the write is staged into the per-transaction overlay
//! (real command tag + read-your-own-writes) and buffered for COMMIT-time
//! replay, invisible to other connections and reverted on ROLLBACK.
//!
//! Also covers the KV `UPSERT INTO ... VALUES` case: KV collections route
//! through this same DSL path, not the typed SQL `INSERT ... ON CONFLICT`
//! path, so it belongs in this suite now that the DSL path is gated.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

fn row_has(msgs: &[SimpleQueryMessage], col: &str, expected: &str) -> bool {
    msgs.iter().any(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(col) == Some(expected),
        _ => false,
    })
}

async fn setup_document(server: &TestServer) {
    server
        .exec("CREATE COLLECTION docs (id TEXT PRIMARY KEY, n INT)")
        .await
        .unwrap();
    server
        .exec("UPSERT INTO docs (id, n) VALUES ('a', 1)")
        .await
        .unwrap();
}

async fn setup_kv(server: &TestServer) {
    server
        .exec("CREATE COLLECTION kv1 (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("UPSERT INTO kv1 (key, n) VALUES ('a', 1)")
        .await
        .unwrap();
}

/// Overwriting an existing key inside a transaction must return a real
/// command tag (not a bare immediate dispatch), must be visible to a SELECT
/// on the same connection (read-your-own-writes), and must NOT be visible to
/// a separate connection before COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_in_tx_upsert_overwrite_is_isolated_until_commit() {
    let server = TestServer::start().await;
    setup_document(&server).await;

    server
        .exec("CREATE USER upsert_reader WITH PASSWORD 'x' ROLE readwrite")
        .await
        .unwrap();
    let (other, _h) = server.connect_as("upsert_reader", "x").await.unwrap();

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("UPSERT INTO docs (id, n) VALUES ('a', 42)")
        .await
        .expect("in-tx UPSERT overwrite must succeed at the statement");
    assert!(
        command_count(&msgs).is_some()
            || msgs
                .iter()
                .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))),
        "in-tx UPSERT must report a real command tag, not be swallowed"
    );

    // Read-your-own-writes on the same connection.
    let own_read = server
        .client
        .simple_query("SELECT n FROM docs WHERE id = 'a'")
        .await
        .unwrap();
    assert!(
        row_has(&own_read, "n", "42"),
        "staged UPSERT must be visible in the same transaction"
    );

    // A separate connection must NOT see the staged overwrite pre-commit.
    let other_read = other
        .simple_query("SELECT n FROM docs WHERE id = 'a'")
        .await
        .unwrap();
    assert!(
        row_has(&other_read, "n", "1"),
        "a separate connection must see the pre-transaction value before COMMIT"
    );
    assert!(
        !row_has(&other_read, "n", "42"),
        "a separate connection must not see the staged UPSERT before COMMIT"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = other
        .simple_query("SELECT n FROM docs WHERE id = 'a'")
        .await
        .unwrap();
    assert!(
        row_has(&committed, "n", "42"),
        "COMMIT must make the UPSERT durable and visible to other connections"
    );
}

/// Inserting a brand-new key via UPSERT inside a transaction, then rolling
/// back, must discard the write entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_in_tx_upsert_new_key_rollback_discards_write() {
    let server = TestServer::start().await;
    setup_document(&server).await;

    server.exec("BEGIN").await.unwrap();

    server
        .client
        .simple_query("UPSERT INTO docs (id, n) VALUES ('new', 7)")
        .await
        .expect("in-tx UPSERT of a new key must succeed at the statement");

    let own_read = server
        .client
        .simple_query("SELECT n FROM docs WHERE id = 'new'")
        .await
        .unwrap();
    assert!(
        row_has(&own_read, "n", "7"),
        "staged UPSERT insert must be visible in the same transaction"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = server
        .query_text("SELECT n FROM docs WHERE id = 'new'")
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "rolled-back UPSERT must not persist, got {after:?}"
    );
}

/// KV collections route UPSERT through the same DSL path. Verify the same
/// isolation + commit + rollback contract holds for KV.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_upsert_overwrite_is_isolated_until_commit() {
    let server = TestServer::start().await;
    setup_kv(&server).await;

    server
        .exec("CREATE USER kv_upsert_reader WITH PASSWORD 'x' ROLE readwrite")
        .await
        .unwrap();
    let (other, _h) = server.connect_as("kv_upsert_reader", "x").await.unwrap();

    server.exec("BEGIN").await.unwrap();

    server
        .client
        .simple_query("UPSERT INTO kv1 (key, n) VALUES ('a', 99)")
        .await
        .expect("in-tx KV UPSERT overwrite must succeed at the statement");

    let own_read = server
        .client
        .simple_query("SELECT n FROM kv1 WHERE key = 'a'")
        .await
        .unwrap();
    assert!(
        row_has(&own_read, "n", "99"),
        "staged KV UPSERT must be visible in the same transaction"
    );

    let other_read = other
        .simple_query("SELECT n FROM kv1 WHERE key = 'a'")
        .await
        .unwrap();
    assert!(
        row_has(&other_read, "n", "1"),
        "a separate connection must see the pre-transaction KV value before COMMIT"
    );
    assert!(
        !row_has(&other_read, "n", "99"),
        "a separate connection must not see the staged KV UPSERT before COMMIT"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = other
        .simple_query("SELECT n FROM kv1 WHERE key = 'a'")
        .await
        .unwrap();
    assert!(
        row_has(&committed, "n", "99"),
        "COMMIT must make the KV UPSERT durable and visible to other connections"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_in_tx_upsert_new_key_rollback_discards_write() {
    let server = TestServer::start().await;
    setup_kv(&server).await;

    server.exec("BEGIN").await.unwrap();

    server
        .client
        .simple_query("UPSERT INTO kv1 (key, n) VALUES ('new', 5)")
        .await
        .expect("in-tx KV UPSERT of a new key must succeed at the statement");

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = server
        .query_text("SELECT n FROM kv1 WHERE key = 'new'")
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "rolled-back KV UPSERT must not persist, got {after:?}"
    );
}

/// Outside a transaction, UPSERT must remain immediately durable
/// (autocommit) exactly as before the gate was wired in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_autocommit_upsert_is_immediately_durable() {
    let server = TestServer::start().await;
    setup_document(&server).await;

    server
        .exec("UPSERT INTO docs (id, n) VALUES ('a', 123)")
        .await
        .unwrap();

    let a = server
        .query_text("SELECT n FROM docs WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(a, vec!["123"]);
}
