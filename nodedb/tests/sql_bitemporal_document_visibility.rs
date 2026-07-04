// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal document visibility across the handler (pgwire) path.
//!
//! A `WITH (bitemporal=true)` document collection must keep its WRITE and
//! READ paths on the SAME storage namespace: INSERT, default SELECT,
//! COUNT(*), UPDATE, and `AS OF SYSTEM TIME` must all agree. The engine
//! layer already appends versions (see `document_bitemporal_dml.rs`); these
//! tests pin the handler routing, where batch INSERT and the default scan
//! historically stayed on the plain (non-versioned) namespace while
//! UPDATE/UPSERT/AS-OF used the versioned one — so a row written by INSERT
//! was invisible to `AS OF`, and an UPDATE was invisible to a plain SELECT.

mod common;

use common::pgwire_harness::TestServer;

/// A far-future system-time cutoff resolves to the latest (current) version.
const FUTURE_MS: i64 = 99_999_999_999_999;

async fn create_bitemporal(srv: &TestServer, name: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {name} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_schemaless', bitemporal=true)"
    ))
    .await
    .unwrap();
}

/// A row written by INSERT must be visible through `AS OF SYSTEM TIME` at a
/// current/future cutoff — i.e. INSERT and the temporal read share the
/// versioned namespace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_insert_visible_via_as_of_system_time() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_ins").await;

    srv.exec("INSERT INTO bt_ins (id, value) VALUES ('r1', 'v1')")
        .await
        .unwrap();

    // Plain SELECT and AS OF must return the SAME single row.
    let plain = srv
        .query_rows("SELECT id, value FROM bt_ins")
        .await
        .unwrap();
    assert_eq!(
        plain.len(),
        1,
        "plain SELECT must see the row, got {plain:?}"
    );

    let as_of = srv
        .query_rows(&format!(
            "SELECT id, value FROM bt_ins AS OF SYSTEM TIME {FUTURE_MS}"
        ))
        .await
        .unwrap();
    // Visibility AND projection shape: AS OF must return the same projected
    // columns as a plain SELECT, not the raw `{id,data}` versioned envelope.
    assert_eq!(
        as_of.len(),
        1,
        "AS OF SYSTEM TIME must see the INSERTed row (write/read namespace must agree), got {as_of:?}"
    );
    assert_eq!(
        as_of[0][0], "r1",
        "AS OF must project the `id` column, not the raw envelope, got {as_of:?}"
    );
    assert_eq!(
        as_of[0][1], "v1",
        "AS OF must project the `value` column, not the raw envelope, got {as_of:?}"
    );
}

/// Batch INSERT of several rows must be fully visible to both the default
/// scan and COUNT(*).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_batch_insert_visible_in_select_and_count() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_batch").await;

    srv.exec(
        "INSERT INTO bt_batch (id, value) VALUES \
         ('a', '1'), ('b', '2'), ('c', '3')",
    )
    .await
    .unwrap();

    let rows = srv.query_rows("SELECT id FROM bt_batch").await.unwrap();
    assert_eq!(
        rows.len(),
        3,
        "batch INSERT rows must all be visible, got {rows:?}"
    );

    let count = srv
        .query_text("SELECT COUNT(*) FROM bt_batch")
        .await
        .unwrap();
    assert_eq!(
        count,
        vec!["3".to_string()],
        "COUNT(*) must agree with the scan"
    );

    let as_of = srv
        .query_rows(&format!(
            "SELECT id FROM bt_batch AS OF SYSTEM TIME {FUTURE_MS}"
        ))
        .await
        .unwrap();
    assert_eq!(
        as_of.len(),
        3,
        "AS OF must see all batch-inserted rows, got {as_of:?}"
    );
}

/// An UPDATE (which appends a version) must be reflected by the default
/// SELECT — the read path must resolve the latest version, not a stale row
/// on a different namespace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_update_reflected_in_default_select() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_upd").await;

    srv.exec("INSERT INTO bt_upd (id, value) VALUES ('r1', 'v1')")
        .await
        .unwrap();
    srv.exec("UPDATE bt_upd SET value = 'v2' WHERE id = 'r1'")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id, value FROM bt_upd WHERE id = 'r1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one current row, got {rows:?}");
    assert_eq!(
        rows[0][1], "v2",
        "default SELECT must return the latest version, got {rows:?}"
    );
}

/// #72 regression: an INSERT issued inside `BEGIN;...;COMMIT;` on a
/// bitemporal collection must not be silently lost. A fresh SELECT (a new
/// implicit read, not reusing the transaction's own view) must see the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_tx_insert_survives_commit() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_tx_ins").await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO bt_tx_ins (id, value) VALUES ('tx1', 'v1')")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    let rows = srv
        .query_rows("SELECT id, value FROM bt_tx_ins WHERE id = 'tx1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "committed transactional INSERT into a bitemporal collection must be \
         visible to a fresh SELECT, got {rows:?}"
    );
    assert_eq!(rows[0][0], "tx1");
    assert_eq!(rows[0][1], "v1");
}

/// #72 regression: a DELETE issued inside `BEGIN;...;COMMIT;` on a
/// bitemporal collection must actually remove the row — a fresh SELECT must
/// return zero rows, not the pre-delete version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_tx_delete_survives_commit() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_tx_del").await;

    srv.exec("INSERT INTO bt_tx_del (id, value) VALUES ('tx2', 'v1')")
        .await
        .unwrap();

    srv.exec("BEGIN").await.unwrap();
    srv.exec("DELETE FROM bt_tx_del WHERE id = 'tx2'")
        .await
        .unwrap();
    srv.exec("COMMIT").await.unwrap();

    let rows = srv
        .query_rows("SELECT id FROM bt_tx_del WHERE id = 'tx2'")
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "committed transactional DELETE on a bitemporal collection must remove \
         the row from a fresh SELECT, got {rows:?}"
    );
}
