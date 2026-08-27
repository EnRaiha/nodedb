// SPDX-License-Identifier: BUSL-1.1

//! Columnar predicate DML (`DELETE ... WHERE` / `UPDATE ... SET ... WHERE`)
//! executes at STATEMENT time inside a transaction — staged into the
//! per-transaction overlay with read-your-own-writes on columnar scans, a real
//! affected-row count, and `ROLLBACK` discarding the staged mutation —
//! mirroring the columnar INSERT staging already in place
//! (`sql_transactions_columnar_overlay.rs`) and the Document bulk predicate-DML
//! staging (`sql_transactions_bulk_dml_overlay.rs`).
//!
//! Pre-fix, `ColumnarOp::Update` / `ColumnarOp::Delete` were NOT on the
//! staging allow-list, so an in-transaction columnar predicate DELETE/UPDATE
//! was buffered (deferred to COMMIT): a same-transaction scan still saw the
//! pre-delete / pre-update rows and the statement's affected count was not
//! reflected mid-transaction. Every assertion below that reads within the
//! transaction fails on the pre-fix code.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a
/// simple-query response.
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

fn rows_of(msgs: &[SimpleQueryMessage], col: &str) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(col).map(str::to_string),
            _ => None,
        })
        .collect()
}

async fn setup(server: &TestServer) {
    server
        .exec("CREATE COLLECTION m (id INT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES (1, 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES (2, 20)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES (3, 30)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_predicate_delete_is_visible_and_rolls_back() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Predicate DELETE reports its real affected count at the statement, not a
    // bare OK deferred to COMMIT.
    let msgs = server
        .client
        .simple_query("DELETE FROM m WHERE v = 20")
        .await
        .expect("in-tx columnar predicate delete should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx columnar DELETE must report the real affected count at statement time"
    );

    // Read-your-own-writes: the matched row is GONE inside the same
    // transaction; the unmatched rows remain.
    let after = server
        .client
        .simple_query("SELECT id FROM m ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&after, "id"),
        vec!["1", "3"],
        "staged columnar delete must remove the matched row inside the transaction"
    );

    let gone = server
        .client
        .simple_query("SELECT id FROM m WHERE v = 20")
        .await
        .unwrap();
    assert!(
        rows_of(&gone, "id").is_empty(),
        "the deleted row must not surface on a filtered in-tx scan"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    // ROLLBACK restores every row.
    let restored = server
        .query_text("SELECT id FROM m ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        restored,
        vec!["1", "2", "3"],
        "rolled-back columnar delete must restore all rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_predicate_update_is_visible_and_rolls_back() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("UPDATE m SET v = 999 WHERE id = 1")
        .await
        .expect("in-tx columnar predicate update should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(1),
        "in-tx columnar UPDATE must report the real affected count at statement time"
    );

    // Read-your-own-writes: the updated column reflects the new value for the
    // matched row.
    let updated = server
        .client
        .simple_query("SELECT v FROM m WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&updated, "v"),
        vec!["999"],
        "staged columnar update must surface the new value inside the transaction"
    );

    // The predicate now matches the updated row (moved into the predicate).
    let by_new_value = server
        .client
        .simple_query("SELECT id FROM m WHERE v = 999")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&by_new_value, "id"),
        vec!["1"],
        "a filtered scan on the new value must surface the updated row"
    );

    // Unmatched rows are untouched.
    let untouched = server
        .client
        .simple_query("SELECT v FROM m WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(rows_of(&untouched, "v"), vec!["20"]);

    server.client.simple_query("ROLLBACK").await.unwrap();

    let restored = server
        .query_text("SELECT v FROM m WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        restored,
        vec!["10"],
        "rolled-back columnar update must restore the original value"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_delete_composes_with_staged_insert() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Stage an INSERT, then DELETE a predicate that matches the just-inserted
    // staged row (not a committed base row).
    server
        .client
        .simple_query("INSERT INTO m (id, v) VALUES (7, 70)")
        .await
        .unwrap();

    let del = server
        .client
        .simple_query("DELETE FROM m WHERE v = 70")
        .await
        .unwrap();
    assert_eq!(
        command_count(&del),
        Some(1),
        "the staged-inserted row must be counted as affected by the delete"
    );

    // The staged-then-deleted row must not be visible in the same transaction.
    let gone = server
        .client
        .simple_query("SELECT id FROM m WHERE id = 7")
        .await
        .unwrap();
    assert!(
        rows_of(&gone, "id").is_empty(),
        "staged insert then delete must compose: the row is gone in-tx"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    // Durably: the insert+delete compose to nothing; base rows survive.
    let after = server
        .query_text("SELECT id FROM m ORDER BY id")
        .await
        .unwrap();
    assert_eq!(after, vec!["1", "2", "3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_update_composes_with_staged_insert() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    server
        .client
        .simple_query("INSERT INTO m (id, v) VALUES (8, 80)")
        .await
        .unwrap();

    // UPDATE a predicate matching the staged-inserted row.
    let upd = server
        .client
        .simple_query("UPDATE m SET v = 88 WHERE id = 8")
        .await
        .unwrap();
    assert_eq!(
        command_count(&upd),
        Some(1),
        "the staged-inserted row must be counted as affected by the update"
    );

    let seen = server
        .client
        .simple_query("SELECT v FROM m WHERE id = 8")
        .await
        .unwrap();
    assert_eq!(
        rows_of(&seen, "v"),
        vec!["88"],
        "staged insert then update must compose: the new value is visible in-tx"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .query_text("SELECT v FROM m WHERE id = 8")
        .await
        .unwrap();
    assert_eq!(
        committed,
        vec!["88"],
        "committed staged insert+update must persist the updated value"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_in_tx_predicate_dml_commit_persists_durably() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let del = server
        .client
        .simple_query("DELETE FROM m WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(command_count(&del), Some(1));

    let upd = server
        .client
        .simple_query("UPDATE m SET v = 111 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(command_count(&upd), Some(1));

    server.client.simple_query("COMMIT").await.unwrap();

    // A fresh autocommit read reflects both the delete and the update durably.
    let ids = server
        .query_text("SELECT id FROM m ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        ids,
        vec!["1", "3"],
        "committed columnar predicate delete must persist"
    );

    let v1 = server
        .query_text("SELECT v FROM m WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        v1,
        vec!["111"],
        "committed columnar predicate update must persist"
    );
}
