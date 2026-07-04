// SPDX-License-Identifier: BUSL-1.1

//! A ROLLBACK of a transaction that put/deleted rows on a bitemporal
//! collection must restore the pre-transaction state exactly — both via a
//! fresh scan/AS-OF read and via the PK point-lookup path (which must not
//! be served a stale post-op value from any point-lookup cache).

mod common;

use common::pgwire_harness::TestServer;

async fn create_bitemporal(srv: &TestServer, name: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {name} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_schemaless', bitemporal=true)"
    ))
    .await
    .unwrap();
}

const FUTURE_MS: i64 = 99_999_999_999_999;

/// ROLLBACK of a transactional INSERT into a bitemporal collection must
/// leave no trace: neither the point-lookup nor `AS OF SYSTEM TIME` may see
/// the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_tx_insert_rollback_restores_pre_txn_absence() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_rb_ins").await;

    srv.exec("BEGIN").await.unwrap();
    srv.exec("INSERT INTO bt_rb_ins (id, value) VALUES ('r1', 'v1')")
        .await
        .unwrap();
    srv.exec("ROLLBACK").await.unwrap();

    let point = srv
        .query_rows("SELECT id, value FROM bt_rb_ins WHERE id = 'r1'")
        .await
        .unwrap();
    assert!(
        point.is_empty(),
        "rolled-back transactional INSERT must not be visible via point-lookup, \
         got {point:?}"
    );

    let as_of = srv
        .query_rows(&format!(
            "SELECT id FROM bt_rb_ins AS OF SYSTEM TIME {FUTURE_MS}"
        ))
        .await
        .unwrap();
    assert!(
        as_of.is_empty(),
        "rolled-back transactional INSERT must not be visible via AS OF SYSTEM TIME, \
         got {as_of:?}"
    );
}

/// ROLLBACK of a transactional DELETE on a bitemporal collection must
/// restore the pre-transaction row exactly, both via a fresh point-lookup
/// (not served a stale "deleted" state from any point-lookup cache) and via
/// `AS OF SYSTEM TIME`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_tx_delete_rollback_restores_pre_txn_row() {
    let srv = TestServer::start().await;
    create_bitemporal(&srv, "bt_rb_del").await;

    srv.exec("INSERT INTO bt_rb_del (id, value) VALUES ('r2', 'v1')")
        .await
        .unwrap();

    srv.exec("BEGIN").await.unwrap();
    srv.exec("DELETE FROM bt_rb_del WHERE id = 'r2'")
        .await
        .unwrap();
    srv.exec("ROLLBACK").await.unwrap();

    // PK point-lookup must return the pre-txn row, not a stale "deleted" view.
    let point = srv
        .query_rows("SELECT id, value FROM bt_rb_del WHERE id = 'r2'")
        .await
        .unwrap();
    assert_eq!(
        point.len(),
        1,
        "rolled-back transactional DELETE must restore the row via point-lookup, \
         got {point:?}"
    );
    assert_eq!(point[0][1], "v1");

    // AS OF SYSTEM TIME must agree.
    let as_of = srv
        .query_rows(&format!(
            "SELECT id, value FROM bt_rb_del AS OF SYSTEM TIME {FUTURE_MS}"
        ))
        .await
        .unwrap();
    assert_eq!(
        as_of.len(),
        1,
        "AS OF SYSTEM TIME must also show the restored row, got {as_of:?}"
    );
    assert_eq!(as_of[0][1], "v1");
}
