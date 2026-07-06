// SPDX-License-Identifier: BUSL-1.1

//! End-to-end companions to the direct handler-level rollback regression.
//!
//! The core regression — that a ROLLED-BACK columnar transaction must not
//! leave a phantom empty engine registered — is asserted directly on
//! `CoreLoop::columnar_engines` membership in the unit test
//! `txn_created_columnar_engine_tests` in
//! `data/executor/dispatch/meta.rs` (a leaked empty engine is invisible to
//! ordinary SELECTs, so SQL cannot observe it; and RESTORE-based probes are
//! confounded by RESTORE's HLC-watermark guard, which fires before the
//! columnar-engine collision guard).
//!
//! These two SQL-level tests cover the observable end-state the fix must NOT
//! regress: COMMIT keeps the engine + row (no over-drop), and a plain
//! autocommit INSERT into a brand-new collection still creates and keeps its
//! engine (the untouched non-transactional path).

mod common;
use common::pgwire_harness::TestServer;

use tokio_postgres::SimpleQueryMessage;

fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

/// COMMIT of the first insert into a new collection must KEEP the engine and
/// its row -- guards against an over-eager fix that unconditionally drops
/// every txn-created engine on `DropTxnOverlay` regardless of commit vs
/// rollback.
#[tokio::test]
async fn commit_of_new_collection_insert_keeps_columnar_engine_populated() {
    let target = TestServer::start().await;
    target
        .exec("CREATE COLLECTION kept COLUMNS (id TEXT, v FLOAT) WITH (engine='columnar')")
        .await
        .expect("CREATE COLLECTION kept");

    target.exec("BEGIN").await.expect("BEGIN");
    let msgs = target
        .client
        .simple_query("INSERT INTO kept (id, v) VALUES ('committed', 5.0)")
        .await
        .expect("staged insert into a new collection must succeed");
    assert_eq!(command_count(&msgs), Some(1));
    target.client.simple_query("COMMIT").await.expect("COMMIT");

    let rows = target
        .query_text("SELECT id FROM kept")
        .await
        .expect("SELECT id FROM kept");
    assert_eq!(
        rows,
        vec!["committed"],
        "committed row must persist after COMMIT of the first insert into a new collection"
    );

    // The engine (and its schema) must have survived COMMIT: a later,
    // unrelated autocommit insert into the same collection must still land.
    target
        .exec("INSERT INTO kept (id, v) VALUES ('after-commit', 6.0)")
        .await
        .expect("autocommit insert after commit must succeed against the surviving engine");
    let rows2 = target
        .query_text("SELECT id FROM kept ORDER BY id")
        .await
        .expect("SELECT id FROM kept ORDER BY id");
    assert_eq!(rows2, vec!["after-commit", "committed"]);
}

/// A plain (non-transactional) autocommit INSERT into a brand-new collection
/// must still create and keep its columnar engine -- this path never staged
/// through the per-txn tracking added by the fix and must be unaffected.
#[tokio::test]
async fn autocommit_insert_into_new_collection_creates_and_keeps_engine() {
    let target = TestServer::start().await;
    target
        .exec("CREATE COLLECTION plain COLUMNS (id TEXT, v FLOAT) WITH (engine='columnar')")
        .await
        .expect("CREATE COLLECTION plain");

    target
        .exec("INSERT INTO plain (id, v) VALUES ('a', 1.0)")
        .await
        .expect("autocommit insert into a new collection must succeed");

    let rows = target
        .query_text("SELECT id FROM plain")
        .await
        .expect("SELECT id FROM plain");
    assert_eq!(rows, vec!["a"]);
}
