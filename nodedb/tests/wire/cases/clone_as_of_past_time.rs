// SPDX-License-Identifier: BUSL-1.1

//! `CLONE DATABASE … AS OF SYSTEM TIME <t>` at a past `t` must show the source
//! as it was committed at `t`, not as it is when the clone is created.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_as_of_past_time_sees_old_state() {
    let srv = TestServer::start().await;

    srv.exec("CREATE DATABASE src_past").await.unwrap();
    srv.exec("USE DATABASE src_past").await.unwrap();
    srv.exec(
        "CREATE COLLECTION docs (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO docs (id, value) VALUES ('k1', 'old')")
        .await
        .unwrap();

    // Margins on both sides of `t` keep millisecond rounding out of the result.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;

    srv.exec("UPDATE docs SET value = 'new' WHERE id = 'k1'")
        .await
        .unwrap();
    srv.exec("INSERT INTO docs (id, value) VALUES ('k2', 'late')")
        .await
        .unwrap();

    srv.exec("USE DATABASE default").await.unwrap();
    srv.exec(&format!(
        "CLONE DATABASE clone_past FROM src_past AS OF SYSTEM TIME {t}"
    ))
    .await
    .unwrap();

    srv.exec("USE DATABASE clone_past").await.unwrap();
    let rows = srv
        .query_rows("SELECT id, value FROM docs ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![vec!["k1".to_string(), "old".to_string()]],
        "a clone at a past time must see the source as committed then"
    );
}

/// A time before anything was committed is refused, never answered with the
/// current state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_as_of_before_any_commit_is_refused() {
    let srv = TestServer::start().await;

    srv.exec("CREATE DATABASE src_early").await.unwrap();
    srv.exec("USE DATABASE src_early").await.unwrap();
    srv.exec(
        "CREATE COLLECTION docs (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO docs (id, value) VALUES ('k1', 'v')")
        .await
        .unwrap();

    srv.exec("USE DATABASE default").await.unwrap();
    let err = srv
        .exec("CLONE DATABASE clone_early FROM src_early AS OF SYSTEM TIME 1")
        .await
        .expect_err("a time before the WAL existed must be refused");
    assert!(err.contains("predates"), "unexpected error: {err}");
}
