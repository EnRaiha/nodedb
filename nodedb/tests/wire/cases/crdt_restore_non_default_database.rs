// SPDX-License-Identifier: BUSL-1.1

//! `RESTORE ... SET VERSION` on a CRDT collection in a non-default database.
//!
//! The restore must address the same CRDT document the ordinary write path
//! maintains, which is keyed by the database-qualified collection name. A
//! same-name collection in `default` is a different document and stays
//! untouched.

use crate::harness::TestServer;

const COLLECTION: &str = "crdt_restore_notes";

async fn exec_ok(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nsql: {sql}"));
}

async fn title_of(server: &TestServer, id: &str) -> String {
    let rows = server
        .query_rows(&format!("SELECT title FROM {COLLECTION} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read {COLLECTION}/{id}: {e}"));
    rows.into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or_else(|| panic!("{COLLECTION}/{id} not found"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_set_version_in_non_default_database_restores_checkpointed_state() {
    let server = TestServer::start().await;
    let create =
        format!("CREATE TABLE {COLLECTION} (id TEXT PRIMARY KEY, title TEXT) WITH (crdt='true')");

    // Default database: same collection name, own row, never restored.
    exec_ok(&server, &create).await;
    exec_ok(
        &server,
        &format!("INSERT INTO {COLLECTION} (id, title) VALUES ('doc', 'default-v1')"),
    )
    .await;
    exec_ok(
        &server,
        &format!("UPDATE {COLLECTION} SET title = 'default-v2' WHERE id = 'doc'"),
    )
    .await;

    exec_ok(&server, "CREATE DATABASE d2").await;
    exec_ok(&server, "USE DATABASE d2").await;
    exec_ok(&server, &create).await;
    exec_ok(
        &server,
        &format!("INSERT INTO {COLLECTION} (id, title) VALUES ('doc', 'd2-v1')"),
    )
    .await;
    exec_ok(
        &server,
        &format!("CREATE CHECKPOINT 'cp1' ON {COLLECTION} WHERE id = 'doc'"),
    )
    .await;
    exec_ok(
        &server,
        &format!("UPDATE {COLLECTION} SET title = 'd2-v2' WHERE id = 'doc'"),
    )
    .await;
    assert_eq!(title_of(&server, "doc").await, "d2-v2");

    exec_ok(
        &server,
        &format!("RESTORE {COLLECTION} SET VERSION = 'cp1' WHERE id = 'doc'"),
    )
    .await;
    assert_eq!(
        title_of(&server, "doc").await,
        "d2-v1",
        "RESTORE in d2 must show the checkpointed state through the normal read path"
    );

    exec_ok(&server, "USE DATABASE default").await;
    assert_eq!(
        title_of(&server, "doc").await,
        "default-v2",
        "RESTORE in d2 must leave the same-name default collection untouched"
    );
}
