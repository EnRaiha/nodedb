// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for a boot-time bug where document_strict collections
//! read back blank after a server restart: the per-core schema registry used
//! to decode Binary Tuple rows was populated only by live DDL, so WAL replay
//! left `SELECT *` decoding a raw tuple. Asserts the original typed column
//! values survive a graceful shutdown + reopen.

use crate::harness::TestServer;

#[tokio::test]
async fn strict_collection_columns_survive_restart() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION people (id INT PRIMARY KEY, name TEXT, age INT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION people");

    srv.exec("INSERT INTO people (id, name, age) VALUES (1, 'Alice', 30)")
        .await
        .expect("INSERT row 1 into people");
    srv.exec("INSERT INTO people (id, name, age) VALUES (2, 'Bob', 42)")
        .await
        .expect("INSERT row 2 into people");

    // Restart: shut the server down cleanly, then reopen the same data dir
    // so recovery goes through WAL replay + boot-time schema rehydration.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path(dir).await;

    // The count survives even with the bug (row bytes are still there via
    // WAL replay); it's not a sufficient guard on its own.
    let count_rows = srv
        .query_rows("SELECT COUNT(*) FROM people")
        .await
        .expect("SELECT COUNT(*) FROM people");
    assert_eq!(count_rows.len(), 1);
    assert_eq!(
        count_rows[0][0], "2",
        "both inserted rows must survive the restart"
    );

    // The real guard: the typed column values must decode correctly, not
    // come back blank or as a raw (id, data) tuple.
    let rows = srv
        .query_rows("SELECT id, name, age FROM people ORDER BY id")
        .await
        .expect("SELECT id, name, age FROM people ORDER BY id");
    assert_eq!(rows.len(), 2, "expected 2 rows after restart, got {rows:?}");
    assert_eq!(
        rows[0],
        vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
        "row 1 must retain its original typed column values after restart, got {:?}",
        rows[0]
    );
    assert_eq!(
        rows[1],
        vec!["2".to_string(), "Bob".to_string(), "42".to_string()],
        "row 2 must retain its original typed column values after restart, got {:?}",
        rows[1]
    );
}
