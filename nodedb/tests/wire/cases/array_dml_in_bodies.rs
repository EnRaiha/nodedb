// SPDX-License-Identifier: BUSL-1.1

//! `INSERT INTO ARRAY` inside a stored-procedure body and inside a trigger
//! body plans and applies.
//!
//! A body plans with the server-internal planner context. That context
//! carries the array catalog, so array DML plans there exactly as it does
//! for a client statement.

use crate::harness::TestServer;

const CREATE_GRID: &str = "CREATE ARRAY {name} \
     DIMS (x INT64 [0..15], y INT64 [0..15]) \
     ATTRS (v FLOAT64) \
     TILE_EXTENTS (4, 4)";

async fn create_grid(server: &TestServer, name: &str) {
    server
        .exec(&CREATE_GRID.replace("{name}", name))
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
}

/// Every row `ARRAY_SLICE` returns over the whole grid, each row's fields
/// joined into one string.
async fn grid_rows(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{name}', '{{\"x\":[0,15],\"y\":[0,15]}}', '*', 100)"
        ))
        .await
        .unwrap_or_else(|e| panic!("ARRAY_SLICE {name}: {e}"))
        .into_iter()
        .map(|row| row.join(","))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_procedure_body_inserts_into_an_array() {
    let server = TestServer::start().await;
    create_grid(&server, "proc_grid").await;

    server
        .exec(
            "CREATE PROCEDURE fill_proc_grid() AS \
             BEGIN \
               INSERT INTO ARRAY proc_grid COORDS (1, 2) VALUES (4.5); \
             END",
        )
        .await
        .expect("CREATE PROCEDURE");
    server
        .exec("CALL fill_proc_grid()")
        .await
        .expect("CALL a procedure whose body inserts into an array");

    let rows = grid_rows(&server, "proc_grid").await;
    assert_eq!(rows.len(), 1, "the procedure wrote one cell: {rows:?}");
    assert!(rows[0].contains("4.5"), "the cell holds 4.5: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_trigger_body_inserts_into_an_array() {
    let server = TestServer::start().await;
    create_grid(&server, "trig_grid").await;
    server
        .exec("CREATE COLLECTION trig_src")
        .await
        .expect("CREATE COLLECTION trig_src");
    server
        .exec(
            "CREATE SYNC TRIGGER fill_trig_grid AFTER INSERT ON trig_src FOR EACH ROW \
             BEGIN \
                 INSERT INTO ARRAY trig_grid COORDS (3, 4) VALUES (7.5); \
             END;",
        )
        .await
        .expect("CREATE SYNC TRIGGER");

    server
        .exec("INSERT INTO trig_src (id, name) VALUES ('s1', 'fires')")
        .await
        .expect("an insert whose trigger body inserts into an array");

    let rows = grid_rows(&server, "trig_grid").await;
    assert_eq!(rows.len(), 1, "the trigger wrote one cell: {rows:?}");
    assert!(rows[0].contains("7.5"), "the cell holds 7.5: {rows:?}");
}
