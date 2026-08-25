// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Array (ND sparse) engine.
//!
//! Covers: CREATE ARRAY DDL, INSERT INTO ARRAY, ARRAY_SLICE TVF query,
//! wrong-DDL rejection (engine='array' must be rejected), and WAL durability.

use crate::harness::TestServer;

#[tokio::test]
async fn create_array_and_insert() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_basic \
         DIMS (row INT64, col INT64) \
         ATTRS (value FLOAT64) \
         TILE_EXTENTS (10, 10)",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO ARRAY arr_basic COORDS (0, 0) VALUES (1.5)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ARRAY arr_basic COORDS (0, 1) VALUES (2.5)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ARRAY arr_basic COORDS (1, 0) VALUES (3.5)")
        .await
        .unwrap();
}

#[tokio::test]
async fn array_slice_query() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_slice \
         DIMS (x INT64, y INT64) \
         ATTRS (temp FLOAT64) \
         TILE_EXTENTS (5, 5)",
    )
    .await
    .unwrap();

    for x in 0..3i64 {
        for y in 0..3i64 {
            let temp = (x * 3 + y) as f64;
            srv.exec(&format!(
                "INSERT INTO ARRAY arr_slice COORDS ({x}, {y}) VALUES ({temp})"
            ))
            .await
            .unwrap();
        }
    }

    // ARRAY_SLICE bounds are inclusive on both ends (closed range).
    // See `nodedb-array/src/query/slice.rs` for the documented semantics.
    let rows = srv
        .query_rows(
            "SELECT * FROM ARRAY_SLICE('arr_slice', \
             '{\"x\":[0,1],\"y\":[0,1]}', '*', 100)",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        4,
        "expected 4 cells from inclusive [0,1] x [0,1] slice, got {}",
        rows.len()
    );
}

#[tokio::test]
async fn engine_array_flag_rejected_in_with_clause() {
    let srv = TestServer::start().await;
    let err = srv
        .exec("CREATE COLLECTION bad_array WITH (engine='array')")
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("array")
            || err.to_lowercase().contains("create array")
            || err.to_lowercase().contains("unsupported"),
        "expected array-rejection error, got: {err}"
    );
}

#[tokio::test]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_wal \
         DIMS (i INT64) \
         ATTRS (v FLOAT64) \
         TILE_EXTENTS (10)",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO ARRAY arr_wal COORDS (5) VALUES (99.0)")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT * FROM ARRAY_SLICE('arr_wal', '{\"i\":[5,6]}', '*', 10)")
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "expected persisted array cell after restart"
    );
}
