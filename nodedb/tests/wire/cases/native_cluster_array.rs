// SPDX-License-Identifier: BUSL-1.1

//! The native (MessagePack) protocol must execute
//! `PhysicalPlan::ClusterArray(ClusterArrayOp::{Slice,Agg,Put,Delete})`
//! exactly as pgwire does: `INSERT`/`DELETE FROM ARRAY` answer with a real
//! affected count, `ARRAY_SLICE`/`ARRAY_AGG` answer with rows, and an
//! in-transaction write is staged and read-your-own-writes visible until
//! `ROLLBACK` discards it.
//!
//! Runs on the full-boot `TestServer`, which runs a cluster topology, so
//! `plan_sql()` emits the `ClusterArrayOp` routing wrappers pgwire's overlay
//! tests exercise (see `sql_transactions_cluster_array_overlay.rs`) — this
//! file drives the same plans over the native wire instead of pgwire.

use nodedb_test_support::native_harness::{do_handshake, send_sql};
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use tokio::net::TcpStream;

use crate::harness::TestServer;

/// Open a native session and complete the handshake against `server`.
async fn native_session(server: &TestServer) -> TcpStream {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.native_port);
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

/// Run `sql` over the native session, asserting success, and return the
/// decoded response.
async fn exec(
    stream: &mut TcpStream,
    seq: u64,
    sql: &str,
) -> nodedb_types::protocol::NativeResponse {
    let resp = send_sql(stream, seq, sql).await;
    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "statement should succeed: {sql}: {:?}",
        resp.error
    );
    resp
}

async fn create_array(stream: &mut TcpStream, seq: u64, array: &str) {
    exec(
        stream,
        seq,
        &format!(
            "CREATE ARRAY {array} \
             DIMS (row INT64, col INT64) \
             ATTRS (value FLOAT64) \
             TILE_EXTENTS (10, 10)"
        ),
    )
    .await;
}

/// `CREATE ARRAY` + `INSERT INTO ARRAY` of three cells over the native wire
/// answers with a real affected count, exactly as pgwire's `INSERT n` tag
/// does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_insert_reports_affected_count() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    create_array(&mut stream, 1, "narr_insert").await;

    let inserted = exec(
        &mut stream,
        2,
        "INSERT INTO ARRAY narr_insert \
         COORDS (1, 1) VALUES (1.0), \
         COORDS (1, 2) VALUES (2.0), \
         COORDS (1, 3) VALUES (3.0)",
    )
    .await;
    assert_eq!(
        inserted.rows_affected,
        Some(3),
        "native INSERT INTO ARRAY of three cells must report rows_affected == 3"
    );
}

/// `SELECT * FROM ARRAY_SLICE(...)` over the native wire returns the
/// inserted cells as rows, one column per dim and per attr.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_slice_returns_rows() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    create_array(&mut stream, 1, "narr_slice").await;
    exec(
        &mut stream,
        2,
        "INSERT INTO ARRAY narr_slice \
         COORDS (1, 1) VALUES (10.0), \
         COORDS (1, 2) VALUES (20.0), \
         COORDS (1, 3) VALUES (30.0)",
    )
    .await;

    let sliced = exec(
        &mut stream,
        3,
        "SELECT * FROM ARRAY_SLICE('narr_slice', '{\"row\":[1,1],\"col\":[1,3]}', '*', 100)",
    )
    .await;
    // A cell row carries the dims, then the attrs, in schema order — the
    // same columns pgwire renders.
    let columns = sliced.columns.expect("slice response carries columns");
    assert_eq!(
        columns,
        ["row", "col", "value"],
        "slice columns must be the dims then the attrs"
    );
    let rows = sliced.rows.expect("slice response carries rows");
    assert_eq!(rows.len(), 3, "slice must return all three inserted cells");
    let mut cells: Vec<(i64, i64, f64)> = rows
        .iter()
        .map(|row| match row.as_slice() {
            [
                nodedb_types::Value::Integer(r),
                nodedb_types::Value::Integer(c),
                nodedb_types::Value::Float(v),
            ] => (*r, *c, *v),
            other => panic!("unexpected slice row {other:?}"),
        })
        .collect();
    cells.sort_by(|a, b| a.partial_cmp(b).expect("finite cells"));
    assert_eq!(cells, [(1, 1, 10.0), (1, 2, 20.0), (1, 3, 30.0)]);
}

/// `SELECT * FROM ARRAY_AGG(...)` over the native wire returns the
/// count/sum reduction as one row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_agg_returns_reduction() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    create_array(&mut stream, 1, "narr_agg").await;
    exec(
        &mut stream,
        2,
        "INSERT INTO ARRAY narr_agg \
         COORDS (1, 1) VALUES (10.0), \
         COORDS (2, 2) VALUES (20.0)",
    )
    .await;

    let count = exec(
        &mut stream,
        3,
        "SELECT * FROM ARRAY_AGG('narr_agg', 'value', 'count')",
    )
    .await;
    let rows = count.rows.expect("agg response carries rows");
    assert_eq!(rows.len(), 1, "agg must answer with one scalar row");

    let sum = exec(
        &mut stream,
        4,
        "SELECT * FROM ARRAY_AGG('narr_agg', 'value', 'sum')",
    )
    .await;
    assert_eq!(sum.rows.expect("agg response carries rows").len(), 1);
}

/// `DELETE FROM ARRAY ... WHERE COORDS IN (...)` over the native wire
/// reports 1 for an existing coordinate and 0 the second time it is
/// deleted, exactly as pgwire's `DELETE n` tag does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_delete_reports_affected_count() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    create_array(&mut stream, 1, "narr_delete").await;
    exec(
        &mut stream,
        2,
        "INSERT INTO ARRAY narr_delete COORDS (5, 5) VALUES (1.0)",
    )
    .await;

    let first = exec(
        &mut stream,
        3,
        "DELETE FROM ARRAY narr_delete WHERE COORDS IN ((5, 5))",
    )
    .await;
    assert_eq!(
        first.rows_affected,
        Some(1),
        "deleting an existing cell over the native protocol must report 1"
    );

    let second = exec(
        &mut stream,
        4,
        "DELETE FROM ARRAY narr_delete WHERE COORDS IN ((5, 5))",
    )
    .await;
    assert_eq!(
        second.rows_affected,
        Some(0),
        "re-deleting an already-deleted cell over the native protocol must report 0"
    );
}

/// An in-transaction `INSERT INTO ARRAY` staged over the native wire is
/// visible to a same-transaction `ARRAY_SLICE` (read-your-own-writes), and
/// disappears after `ROLLBACK`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_in_txn_slice_sees_staged_put_then_rollback() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    create_array(&mut stream, 1, "narr_txn").await;

    exec(&mut stream, 2, "BEGIN").await;
    exec(
        &mut stream,
        3,
        "INSERT INTO ARRAY narr_txn COORDS (9, 9) VALUES (7.0)",
    )
    .await;

    let staged = exec(
        &mut stream,
        4,
        "SELECT * FROM ARRAY_SLICE('narr_txn', '{\"row\":[9,9],\"col\":[9,9]}', '*', 100)",
    )
    .await;
    assert_eq!(
        staged.rows.expect("slice response carries rows").len(),
        1,
        "an in-tx ARRAY_SLICE over the native protocol must observe the transaction's own \
         staged cell"
    );

    exec(&mut stream, 5, "ROLLBACK").await;

    let after_rollback = exec(
        &mut stream,
        6,
        "SELECT * FROM ARRAY_SLICE('narr_txn', '{\"row\":[9,9],\"col\":[9,9]}', '*', 100)",
    )
    .await;
    // An empty native result set carries `rows: None`.
    assert!(
        after_rollback.rows.unwrap_or_default().is_empty(),
        "a rolled-back staged cell must not persist over the native protocol"
    );
}

/// A multi-statement native batch — two `ClusterArray` reads sent as one
/// `;`-separated SQL frame — folds both statements' rows into the single
/// response the native dispatch loop answers with, exercising the same
/// per-task accumulation pgwire's `dispatch_task_loop` performs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_multi_statement_batch_folds_rows() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    create_array(&mut stream, 1, "narr_batch").await;
    exec(
        &mut stream,
        2,
        "INSERT INTO ARRAY narr_batch COORDS (1, 1) VALUES (1.0), COORDS (2, 2) VALUES (2.0)",
    )
    .await;

    let batch = exec(
        &mut stream,
        3,
        "SELECT * FROM ARRAY_SLICE('narr_batch', '{\"row\":[1,1],\"col\":[1,1]}', '*', 100); \
         SELECT * FROM ARRAY_AGG('narr_batch', 'value', 'count')",
    )
    .await;
    let rows = batch.rows.expect("batch response carries rows");
    assert_eq!(
        rows.len(),
        2,
        "a two-statement native batch of ClusterArray reads must fold both statements' rows \
         into one response, got {rows:?}"
    );
}

/// NDVI over a cluster array read on the native wire: the computed column,
/// the attr filter, the ordering and the row bound all apply to the slice's
/// named rows, which the cluster coordinator gathers from every shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_cluster_array_slice_ndvi_where_order() {
    let server = TestServer::start().await;
    let mut stream = native_session(&server).await;
    exec(
        &mut stream,
        1,
        "CREATE ARRAY narr_ndvi \
         DIMS (x INT64 [0..15], y INT64 [0..15]) \
         ATTRS (nir FLOAT64, red FLOAT64) \
         TILE_EXTENTS (2, 2)",
    )
    .await;
    // Cells on tiles far apart, so the slice spans several shards. NDVI is
    // (nir - red) / (nir + red): 0.6, 0.2, -0.2 and 0.8 in insert order.
    exec(
        &mut stream,
        2,
        "INSERT INTO ARRAY narr_ndvi \
         COORDS (0, 0) VALUES (0.8, 0.2), \
         COORDS (5, 9) VALUES (0.6, 0.4), \
         COORDS (11, 3) VALUES (0.4, 0.6), \
         COORDS (15, 15) VALUES (0.9, 0.1)",
    )
    .await;

    let resp = exec(
        &mut stream,
        3,
        "SELECT x, y, (nir - red) / (nir + red) AS ndvi \
         FROM ARRAY_SLICE('narr_ndvi', '{x: [0, 15], y: [0, 15]}') \
         WHERE nir > red \
         ORDER BY ndvi DESC \
         LIMIT 2",
    )
    .await;
    assert_eq!(
        resp.columns.expect("columns"),
        ["x", "y", "ndvi"],
        "the SELECT list names the columns"
    );
    let rows = resp.rows.expect("rows");
    let got: Vec<(i64, i64, f64)> = rows
        .iter()
        .map(|row| match row.as_slice() {
            [
                nodedb_types::Value::Integer(x),
                nodedb_types::Value::Integer(y),
                nodedb_types::Value::Float(ndvi),
            ] => (*x, *y, *ndvi),
            other => panic!("unexpected NDVI row {other:?}"),
        })
        .collect();
    assert_eq!(got.len(), 2, "LIMIT 2 keeps two rows: {got:?}");
    assert_eq!((got[0].0, got[0].1), (15, 15));
    assert_eq!((got[1].0, got[1].1), (0, 0));
    assert!((got[0].2 - 0.8).abs() < 1e-9, "ndvi of (15, 15): {got:?}");
    assert!((got[1].2 - 0.6).abs() < 1e-9, "ndvi of (0, 0): {got:?}");
}
