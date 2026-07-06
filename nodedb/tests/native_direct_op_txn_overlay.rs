// SPDX-License-Identifier: BUSL-1.1

//! Regression test for TW-4 (shared root with TW-14): native direct-op
//! dispatch (`handle_direct_op` in
//! `control/server/native/dispatch/direct_ops.rs`) hardcoded `txn_id: None`
//! on every dispatched `PhysicalTask`, so a native direct RANGE scan issued
//! inside an explicit transaction could never see the transaction's own
//! staged writes — even though the Data Plane's bitemporal RangeScan handler
//! (`data/executor/handlers/control/range_scan_versioned.rs`) already merges
//! the per-transaction staging overlay whenever `task.request.txn_id` is
//! `Some`.
//!
//! This drives a native RANGE scan via the *direct-op* wire path
//! (`OpCode::RangeScan` + `TextFields`, not a planned SQL `SELECT`) inside a
//! `BEGIN` block on a `bitemporal=true` collection and asserts it observes a
//! same-connection, same-transaction staged `INSERT` (read-your-own-writes),
//! then asserts `ROLLBACK` removes it from view again.

mod common;

use common::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Native direct RANGE scan (`OpCode::RangeScan`) over the `id` field,
/// lexically bounded `[a, z)` — covers every lowercase-letter id this test
/// inserts.
async fn range_scan(stream: &mut TcpStream, seq: u64, collection: &str) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::RangeScan,
        TextFields {
            collection: Some(collection.to_string()),
            field: Some("id".to_string()),
            lower_bound: Some(b"a".to_vec()),
            upper_bound: Some(b"z".to_vec()),
            limit: Some(100),
            ..Default::default()
        },
    )
    .await
}

#[tokio::test]
async fn native_direct_range_scan_sees_own_staged_write_in_txn() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create_resp = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION native_rov (id STRING PRIMARY KEY, n INT) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await;
    assert_ne!(
        create_resp.status,
        ResponseStatus::Error,
        "CREATE must succeed: {create_resp:?}"
    );

    let insert_committed = send_sql(
        &mut stream,
        2,
        "INSERT INTO native_rov (id, n) VALUES ('a', 1)",
    )
    .await;
    assert_ne!(
        insert_committed.status,
        ResponseStatus::Error,
        "committed INSERT must succeed: {insert_committed:?}"
    );

    // Autocommit baseline: the direct RANGE scan sees exactly the committed
    // row. Establishes that the direct-op path works at all outside a txn
    // (autocommit `txn_id` is `None` both before and after this fix).
    let baseline = range_scan(&mut stream, 3, "native_rov").await;
    assert_ne!(
        baseline.status,
        ResponseStatus::Error,
        "baseline RANGE scan must succeed: {baseline:?}"
    );
    assert_eq!(
        baseline.rows.as_ref().map(Vec::len).unwrap_or(0),
        1,
        "baseline must see exactly the committed row: {baseline:?}"
    );

    let begin_resp = send_sql(&mut stream, 4, "BEGIN").await;
    assert_ne!(
        begin_resp.status,
        ResponseStatus::Error,
        "BEGIN must succeed: {begin_resp:?}"
    );

    let staged_insert = send_sql(
        &mut stream,
        5,
        "INSERT INTO native_rov (id, n) VALUES ('b', 2)",
    )
    .await;
    assert_ne!(
        staged_insert.status,
        ResponseStatus::Error,
        "in-tx INSERT must succeed: {staged_insert:?}"
    );

    // The regression: a native direct RANGE scan run INSIDE the same
    // transaction, on the same connection, must see its own staged 'b' row
    // (read-your-own-writes). Pre-fix, `handle_direct_op` hardcoded
    // `txn_id: None` on the dispatched `PhysicalTask`, so the Data Plane's
    // overlay merge in `range_scan_versioned.rs` (gated on
    // `task.request.txn_id.is_some()`) never fired — this assertion fails
    // on the pre-fix tree with exactly 1 row (only the committed 'a').
    let in_txn_scan = range_scan(&mut stream, 6, "native_rov").await;
    assert_ne!(
        in_txn_scan.status,
        ResponseStatus::Error,
        "in-txn RANGE scan must succeed: {in_txn_scan:?}"
    );
    let in_txn_rows = in_txn_scan.rows.expect("rows present");
    assert_eq!(
        in_txn_rows.len(),
        2,
        "in-txn direct RANGE scan must see the transaction's own staged insert \
         (read-your-own-writes), got: {in_txn_rows:?}"
    );
    assert!(
        in_txn_rows
            .iter()
            .flatten()
            .any(|v| *v == Value::String("b".into())),
        "staged row 'b' must be visible in the in-txn RANGE scan result: {in_txn_rows:?}"
    );

    let rollback_resp = send_sql(&mut stream, 7, "ROLLBACK").await;
    assert_ne!(
        rollback_resp.status,
        ResponseStatus::Error,
        "ROLLBACK must succeed: {rollback_resp:?}"
    );

    // After ROLLBACK the connection is back in autocommit (this connection's
    // active txn id is cleared), so the direct RANGE scan must revert to
    // seeing only the durably committed row — the staged 'b' must not leak
    // past the transaction.
    let after_rollback = range_scan(&mut stream, 8, "native_rov").await;
    server.shutdown().await;
    assert_ne!(
        after_rollback.status,
        ResponseStatus::Error,
        "post-rollback RANGE scan must succeed: {after_rollback:?}"
    );
    let after_rows = after_rollback.rows.expect("rows present");
    assert_eq!(
        after_rows.len(),
        1,
        "ROLLBACK must remove the staged row from view: {after_rows:?}"
    );
}
