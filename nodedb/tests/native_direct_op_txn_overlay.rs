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

/// Native `OpCode::KvScan` over the whole (small) collection -- no cursor,
/// filters, or limit override, matching `build_scan`'s all-optional
/// `TextFields` defaults.
///
/// Used instead of `OpCode::KvBatchGet` to observe KV state in the tests
/// below: `execute_kv_batch_get`'s response is a flat JSON array of
/// base64-or-null scalars (`response_codec::encode_json_vec`), and the
/// shared native response shaper's `push_flat_rows`
/// (`control/server/response_shape/project.rs`) silently drops non-object
/// array items -- so a native `KvBatchGet`'s fetched values never reach
/// `NativeResponse.rows` today (a pre-existing response-shaping gap,
/// independent of the transaction-atomicity bug this test targets).
/// `execute_kv_scan` (`data/executor/handlers/kv/scan.rs`) instead emits one
/// JSON *object* per entry (`{"key": ..., "value": ...}`), which
/// `push_flat_rows` preserves as a real row, and it already merges this
/// transaction's staging overlay via `merge_kv_overlay_into_scan` whenever
/// `task.request.txn_id` is `Some` -- giving read-your-own-writes for free.
async fn kv_scan(stream: &mut TcpStream, seq: u64, collection: &str) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::KvScan,
        TextFields {
            collection: Some(collection.to_string()),
            ..Default::default()
        },
    )
    .await
}

/// Native `OpCode::KvBatchPut` of `entries` (raw key/value byte pairs) with
/// no TTL.
async fn kv_batch_put(
    stream: &mut TcpStream,
    seq: u64,
    collection: &str,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::KvBatchPut,
        TextFields {
            collection: Some(collection.to_string()),
            entries: Some(entries),
            ..Default::default()
        },
    )
    .await
}

/// Regression test for TW-14: a native direct-op `KvBatchPut` issued inside
/// an explicit transaction used to write straight through to durable storage
/// (`execute_kv_batch_put` in `data/executor/handlers/kv/batch.rs`, called
/// unconditionally from `handle_direct_op` via `dispatch_single_task`,
/// bypassing the protocol-neutral staging gate entirely) -- so `ROLLBACK`
/// never undid it, a transaction-atomicity violation. `KvOp::BatchPut` was
/// already on the `is_stageable_write` allow-list
/// (`shared/sql/staging_predicates.rs`) and its Data Plane staging handler
/// (`stage_kv_atomic::stage_kv_batch_put`) and COMMIT-replay handling
/// (`transaction/sub_plan_kv_ops.rs`) were already implemented and correct --
/// the SQL path just never had a way to reach `BatchPut` (`KV_BATCH_PUT` has
/// no SQL surface) and the native `handle_direct_op` path never routed
/// through `route_in_tx_write` for ANY direct op. The fix makes
/// `dispatch_single_task` route every direct-op task through the same
/// `route_in_tx_write`/`stage_write` gate `sql_loop.rs`'s SQL-planned
/// dispatch loop already uses, so a `KvBatchPut` inside `BEGIN...COMMIT` is
/// staged into the per-transaction overlay instead of hitting durable
/// storage immediately.
#[tokio::test]
async fn native_kv_batch_put_in_txn_is_staged_and_rolled_back() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create_resp = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION native_kv_batch (key TEXT PRIMARY KEY, val TEXT) \
         WITH (engine='kv')",
    )
    .await;
    assert_ne!(
        create_resp.status,
        ResponseStatus::Error,
        "CREATE must succeed: {create_resp:?}"
    );

    // Baseline row, committed via ordinary SQL INSERT (which allocates a
    // real surrogate up front, unlike `BatchPut` on a fresh key -- not load
    // bearing here, just establishes a durable row that must survive
    // ROLLBACK untouched).
    let insert_committed = send_sql(
        &mut stream,
        2,
        "INSERT INTO native_kv_batch (key, val) VALUES ('base', 'v0')",
    )
    .await;
    assert_ne!(
        insert_committed.status,
        ResponseStatus::Error,
        "committed INSERT must succeed: {insert_committed:?}"
    );

    // Autocommit sanity: exactly the baseline row is visible.
    let baseline = kv_scan(&mut stream, 3, "native_kv_batch").await;
    assert_ne!(
        baseline.status,
        ResponseStatus::Error,
        "baseline KvScan must succeed: {baseline:?}"
    );
    assert_eq!(
        baseline.rows.as_ref().map(Vec::len).unwrap_or(0),
        1,
        "baseline KvScan must see exactly the committed row: {baseline:?}"
    );

    let begin_resp = send_sql(&mut stream, 4, "BEGIN").await;
    assert_ne!(
        begin_resp.status,
        ResponseStatus::Error,
        "BEGIN must succeed: {begin_resp:?}"
    );

    let staged_put = kv_batch_put(
        &mut stream,
        5,
        "native_kv_batch",
        vec![
            (b"nk1".to_vec(), b"nv1".to_vec()),
            (b"nk2".to_vec(), b"nv2".to_vec()),
        ],
    )
    .await;
    assert_ne!(
        staged_put.status,
        ResponseStatus::Error,
        "in-tx native KvBatchPut must succeed: {staged_put:?}"
    );

    // Read-your-own-writes: a same-txn, same-connection KvScan must already
    // see the staged keys, exactly as if they had been durably written.
    let in_txn_scan = kv_scan(&mut stream, 6, "native_kv_batch").await;
    assert_ne!(
        in_txn_scan.status,
        ResponseStatus::Error,
        "in-txn KvScan must succeed: {in_txn_scan:?}"
    );
    let in_txn_rows = in_txn_scan.rows.expect("rows present");
    assert_eq!(
        in_txn_rows.len(),
        3,
        "in-txn KvScan must see the baseline row plus both staged BatchPut entries \
         (read-your-own-writes): {in_txn_rows:?}"
    );
    for expected in ["nk1", "nk2"] {
        assert!(
            in_txn_rows
                .iter()
                .flatten()
                .any(|v| *v == Value::String(expected.into())),
            "staged key '{expected}' must be visible in the in-txn KvScan result: \
             {in_txn_rows:?}"
        );
    }

    let rollback_resp = send_sql(&mut stream, 7, "ROLLBACK").await;
    assert_ne!(
        rollback_resp.status,
        ResponseStatus::Error,
        "ROLLBACK must succeed: {rollback_resp:?}"
    );

    // The load-bearing assertion: a FRESH, autocommit KvScan after ROLLBACK
    // must see ONLY the baseline row -- the staged BatchPut entries must be
    // gone. Pre-fix, `handle_direct_op` dispatched the `KvBatchPut` straight
    // to `execute_kv_batch_put`, which wrote `nk1`/`nk2` directly into
    // durable KV storage at statement time; ROLLBACK never touched durable
    // storage (it only drops the per-txn overlay), so this scan would still
    // see all 3 rows on the pre-fix tree, failing this assertion.
    let after_rollback = kv_scan(&mut stream, 8, "native_kv_batch").await;
    assert_ne!(
        after_rollback.status,
        ResponseStatus::Error,
        "post-rollback KvScan must succeed: {after_rollback:?}"
    );
    let after_rows = after_rollback.rows.expect("rows present");
    assert_eq!(
        after_rows.len(),
        1,
        "ROLLBACK must discard the staged BatchPut entries, leaving only the \
         baseline row: {after_rows:?}"
    );
    for leaked in ["nk1", "nk2"] {
        assert!(
            !after_rows
                .iter()
                .flatten()
                .any(|v| *v == Value::String(leaked.into())),
            "key '{leaked}' must NOT survive ROLLBACK: {after_rows:?}"
        );
    }

    // COMMIT persists a staged batch: BEGIN, KvBatchPut, COMMIT, then a
    // fresh autocommit KvScan must see the newly committed entries.
    let begin2 = send_sql(&mut stream, 9, "BEGIN").await;
    assert_ne!(
        begin2.status,
        ResponseStatus::Error,
        "second BEGIN must succeed: {begin2:?}"
    );

    let staged_put2 = kv_batch_put(
        &mut stream,
        10,
        "native_kv_batch",
        vec![(b"ck1".to_vec(), b"cv1".to_vec())],
    )
    .await;
    assert_ne!(
        staged_put2.status,
        ResponseStatus::Error,
        "second in-tx native KvBatchPut must succeed: {staged_put2:?}"
    );

    let commit_resp = send_sql(&mut stream, 11, "COMMIT").await;
    assert_ne!(
        commit_resp.status,
        ResponseStatus::Error,
        "COMMIT must succeed: {commit_resp:?}"
    );

    let after_commit = kv_scan(&mut stream, 12, "native_kv_batch").await;
    server.shutdown().await;
    assert_ne!(
        after_commit.status,
        ResponseStatus::Error,
        "post-commit KvScan must succeed: {after_commit:?}"
    );
    let after_commit_rows = after_commit.rows.expect("rows present");
    assert_eq!(
        after_commit_rows.len(),
        2,
        "COMMIT must persist the staged BatchPut entry alongside the baseline row: \
         {after_commit_rows:?}"
    );
    assert!(
        after_commit_rows
            .iter()
            .flatten()
            .any(|v| *v == Value::String("ck1".into())),
        "committed key 'ck1' must be visible after COMMIT: {after_commit_rows:?}"
    );
}
