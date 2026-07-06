// SPDX-License-Identifier: BUSL-1.1

//! Regression test: native direct-op
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
/// Used to observe KV state in the transaction-atomicity tests below
/// (`kv_scan` already merges this transaction's staging overlay via
/// `merge_kv_overlay_into_scan` whenever `task.request.txn_id` is `Some`,
/// giving read-your-own-writes for free); `OpCode::KvBatchGet` response
/// shaping is covered separately by
/// `native_kv_batch_get_returns_fetched_values` below.
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

/// Native `OpCode::KvBatchGet` for `keys`.
async fn kv_batch_get(
    stream: &mut TcpStream,
    seq: u64,
    collection: &str,
    keys: Vec<Vec<u8>>,
) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::KvBatchGet,
        TextFields {
            collection: Some(collection.to_string()),
            keys: Some(keys),
            ..Default::default()
        },
    )
    .await
}

/// Regression test: a native `OpCode::KvBatchGet` fetched the
/// requested keys' values correctly on the Data Plane
/// (`execute_kv_batch_get` in `data/executor/handlers/kv/batch.rs`), but the
/// native-protocol response shaping dropped them -- `apply_kv_wrap`
/// (`control/server/response_shape/kv.rs`) had a wrapping arm for the
/// single-key `KvOp::Get` but none for `KvOp::BatchGet`, so `BatchGet`'s
/// payload (a bare msgpack array of per-key `base64-string-or-null`
/// scalars, positionally parallel to the requested keys) passed through
/// unwrapped into the generic row-flattener (`push_flat_rows`), whose
/// catch-all silently drops scalar array elements (it only keeps
/// objects/arrays) -- so `NativeResponse.rows` came back empty regardless
/// of the real fetched values. This test asserts the fetched values
/// actually reach `rows`; it fails on the pre-fix tree with `rows` empty
/// (`in_txn` unreachable -- kept autocommit throughout since this is a
/// response-shaping bug, independent of transactions).
#[tokio::test]
async fn native_kv_batch_get_returns_fetched_values() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create_resp = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION native_kv_batch_get (key TEXT PRIMARY KEY, val TEXT) \
         WITH (engine='kv')",
    )
    .await;
    assert_ne!(
        create_resp.status,
        ResponseStatus::Error,
        "CREATE must succeed: {create_resp:?}"
    );

    let put_resp = kv_batch_put(
        &mut stream,
        2,
        "native_kv_batch_get",
        vec![
            (b"bg1".to_vec(), b"value-one".to_vec()),
            (b"bg2".to_vec(), b"value-two".to_vec()),
        ],
    )
    .await;
    assert_ne!(
        put_resp.status,
        ResponseStatus::Error,
        "KvBatchPut must succeed: {put_resp:?}"
    );

    // Autocommit: fetch both present keys plus one key that was never
    // written. Pre-fix, `rows` is empty (or absent) regardless of what was
    // fetched -- the load-bearing assertions below fail on that tree.
    let batch_get = kv_batch_get(
        &mut stream,
        3,
        "native_kv_batch_get",
        vec![b"bg1".to_vec(), b"bg2".to_vec(), b"bg-missing".to_vec()],
    )
    .await;
    server.shutdown().await;
    assert_ne!(
        batch_get.status,
        ResponseStatus::Error,
        "KvBatchGet must succeed: {batch_get:?}"
    );

    let columns = batch_get.columns.clone().expect("columns present");
    let key_idx = columns
        .iter()
        .position(|c| c == "key")
        .expect("'key' column present");
    let value_idx = columns
        .iter()
        .position(|c| c == "value")
        .expect("'value' column present");

    let rows = batch_get.rows.expect("rows present");
    assert_eq!(
        rows.len(),
        3,
        "KvBatchGet must return one row per requested key, present or missing: {rows:?}"
    );

    let decode_b64 = |v: &Value| -> Vec<u8> {
        let Value::String(s) = v else {
            panic!("expected a base64 string value, got: {v:?}");
        };
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
            .expect("value must be valid base64")
    };

    let mut by_key: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for row in &rows {
        let Value::String(key) = &row[key_idx] else {
            panic!("expected a string 'key' cell, got: {:?}", row[key_idx]);
        };
        by_key.insert(key.clone(), row[value_idx].clone());
    }

    assert_eq!(
        decode_b64(by_key.get("bg1").expect("row for 'bg1' present")),
        b"value-one".to_vec(),
        "KvBatchGet must return the real stored value for 'bg1': {rows:?}"
    );
    assert_eq!(
        decode_b64(by_key.get("bg2").expect("row for 'bg2' present")),
        b"value-two".to_vec(),
        "KvBatchGet must return the real stored value for 'bg2': {rows:?}"
    );
    assert_eq!(
        by_key
            .get("bg-missing")
            .expect("row for 'bg-missing' present"),
        &Value::Null,
        "a missing key must be represented as a null value, not omitted: {rows:?}"
    );
}

/// Regression test: a native direct-op `KvBatchPut` issued inside
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

/// Regression test: a native `KvBatchPut` (`build_batch_put` in
/// `control/server/native/dispatch/plan_builder/kv.rs`) never called the
/// CP-side `SurrogateAssigner`, so `execute_kv_batch_put`
/// (`data/executor/handlers/kv/batch.rs`) wrote every batch-put row through
/// `KvEngine::batch_put` with `Surrogate::ZERO` -- the unbound sentinel --
/// unlike a single-key `Put`/`Insert`/`PointPut`, which always plans a real
/// surrogate via `assign_kv_surrogate`. A `Surrogate::ZERO` row is invisible
/// to any cross-engine surrogate-keyed prefilter/join (real bitmaps never
/// contain the reserved `0` element), a correctness gap versus single-key
/// puts.
///
/// No client-facing read in this codebase currently surfaces or gates on a
/// KV row's per-row surrogate: `KvOp::Scan`'s `surrogate_ceiling` treats
/// `s == 0` as *always visible* by design (so it cannot distinguish
/// zero from non-zero), and no native opcode exposes
/// `KvEngine::get_with_surrogate` / `key_for_surrogate`. So this test
/// cannot observe the surrogate value itself over the wire; the direct,
/// fails-pre-fix-passes-post-fix observable for the surrogate value lives
/// in `nodedb::engine::kv::engine::tests::batch_put_stores_real_per_entry_surrogates`
/// (`src/engine/kv/engine.rs`), which asserts `KvEngine::batch_put` stores
/// each entry's real assigned surrogate via `get_with_surrogate` (pre-fix,
/// that call took no `surrogates` parameter and hardcoded
/// `Surrogate::ZERO` for every entry).
///
/// What this test covers instead, end-to-end over the native wire: a
/// `KvBatchPut` writes rows that are functionally indistinguishable from
/// rows written by single-key `PointPut` calls on the same collection --
/// both are fully visible via `KvScan` and `KvBatchGet` immediately
/// afterward, i.e. the fix does not regress the batch path's observable
/// read behavior while it starts assigning real surrogates underneath.
#[tokio::test]
async fn native_kv_batch_put_rows_visible_same_as_single_put() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create_resp = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION native_kv_batch_parity (key TEXT PRIMARY KEY, val TEXT) \
         WITH (engine='kv')",
    )
    .await;
    assert_ne!(
        create_resp.status,
        ResponseStatus::Error,
        "CREATE must succeed: {create_resp:?}"
    );

    // Single-key native PointPut, same code path (`assign_kv_surrogate`) a
    // RESP `SET` or `INSERT` would take -- establishes the baseline "real
    // surrogate" row shape batch-put rows must match functionally.
    let single_put = send_request(
        &mut stream,
        2,
        OpCode::PointPut,
        TextFields {
            collection: Some("native_kv_batch_parity".to_string()),
            document_id: Some("single1".to_string()),
            data: Some(b"single-value".to_vec()),
            ..Default::default()
        },
    )
    .await;
    assert_ne!(
        single_put.status,
        ResponseStatus::Error,
        "single-key PointPut must succeed: {single_put:?}"
    );

    let put_resp = kv_batch_put(
        &mut stream,
        3,
        "native_kv_batch_parity",
        vec![
            (b"batch1".to_vec(), b"batch-value-one".to_vec()),
            (b"batch2".to_vec(), b"batch-value-two".to_vec()),
        ],
    )
    .await;
    assert_ne!(
        put_resp.status,
        ResponseStatus::Error,
        "KvBatchPut must succeed: {put_resp:?}"
    );

    // KvScan must see all three rows (one single-put, two batch-put).
    let scan = kv_scan(&mut stream, 4, "native_kv_batch_parity").await;
    server.shutdown().await;
    assert_ne!(
        scan.status,
        ResponseStatus::Error,
        "KvScan must succeed: {scan:?}"
    );
    let rows = scan.rows.expect("rows present");
    assert_eq!(
        rows.len(),
        3,
        "KvScan must see the single-put row plus both batch-put rows: {rows:?}"
    );
    for expected_key in ["single1", "batch1", "batch2"] {
        assert!(
            rows.iter()
                .flatten()
                .any(|v| *v == Value::String(expected_key.into())),
            "key '{expected_key}' must be visible via KvScan: {rows:?}"
        );
    }
}
