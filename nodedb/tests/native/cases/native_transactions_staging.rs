// SPDX-License-Identifier: BUSL-1.1

//! In-transaction statement-time staging over the native (MessagePack/JSON)
//! protocol, mirroring the pgwire coverage in `sql_transactions_kv_overlay.rs`
//! and `sql_transactions_unique_violation.rs`. Native transactions route
//! through the same protocol-neutral staging gate pgwire uses
//! (`shared::session::staging_gate::route_in_tx_write`), so a native
//! connection must get the same three behaviors:
//!   - a point write inside `BEGIN;...` reports its real affected count at
//!     the statement, not a blind buffered "1" (verified here on a single
//!     row, since every stageable write is a point write returning 1 either
//!     way -- the interesting divergence catches a regression to the old
//!     buffer-and-defer path, which returned 1 for a write that did NOT
//!     actually apply anything yet).
//!   - an in-transaction `SELECT` sees a write staged earlier in the same
//!     transaction (read-your-own-writes), which the old native buffer-only
//!     path could not provide.
//!   - a duplicate-key `INSERT` inside a transaction is rejected AT THE
//!     STATEMENT with a native error frame, not silently buffered for
//!     COMMIT.
//!
//! DDL and the pre-existing row are created over pgwire (the catalog-backed
//! path other native tests use); the transaction itself runs entirely over
//! one native connection, since transaction state is keyed by the native
//! session's peer address.

mod common;

use common::native_harness::{do_handshake, send_sql};
use common::pgwire_harness::TestServer;

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Open a native session against the server's native listener and complete
/// the handshake. The returned stream is ready for `send_sql`.
async fn native_session(srv: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_in_tx_insert_stages_real_count_and_is_visible_in_tx() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION native_tx_kv (key TEXT PRIMARY KEY, n INT) \
             WITH (engine='kv')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO native_tx_kv (key, n) VALUES ('a', 1)")
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    let mut seq = 1u64;

    let begin_resp = send_sql(&mut stream, seq, "BEGIN").await;
    assert_eq!(begin_resp.status, ResponseStatus::Ok, "BEGIN must succeed");

    // A staged point INSERT reports a real affected count at statement time.
    seq += 1;
    let insert_resp = send_sql(
        &mut stream,
        seq,
        "INSERT INTO native_tx_kv (key, n) VALUES ('b', 2)",
    )
    .await;
    assert_eq!(
        insert_resp.status,
        ResponseStatus::Ok,
        "in-tx native INSERT should be staged, not rejected"
    );
    assert_eq!(
        insert_resp.rows_affected,
        Some(1),
        "staged INSERT must report its real affected count at the statement"
    );

    // Read-your-own-writes: the staged row is visible to a SELECT on the
    // same native connection, inside the same transaction, before COMMIT.
    seq += 1;
    let select_resp = send_sql(
        &mut stream,
        seq,
        "SELECT n FROM native_tx_kv WHERE key = 'b'",
    )
    .await;
    assert_eq!(select_resp.status, ResponseStatus::Ok);
    let rows = select_resp
        .rows
        .expect("in-tx SELECT must return rows for the staged write");
    assert_eq!(
        rows.first().and_then(|r| r.first()).cloned(),
        Some(Value::Integer(2)),
        "staged write must be visible to an in-transaction native SELECT"
    );

    seq += 1;
    let commit_resp = send_sql(&mut stream, seq, "COMMIT").await;
    assert_eq!(
        commit_resp.status,
        ResponseStatus::Ok,
        "COMMIT must succeed"
    );

    // Committed row is now visible over pgwire too.
    let committed = server
        .query_text("SELECT n FROM native_tx_kv WHERE key = 'b'")
        .await
        .unwrap();
    assert_eq!(committed, vec!["2".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_in_tx_rollback_discards_staged_write() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION native_tx_rb (key TEXT PRIMARY KEY, n INT) \
             WITH (engine='kv')",
        )
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    let mut seq = 1u64;

    let begin_resp = send_sql(&mut stream, seq, "BEGIN").await;
    assert_eq!(begin_resp.status, ResponseStatus::Ok);

    seq += 1;
    let insert_resp = send_sql(
        &mut stream,
        seq,
        "INSERT INTO native_tx_rb (key, n) VALUES ('r', 9)",
    )
    .await;
    assert_eq!(insert_resp.status, ResponseStatus::Ok);
    assert_eq!(insert_resp.rows_affected, Some(1));

    seq += 1;
    let rollback_resp = send_sql(&mut stream, seq, "ROLLBACK").await;
    assert_eq!(rollback_resp.status, ResponseStatus::Ok);

    let after_rollback = server
        .query_text("SELECT n FROM native_tx_rb WHERE key = 'r'")
        .await
        .unwrap();
    assert!(
        after_rollback.is_empty(),
        "ROLLBACK must discard the staged write, found: {after_rollback:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_in_tx_duplicate_key_insert_rejected_at_statement() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION native_tx_dup (id STRING NOT NULL PRIMARY KEY, n INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO native_tx_dup (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    let mut seq = 1u64;

    let begin_resp = send_sql(&mut stream, seq, "BEGIN").await;
    assert_eq!(begin_resp.status, ResponseStatus::Ok);

    seq += 1;
    let dup_resp = send_sql(
        &mut stream,
        seq,
        "INSERT INTO native_tx_dup (id, n) VALUES ('dup', 2)",
    )
    .await;
    assert_eq!(
        dup_resp.status,
        ResponseStatus::Error,
        "duplicate PK insert inside a native transaction must fail at the statement"
    );
    let err = dup_resp.error.expect("error payload expected");
    assert_eq!(
        err.code, "23505",
        "duplicate key must surface as a unique-violation SQLSTATE, got {}",
        err.code
    );

    // The offending statement never applied and the surrounding transaction
    // is still usable (it was not silently deferred to COMMIT).
    seq += 1;
    let rollback_resp = send_sql(&mut stream, seq, "ROLLBACK").await;
    assert_eq!(rollback_resp.status, ResponseStatus::Ok);

    let rows = server
        .query_text("SELECT n FROM native_tx_dup WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["1".to_string()],
        "original row must be unaffected by the rejected duplicate insert"
    );
}
