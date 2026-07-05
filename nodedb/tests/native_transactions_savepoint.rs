// SPDX-License-Identifier: BUSL-1.1

//! SQL savepoint control over the native (MessagePack) protocol, mirroring the
//! pgwire coverage in `sql_transactions_savepoint_overlay.rs`. Native savepoints
//! route through the same protocol-neutral `SessionStore` savepoint stack and
//! dispatch the same `MetaOp::MarkSavepoint` / `MetaOp::RollbackToSavepoint`
//! overlay meta-ops pgwire uses, so a native connection must get identical
//! semantics:
//!   - `SAVEPOINT` marks the staged value/graph overlay journals;
//!   - `ROLLBACK TO SAVEPOINT` rewinds staged writes made after the mark while
//!     keeping earlier ones, visible to a read-your-own-writes SELECT;
//!   - `RELEASE SAVEPOINT` of an unknown name → SQLSTATE 3B001;
//!   - a savepoint command outside a transaction block → SQLSTATE 25P01.
//!
//! DDL is created over pgwire (the catalog-backed path other native tests use);
//! the transaction runs entirely over one native connection, since transaction
//! state is keyed by the native session's peer address.

mod common;

use common::native_harness::{do_handshake, send_sql};
use common::pgwire_harness::TestServer;

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::value::Value;
use tokio::net::TcpStream;

/// Open a native session against the server's native listener and complete the
/// handshake. The returned stream is ready for `send_sql`.
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
async fn native_rollback_to_savepoint_reverts_later_staged_write() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION native_sp_kv (key TEXT PRIMARY KEY, n INT) \
             WITH (engine='kv')",
        )
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    let mut seq = 1u64;

    let begin = send_sql(&mut stream, seq, "BEGIN").await;
    assert_eq!(begin.status, ResponseStatus::Ok, "BEGIN must succeed");

    // Stage row 'a' before the savepoint.
    seq += 1;
    let ins_a = send_sql(
        &mut stream,
        seq,
        "INSERT INTO native_sp_kv (key, n) VALUES ('a', 1)",
    )
    .await;
    assert_eq!(ins_a.status, ResponseStatus::Ok);

    // Establish the savepoint.
    seq += 1;
    let sp = send_sql(&mut stream, seq, "SAVEPOINT s1").await;
    assert_eq!(sp.status, ResponseStatus::Ok, "SAVEPOINT must succeed");

    // Stage row 'b' after the savepoint.
    seq += 1;
    let ins_b = send_sql(
        &mut stream,
        seq,
        "INSERT INTO native_sp_kv (key, n) VALUES ('b', 2)",
    )
    .await;
    assert_eq!(ins_b.status, ResponseStatus::Ok);

    // Before rollback, a read-your-own-writes SELECT sees BOTH a and b.
    seq += 1;
    let both = send_sql(
        &mut stream,
        seq,
        "SELECT n FROM native_sp_kv WHERE key = 'b'",
    )
    .await;
    assert_eq!(both.status, ResponseStatus::Ok);
    assert_eq!(
        both.rows
            .expect("b visible pre-rollback")
            .first()
            .and_then(|r| r.first())
            .cloned(),
        Some(Value::Integer(2)),
        "row staged after SAVEPOINT must be visible before ROLLBACK TO"
    );

    // Rewind to the savepoint: row 'b' is discarded, 'a' survives.
    seq += 1;
    let rb = send_sql(&mut stream, seq, "ROLLBACK TO SAVEPOINT s1").await;
    assert_eq!(
        rb.status,
        ResponseStatus::Ok,
        "ROLLBACK TO SAVEPOINT must succeed"
    );

    // After rollback, 'b' is gone from the in-transaction view.
    seq += 1;
    let after_b = send_sql(
        &mut stream,
        seq,
        "SELECT n FROM native_sp_kv WHERE key = 'b'",
    )
    .await;
    assert_eq!(after_b.status, ResponseStatus::Ok);
    assert!(
        after_b.rows.map(|r| r.is_empty()).unwrap_or(true),
        "row staged after SAVEPOINT must be gone after ROLLBACK TO"
    );

    // ...but 'a' (staged before the savepoint) is still visible.
    seq += 1;
    let after_a = send_sql(
        &mut stream,
        seq,
        "SELECT n FROM native_sp_kv WHERE key = 'a'",
    )
    .await;
    assert_eq!(after_a.status, ResponseStatus::Ok);
    assert_eq!(
        after_a
            .rows
            .expect("a visible post-rollback")
            .first()
            .and_then(|r| r.first())
            .cloned(),
        Some(Value::Integer(1)),
        "row staged before SAVEPOINT must survive ROLLBACK TO"
    );

    // COMMIT persists only 'a'.
    seq += 1;
    let commit = send_sql(&mut stream, seq, "COMMIT").await;
    assert_eq!(commit.status, ResponseStatus::Ok, "COMMIT must succeed");

    let committed_a = server
        .query_text("SELECT n FROM native_sp_kv WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(committed_a, vec!["1".to_string()], "'a' must persist");

    let committed_b = server
        .query_text("SELECT n FROM native_sp_kv WHERE key = 'b'")
        .await
        .unwrap();
    assert!(
        committed_b.is_empty(),
        "'b' must NOT persist after ROLLBACK TO, found: {committed_b:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_release_unknown_savepoint_errors_3b001() {
    let server = TestServer::start().await;

    let mut stream = native_session(&server).await;
    let mut seq = 1u64;

    let begin = send_sql(&mut stream, seq, "BEGIN").await;
    assert_eq!(begin.status, ResponseStatus::Ok);

    seq += 1;
    let release = send_sql(&mut stream, seq, "RELEASE SAVEPOINT nope").await;
    assert_eq!(
        release.status,
        ResponseStatus::Error,
        "RELEASE of an unknown savepoint must fail"
    );
    let err = release.error.expect("error payload expected");
    assert_eq!(
        err.code, "3B001",
        "unknown savepoint must surface SQLSTATE 3B001, got {}",
        err.code
    );

    // The transaction is still usable and can be rolled back cleanly.
    seq += 1;
    let rollback = send_sql(&mut stream, seq, "ROLLBACK").await;
    assert_eq!(rollback.status, ResponseStatus::Ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_savepoint_outside_transaction_errors_25p01() {
    let server = TestServer::start().await;

    let mut stream = native_session(&server).await;

    // No BEGIN: SAVEPOINT outside a transaction block is rejected.
    let sp = send_sql(&mut stream, 1, "SAVEPOINT s1").await;
    assert_eq!(
        sp.status,
        ResponseStatus::Error,
        "SAVEPOINT outside a transaction must fail"
    );
    let err = sp.error.expect("error payload expected");
    assert_eq!(
        err.code, "25P01",
        "SAVEPOINT outside a transaction must surface SQLSTATE 25P01, got {}",
        err.code
    );
}
