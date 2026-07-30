// SPDX-License-Identifier: BUSL-1.1

//! `rows_affected` on autocommit native-protocol DML must be REPORTED by the
//! write that ran, never taken from the dispatcher's expectation.
//!
//! The native (MessagePack) protocol is the canonical client transport, and its
//! `rows_affected` field answers the same question a pgwire `DELETE n` tag does:
//! "did my statement touch a row?" It is fed by the same Data-Plane responses,
//! so it carries the same contract — a delete against an absent row reports 0,
//! including a row deleted by an earlier statement.

mod common;

use common::native_harness::{NativeTestServer, do_handshake, send_sql};

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use tokio::net::TcpStream;

/// Open a native session and complete the handshake.
async fn native_session(server: &NativeTestServer) -> TcpStream {
    let (stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

/// Run `sql` over the native session and return its `rows_affected`.
async fn affected(stream: &mut TcpStream, seq: u64, sql: &str) -> Option<u64> {
    let resp = send_sql(stream, seq, sql).await;
    assert_eq!(
        resp.status,
        ResponseStatus::Ok,
        "statement should succeed: {sql}"
    );
    resp.rows_affected
}

/// Over the native protocol, re-deleting a primary key that was already
/// deleted reports 0 rows affected — the count comes from the delete's outcome,
/// not from the dispatcher's per-statement fallback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_redelete_of_deleted_primary_key_reports_zero() {
    let server = NativeTestServer::start().await;
    let mut stream = native_session(&server).await;

    let create = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION native_del_probe (id TEXT PRIMARY KEY, v INT)",
    )
    .await;
    assert_ne!(
        create.status,
        ResponseStatus::Error,
        "collection creation should succeed"
    );
    let insert = send_sql(
        &mut stream,
        2,
        "INSERT INTO native_del_probe (id, v) VALUES ('a', 1)",
    )
    .await;
    assert_eq!(insert.status, ResponseStatus::Ok, "insert should succeed");

    let first = affected(
        &mut stream,
        3,
        "DELETE FROM native_del_probe WHERE id = 'a'",
    )
    .await;
    assert_eq!(
        first,
        Some(1),
        "the native delete that removed the row must report 1"
    );

    let second = affected(
        &mut stream,
        4,
        "DELETE FROM native_del_probe WHERE id = 'a'",
    )
    .await;
    assert_eq!(
        second,
        Some(0),
        "re-deleting an already-deleted primary key over the native protocol must report 0"
    );
}
