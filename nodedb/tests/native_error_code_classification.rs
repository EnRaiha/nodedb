// SPDX-License-Identifier: BUSL-1.1

//! Typed error classification survives the native (MessagePack) protocol.
//!
//! A Data-Plane refusal is classified once, deterministically, into an
//! `ErrorCode`. Three layers used to destroy that classification on the way
//! out over native — the handler stringified the typed error into
//! `Internal { detail }`, the dispatch layer stamped `XX000` and a `{:?}`
//! dump, and the wire frame carried no numeric code at all — so every typed
//! condition reached the client as NDB-9000 and a duplicate key was
//! indistinguishable from a crashed database.
//!
//! These tests pin the wire contract at the frame level: the SQLSTATE comes
//! from the same protocol-neutral mapping pgwire uses, and the stable numeric
//! NodeDB code rides alongside it. The client-side half (rebuilding the typed
//! error, so `is_constraint_violation()` / `is_not_found()` answer correctly)
//! lives in `nodedb-client-tests`.
//!
//! Both a UNIQUE-index violation and a not-found read are covered: one
//! condition alone would not distinguish a general fix from a
//! constraint-shaped special case.

mod common;

use common::native_harness::{do_handshake, send_request, send_sql};
use common::pgwire_harness::TestServer;

use nodedb_types::error::{ErrorCode, sqlstate};
use nodedb_types::protocol::opcodes::{OpCode, ResponseStatus};
use nodedb_types::protocol::{HelloFrame, TextFields};
use tokio::net::TcpStream;

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
async fn native_unique_index_violation_carries_constraint_code() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION native_err_unique")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX ON native_err_unique(email)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO native_err_unique (id, email) VALUES ('a', 'x@y.z')")
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    // Fresh primary key, duplicate indexed value: the refusal comes from the
    // secondary-index enforcement inside the apply, which is the path that
    // used to stringify the typed error into `Internal { detail }`.
    let resp = send_sql(
        &mut stream,
        1,
        "INSERT INTO native_err_unique (id, email) VALUES ('b', 'x@y.z')",
    )
    .await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "a duplicate unique-index value must be refused"
    );
    let err = resp.error.expect("error payload expected");
    assert_eq!(
        err.code,
        sqlstate::UNIQUE_VIOLATION,
        "unique-index violation must map to its own SQLSTATE, got {}: {}",
        err.code,
        err.message
    );
    assert_eq!(
        err.ndb_code,
        ErrorCode::CONSTRAINT_VIOLATION.0,
        "the frame must carry the numeric constraint-violation code, got {} ({})",
        err.ndb_code,
        err.message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_duplicate_primary_key_carries_constraint_code() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION native_err_pk")
        .await
        .unwrap();
    server
        .exec("INSERT INTO native_err_pk (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    let resp = send_sql(
        &mut stream,
        1,
        "INSERT INTO native_err_pk (id, n) VALUES ('dup', 2)",
    )
    .await;

    assert_eq!(resp.status, ResponseStatus::Error);
    let err = resp.error.expect("error payload expected");
    assert_eq!(err.code, sqlstate::UNIQUE_VIOLATION);
    assert_eq!(
        err.ndb_code,
        ErrorCode::CONSTRAINT_VIOLATION.0,
        "duplicate primary key must classify as a constraint violation, got {}",
        err.ndb_code
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_absent_key_read_carries_not_found_code() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION native_err_kv (key TEXT PRIMARY KEY, n INT) \
             WITH (engine='kv')",
        )
        .await
        .unwrap();

    let mut stream = native_session(&server).await;
    // A field read of a key that was never written is refused by the Data
    // Plane as `NotFound`. It travels the direct-op response path, which is
    // the sibling of the SQL path exercised above — a fix that only rescued
    // constraint violations would leave this one at XX000 / NDB-9000.
    let resp = send_request(
        &mut stream,
        1,
        OpCode::KvFieldGet,
        TextFields {
            collection: Some("native_err_kv".into()),
            key: Some("no-such-key".into()),
            fields: Some(vec!["n".into()]),
            ..Default::default()
        },
    )
    .await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "a field read of an absent key must be refused, not answered empty"
    );
    let err = resp.error.expect("error payload expected");
    assert_eq!(
        err.code,
        sqlstate::NO_DATA,
        "not-found must map to its own SQLSTATE, got {}: {}",
        err.code,
        err.message
    );
    assert_eq!(
        err.ndb_code,
        ErrorCode::DOCUMENT_NOT_FOUND.0,
        "the frame must carry the numeric not-found code, got {} ({})",
        err.ndb_code,
        err.message
    );
}
