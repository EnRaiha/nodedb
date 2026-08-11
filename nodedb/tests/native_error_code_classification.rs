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
//!
//! Control-Plane refusals are covered alongside them. They never reach a
//! Data-Plane `ErrorCode` at all — a statement naming a collection the
//! catalog does not have, or one the caller may not read, is decided while
//! planning — so they travel a different rendering path and were the last
//! place a typed classification was still being flattened to NDB-9000.

mod common;

use common::native_harness::{
    NativeTestServer, do_handshake, send_api_key_auth, send_request, send_sql,
};
use common::pgwire_harness::TestServer;

use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};
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

/// Mint an API key for `username`, creating the service account first unless
/// it is the harness superuser (which already exists).
fn create_api_key(shared: &SharedState, username: &str, roles: Vec<Role>) -> String {
    let user_id = if username == "nodedb" {
        shared
            .credentials
            .get_user(username)
            .expect("harness superuser")
            .user_id
    } else {
        shared
            .credentials
            .create_service_account(username, TenantId::new(1), roles, vec![DatabaseId::DEFAULT])
            .expect("create native service account")
    };

    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![DatabaseId::DEFAULT],
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create native API key")
}

async fn authenticated_stream(server: &NativeTestServer, token: String) -> TcpStream {
    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_api_key_auth(&mut stream, 1, token).await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native API key auth");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_unknown_collection_carries_collection_not_found_code() {
    let server = TestServer::start().await;

    let mut stream = native_session(&server).await;
    // The catalog lookup fails during planning, so nothing is ever dispatched
    // and no Data-Plane code exists to classify the failure. The refusal is
    // rendered from the Control Plane's own error, which is the half that
    // still reported NDB-9000 after the Data-Plane path was fixed.
    let resp = send_sql(&mut stream, 1, "SELECT * FROM native_err_absent_collection").await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "a query naming an absent collection must be refused"
    );
    let err = resp.error.expect("error payload expected");
    assert_eq!(
        err.code,
        sqlstate::UNDEFINED_TABLE,
        "an absent collection must map to its own SQLSTATE, got {}: {}",
        err.code,
        err.message
    );
    assert_eq!(
        err.ndb_code,
        ErrorCode::COLLECTION_NOT_FOUND.0,
        "the frame must carry the numeric collection-not-found code, got {} ({})",
        err.ndb_code,
        err.message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_permission_denial_carries_authorization_code() {
    let server = NativeTestServer::start_authenticated().await;

    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;
    let create = send_sql(&mut admin, 2, "CREATE COLLECTION native_err_private").await;
    assert_eq!(create.status, ResponseStatus::Ok, "create the target");

    // A second Control-Plane condition, deliberately unrelated to the catalog:
    // authorization is decided before dispatch too, so a fix that only rescued
    // not-found would be another special case rather than one classification.
    let token = create_api_key(
        &server.shared,
        "native_err_reader",
        vec![Role::Custom("native_err_reader_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;
    let resp = send_sql(&mut stream, 2, "SELECT * FROM native_err_private").await;
    drop(stream);
    drop(admin);
    server.shutdown().await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "a role without a grant must not read the collection"
    );
    let err = resp.error.expect("error payload expected");
    assert_eq!(
        err.code,
        sqlstate::INSUFFICIENT_PRIVILEGE,
        "a denial must map to its own SQLSTATE, got {}: {}",
        err.code,
        err.message
    );
    assert_eq!(
        err.ndb_code,
        ErrorCode::AUTHORIZATION_DENIED.0,
        "the frame must carry the numeric authorization-denied code, got {} ({})",
        err.ndb_code,
        err.message
    );
}
