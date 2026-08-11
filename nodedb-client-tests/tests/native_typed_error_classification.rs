// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that a server-classified failure reaches a native client
//! as the *same* typed error, not as a generic internal one.
//!
//! `NodeDbError` carries a stable numeric code and a machine-matchable
//! `ErrorDetails`, and callers branch on `is_constraint_violation()` /
//! `is_not_found()` rather than parsing prose. Over the native protocol every
//! one of those predicates used to answer `false`: the classification was
//! destroyed on the server (stringified into an internal error, then rendered
//! as `XX000`) and the frame carried no numeric code for the client to
//! rebuild from, so a duplicate key arrived indistinguishable from a crashed
//! database.
//!
//! Two different conditions are asserted deliberately. A fix that only
//! rescued constraint violations would be a constraint-shaped special case;
//! not-found travelling the sibling response path proves the whole pipeline
//! preserves classification.
//!
//! A Control-Plane refusal is asserted alongside them. It is decided before
//! anything is dispatched, so it never has a Data-Plane code to be rendered
//! from and is rendered from the internal error instead — the last path that
//! was still shipping a bare SQLSTATE and collapsing to `internal` here.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb};
use nodedb_test_support::pgwire_harness::TestServer;

fn native_client(server: &TestServer) -> NativeClient {
    // The harness provisions superuser `nodedb`; `PoolConfig` has no default
    // identity, so state it explicitly.
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

#[tokio::test]
async fn duplicate_unique_index_write_is_a_constraint_violation_on_the_client() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION client_err_unique")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX ON client_err_unique(email)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO client_err_unique (id, email) VALUES ('a', 'x@y.z')")
        .await
        .unwrap();

    let native = native_client(&server);
    // Fresh primary key, duplicate indexed value — the reported repro.
    let err = native
        .execute_sql(
            "INSERT INTO client_err_unique (id, email) VALUES ('b', 'x@y.z')",
            &[],
        )
        .await
        .expect_err("a duplicate unique-index value must be refused");

    assert!(
        err.is_constraint_violation(),
        "the client must rebuild the server's constraint violation, got {} ({:?})",
        err,
        err.details()
    );
    assert_eq!(
        err.code(),
        nodedb_types::error::ErrorCode::CONSTRAINT_VIOLATION
    );
    assert!(
        !err.is_internal(),
        "a refused write must never present as an internal failure: {err}"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn absent_index_read_is_not_found_on_the_client() {
    let server = TestServer::start().await;

    // A plain document collection carries no vector index, so a vector search
    // against it is refused by the Data Plane as `NotFound`. It travels the
    // direct-op response path rather than the SQL one, so it exercises the
    // sibling half of the same pipeline.
    server
        .exec("CREATE COLLECTION client_err_no_index")
        .await
        .unwrap();

    let native = native_client(&server);
    let err = native
        .vector_search("client_err_no_index", &[0.1, 0.2, 0.3], 5, None, None)
        .await
        .expect_err("a search with no index to search must be refused");

    assert!(
        err.is_not_found(),
        "the client must rebuild the server's not-found classification, got {} ({:?})",
        err,
        err.details()
    );
    assert!(
        !err.is_internal(),
        "a not-found must never present as an internal failure: {err}"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn unknown_collection_is_not_found_on_the_client() {
    let server = TestServer::start().await;

    let native = native_client(&server);
    // Nothing is ever dispatched: the catalog lookup fails while planning, so
    // this failure is classified entirely on the Control Plane. That path
    // carried no numeric code on the frame even after the Data-Plane one was
    // fixed, so a planner's "no such collection" arrived here as NDB-9000 and
    // `is_not_found()` answered `false`.
    let err = native
        .execute_sql("SELECT * FROM client_err_absent_collection", &[])
        .await
        .expect_err("a query naming an absent collection must be refused");

    assert!(
        err.is_not_found(),
        "the client must rebuild the planner's not-found classification, got {} ({:?})",
        err,
        err.details()
    );
    assert_eq!(
        err.code(),
        nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND
    );
    assert!(
        !err.is_internal(),
        "a planning refusal must never present as an internal failure: {err}"
    );

    server.graceful_shutdown().await;
}
