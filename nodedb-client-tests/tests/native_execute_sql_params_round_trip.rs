// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NativeClient::execute_sql` carries non-empty
//! `params` through to the server as bound values.
//!
//! Spec: `execute_sql("SELECT $1::bigint AS n", &[Value::Integer(42)])`
//! must return a single row whose only column is `42`. A client that
//! silently drops `params` and lets the server reject the unbound
//! placeholder is the silent-wrong pattern this test guards against —
//! the server-side error becomes the only signal, and it gives no hint
//! that the bindings never reached the wire.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, Value};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn native_execute_sql_with_bound_params_round_trips() {
    let server = TestServer::start().await;

    // The harness provisions superuser `nodedb`; `PoolConfig` has no default
    // identity (see `PoolConfig::new`'s doc comment), so state it explicitly.
    let pool = PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    );
    let native = NativeClient::new(pool);

    // Spec: a bound parameter binds to `$1` and the server returns a
    // row built from that value — Ok with one row containing 42.
    //
    // Will stay RED until `TextFields::sql_params` (or equivalent) is
    // added to the native protocol envelope and the server's dispatch
    // path threads bound values into the planner. Today's
    // `Err("not yet wired through the native protocol envelope")` is
    // correct negative behavior but not the spec. Do not soften the
    // assertion to accept the typed Err — that locks the gap in as
    // the contract.
    let params = vec![Value::Integer(42)];
    let qr = native
        .execute_sql("SELECT $1::bigint AS n", &params)
        .await
        .expect("native execute_sql must round-trip bound params end-to-end");
    assert!(
        !qr.rows.is_empty(),
        "execute_sql must return the row built from the bound param"
    );

    server.graceful_shutdown().await;
}
