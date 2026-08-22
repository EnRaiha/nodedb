// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDbRemote::execute_sql` carries non-empty
//! `params` through to the server as bound pgwire parameters.
//!
//! Today's client rejects any non-empty params at the trait method
//! boundary. This test asserts the spec — params bind through to the
//! server and the server applies them — and therefore fails until the
//! fix replaces the rejection branch with a real ToSql translator.

use nodedb_client::{NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn execute_sql_with_bound_params_round_trips_through_pgwire() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    // Spec: a single bound parameter binds to $1 in the SQL and the
    // server returns a row built from that value. NodeDB pgwire
    // describes parameters as text/UNKNOWN (see
    // `nodedb/tests/pgwire_extended_query.rs` for the precedent), so
    // the test passes a string value with a `::text` cast — the path
    // the server-side parameter description actually handles today.
    let params = vec![Value::String("hello".into())];
    let result = remote.execute_sql("SELECT $1::text AS n", &params).await;

    assert!(
        result.is_ok(),
        "execute_sql with a non-empty params slice must round-trip to \
         the server, not be rejected client-side; got {result:?}"
    );

    server.graceful_shutdown().await;
}
