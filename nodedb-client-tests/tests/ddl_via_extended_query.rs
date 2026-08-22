// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test for DDL through the trait `execute_sql` path.
//!
//! `NodeDb::execute_sql` is the only generic SQL surface on the trait,
//! so DDL statements (`CREATE`, `DROP`, `ALTER`) must succeed through
//! it alongside `SELECT` and DML. The pgwire extended-query protocol
//! (Parse / Bind / Describe / Execute) the underlying `Client::query`
//! uses requires a row description that the server does not produce
//! for DDL — so a parameterless DDL request must take the simple-query
//! protocol path.
//!
//! The test runs `CREATE COLLECTION` followed by `DROP COLLECTION`;
//! both must succeed end-to-end. A regression where DDL via
//! `execute_sql` returns "pgwire query failed: db error" indicates the
//! parameterless path was rerouted onto the extended-query protocol
//! without a Describe-bypass.

use nodedb_client::{NodeDb, NodeDbRemote};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn ddl_via_execute_sql_succeeds() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    remote
        .execute_sql(
            "CREATE COLLECTION ddl_extended_query_smoke TYPE document",
            &[],
        )
        .await
        .expect("CREATE COLLECTION via execute_sql must succeed");

    remote
        .execute_sql("DROP COLLECTION ddl_extended_query_smoke", &[])
        .await
        .expect("DROP COLLECTION via execute_sql must succeed");

    server.graceful_shutdown().await;
}
