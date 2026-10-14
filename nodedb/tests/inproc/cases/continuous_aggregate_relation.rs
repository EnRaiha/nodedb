// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate as a relation: read-only, in one namespace with
//! collections, gated by grants on its own name, and creatable over a
//! source with a read policy only by a creator that policy does not bind.

use nodedb_test_support::pgwire_harness::TestServer;

async fn source_and_view(srv: &TestServer, src: &str, view: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {src} (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {src}: {e}"));
    srv.exec(&format!(
        "CREATE CONTINUOUS AGGREGATE {view} ON {src} BUCKET '1m' \
         AGGREGATE SUM(value) AS s GROUP BY host"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {view}: {e}"));
}

#[tokio::test]
async fn continuous_aggregate_is_read_only() {
    let srv = TestServer::start().await;
    source_and_view(&srv, "cagg_ro_src", "cagg_ro_view").await;
    for sql in [
        "INSERT INTO cagg_ro_view (bucket, host, s) VALUES (0, 'a', 1.0)",
        "UPDATE cagg_ro_view SET s = 2.0 WHERE host = 'a'",
        "DELETE FROM cagg_ro_view WHERE host = 'a'",
        "TRUNCATE cagg_ro_view",
    ] {
        srv.expect_error(sql, "42809").await;
        srv.expect_error(sql, "read-only").await;
    }
    srv.query_rows("SELECT host, s FROM cagg_ro_view")
        .await
        .unwrap_or_else(|e| panic!("a read is allowed: {e}"));
}

#[tokio::test]
async fn continuous_aggregate_name_collides_with_collection() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cagg_ns_src (ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    srv.exec("CREATE COLLECTION cagg_ns_taken")
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    srv.expect_error(
        "CREATE CONTINUOUS AGGREGATE cagg_ns_taken ON cagg_ns_src BUCKET '1m' \
         AGGREGATE SUM(value) AS s",
        "42P07",
    )
    .await;

    srv.exec(
        "CREATE CONTINUOUS AGGREGATE cagg_ns_view ON cagg_ns_src BUCKET '1m' \
         AGGREGATE SUM(value) AS s",
    )
    .await
    .unwrap_or_else(|e| panic!("create aggregate: {e}"));
    srv.expect_error("CREATE COLLECTION cagg_ns_view", "42P07")
        .await;
    srv.expect_error(
        "CREATE CONTINUOUS AGGREGATE pg_class ON cagg_ns_src BUCKET '1m' \
         AGGREGATE SUM(value) AS s",
        "42P07",
    )
    .await;
}

#[tokio::test]
async fn continuous_aggregate_read_requires_select() {
    let srv = TestServer::start().await;
    source_and_view(&srv, "cagg_grant_src", "cagg_grant_view").await;
    // The default role, readwrite, reads every relation. `monitor` reads
    // none, so only a grant on the aggregate lets this user read it.
    srv.exec("CREATE USER cagg_reader WITH PASSWORD 'reader-secret-42' ROLE monitor")
        .await
        .unwrap_or_else(|e| panic!("create user: {e}"));
    let (client, connection) = srv
        .connect_as("cagg_reader", "reader-secret-42")
        .await
        .unwrap_or_else(|e| panic!("connect as reader: {e}"));

    let denied = client
        .simple_query("SELECT host, s FROM cagg_grant_view")
        .await
        .expect_err("a read without a grant is refused");
    assert_eq!(
        denied.as_db_error().map(|e| e.code().code()),
        Some("42501"),
        "{denied:?}"
    );

    srv.exec("GRANT READ ON cagg_grant_view TO cagg_reader")
        .await
        .unwrap_or_else(|e| panic!("grant: {e}"));
    client
        .simple_query("SELECT host, s FROM cagg_grant_view")
        .await
        .unwrap_or_else(|e| panic!("a granted read succeeds: {e}"));
    drop(client);
    connection.abort();
}

#[tokio::test]
async fn continuous_aggregate_on_rls_source_requires_bypass() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION cagg_rls_src (ts BIGINT TIME_KEY, owner TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    srv.exec("CREATE RLS POLICY cagg_rls_own ON cagg_rls_src FOR READ USING (owner = $auth.id)")
        .await
        .unwrap_or_else(|e| panic!("create policy: {e}"));
    srv.exec("CREATE USER cagg_creator PASSWORD 'creator-secret-42'")
        .await
        .unwrap_or_else(|e| panic!("create user: {e}"));
    srv.exec("GRANT READ ON cagg_rls_src TO cagg_creator")
        .await
        .unwrap_or_else(|e| panic!("grant: {e}"));
    let (client, connection) = srv
        .connect_as("cagg_creator", "creator-secret-42")
        .await
        .unwrap_or_else(|e| panic!("connect as creator: {e}"));

    let refused = client
        .simple_query(
            "CREATE CONTINUOUS AGGREGATE cagg_rls_view ON cagg_rls_src BUCKET '1m' \
             AGGREGATE SUM(value) AS s",
        )
        .await
        .expect_err("a creator the read policy binds is refused");
    let db_error = refused.as_db_error().expect("a server error");
    assert_eq!(db_error.code().code(), "42501", "{db_error:?}");
    assert!(
        db_error.message().contains("read policy"),
        "the refusal names the policy: {db_error:?}"
    );
    drop(client);
    connection.abort();

    // A superuser is not bound by the policy.
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE cagg_rls_view ON cagg_rls_src BUCKET '1m' \
         AGGREGATE SUM(value) AS s",
    )
    .await
    .unwrap_or_else(|e| panic!("superuser create: {e}"));
}
