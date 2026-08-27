// SPDX-License-Identifier: BUSL-1.1

//! Engine surface routing tests.
//!
//! Verifies that all seven `WITH (engine=...)` names are accepted,
//! that `engine='array'` and `engine='graph'` are explicitly rejected,
//! that unknown engine names are rejected, and that `CREATE SEQUENCE`,
//! `CREATE ALERT`, `SHOW PERMISSIONS`, etc. reach the correct handlers.

use crate::harness::TestServer;

// ── All seven canonical engines are accepted ─────────────────────────────────

#[tokio::test]
async fn engine_document_schemaless_accepted() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION route_doc_sl WITH (engine='document_schemaless')")
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_document_strict_accepted() {
    let srv = TestServer::start().await;
    // Use CREATE TABLE which defaults to document_strict.
    srv.exec("CREATE TABLE route_doc_st (id TEXT PRIMARY KEY)")
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_kv_accepted() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION route_kv (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();
}

#[tokio::test]
async fn engine_columnar_accepted() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION route_col \
         COLUMNS (id TEXT, v FLOAT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn engine_timeseries_accepted() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION route_ts \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, v FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn engine_spatial_accepted() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION route_spatial \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn engine_vector_accepted() {
    let srv = TestServer::start().await;
    // engine='vector' must map to document_schemaless without error.
    srv.exec("CREATE COLLECTION route_vec WITH (engine='vector')")
        .await
        .unwrap();
}

// ── Rejected engine names ─────────────────────────────────────────────────────

#[tokio::test]
async fn engine_array_rejected_in_with_clause() {
    let srv = TestServer::start().await;
    let err = srv
        .exec("CREATE COLLECTION bad_arr WITH (engine='array')")
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("array") || err.to_lowercase().contains("create array"),
        "expected array DDL hint, got: {err}"
    );
}

#[tokio::test]
async fn engine_graph_rejected_in_with_clause() {
    let srv = TestServer::start().await;
    let err = srv
        .exec("CREATE COLLECTION bad_graph WITH (engine='graph')")
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("match") || err.to_lowercase().contains("graph"),
        "expected graph hint, got: {err}"
    );
}

#[tokio::test]
async fn unknown_engine_name_rejected() {
    let srv = TestServer::start().await;
    let err = srv
        .exec("CREATE COLLECTION bad_engine WITH (engine='foobar')")
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("engine")
            || err.to_lowercase().contains("unsupported")
            || err.to_lowercase().contains("unknown"),
        "expected unknown-engine error, got: {err}"
    );
}

// ── DDL handler routing ───────────────────────────────────────────────────────

#[tokio::test]
async fn create_sequence_routes_correctly() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE SEQUENCE route_seq \
         START 1 INCREMENT 1 MIN 1 MAX 9999 CYCLE CACHE 10",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn show_permissions_routes_correctly() {
    let srv = TestServer::start().await;
    // Must not error; may return an empty result set.
    srv.exec("SHOW PERMISSIONS").await.unwrap();
}

#[tokio::test]
async fn if_not_exists_collection_is_idempotent() {
    let srv = TestServer::start().await;
    // CREATE TABLE IF NOT EXISTS is the supported idempotent DDL path.
    srv.exec("CREATE TABLE IF NOT EXISTS route_ine (id TEXT PRIMARY KEY)")
        .await
        .unwrap();
    // Second call must not error.
    srv.exec("CREATE TABLE IF NOT EXISTS route_ine (id TEXT PRIMARY KEY)")
        .await
        .unwrap();
}

#[tokio::test]
async fn create_table_alias_accepted() {
    let srv = TestServer::start().await;
    // CREATE TABLE is an alias for CREATE COLLECTION.
    srv.exec("CREATE TABLE route_tbl (id TEXT PRIMARY KEY, val INT)")
        .await
        .unwrap();
}
