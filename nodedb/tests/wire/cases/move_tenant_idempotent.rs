// SPDX-License-Identifier: BUSL-1.1

//! Idempotency test for `MOVE TENANT`.
//!
//! Re-issuing `MOVE TENANT` after a successful completion must return
//! `MOVE_TENANT_ALREADY_AT_TARGET` (SQLSTATE `02000`) rather than re-running
//! the full operation.

use crate::harness::TestServer;

/// Re-issue `MOVE TENANT` after it has already completed.
///
/// The second invocation must fail with SQLSTATE `02000`
/// (`MOVE_TENANT_ALREADY_AT_TARGET`).
#[tokio::test]
async fn move_tenant_idempotent_already_at_target() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // ── Source database ───────────────────────────────────────────────────────
    client
        .simple_query("CREATE DATABASE idem_src")
        .await
        .expect("CREATE DATABASE idem_src");
    client
        .simple_query("USE DATABASE idem_src")
        .await
        .expect("USE idem_src");
    client
        .simple_query("CREATE TENANT idem_tenant ID 30")
        .await
        .expect("CREATE TENANT idem_tenant");
    client
        .simple_query(
            "CREATE COLLECTION events \
             (event_id STRING PRIMARY KEY, payload STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION events");
    client
        .simple_query("INSERT INTO events (event_id, payload) VALUES ('e1', 'data')")
        .await
        .expect("INSERT e1");

    // ── Target database ───────────────────────────────────────────────────────
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CREATE DATABASE idem_tgt")
        .await
        .expect("CREATE DATABASE idem_tgt");
    client
        .simple_query("USE DATABASE idem_tgt")
        .await
        .expect("USE idem_tgt");
    client
        .simple_query(
            "CREATE COLLECTION events \
             (event_id STRING PRIMARY KEY, payload STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION events in tgt");

    // ── First MOVE TENANT — must succeed ─────────────────────────────────────
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("MOVE TENANT idem_tenant FROM idem_src TO idem_tgt")
        .await
        .expect("first MOVE TENANT must succeed");

    // ── Second MOVE TENANT — must return MOVE_TENANT_ALREADY_AT_TARGET ────────
    let result = client
        .simple_query("MOVE TENANT idem_tenant FROM idem_src TO idem_tgt")
        .await;

    let err = result.expect_err("second MOVE TENANT should return an error");
    let db_err = err
        .as_db_error()
        .expect("expected a database error from second MOVE TENANT");

    // SQLSTATE 02000 = no_data / MOVE_TENANT_ALREADY_AT_TARGET.
    assert_eq!(
        db_err.code().code(),
        "02000",
        "second MOVE TENANT must fail with SQLSTATE 02000 (MOVE_TENANT_ALREADY_AT_TARGET), got: {}",
        db_err.code().code()
    );
}

/// Moving a non-existent tenant must fail with SQLSTATE `42P01`.
#[tokio::test]
async fn move_tenant_unknown_tenant_fails() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE mt_err_src")
        .await
        .expect("CREATE DATABASE mt_err_src");
    client
        .simple_query("CREATE DATABASE mt_err_tgt")
        .await
        .expect("CREATE DATABASE mt_err_tgt");

    let result = client
        .simple_query("MOVE TENANT ghost_tenant FROM mt_err_src TO mt_err_tgt")
        .await;

    let err = result.expect_err("MOVE TENANT with unknown tenant must fail");
    let db_err = err
        .as_db_error()
        .expect("expected a database error for unknown tenant");
    assert_eq!(
        db_err.code().code(),
        "42P01",
        "unknown tenant should give SQLSTATE 42P01, got: {}",
        db_err.code().code()
    );
}

/// Moving a tenant to a non-existent target database must fail with SQLSTATE `42P01`.
#[tokio::test]
async fn move_tenant_unknown_target_database_fails() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE mt_known_src")
        .await
        .expect("CREATE DATABASE mt_known_src");
    client
        .simple_query("USE DATABASE mt_known_src")
        .await
        .expect("USE mt_known_src");
    client
        .simple_query("CREATE TENANT err_tenant_db ID 40")
        .await
        .expect("CREATE TENANT err_tenant_db");
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");

    let result = client
        .simple_query("MOVE TENANT err_tenant_db FROM mt_known_src TO nonexistent_db")
        .await;

    let err = result.expect_err("MOVE TENANT to nonexistent target must fail");
    let db_err = err
        .as_db_error()
        .expect("expected a database error for nonexistent target");
    assert_eq!(
        db_err.code().code(),
        "42P01",
        "nonexistent target database should give SQLSTATE 42P01, got: {}",
        db_err.code().code()
    );
}
