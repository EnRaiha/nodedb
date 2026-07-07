// SPDX-License-Identifier: BUSL-1.1

//! Cross-cutting auth-hardening invariants.
//!
//! Combines per-database audit-row tagging, admin-DDL gating, and the
//! session-revocation infrastructure to prove the integrated guarantees:
//!
//! - every database DDL produces an audit row carrying the right
//!   `database_id`;
//! - `CLONE DATABASE` rejects non-Superuser callers (and emits a
//!   `PermissionDenied` audit) before reaching the underlying handler;
//! - `ALTER DATABASE … SET IDLE_TIMEOUT` updates the in-memory cache that
//!   the idle-sweep loop consults each tick.

mod common;

use common::pgwire_auth_helpers::{
    assert_audit_has, cluster_admin_user, ddl_err, ddl_ok, make_state_with_catalog, readonly_user,
    superuser,
};
use nodedb::control::security::audit::AuditEvent;
use nodedb_types::id::DatabaseId;

// ── H.1 Per-database audit smoke ────────────────────────────────────────────

/// Every database DDL produces a `Database*`-tagged audit row carrying
/// `database_id`.
#[tokio::test]
async fn database_ddl_audit_smoke() {
    let state = make_state_with_catalog();
    let su = superuser();
    let cat_handle = state.credentials.catalog();

    // CREATE DATABASE → DatabaseCreated, db_id = new id.
    ddl_ok(&state, &su, "CREATE DATABASE smoke_a").await;
    let smoke_a = cat_handle
        .get_database_id_by_name("smoke_a")
        .unwrap()
        .unwrap();
    assert_audit_has(&state, AuditEvent::DatabaseCreated, Some(smoke_a));

    // ALTER … RENAME → DatabaseRenamed, db_id = same id.
    ddl_ok(&state, &su, "ALTER DATABASE smoke_a RENAME TO smoke_a2").await;
    assert_audit_has(&state, AuditEvent::DatabaseRenamed, Some(smoke_a));

    // ALTER … SET QUOTA → DatabaseQuotaChanged.
    ddl_ok(
        &state,
        &su,
        "ALTER DATABASE smoke_a2 SET QUOTA (max_memory_bytes = 1024)",
    )
    .await;
    assert_audit_has(&state, AuditEvent::DatabaseQuotaChanged, Some(smoke_a));

    // ALTER … SET AUDIT_DML → DatabaseAuditDmlChanged.
    ddl_ok(
        &state,
        &su,
        "ALTER DATABASE smoke_a2 SET AUDIT_DML = 'writes'",
    )
    .await;
    assert_audit_has(&state, AuditEvent::DatabaseAuditDmlChanged, Some(smoke_a));

    // ALTER … SET IDLE_TIMEOUT → DatabaseIdleTimeoutChanged.
    ddl_ok(
        &state,
        &su,
        "ALTER DATABASE smoke_a2 SET IDLE_TIMEOUT = 1800",
    )
    .await;
    assert_audit_has(
        &state,
        AuditEvent::DatabaseIdleTimeoutChanged,
        Some(smoke_a),
    );

    // CLONE DATABASE → DatabaseCloned, db_id = target (new clone) id.
    ddl_ok(&state, &su, "CLONE DATABASE smoke_clone FROM smoke_a2").await;
    let smoke_clone_id = cat_handle
        .get_database_id_by_name("smoke_clone")
        .unwrap()
        .unwrap();
    assert_audit_has(&state, AuditEvent::DatabaseCloned, Some(smoke_clone_id));

    // DROP DATABASE → DatabaseDropped, db_id = id of dropped db.
    // FORCE because we just cloned from smoke_a2; the dependency check would
    // otherwise reject the drop. Both regular and FORCE paths emit the same
    // event per the locked gating matrix.
    ddl_ok(&state, &su, "DROP DATABASE smoke_a2 FORCE").await;
    assert_audit_has(&state, AuditEvent::DatabaseDropped, Some(smoke_a));
}

// ── H.2 Cross-checklist: CLONE without Superuser ────────────────────────────

/// CLONE DATABASE rejected without Superuser (D), accepted with Superuser
/// (A), audit row produced.
#[tokio::test]
async fn clone_database_requires_superuser_audit() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE DATABASE clone_src").await;
    let cat_handle = state.credentials.catalog();
    let src_id = cat_handle
        .get_database_id_by_name("clone_src")
        .unwrap()
        .unwrap();

    // Cluster admin (not Superuser) tries to clone → 42501.
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "CLONE DATABASE c1 FROM clone_src").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501, got: {err}"
    );
    // PermissionDenied audit row tagged with source db_id.
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(src_id));

    // Readonly user → also denied.
    let viewer = readonly_user();
    let err = ddl_err(&state, &viewer, "CLONE DATABASE c2 FROM clone_src").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501, got: {err}"
    );

    // Superuser succeeds; audit row uses target (new clone) id.
    ddl_ok(&state, &su, "CLONE DATABASE c3 FROM clone_src").await;
    let c3_id = cat_handle.get_database_id_by_name("c3").unwrap().unwrap();
    assert_audit_has(&state, AuditEvent::DatabaseCloned, Some(c3_id));
}

// ── H.3 Idle-timeout cache observable after ALTER ───────────────────────────

/// `ALTER DATABASE SET IDLE_TIMEOUT` updates the in-memory cache observable
/// by the idle-sweep loop (integration contract).
#[tokio::test]
async fn idle_timeout_cache_observable_after_alter() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "ALTER DATABASE default SET IDLE_TIMEOUT = 600").await;
    assert_eq!(state.idle_timeout_cache.get(DatabaseId::DEFAULT), 600);

    // Setting back to 0 must remove the per-database override.
    ddl_ok(&state, &su, "ALTER DATABASE default SET IDLE_TIMEOUT = 0").await;
    assert_eq!(state.idle_timeout_cache.get(DatabaseId::DEFAULT), 0);
}
