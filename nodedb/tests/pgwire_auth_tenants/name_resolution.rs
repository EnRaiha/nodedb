// SPDX-License-Identifier: BUSL-1.1

//! Name-based tenant reference resolution on the DROP / ALTER / PURGE
//! TENANT paths.
//!
//! Verifies that the legacy `DROP TENANT <id>` form keeps working and that
//! a tenant name (bare or single-quoted) resolves to the same id via the
//! shared `resolve_tenant_ref` helper, mirroring the already-shipped
//! `CREATE TENANT <name>` / `SHOW TENANT <name>` paths.

use crate::common::pgwire_auth_helpers::{ddl_err, ddl_ok, make_state_with_catalog, superuser};

// ─── DROP TENANT by name ─────────────────────────────────────────────────────

/// `DROP TENANT <id>` (numeric) — regression that the legacy form still
/// works after the resolver refactor.
#[tokio::test]
async fn drop_tenant_by_numeric_id_still_works() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_drop_num ID 7142").await;
    ddl_ok(&state, &su, "DROP TENANT 7142").await;
}

/// `DROP TENANT <name>` — the new path; name resolves to the catalog id.
#[tokio::test]
async fn drop_tenant_by_bare_name() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_drop_name ID 7143").await;
    ddl_ok(&state, &su, "DROP TENANT acme_drop_name").await;
}

/// `DROP TENANT '<name>'` — single-quoted name, matches the AST
/// `TenantSelector` behavior on CREATE/SHOW.
#[tokio::test]
async fn drop_tenant_by_quoted_name() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_drop_quoted ID 7144").await;
    ddl_ok(&state, &su, "DROP TENANT 'acme_drop_quoted'").await;
}

/// `DROP TENANT <unknown_name>` without `IF EXISTS` errors with `42704`.
#[tokio::test]
async fn drop_tenant_unknown_name_without_if_exists_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "DROP TENANT no_such_tenant").await;
    assert!(
        err.contains("does not exist") && err.contains("42704"),
        "expected 42704/does not exist, got: {err}"
    );
}

/// `DROP TENANT IF EXISTS <unknown_name>` is a no-op success — parallels the
/// `IF EXISTS <unknown_id>` semantics.
#[tokio::test]
async fn drop_tenant_if_exists_unknown_name_is_noop() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "DROP TENANT IF EXISTS no_such_tenant").await;
}

/// `DROP TENANT ''` (empty quoted name) → `42601` syntax error.
#[tokio::test]
async fn drop_tenant_empty_name_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "DROP TENANT ''").await;
    assert!(
        err.contains("42601") && err.contains("numeric id or a tenant name"),
        "expected 42601 empty-name error, got: {err}"
    );
}

// ─── ALTER TENANT by name ────────────────────────────────────────────────────

/// `ALTER TENANT <id> SET QUOTA ...` — regression: numeric form still works.
#[tokio::test]
async fn alter_tenant_by_numeric_id_still_works() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_alter_num ID 7145").await;
    ddl_ok(&state, &su, "ALTER TENANT 7145 SET QUOTA max_qps = 250").await;
}

/// `ALTER TENANT <name> SET QUOTA ...` — name resolves to id.
#[tokio::test]
async fn alter_tenant_by_name() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_alter_name ID 7146").await;
    ddl_ok(
        &state,
        &su,
        "ALTER TENANT acme_alter_name SET QUOTA max_qps = 250",
    )
    .await;
}

/// `ALTER TENANT <unknown_name> SET QUOTA ...` errors with `42704`.
#[tokio::test]
async fn alter_tenant_unknown_name_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(
        &state,
        &su,
        "ALTER TENANT no_such_tenant SET QUOTA max_qps = 250",
    )
    .await;
    assert!(
        err.contains("does not exist") && err.contains("42704"),
        "expected 42704/does not exist, got: {err}"
    );
}

/// `ALTER TENANT '<name>' SET QUOTA ...` — single-quoted name resolves to id,
/// matching the quoted-name path already covered on DROP.
#[tokio::test]
async fn alter_tenant_by_quoted_name() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_alter_quoted ID 7147").await;
    ddl_ok(
        &state,
        &su,
        "ALTER TENANT 'acme_alter_quoted' SET QUOTA max_qps = 250",
    )
    .await;
}

/// `ALTER TENANT '' SET QUOTA ...` (empty quoted name) → `42601`, the same
/// resolver guard exercised on DROP.
#[tokio::test]
async fn alter_tenant_empty_name_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "ALTER TENANT '' SET QUOTA max_qps = 250").await;
    assert!(
        err.contains("42601") && err.contains("numeric id or a tenant name"),
        "expected 42601 empty-name error, got: {err}"
    );
}

// ─── PURGE TENANT by name ────────────────────────────────────────────────────
//
// `purge_tenant` resolves the tenant reference, rejects the system tenant, and
// only then dispatches the destructive meta op to the Data Plane. The
// resolution and guard branches all return before that dispatch, so they are
// covered here without a Data Plane; the post-dispatch happy path is exercised
// by the executor-level purge tests.

/// `PURGE TENANT <name> CONFIRM` on an unknown name → `42704`, before any
/// destructive dispatch.
#[tokio::test]
async fn purge_tenant_unknown_name_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "PURGE TENANT no_such_tenant CONFIRM").await;
    assert!(
        err.contains("does not exist") && err.contains("42704"),
        "expected 42704/does not exist, got: {err}"
    );
}

/// `PURGE TENANT '' CONFIRM` (empty quoted name) → `42601` resolver guard.
#[tokio::test]
async fn purge_tenant_empty_name_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "PURGE TENANT '' CONFIRM").await;
    assert!(
        err.contains("42601") && err.contains("numeric id or a tenant name"),
        "expected 42601 empty-name error, got: {err}"
    );
}

/// `PURGE TENANT <name>` with a malformed confirmation token resolves the name
/// first, then fails the `CONFIRM` gate with `42601`. The `42601`/CONFIRM error
/// (rather than `42704`) proves the name resolved — covering the by-name purge
/// path up to the Data Plane boundary.
#[tokio::test]
async fn purge_tenant_by_name_resolves_then_requires_confirm() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_purge_name ID 7148").await;
    let err = ddl_err(&state, &su, "PURGE TENANT acme_purge_name PLEASE").await;
    assert!(
        err.contains("42601") && err.contains("CONFIRM"),
        "expected name to resolve then hit the CONFIRM gate, got: {err}"
    );
}

/// `PURGE TENANT <id>` (numeric) for an existing tenant with a malformed
/// confirmation token — the legacy numeric path resolves and passes the
/// existence gate, then fails the `CONFIRM` gate. Regression that the resolver
/// refactor kept the numeric fast path intact for purge.
#[tokio::test]
async fn purge_tenant_by_numeric_id_resolves_then_requires_confirm() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme_purge_num ID 7149").await;
    let err = ddl_err(&state, &su, "PURGE TENANT 7149 PLEASE").await;
    assert!(
        err.contains("42601") && err.contains("CONFIRM"),
        "expected numeric id to resolve then hit the CONFIRM gate, got: {err}"
    );
}

// ─── Unknown numeric id parity (id form must match name form) ─────────────────
//
// A numeric id that matches no tenant must behave exactly like an unknown
// name: `42704`, or an `IF EXISTS` no-op for DROP. Before the existence gate
// these silently proceeded (DROP proposed a delete, ALTER seeded a default
// quota, PURGE dispatched a destructive op) — the id/name asymmetry.

/// `DROP TENANT <unknown_id>` without `IF EXISTS` → `42704`, matching the
/// unknown-name behavior.
#[tokio::test]
async fn drop_tenant_unknown_numeric_id_without_if_exists_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "DROP TENANT 999001").await;
    assert!(
        err.contains("42704") && err.contains("does not exist"),
        "expected 42704 for unknown numeric id, got: {err}"
    );
}

/// `DROP TENANT IF EXISTS <unknown_id>` is a no-op success, matching the
/// unknown-name `IF EXISTS` behavior.
#[tokio::test]
async fn drop_tenant_if_exists_unknown_numeric_id_is_noop() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "DROP TENANT IF EXISTS 999002").await;
}

/// `ALTER TENANT <unknown_id> SET QUOTA ...` → `42704`, never a silent
/// default-quota seed for a phantom id.
#[tokio::test]
async fn alter_tenant_unknown_numeric_id_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "ALTER TENANT 999003 SET QUOTA max_qps = 250").await;
    assert!(
        err.contains("42704") && err.contains("does not exist"),
        "expected 42704 for unknown numeric id, got: {err}"
    );
}

/// `PURGE TENANT <unknown_id> CONFIRM` → `42704`, never a destructive dispatch
/// for a tenant that does not exist.
#[tokio::test]
async fn purge_tenant_unknown_numeric_id_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "PURGE TENANT 999004 CONFIRM").await;
    assert!(
        err.contains("42704") && err.contains("does not exist"),
        "expected 42704 for unknown numeric id, got: {err}"
    );
}

// ─── System tenant guards ────────────────────────────────────────────────────

/// `DROP TENANT 0` — the system tenant is protected with `42501` regardless of
/// the resolver refactor.
#[tokio::test]
async fn drop_system_tenant_numeric_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "DROP TENANT 0").await;
    assert!(
        err.contains("42501") && err.contains("system tenant"),
        "expected 42501 system-tenant guard, got: {err}"
    );
}

/// `PURGE TENANT 0 CONFIRM` — the system tenant cannot be purged.
#[tokio::test]
async fn purge_system_tenant_numeric_errors() {
    let state = make_state_with_catalog();
    let su = superuser();

    let err = ddl_err(&state, &su, "PURGE TENANT 0 CONFIRM").await;
    assert!(
        err.contains("42501") && err.contains("system tenant"),
        "expected 42501 system-tenant guard, got: {err}"
    );
}
