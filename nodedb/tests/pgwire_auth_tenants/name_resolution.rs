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
    ddl_ok(
        &state,
        &su,
        "ALTER TENANT 7145 SET QUOTA max_qps = 250",
    )
    .await;
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
