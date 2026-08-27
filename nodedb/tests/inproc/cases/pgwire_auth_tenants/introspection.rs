// SPDX-License-Identifier: BUSL-1.1

//! Per-tenant introspection by name / id.
//!
//! `SHOW TENANTS` lists every tenant; `SHOW TENANT` (no args) reports the
//! session's effective tenant; `SHOW TENANT QUOTA|USAGE FOR <name> IN
//! DATABASE <db>` reports quota/usage scoped to a database. The missing
//! rung is **per-tenant introspection by identifier** — an admin who
//! knows a tenant's name or id should be able to look up that single
//! tenant's row (id, name, database, quotas, counters) without scanning
//! `SHOW TENANTS` and filtering client-side. The same gap exists for
//! `SHOW TENANTS WITH NAME <name>` as a server-side filter.

use nodedb_test_support::pgwire_auth_helpers::{
    assert_readonly_denied, ddl_err, ddl_ok, make_state, make_state_with_catalog, superuser,
};

/// `SHOW TENANT <name>` returns the single tenant identified by name.
/// Uses the catalog-backed harness so the tenant's name is registered.
#[tokio::test]
async fn show_tenant_by_name() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(&state, &su, "SHOW TENANT acme").await;
}

/// `SHOW TENANT <id>` returns the single tenant identified by numeric id.
#[tokio::test]
async fn show_tenant_by_id() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(&state, &su, "SHOW TENANT 42").await;
}

/// `SHOW TENANTS WITH NAME <name>` is the filtered list form — same
/// schema as `SHOW TENANTS`, restricted to rows whose name matches. A
/// query for a name that does not exist must error (or return zero
/// rows via an explicit handler), not silently fall through to the
/// unfiltered `SHOW TENANTS` listing.
#[tokio::test]
async fn show_tenants_with_name_filter_unknown_name_does_not_silently_listall() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    // Regression guard against silent prefix-routing: `SHOW TENANTS WITH
    // NAME nosuch` must not be matched by the `SHOW TENANTS` prefix
    // branch — that would silently drop the filter and return every
    // tenant in the system. The proper handler errors on unknown name.
    let err = ddl_err(&state, &su, "SHOW TENANTS WITH NAME nosuch").await;
    assert!(
        err.contains("not found") || err.contains("unknown") || err.contains("no such"),
        "expected a not-found error for unknown tenant filter, got: {err}"
    );
}

/// Per-tenant introspection by name/id is privileged — readonly users
/// must be denied, matching the gate on `SHOW TENANTS`.
#[tokio::test]
async fn show_tenant_by_name_requires_superuser() {
    let state = make_state();
    assert_readonly_denied(&state, "SHOW TENANT acme").await;
}
