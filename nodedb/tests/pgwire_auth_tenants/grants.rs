// SPDX-License-Identifier: BUSL-1.1

//! `GRANT/REVOKE <perm> ON TENANT <name>` end-to-end.
//!
//! Tenant-scoped permission grants are advertised by the BACKUP TENANT
//! surface: a permission granted on `tenant:<id>` applies to every
//! collection in the tenant and is the non-superuser path to running
//! `BACKUP TENANT`. The grammar (`ON TENANT <name>` arm), the
//! `Permission::Backup` variant, the `tenant_target` builder, and the
//! `check_tenant` enforcement at the COPY entry point are all in scope.
//! These tests lock the round-trip: GRANT stores on `tenant:<id>`,
//! REVOKE removes it, the parser accepts `BACKUP` as a permission name
//! (no "unknown permission" regression), and the resulting grant
//! actually authorizes `check_tenant`.

use crate::common::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state, make_state_with_catalog, superuser,
};
use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Permission, Role};
use nodedb::control::security::permission::tenant_target;
use nodedb::types::TenantId;

fn ops_user_in_tenant(tenant_id: u64) -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        200,
        "ops_user",
        TenantId::new(tenant_id),
        AuthMethod::Trust,
        vec![Role::ReadOnly],
        None,
        AuthenticatedIdentity::default_database_set(false),
    )
}

/// `GRANT BACKUP ON TENANT <name> TO <user>` stores a grant whose target
/// is the canonical `tenant:<id>` key built from the resolved tenant id,
/// not a collection-shaped fallback target.
#[tokio::test]
async fn grant_backup_on_tenant_stores_tenant_target() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(&state, &su, "CREATE USER ops_user WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "GRANT BACKUP ON TENANT acme TO ops_user").await;

    let grants = state.permissions.snapshot_grants();
    let matched: Vec<_> = grants
        .iter()
        .filter(|g| {
            g.grantee == "user:ops_user"
                && g.permission == Permission::Backup
                && g.target == tenant_target(TenantId::new(42))
        })
        .collect();
    assert_eq!(
        matched.len(),
        1,
        "GRANT BACKUP ON TENANT must store a single tenant-scoped grant; grants = {grants:?}"
    );
    // Regression guard: must NOT fall through to a collection-shaped
    // target named `TENANT` — that was the pre-fix behaviour where the
    // TENANT keyword was consumed as an object name.
    assert!(
        !grants
            .iter()
            .any(|g| g.target.starts_with("collection:") && g.target.ends_with(":acme")),
        "TENANT keyword leaked into a collection-shaped target: {grants:?}"
    );
}

/// `GRANT BACKUP ON TENANT <id>` accepts a numeric id in place of a name
/// and stores the same canonical `tenant:<id>` target — the resolver
/// must treat an all-digit token as a literal tenant id.
#[tokio::test]
async fn grant_backup_on_tenant_by_numeric_id() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER ops_user WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "GRANT BACKUP ON TENANT 42 TO ops_user").await;

    let grants = state.permissions.snapshot_grants();
    assert!(
        grants.iter().any(|g| g.grantee == "user:ops_user"
            && g.permission == Permission::Backup
            && g.target == tenant_target(TenantId::new(42))),
        "numeric tenant id must resolve to tenant:42; grants = {grants:?}"
    );
}

/// `REVOKE BACKUP ON TENANT <name> FROM <user>` removes the matching
/// tenant-scoped grant — `REVOKE` must traverse the same `TENANT` arm
/// the matching `GRANT` did, not silently no-op on a non-existent
/// collection target.
#[tokio::test]
async fn revoke_backup_on_tenant_removes_grant() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(&state, &su, "CREATE USER ops_user WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "GRANT BACKUP ON TENANT acme TO ops_user").await;
    ddl_ok(&state, &su, "REVOKE BACKUP ON TENANT acme FROM ops_user").await;

    let grants = state.permissions.snapshot_grants();
    assert!(
        !grants
            .iter()
            .any(|g| g.grantee == "user:ops_user" && g.permission == Permission::Backup),
        "REVOKE must remove the tenant-scoped Backup grant; grants = {grants:?}"
    );
}

/// `GRANT BACKUP ON TENANT <name>` must NOT report `unknown permission:
/// BACKUP`. The pre-fix grammar fell through the `TENANT` arm, classified
/// `BACKUP` as the permission and `TENANT` as the object name, then
/// rejected `BACKUP` via `parse_permission`. The regression guard is the
/// explicit error-string check — a clean success is the spec, but if the
/// statement ever errors again, the failure mode must not be the old one.
#[tokio::test]
async fn grant_backup_on_tenant_not_unknown_permission() {
    let state = make_state();
    let su = superuser();
    // Use numeric id so the test does not depend on the catalog. The
    // assertion is about the permission/object classification, not name
    // resolution.
    ddl_ok(&state, &su, "CREATE USER ops_user WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "GRANT BACKUP ON TENANT 42 TO ops_user").await;
}

/// `GRANT BACKUP ON TENANT TO <user>` — no tenant name between `TENANT`
/// and the pivot — must produce a clear parse error, not silently
/// consume `TO` as the tenant name.
#[tokio::test]
async fn grant_on_tenant_missing_name_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "GRANT BACKUP ON TENANT TO ops_user").await;
    assert!(
        err.to_lowercase().contains("tenant"),
        "missing-name parse error must name the TENANT clause, got: {err}"
    );
    // Regression guard against the silent-consume bug: `TO` must not
    // have been captured as the tenant id/name.
    let grants = state.permissions.snapshot_grants();
    assert!(
        grants.is_empty(),
        "a rejected GRANT must leave the grant store untouched; grants = {grants:?}"
    );
}

/// After `GRANT BACKUP ON TENANT <id> TO ops_user`, `check_tenant` must
/// authorize that user against `Permission::Backup` on the granted
/// tenant — and only the granted tenant. This locks the
/// `tenant_target` ↔ `check_tenant` round-trip end-to-end.
#[tokio::test]
async fn grant_backup_on_tenant_authorizes_check_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER ops_user WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "GRANT BACKUP ON TENANT 42 TO ops_user").await;

    let ops = ops_user_in_tenant(42);
    let allowed = state.permissions.check_tenant(
        &ops,
        Permission::Backup,
        TenantId::new(42),
        &state.roles,
        &NoopAuditEmitter,
    );
    assert!(
        allowed,
        "GRANT BACKUP ON TENANT 42 must authorize ops_user for Backup on tenant 42"
    );

    // Negative: the grant is scoped to tenant 42 — Backup on tenant 99
    // must still be denied.
    let denied = state.permissions.check_tenant(
        &ops,
        Permission::Backup,
        TenantId::new(99),
        &state.roles,
        &NoopAuditEmitter,
    );
    assert!(
        !denied,
        "tenant-scoped grant on 42 must not authorize Backup on tenant 99"
    );
}

/// Without a tenant-scoped Backup grant, a non-superuser must be denied.
/// Pairs with the positive case to prove `check_tenant` actually
/// consults the grant store, not just role membership.
#[tokio::test]
async fn check_tenant_without_grant_is_denied() {
    let state = make_state();
    let ops = ops_user_in_tenant(42);
    let allowed = state.permissions.check_tenant(
        &ops,
        Permission::Backup,
        TenantId::new(42),
        &state.roles,
        &NoopAuditEmitter,
    );
    assert!(
        !allowed,
        "an ungranted non-superuser must be denied Backup on the tenant"
    );
}

/// A tenant_admin in tenant A trying to `GRANT BACKUP ON TENANT B` must
/// be denied — managing permissions across tenant boundaries requires
/// superuser. This locks the `resolve_target` cross-tenant check.
#[tokio::test]
async fn grant_on_tenant_cross_tenant_requires_superuser() {
    let state = make_state();
    let tenant_a_admin = AuthenticatedIdentity::new_regular(
        300,
        "ta",
        TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::TenantAdmin],
        None,
        AuthenticatedIdentity::default_database_set(false),
    );

    let err = ddl_err(
        &state,
        &tenant_a_admin,
        "GRANT BACKUP ON TENANT 2 TO ops_user",
    )
    .await;
    assert!(
        err.contains("superuser"),
        "cross-tenant grant must require superuser, got: {err}"
    );

    let grants = state.permissions.snapshot_grants();
    assert!(
        grants.is_empty(),
        "a rejected cross-tenant grant must not mutate the grant store; grants = {grants:?}"
    );
}
