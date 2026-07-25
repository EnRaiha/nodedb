// SPDX-License-Identifier: BUSL-1.1

//! GRANT / REVOKE role over the pgwire DDL path, plus the readonly guard
//! that covers the same surface.

mod common;

use common::pgwire_auth_helpers::{
    assert_readonly_denied, ddl_err, ddl_ok, make_state, make_state_with_catalog, superuser,
};
use nodedb::control::security::audit::AuditEvent;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Permission, Role};
use nodedb::types::TenantId;

#[tokio::test]
async fn grant_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER grace WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE readwrite TO grace").await;

    let user = state.credentials.get_user("grace").unwrap();
    assert!(user.roles.contains(&Role::ReadOnly));
    assert!(user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn revoke_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER heidi WITH PASSWORD 'pass' ROLE readwrite",
    )
    .await;
    ddl_ok(&state, &su, "REVOKE ROLE readwrite FROM heidi").await;

    let user = state.credentials.get_user("heidi").unwrap();
    assert!(!user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn grant_superuser_requires_superuser() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER ivan WITH PASSWORD 'pass'").await;

    let admin = AuthenticatedIdentity::new_regular(
        50,
        "ta",
        TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::TenantAdmin],
        None,
        AuthenticatedIdentity::default_database_set(false),
    );
    let err = ddl_err(&state, &admin, "GRANT ROLE superuser TO ivan").await;
    assert!(err.contains("only superuser"), "{err}");
}

#[tokio::test]
async fn revoke_own_superuser_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "REVOKE ROLE superuser FROM nodedb").await;
    assert!(err.contains("cannot revoke your own superuser"), "{err}");
}

#[tokio::test]
async fn readonly_cannot_grant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER target WITH PASSWORD 'pass'").await;

    assert_readonly_denied(&state, "GRANT ROLE superuser TO target").await;
}

// ── Role-membership grants without the `ROLE` keyword ────────────────
//
// SQL-standard role membership is `GRANT <role> TO <grantee>` — no
// `ROLE` keyword. The disambiguator from an object-permission grant is
// the absence of the `ON` clause, not the presence of `ROLE`. A `GRANT`
// with no `ON` clause is a role grant; with an `ON` clause it is an
// object-permission grant.

#[tokio::test]
async fn grant_builtin_role_without_role_keyword() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER eman WITH PASSWORD 'pass' ROLE readwrite",
    )
    .await;

    // Standard syntax — no `ROLE` keyword, no `ON` clause.
    ddl_ok(&state, &su, "GRANT tenant_admin TO eman").await;

    let user = state.credentials.get_user("eman").unwrap();
    assert!(
        user.roles.contains(&Role::TenantAdmin),
        "GRANT <role> TO <user> must add the role; roles = {:?}",
        user.roles
    );
}

#[tokio::test]
async fn grant_custom_role_without_role_keyword() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE mae8_admin").await;
    ddl_ok(&state, &su, "CREATE USER xyfer WITH PASSWORD 'pass'").await;

    ddl_ok(&state, &su, "GRANT mae8_admin TO xyfer").await;

    let user = state.credentials.get_user("xyfer").unwrap();
    assert!(
        user.roles.contains(&Role::Custom("mae8_admin".into())),
        "GRANT <custom_role> TO <user> must add the custom role; roles = {:?}",
        user.roles
    );
}

#[tokio::test]
async fn revoke_builtin_role_without_role_keyword() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER demoter WITH PASSWORD 'pass' ROLE tenant_admin",
    )
    .await;

    ddl_ok(&state, &su, "REVOKE tenant_admin FROM demoter").await;

    let user = state.credentials.get_user("demoter").unwrap();
    assert!(
        !user.roles.contains(&Role::TenantAdmin),
        "REVOKE <role> FROM <user> must remove the role; roles = {:?}",
        user.roles
    );
}

#[tokio::test]
async fn revoke_custom_role_without_role_keyword() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE mae8_reader").await;
    ddl_ok(
        &state,
        &su,
        "CREATE USER inspector WITH PASSWORD 'pass' ROLE mae8_reader",
    )
    .await;

    ddl_ok(&state, &su, "REVOKE mae8_reader FROM inspector").await;

    let user = state.credentials.get_user("inspector").unwrap();
    assert!(
        !user.roles.contains(&Role::Custom("mae8_reader".into())),
        "REVOKE <custom_role> FROM <user> must remove the custom role; roles = {:?}",
        user.roles
    );
}

#[tokio::test]
async fn grant_role_name_aliasing_permission_does_not_misroute() {
    // `monitor` is both a built-in role and a permission alias. A `GRANT
    // monitor TO eman` with no `ON` clause is a role grant — it must NOT
    // silently succeed as an object-permission grant on a phantom
    // collection named after the grantee.
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER eman WITH PASSWORD 'pass'").await;

    ddl_ok(&state, &su, "GRANT monitor TO eman").await;

    let user = state.credentials.get_user("eman").unwrap();
    assert!(
        user.roles.contains(&Role::Monitor),
        "GRANT monitor TO <user> must add the Monitor role; roles = {:?}",
        user.roles
    );
    // Regression guard against the silent misroute: no object-permission
    // grant may exist — the statement had no `ON` clause.
    assert!(
        state.permissions.snapshot_grants().is_empty(),
        "GRANT monitor TO <user> must not create an object-permission \
         grant; grants = {:?}",
        state.permissions.snapshot_grants()
    );
}

#[tokio::test]
async fn grant_comma_separated_roles() {
    // SQL-standard multi-role grant.
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER multi WITH PASSWORD 'pass'").await;

    ddl_ok(&state, &su, "GRANT readonly, readwrite TO multi").await;

    let user = state.credentials.get_user("multi").unwrap();
    assert!(
        user.roles.contains(&Role::ReadOnly) && user.roles.contains(&Role::ReadWrite),
        "GRANT a, b TO <user> must add every listed role; roles = {:?}",
        user.roles
    );
}

#[tokio::test]
async fn grant_comma_separated_permissions() {
    // Advertised in docs/security/rbac.md: `GRANT INSERT, UPDATE ON orders ...`.
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER analyst WITH PASSWORD 'pass'").await;

    ddl_ok(&state, &su, "GRANT SELECT, INSERT ON orders TO analyst").await;

    let perms: Vec<Permission> = state
        .permissions
        .snapshot_grants()
        .into_iter()
        .filter(|g| g.grantee == "user:analyst")
        .map(|g| g.permission)
        .collect();
    assert!(
        perms.contains(&Permission::Read) && perms.contains(&Permission::Write),
        "GRANT a, b ON <obj> TO <grantee> must grant every listed \
         permission; granted = {perms:?}"
    );
}

#[tokio::test]
async fn grant_role_to_role_membership() {
    // `GRANT <parent_role> TO <child_role>` makes the child inherit the
    // parent — the role-hierarchy form of role membership.
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE mae8_ingester").await;
    ddl_ok(&state, &su, "CREATE ROLE mae8_contributor").await;

    ddl_ok(&state, &su, "GRANT mae8_ingester TO mae8_contributor").await;

    let child = state.roles.get_role("mae8_contributor").unwrap();
    assert_eq!(
        child.parent.as_deref(),
        Some("mae8_ingester"),
        "GRANT <role> TO <role> must establish role inheritance"
    );
}

#[tokio::test]
async fn grant_multiple_roles_to_role_rejected() {
    // The role hierarchy permits a single parent, so granting more than one
    // role to a role must fail with a clear error rather than silently
    // dropping all but one.
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE mae8_ingester").await;
    ddl_ok(&state, &su, "CREATE ROLE mae8_dreamer").await;
    ddl_ok(&state, &su, "CREATE ROLE mae8_contributor").await;

    let err = ddl_err(
        &state,
        &su,
        "GRANT mae8_ingester, mae8_dreamer TO mae8_contributor",
    )
    .await;
    assert!(
        err.contains("only one parent"),
        "expected single-parent rejection, got: {err}"
    );
}

#[tokio::test]
async fn grant_execute_on_procedure() {
    // `GRANT EXECUTE ON PROCEDURE ...` is advertised in docs/security/rbac.md.
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER engineer WITH PASSWORD 'pass'").await;

    ddl_ok(
        &state,
        &su,
        "GRANT EXECUTE ON PROCEDURE transfer_funds TO engineer",
    )
    .await;

    let grants = state.permissions.snapshot_grants();
    assert!(
        grants.iter().any(|g| g.grantee == "user:engineer"
            && g.permission == Permission::Execute
            && g.target.starts_with("procedure:")
            && g.target.ends_with(":transfer_funds")),
        "GRANT EXECUTE ON PROCEDURE must store a procedure-targeted grant; \
         grants = {grants:?}"
    );
}

#[tokio::test]
async fn grant_role_to_role_cycle_rejected() {
    // base ← mid (mid inherits base). Granting mid to base would close the
    // loop base → mid → base — it must be rejected at write time, not left
    // to surface as a depth error when the chain is later resolved.
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE base").await;
    ddl_ok(&state, &su, "CREATE ROLE mid").await;
    ddl_ok(&state, &su, "GRANT base TO mid").await;

    let err = ddl_err(&state, &su, "GRANT mid TO base").await;
    assert!(
        err.to_lowercase().contains("cycle"),
        "expected an inheritance-cycle rejection, got: {err}"
    );

    // The rejected grant must not have mutated base's parent.
    assert!(
        state
            .roles
            .get_role("base")
            .and_then(|r| r.parent)
            .is_none(),
        "a rejected role-to-role grant must leave the role unchanged"
    );
}

#[tokio::test]
async fn grant_role_to_itself_rejected() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE solo").await;

    let err = ddl_err(&state, &su, "GRANT solo TO solo").await;
    assert!(
        err.to_lowercase().contains("cycle"),
        "a role must not be able to inherit from itself, got: {err}"
    );
}

/// `CREATE ROLE IF NOT EXISTS <name>` creates a role named `<name>`, not
/// one named after the `IF` clause keyword.
#[tokio::test]
async fn create_role_if_not_exists_names_real_role() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE IF NOT EXISTS auditor").await;

    let log = state.audit.lock().unwrap();
    let details: Vec<&String> = log
        .query_by_event(&AuditEvent::PrivilegeChange)
        .iter()
        .map(|e| &e.detail)
        .collect();
    assert!(
        details.iter().any(|d| d.contains("created role 'auditor'")),
        "{details:?}"
    );
    // Regression guard: the `IF NOT EXISTS` keywords must never leak
    // into the role name.
    assert!(
        !details.iter().any(|d| d.contains("created role 'IF'")),
        "clause keyword used as role name: {details:?}"
    );
}

/// `DROP ROLE IF EXISTS <name>` on a role that does not exist is a no-op
/// success, not an error.
#[tokio::test]
async fn drop_role_if_exists_missing_is_noop() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "DROP ROLE IF EXISTS ghost").await;
}

/// `DROP ROLE IF EXISTS <name>` on an existing role actually drops it —
/// the `IF EXISTS` clause must not turn the statement into a total no-op.
#[tokio::test]
async fn drop_role_if_exists_existing_drops() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE auditor").await;
    ddl_ok(&state, &su, "DROP ROLE IF EXISTS auditor").await;

    let log = state.audit.lock().unwrap();
    let details: Vec<&String> = log
        .query_by_event(&AuditEvent::PrivilegeChange)
        .iter()
        .map(|e| &e.detail)
        .collect();
    assert!(
        details.iter().any(|d| d.contains("dropped role 'auditor'")),
        "{details:?}"
    );
}

/// `ALTER ROLE <name> <unknown>` — the parser currently has a catch-all that
/// silently rewrites any unrecognized sub-command into `AlterRoleOp::SetInherit`
/// with an empty parent. Same systemic flaw as the ALTER USER fall-through:
/// unknown syntax must produce a clear parser error naming the actual input,
/// not be silently rerouted into a default variant.
#[tokio::test]
async fn alter_role_unknown_subcommand_rejected_cleanly() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE ROLE auditor").await;

    let err = ddl_err(&state, &su, "ALTER ROLE auditor FROBNICATE foo").await;

    assert!(
        err.to_uppercase().contains("FROBNICATE"),
        "error must name the unrecognized token, not silently reroute to SET INHERIT: {err}"
    );
}
