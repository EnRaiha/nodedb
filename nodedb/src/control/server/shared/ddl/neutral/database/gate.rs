// SPDX-License-Identifier: BUSL-1.1

//! Privilege-gate helpers for the protocol-neutral database DDL handlers.
//!
//! These mirror the pgwire `types::privilege` gates verbatim — same allowed
//! roles, same `AuditEvent::PermissionDenied`-on-denial behaviour, same
//! SQLSTATE (42501, `INSUFFICIENT_PRIVILEGE`), and byte-identical messages —
//! but return [`DdlError`] instead of `PgWireError` so they carry no pgwire
//! types.

use nodedb_types::error::sqlstate;
use nodedb_types::id::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// Require that the identity is a superuser.
///
/// Emits `AuditEvent::PermissionDenied` and returns SQLSTATE 42501 on failure.
///
/// Visibility is widened to the whole `neutral` tree (not just `database`) so
/// the tenant family's `MOVE TENANT` handler — which uses this exact gate
/// verbatim from the pgwire `types::privilege::require_superuser` — can reuse
/// it instead of duplicating the audit-on-denial logic.
pub(in crate::control::server::shared::ddl::neutral) fn require_superuser(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_id: Option<DatabaseId>,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        Ok(())
    } else {
        audit_permission_denied(state, identity, db_id, action);
        Err(ddl_err(
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!("permission denied: {action} requires superuser"),
        ))
    }
}

/// Required role: `ClusterAdmin` or `Superuser`.
///
/// On failure, emits `AuditEvent::PermissionDenied` and returns SQLSTATE 42501.
pub(super) fn require_cluster_admin(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_id: Option<DatabaseId>,
    action: &str,
) -> Result<(), DdlError> {
    if identity.has_cluster_admin() {
        Ok(())
    } else {
        audit_permission_denied(state, identity, db_id, action);
        Err(ddl_err(
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!("permission denied: {action} requires cluster_admin or superuser"),
        ))
    }
}

/// Required role: `DatabaseOwner(db)` or `Superuser`.
///
/// On failure, emits `AuditEvent::PermissionDenied` and returns SQLSTATE 42501.
pub(super) fn require_database_owner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_id: DatabaseId,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_database_owner(db_id) {
        Ok(())
    } else {
        audit_permission_denied(state, identity, Some(db_id), action);
        Err(ddl_err(
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!(
                "permission denied: {action} requires database_owner of this database or superuser"
            ),
        ))
    }
}

/// Required role: `DatabaseOwner(db)`, `ClusterAdmin`, or `Superuser`.
///
/// On failure, emits `AuditEvent::PermissionDenied` and returns SQLSTATE 42501.
pub(super) fn require_database_owner_or_higher(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_id: DatabaseId,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser || identity.has_cluster_admin() || identity.is_database_owner(db_id) {
        Ok(())
    } else {
        audit_permission_denied(state, identity, Some(db_id), action);
        Err(ddl_err(
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!(
                "permission denied: {action} requires database_owner of this database, \
                 cluster_admin, or superuser"
            ),
        ))
    }
}

/// Require that the identity is superuser or tenant_admin.
///
/// Mirrors pgwire `require_tenant_admin`: this gate does NOT emit an audit
/// record on denial. Used by the read-only database SHOW handlers, and reused
/// (visibility widened to the `neutral` tree) by the tenant family's
/// `SHOW TENANT QUOTA|USAGE FOR ... IN DATABASE ...` and
/// `ALTER TENANT ... IN DATABASE ... SET QUOTA` handlers, which used the
/// identical pgwire gate.
pub(in crate::control::server::shared::ddl::neutral) fn require_tenant_admin(
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    use crate::control::security::identity::Role;
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(ddl_err(
            sqlstate::INSUFFICIENT_PRIVILEGE,
            format!("permission denied: only superuser or tenant_admin can {action}"),
        ))
    }
}

/// Centralized denial-audit emitter used by all `require_*` helpers that
/// surface a database scope.
fn audit_permission_denied(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_id: Option<DatabaseId>,
    action: &str,
) {
    state.audit_record_with_db(
        AuditEvent::PermissionDenied,
        Some(identity.tenant_id),
        db_id,
        &identity.username,
        action,
    );
}
