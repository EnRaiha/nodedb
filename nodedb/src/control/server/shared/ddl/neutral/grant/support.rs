// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral `GRANT` / `REVOKE` handlers:
//! the tenant-admin gate, single-tag status construction, role-name parsing,
//! and pgwire-error → [`DdlError`] conversion for the one shared helper
//! (`set_role_parent`) that still lives in the pgwire role family.

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};

/// Build a single-tag status result.
pub(super) fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Require that the identity is superuser or tenant_admin.
///
/// Folded in verbatim from the pgwire `require_tenant_admin` helper: it does
/// NOT emit an audit record on denial and returns SQLSTATE 42501 with the
/// identical message.
pub(super) fn require_tenant_admin(
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(DdlError {
            sqlstate: "42501".to_string(),
            message: format!("permission denied: only superuser or tenant_admin can {action}"),
        })
    }
}

/// Parse a role name string into a `Role`.
///
/// Known roles map to their enum variants; unknown names become `Role::Custom`.
/// `Role::from_str` is `Infallible`, so the `Err` arm is unreachable.
pub(super) fn parse_role(name: &str) -> Role {
    match name.parse() {
        Ok(role) => role,
        Err(e) => match e {},
    }
}

/// Convert a pgwire error from the shared `set_role_parent` helper into a
/// protocol-neutral [`DdlError`], preserving SQLSTATE and message exactly
/// (the same mapping the transitional dispatch wrapper uses).
pub(super) fn map_pg_err(err: pgwire::error::PgWireError) -> DdlError {
    match err {
        pgwire::error::PgWireError::UserError(info) => DdlError {
            sqlstate: info.code.clone(),
            message: info.message.clone(),
        },
        other => DdlError {
            sqlstate: "XX000".to_string(),
            message: other.to_string(),
        },
    }
}
