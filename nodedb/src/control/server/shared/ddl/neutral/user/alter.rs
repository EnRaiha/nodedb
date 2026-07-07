// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER USER` DDL handler — typed dispatch for every
//! `AlterUserOp`.
//!
//! Ported from the pgwire `ddl::user::alter` handler. All non-return logic
//! (self-vs-admin permission gates, per-op `prepare_*` credential mutations,
//! ISO-8601 expiry parsing, session-invalidation reasons, default-database
//! resolution, catalog propose + single-node `log_index == 0` fallback +
//! `install_replicated_user`, and `audit_record`) is preserved verbatim; only
//! the result construction changed from pgwire `Response` / `PgWireError` to
//! [`DdlResult`] / [`DdlError`].

use nodedb_sql::ddl_ast::AlterUserOp;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{parse_role, require_tenant_admin, status};
use super::iso8601::parse_iso8601_to_unix;

/// ALTER USER <name> ... — typed dispatch for all AlterUserOp forms.
pub fn alter_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    username: &str,
    op: &AlterUserOp,
) -> Result<Vec<DdlResult>, DdlError> {
    if username.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: ALTER USER <name> SET PASSWORD '<password>' | SET ROLE <role> | MUST CHANGE PASSWORD | PASSWORD NEVER EXPIRES | PASSWORD EXPIRES ...".to_string(),
        });
    }

    // Users can change their own password; admin required for anything else.
    let is_self = username == identity.username;
    let can_alter = is_self || identity.is_superuser || identity.has_role(&Role::TenantAdmin);

    match op {
        AlterUserOp::SetPassword { password } => {
            if !can_alter {
                return Err(DdlError {
                    sqlstate: "42501".to_string(),
                    message: "permission denied: can only alter your own user, or be superuser/tenant_admin".to_string(),
                });
            }
            if password.is_empty() {
                return Err(DdlError {
                    sqlstate: "42601".to_string(),
                    message: "password must be a non-empty single-quoted string".to_string(),
                });
            }
            let stored = state
                .credentials
                .prepare_user_update(username, Some(password.as_str()), None)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            // Password change — no role/access change; no invalidation.
            propose_and_install(state, stored, None)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("changed password for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::SetRole { role } => {
            if is_self && !identity.is_superuser {
                return Err(DdlError {
                    sqlstate: "42501".to_string(),
                    message: "cannot change your own role".to_string(),
                });
            }
            require_tenant_admin(identity, "change roles")?;
            if role.is_empty() {
                return Err(DdlError {
                    sqlstate: "42601".to_string(),
                    message: "expected role name after SET ROLE".to_string(),
                });
            }
            let parsed_role: Role = parse_role(role);
            let stored = state
                .credentials
                .prepare_user_update(username, None, Some(vec![parsed_role.clone()]))
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            propose_and_install(
                state,
                stored,
                Some(crate::control::security::buses::SessionInvalidationReason::RoleAltered),
            )?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set role '{parsed_role}' for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::MustChangePassword => {
            require_tenant_admin(identity, "set must_change_password")?;
            let stored = state
                .credentials
                .prepare_set_must_change_password(username, true)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            propose_and_install(state, stored, None)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set must_change_password for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::PasswordNeverExpires => {
            require_tenant_admin(identity, "set password expiry")?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at(username, 0)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            propose_and_install(state, stored, None)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD NEVER EXPIRES for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::PasswordExpiresAt { iso8601 } => {
            require_tenant_admin(identity, "set password expiry")?;
            let expires_at = parse_iso8601_to_unix(iso8601).map_err(|e| DdlError {
                sqlstate: "22007".to_string(),
                message: format!("invalid ISO-8601 datetime '{iso8601}': {e}"),
            })?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at(username, expires_at)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            propose_and_install(state, stored, None)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD EXPIRES '{iso8601}' for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::PasswordExpiresInDays { days } => {
            require_tenant_admin(identity, "set password expiry")?;
            if *days == 0 {
                return Err(DdlError {
                    sqlstate: "22003".to_string(),
                    message: "PASSWORD EXPIRES IN requires a positive day count".to_string(),
                });
            }
            let expires_at = crate::control::security::time::now_secs() + (*days as u64) * 86400;
            let stored = state
                .credentials
                .prepare_set_password_expires_at(username, expires_at)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            propose_and_install(state, stored, None)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD EXPIRES IN {days} DAYS for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::SetDefaultDatabase { db_name } => {
            // Users can set their own default database; admin may set for others.
            if !can_alter {
                return Err(DdlError {
                    sqlstate: "42501".to_string(),
                    message: "permission denied: can only alter your own user, or be superuser/tenant_admin".to_string(),
                });
            }
            if db_name.is_empty() {
                return Err(DdlError {
                    sqlstate: "42601".to_string(),
                    message: "syntax: ALTER USER <name> SET DEFAULT DATABASE <db_name>".to_string(),
                });
            }
            // Resolve the database name to an ID via the system catalog.
            let catalog = state.credentials.catalog();
            let db_id = catalog
                .get_database_id_by_name(db_name)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: format!("catalog lookup: {e}"),
                })?
                .ok_or_else(|| DdlError {
                    sqlstate: "42704".to_string(),
                    message: format!("database '{db_name}' does not exist"),
                })?;
            let stored = state
                .credentials
                .prepare_set_default_database(username, db_id.as_u64())
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: e.to_string(),
                })?;
            propose_and_install(state, stored, None)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set default database '{db_name}' for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }
    }
}

/// Propose a `StoredUser` via Raft and install it locally on single-node.
///
/// `invalidation` is passed to `install_replicated_user` for in-process
/// session notification in single-node mode. Cluster-mode notifications
/// arrive via `post_apply::user::put` after Raft commit.
fn propose_and_install(
    state: &SharedState,
    stored: crate::control::security::catalog::StoredUser,
    invalidation: Option<crate::control::security::buses::SessionInvalidationReason>,
) -> Result<(), DdlError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    if log_index == 0 {
        {
            let catalog = state.credentials.catalog();
            catalog.put_user(&stored).map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog write: {e}"),
            })?;
        }
        state
            .credentials
            .install_replicated_user(&stored, invalidation);
    }
    Ok(())
}
