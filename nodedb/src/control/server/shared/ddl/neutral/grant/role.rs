// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `GRANT/REVOKE ROLE x TO/FROM user` handlers.
//!
//! Ported from the pgwire `ddl::grant::role` handlers. All non-return logic
//! (tenant-admin gate, superuser-grant guard, self-superuser-revoke guard,
//! grantee resolution, role-list mutation, `prepare_user_update`, catalog
//! propose + single-node fallback, `install_replicated_user`, the role-to-role
//! `set_role_parent` delegation, and `audit_record`) is preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Reuses the existing `CatalogEntry::PutUser` variant. The mutated role list
//! is built locally from the user's current record, then
//! `CredentialStore::prepare_user_update` clones the `StoredUser` with the new
//! roles and the proposer ships the whole record through raft. Followers'
//! appliers reinstall the updated user via `install_replicated_user` — no
//! separate `Add/RemoveRole` variant needed.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{parse_role, require_tenant_admin, status};

fn current_roles(state: &SharedState, username: &str) -> Result<Vec<Role>, DdlError> {
    state
        .credentials
        .get_user(username)
        .map(|r| r.roles)
        .ok_or_else(|| DdlError {
            sqlstate: "42704".to_string(),
            message: format!("user '{username}' not found"),
        })
}

fn propose_user_with_roles(
    state: &SharedState,
    username: &str,
    new_roles: Vec<Role>,
    invalidation: crate::control::security::buses::SessionInvalidationReason,
) -> Result<(), DdlError> {
    let stored = state
        .credentials
        .prepare_user_update(username, None, Some(new_roles))
        .map_err(|e| DdlError {
            sqlstate: "42704".to_string(),
            message: e.to_string(),
        })?;
    let entry = CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
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
            .install_replicated_user(&stored, Some(invalidation));
    }
    Ok(())
}

/// `GRANT <role>[, ...] TO <grantee>`.
///
/// The grantee is resolved to a user or a custom role. For a user, every
/// listed role is added to the user's role set. For a role, the single
/// listed role becomes the grantee's inheritance parent (role-to-role
/// membership) — the role hierarchy permits one parent, so granting more
/// than one role to a role is rejected.
pub fn grant_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    roles: &[String],
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "grant roles")?;
    if roles.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "GRANT: missing role name".to_string(),
        });
    }

    if state.credentials.get_user(grantee).is_some() {
        grant_roles_to_user(state, identity, roles, grantee)
    } else if state.roles.get_role(grantee).is_some() {
        grant_role_to_role(state, identity, roles, grantee)
    } else {
        Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("grantee '{grantee}' is not a known user or role"),
        })
    }
}

fn grant_roles_to_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    username: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let mut roles = current_roles(state, username)?;
    for name in role_names {
        let role = parse_role(name);
        if matches!(role, Role::Superuser) && !identity.is_superuser {
            return Err(DdlError {
                sqlstate: "42501".to_string(),
                message: "only superuser can grant superuser role".to_string(),
            });
        }
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    propose_user_with_roles(
        state,
        username,
        roles,
        crate::control::security::buses::SessionInvalidationReason::RoleGranted,
    )?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "granted role(s) {} to user '{username}'",
            role_names.join(", ")
        ),
    );

    Ok(status("GRANT"))
}

fn grant_role_to_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    child: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if role_names.len() != 1 {
        return Err(DdlError {
            sqlstate: "0A000".to_string(),
            message: "a role can inherit from only one parent role; grant one role at a time"
                .to_string(),
        });
    }
    let parent = &role_names[0];
    super::super::role::set_role_parent(state, child, Some(parent))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted role '{parent}' to role '{child}'"),
    );

    Ok(status("GRANT"))
}

/// `REVOKE <role>[, ...] FROM <grantee>`.
pub fn revoke_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    roles: &[String],
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "revoke roles")?;
    if roles.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "REVOKE: missing role name".to_string(),
        });
    }

    // Reject self-superuser revocation before resolving the grantee — an
    // admin must not be able to strip their own superuser role even if the
    // grantee name does not resolve to a stored user record.
    if grantee == identity.username
        && roles
            .iter()
            .any(|r| matches!(parse_role(r), Role::Superuser))
    {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message: "cannot revoke your own superuser role".to_string(),
        });
    }

    if state.credentials.get_user(grantee).is_some() {
        revoke_roles_from_user(state, identity, roles, grantee)
    } else if state.roles.get_role(grantee).is_some() {
        revoke_role_from_role(state, identity, roles, grantee)
    } else {
        Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("grantee '{grantee}' is not a known user or role"),
        })
    }
}

fn revoke_roles_from_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    username: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let mut roles = current_roles(state, username)?;
    let revoked: Vec<Role> = role_names.iter().map(|n| parse_role(n)).collect();
    for role in &revoked {
        if !roles.contains(role) {
            return Err(DdlError {
                sqlstate: "42704".to_string(),
                message: format!("user '{username}' does not have role '{role}'"),
            });
        }
    }
    roles.retain(|r| !revoked.contains(r));
    propose_user_with_roles(
        state,
        username,
        roles,
        crate::control::security::buses::SessionInvalidationReason::RoleRevoked,
    )?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "revoked role(s) {} from user '{username}'",
            role_names.join(", ")
        ),
    );

    Ok(status("REVOKE"))
}

fn revoke_role_from_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    child: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if role_names.len() != 1 {
        return Err(DdlError {
            sqlstate: "0A000".to_string(),
            message: "a role inherits from at most one parent role; revoke one role at a time"
                .to_string(),
        });
    }
    let parent = &role_names[0];
    let current_parent = state.roles.get_role(child).and_then(|r| r.parent);
    if current_parent.as_deref() != Some(parent.as_str()) {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("role '{child}' does not inherit from '{parent}'"),
        });
    }
    super::super::role::set_role_parent(state, child, None)?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked role '{parent}' from role '{child}'"),
    );

    Ok(status("REVOKE"))
}
