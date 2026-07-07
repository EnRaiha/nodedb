// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE USER` DDL handler.
//!
//! Ported from the pgwire `ddl::user::create` handler. All non-return logic
//! (tenant-admin gate, IF NOT EXISTS short-circuit, tenant-selector
//! resolution, `prepare_user`, catalog propose + single-node `log_index == 0`
//! fallback, cluster-mode `get_user` truncation retry, `install_replicated_user`,
//! and `audit_record`) is preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to [`DdlResult`] / [`DdlError`].

use nodedb_sql::ddl_ast::TenantSelector;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{parse_role, require_tenant_admin, status};

/// Resolve a [`TenantSelector`] to a numeric [`TenantId`]. Numeric ids pass
/// through; names are resolved against the redb catalog.
fn resolve_tenant_selector(
    state: &SharedState,
    selector: &TenantSelector,
) -> Result<TenantId, DdlError> {
    match selector {
        TenantSelector::Id(id) => Ok(TenantId::new(*id)),
        TenantSelector::Name(name) => {
            let catalog = state.credentials.catalog();
            let stored = catalog
                .find_tenant_by_name(name)
                .map_err(|e| DdlError {
                    sqlstate: "XX000".to_string(),
                    message: format!("catalog read: {e}"),
                })?
                .ok_or_else(|| DdlError {
                    sqlstate: "42704".to_string(),
                    message: format!("tenant '{name}' not found"),
                })?;
            Ok(TenantId::new(stored.tenant_id))
        }
    }
}

/// CREATE USER [IF NOT EXISTS] <name> WITH PASSWORD '<password>' [ROLE <role>]
/// [TENANT <id> | TENANT '<name>']
pub fn create_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    username: &str,
    password: &str,
    role_name: Option<&str>,
    tenant: Option<&TenantSelector>,
    if_not_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create users")?;

    if username.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message:
                "syntax: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]"
                    .to_string(),
        });
    }

    // `IF NOT EXISTS`: re-creating an existing user is a no-op success.
    if if_not_exists && state.credentials.get_user(username).is_some() {
        return Ok(status("CREATE USER"));
    }

    if password.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "password must be a single-quoted string".to_string(),
        });
    }

    let role = role_name.map(parse_role).unwrap_or(Role::ReadWrite);
    let tenant_id = if let Some(selector) = tenant {
        if !identity.is_superuser {
            return Err(DdlError {
                sqlstate: "42501".to_string(),
                message: "only superuser can assign tenants".to_string(),
            });
        }
        resolve_tenant_selector(state, selector)?
    } else {
        identity.tenant_id
    };

    // Build the full `StoredUser` locally (hash + salt + user_id).
    // Followers cannot reproduce the random salt, so this step
    // MUST happen on the proposer node. The computed record is
    // then replicated verbatim.
    let stored = state
        .credentials
        .prepare_user(username, password, tenant_id, vec![role])
        .map_err(|e| DdlError {
            sqlstate: "42710".to_string(),
            message: e.to_string(),
        })?;

    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    if log_index == 0 {
        // Single-node / no-cluster fallback: install into the
        // in-memory cache so subsequent reads see the user.
        // Persist to redb when a catalog is wired up — the
        // catalog write is best-effort durability, not a gate
        // on the cache update. Test fixtures (and any future
        // fully-in-memory deployment) can run without a redb
        // catalog and still get correct read-after-write.
        {
            let catalog = state.credentials.catalog();
            catalog.put_user(&stored).map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog write: {e}"),
            })?;
        }
        // CREATE USER: no open sessions exist for a brand-new user.
        state.credentials.install_replicated_user(&stored, None);
    } else {
        // Cluster mode: `propose_catalog_entry` waits for the
        // entry to be applied on THIS node, which runs the
        // synchronous post_apply (`install_replicated_user`)
        // inline BEFORE the applied-index watermark bumps. So if
        // our entry really committed, `get_user` must see it now.
        //
        // If `get_user` returns None, the Raft log entry at the
        // index our leader assigned has been truncated and
        // overwritten with a noop from a new leader term (a known
        // Raft subtlety: `propose` returns the assigned log index
        // without waiting for commit; if leadership changes
        // before the quorum ack, the entry is dropped). Return a
        // retryable error so `exec_ddl_on_any_leader` re-proposes
        // on the next attempt against whoever is now leader.
        if state.credentials.get_user(username).is_none() {
            return Err(DdlError {
                sqlstate: "40001".to_string(),
                message: "transient: metadata entry truncated by leader change, retry".to_string(),
            });
        }
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created user '{username}' in tenant {tenant_id}"),
    );

    Ok(status("CREATE USER"))
}
