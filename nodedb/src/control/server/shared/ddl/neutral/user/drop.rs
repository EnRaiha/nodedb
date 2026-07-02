// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP USER` DDL handler.
//!
//! Ported from the pgwire `ddl::user::drop` handler. All non-return logic
//! (tenant-admin gate, self-drop guard, IF EXISTS short-circuit, owner-tenant
//! lookup, `DropUser` catalog propose + single-node `log_index == 0` fallback,
//! collection-ownership reassignment to the tenant admin, and `audit_record`)
//! is preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to [`DdlResult`] / [`DdlError`].

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status, strip_if_exists};

/// DROP USER [IF EXISTS] <name>
pub fn drop_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop users")?;

    let (if_exists, parts) = strip_if_exists(parts, 2);

    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: DROP USER [IF EXISTS] <name>".to_string(),
        });
    }

    let username = parts[2];

    if username == identity.username {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message: "cannot drop your own user".to_string(),
        });
    }

    // Look up user's tenant before dropping (for ownership reassignment).
    let user_tenant = state
        .credentials
        .get_user(username)
        .map(|u| u.tenant_id)
        .unwrap_or(identity.tenant_id);

    // Pre-check existence so a DROP USER on a missing user is a
    // clean error that doesn't touch raft.
    let exists_before = state.credentials.get_user(username).is_some();
    if !exists_before {
        // `IF EXISTS`: dropping a missing user is a no-op success.
        if if_exists {
            return Ok(status("DROP USER"));
        }
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("user '{username}' does not exist"),
        });
    }

    // `DropUser` fully removes the identity record on every node —
    // in-memory cache and redb catalog — so the username is freed
    // for reuse. A soft-delete tombstone would block a later
    // `CREATE USER` of the same name.
    let entry = crate::control::catalog_entry::CatalogEntry::DropUser {
        username: username.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    let dropped = if log_index == 0 {
        // Single-node fallback.
        state
            .credentials
            .drop_user(username)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: e.to_string(),
            })?
    } else {
        // Cluster mode: the raft entry committed, so the
        // drop WILL be applied on every node. The
        // `post_apply` hook that updates the local in-memory
        // cache runs in a spawned tokio task and may not be
        // visible by the time this function returns — trust the
        // log index rather than re-reading the cache.
        true
    };

    if dropped {
        // Reassign owned collections to the tenant_admin of the
        // user's tenant. Mutating the parent `StoredCollection`
        // and re-proposing `PutCollection` is the durable path —
        // a bare `PutOwner` would be silently overwritten the
        // next time anyone re-proposed the parent.
        let admin_name = format!("{}_admin", user_tenant.as_u64());
        let grants = state.permissions.grants_for(&format!("user:{username}"));
        if let Some(catalog) = state.credentials.catalog() {
            for grant in &grants {
                let Some(owner_obj) = extract_collection_from_target(&grant.target) else {
                    continue;
                };
                if state
                    .permissions
                    .get_owner("collection", user_tenant, owner_obj)
                    .as_deref()
                    != Some(username)
                {
                    continue;
                }
                let mut stored = match catalog.get_collection(
                    DatabaseId::DEFAULT,
                    user_tenant.as_u64(),
                    owner_obj,
                ) {
                    Ok(Some(c)) => c,
                    _ => continue,
                };
                stored.owner = admin_name.clone();
                let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(
                    stored.clone(),
                ));
                if let Ok(idx) =
                    crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
                    && idx == 0
                {
                    let _ = catalog.put_collection(DatabaseId::DEFAULT, &stored);
                    state.permissions.install_replicated_owner(
                        &crate::control::security::catalog::StoredOwner {
                            object_type: "collection".into(),
                            object_name: stored.name.clone(),
                            tenant_id: stored.tenant_id,
                            owner_username: stored.owner.clone(),
                        },
                    );
                }
            }
        }

        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped user '{username}' (ownership reassigned to '{admin_name}')"),
        );
        Ok(status("DROP USER"))
    } else {
        Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("user '{username}' does not exist"),
        })
    }
}

/// Extract collection name from a permission target like "collection:1:users".
fn extract_collection_from_target(target: &str) -> Option<&str> {
    let parts: Vec<&str> = target.splitn(3, ':').collect();
    if parts.len() == 3 && parts[0] == "collection" {
        Some(parts[2])
    } else {
        None
    }
}
