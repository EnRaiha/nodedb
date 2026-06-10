// SPDX-License-Identifier: BUSL-1.1

//! `DROP TENANT [IF EXISTS] <id|name>` handler. Migrated to
//! `CatalogEntry::DeleteTenant` in phase 1k.6.
//!
//! Accepts either a numeric tenant id or a tenant name (single-quoted
//! optional), parallel to the `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>` paths.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::sqlstate_error;
use super::super::parse_utils::strip_if_exists;
use super::tenant_exists;

pub fn drop_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can drop tenants",
        ));
    }

    let (if_exists, parts) = strip_if_exists(parts, 2);

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP TENANT [IF EXISTS] <id|name>",
        ));
    }

    // Accept either a numeric id or a tenant name; mirror the existing
    // CREATE TENANT name-resolution path. A name that matches no tenant yields
    // `None` here; an unknown numeric id resolves to a candidate that the
    // existence gate below rejects, so both forms behave identically.
    let tenant_id = match super::resolve_tenant_ref(state, parts[2])? {
        Some(tid) => tid,
        None => {
            // Name token did not resolve to any tenant.
            if if_exists {
                return Ok(vec![Response::Execution(Tag::new("DROP TENANT"))]);
            }
            return Err(sqlstate_error(
                "42704",
                &format!("tenant '{}' does not exist", parts[2]),
            ));
        }
    };
    let tid = tenant_id.as_u64();

    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot drop system tenant (0)"));
    }

    // Existence gate, uniform across numeric ids and resolved names: an unknown
    // tenant is a no-op under `IF EXISTS`, otherwise `42704` — never a silent
    // delete proposal for a tenant that does not exist.
    if !tenant_exists(state, tenant_id)? {
        if if_exists {
            return Ok(vec![Response::Execution(Tag::new("DROP TENANT"))]);
        }
        return Err(sqlstate_error(
            "42704",
            &format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    // Reconcile the tenant's users before removing the tenant row.
    //
    // `SHOW TENANTS` derives its row set from the union of catalog
    // tenants and every user's `tenant_id`, so any user left pointing
    // at this tenant resurrects it as a ghost row (retained id, empty
    // name) after the catalog row is gone. To keep `DROP TENANT`
    // consistent with `DROP USER` (hard-delete, disappears from
    // `SHOW`), the tenant's users must be reconciled here:
    //
    //   * the lifecycle-owned `<name>_admin` auto-provisioned by
    //     `CREATE TENANT` is dropped as part of the tenant lifecycle;
    //   * any other user is real and operator-owned — refuse the drop
    //     (`42501`) and name them, so nobody is silently hard-deleted.
    reconcile_tenant_users(state, identity, tenant_id)?;

    let entry = CatalogEntry::DeleteTenant { tenant_id: tid };
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .delete_tenant(tid)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        let mut tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        tenants.remove_quota(tenant_id);
    }

    state.audit_record(
        AuditEvent::TenantDeleted,
        Some(tenant_id),
        &identity.username,
        &format!("dropped tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP TENANT"))])
}

/// Reconcile the users that belong to `tenant_id` before its catalog
/// row is removed, so the tenant cannot survive as a ghost in
/// `SHOW TENANTS` (which unions catalog tenants with every user's
/// `tenant_id`).
///
/// The lifecycle-owned `<name>_admin` auto-provisioned by
/// `CREATE TENANT` is hard-dropped through the canonical `DROP USER`
/// path. Any other user is operator-owned: the drop is refused with
/// `42501` and the remaining users are named, so no real account is
/// ever silently hard-deleted by a tenant drop.
fn reconcile_tenant_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
) -> PgWireResult<()> {
    // Tenant identity — its name, hence the `<name>_admin` of the
    // lifecycle-owned admin — lives only in the catalog. Without a
    // catalog there is no persisted `StoredTenant` to surface as a
    // ghost in `SHOW TENANTS`' catalog union, and no name to
    // reconstruct the admin from, so there is nothing to reconcile.
    let Some(tenant_name) = state
        .credentials
        .catalog()
        .as_ref()
        .and_then(|cat| cat.load_all_tenants().ok())
        .and_then(|all| {
            all.into_iter()
                .find(|t| t.tenant_id == tenant_id.as_u64())
                .map(|t| t.name)
        })
    else {
        return Ok(());
    };
    let admin_username = super::create::default_admin_username(&tenant_name);

    // The same active-user set `SHOW TENANTS` unions over — reconciling
    // exactly this set is what clears the ghost.
    let members: Vec<String> = state
        .credentials
        .list_user_details()
        .into_iter()
        .filter(|u| u.tenant_id == tenant_id)
        .map(|u| u.username)
        .collect();

    let mut lifecycle_admin = None;
    let mut others = Vec::new();
    for username in members {
        if username == admin_username {
            lifecycle_admin = Some(username);
        } else {
            others.push(username);
        }
    }

    if !others.is_empty() {
        others.sort();
        return Err(sqlstate_error(
            "42501",
            &format!(
                "cannot drop tenant: {} user(s) still belong to it; drop or \
                 reassign them first: {}",
                others.len(),
                others.join(", ")
            ),
        ));
    }

    // Only the lifecycle-owned admin remains (if any): hard-delete it
    // through the canonical `DROP USER` handler so ownership
    // reassignment, session invalidation, and catalog + redb removal
    // all run — the same guarantees `DROP USER` gives directly.
    if let Some(admin) = lifecycle_admin {
        let parts = ["DROP", "USER", admin.as_str()];
        super::super::user::drop_user(state, identity, &parts)?;
    }

    Ok(())
}
