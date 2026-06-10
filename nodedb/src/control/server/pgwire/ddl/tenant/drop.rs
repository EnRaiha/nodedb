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
