// SPDX-License-Identifier: BUSL-1.1

//! `CREATE TENANT [IF NOT EXISTS] <name> [ID <id>] [WITH ADMIN <user>]`
//! handler. Migrated to `CatalogEntry::PutTenant` in phase 1k.6.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredTenant;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::sqlstate_error;
use super::super::parse_utils::strip_if_not_exists;

/// Optional `ID <id>` and `WITH ADMIN <user>` clauses parsed from the
/// tokens that follow the tenant name.
struct TenantOptions<'a> {
    explicit_id: Option<u64>,
    admin_override: Option<&'a str>,
}

/// Scan the tokens after the tenant name for `ID <id>` and `WITH ADMIN
/// <user>`. Both clauses are optional and order-independent.
fn parse_tenant_options<'a>(rest: &[&'a str]) -> PgWireResult<TenantOptions<'a>> {
    let mut explicit_id = None;
    let mut admin_override = None;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("ID") && i + 1 < rest.len() {
            let id: u64 = rest[i + 1]
                .parse()
                .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
            explicit_id = Some(id);
            i += 2;
        } else if rest[i].eq_ignore_ascii_case("WITH")
            && i + 2 < rest.len()
            && rest[i + 1].eq_ignore_ascii_case("ADMIN")
        {
            admin_override = Some(rest[i + 2]);
            i += 3;
        } else {
            i += 1;
        }
    }
    Ok(TenantOptions {
        explicit_id,
        admin_override,
    })
}

/// The default username auto-provisioned as a tenant's `tenant_admin`
/// when `CREATE TENANT` is run without an explicit `WITH ADMIN <user>`
/// clause. Defined once here so the `DROP TENANT` cleanup path can
/// identify and remove this lifecycle-owned account without the
/// convention drifting between the two sites.
pub(super) fn default_admin_username(tenant_name: &str) -> String {
    format!("{tenant_name}_admin")
}

/// `CREATE TENANT [IF NOT EXISTS] <name> [ID <id>] [WITH ADMIN <user>]`
///
/// Creates a tenant with default quotas. Only superuser can create tenants.
/// `name` is for display; the numeric ID is what's used internally. With
/// `IF NOT EXISTS`, re-creating an existing tenant is a no-op success.
pub fn create_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can create tenants",
        ));
    }

    let (if_not_exists, parts) = strip_if_not_exists(parts, 2);

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE TENANT [IF NOT EXISTS] <name> [ID <id>] [WITH ADMIN <user>]",
        ));
    }

    let name = parts[2];
    let opts = parse_tenant_options(&parts[3..])?;

    // `IF NOT EXISTS`: if a tenant with this name already exists, the
    // statement is a no-op success — do not allocate a second id.
    if if_not_exists
        && let Some(catalog) = state.credentials.catalog()
        && catalog
            .find_tenant_by_name(name)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog read: {e}")))?
            .is_some()
    {
        return Ok(vec![Response::Execution(Tag::new("CREATE TENANT"))]);
    }

    // Pick the tenant id. An explicit `ID <n>` is honored verbatim;
    // otherwise allocate a fresh id from the durable, monotonic
    // high-water-mark so two `CREATE TENANT`s never collide and a
    // dropped id is never reused. The catalog counter is authoritative
    // when wired up; the in-memory mirror covers the no-catalog path.
    let tenant_id = match opts.explicit_id {
        Some(id) => TenantId::new(id),
        None => match state.credentials.catalog().as_ref() {
            Some(catalog) => TenantId::new(
                catalog
                    .allocate_tenant_id()
                    .map_err(|e| sqlstate_error("XX000", &format!("tenant id alloc: {e}")))?,
            ),
            None => {
                let mut tenants = match state.tenants.lock() {
                    Ok(t) => t,
                    Err(p) => p.into_inner(),
                };
                TenantId::new(tenants.allocate_tenant_id())
            }
        },
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let stored = StoredTenant {
        tenant_id: tenant_id.as_u64(),
        name: name.to_string(),
        created_at: now,
        is_active: true,
    };

    let entry = CatalogEntry::PutTenant(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        // Single-node fallback: write redb + seed in-memory quota
        // ourselves since post_apply only runs on the raft path.
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_tenant(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        let mut tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        if !tenants.has_quota(tenant_id) {
            tenants.set_quota(tenant_id, TenantQuota::default());
        }
    }

    // Auto-create a tenant_admin user for the new tenant. `WITH ADMIN
    // <user>` names it explicitly; otherwise it defaults to `<name>_admin`.
    let admin_name = opts
        .admin_override
        .map(str::to_string)
        .unwrap_or_else(|| default_admin_username(name));
    let admin_password = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(name.as_bytes());
        hasher.update(tenant_id.as_u64().to_le_bytes());
        hasher.update(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_le_bytes(),
        );
        let hash = hasher.finalize();
        let hex: String = hash.iter().take(12).map(|b| format!("{b:02x}")).collect();
        format!("ndb_{hex}")
    };
    match state.credentials.create_user(
        &admin_name,
        &admin_password,
        tenant_id,
        vec![Role::TenantAdmin],
    ) {
        Ok(_) => {
            tracing::info!(tenant = %name, admin = %admin_name, "auto-created tenant admin");
        }
        Err(e) => {
            tracing::warn!(tenant = %name, error = %e, "failed to auto-create tenant admin");
        }
    }

    state.audit_record(
        AuditEvent::TenantCreated,
        Some(tenant_id),
        &identity.username,
        &format!("created tenant '{name}' (id {tenant_id}) with admin '{admin_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TENANT"))])
}
