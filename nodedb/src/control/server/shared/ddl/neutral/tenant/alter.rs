// SPDX-License-Identifier: BUSL-1.1

//! `ALTER TENANT <id|name> SET QUOTA <field> = <value>` handler.
//!
//! Tenant quotas live in the in-memory `TenantStore` and are not part
//! of `StoredTenant`. Quota replication is handled separately from
//! the tenant identity record.
//!
//! The tenant reference accepts either a numeric id or a tenant name
//! (single-quoted optional), parallel to `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>`.
//!
//! Ported verbatim from the pgwire `ddl::tenant::alter` handler. The neutral
//! string-prefix dispatch that reaches this handler must guard against the
//! `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)` typed form (handled
//! by [`super::alter_quota::handle_alter_tenant_quota`]) — that guard lives
//! at the call site in `neutral::router`, not here, mirroring how the pgwire
//! parser's typed-vs-string split worked (the typed AST claimed the
//! `IN DATABASE` form first).

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{ddl_err, resolve_tenant_ref, status, tenant_exists};

pub fn alter_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can alter tenants",
        ));
    }

    if parts.len() < 7 {
        return Err(ddl_err(
            "42601",
            "syntax: ALTER TENANT <id|name> SET QUOTA <field> = <value>",
        ));
    }

    // Accept either a numeric id or a tenant name (mirrors CREATE/SHOW/DROP).
    let tenant_id = resolve_tenant_ref(state, parts[2])?
        .ok_or_else(|| ddl_err("42704", format!("tenant '{}' does not exist", parts[2])))?;

    // Existence gate, uniform across numeric ids and resolved names: altering an
    // unknown tenant must error rather than silently seed a default quota for a
    // phantom id.
    if !tenant_exists(state, tenant_id)? {
        return Err(ddl_err(
            "42704",
            format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    if !parts[3].eq_ignore_ascii_case("SET") || !parts[4].eq_ignore_ascii_case("QUOTA") {
        return Err(ddl_err(
            "42601",
            "expected SET QUOTA after tenant id or name",
        ));
    }

    let field = parts[5].to_lowercase();
    let value_idx = if parts.len() > 7 && parts[6] == "=" {
        7
    } else {
        6
    };
    if value_idx >= parts.len() {
        return Err(ddl_err("42601", "expected value after field name"));
    }

    let value: u64 = parts[value_idx]
        .parse()
        .map_err(|_| ddl_err("42601", "quota value must be a positive integer"))?;

    let mut tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let mut quota = tenants.quota(tenant_id).clone();
    match field.as_str() {
        "max_memory_bytes" => quota.max_memory_bytes = value,
        "max_storage_bytes" => quota.max_storage_bytes = value,
        "max_concurrent_requests" => quota.max_concurrent_requests = value as u32,
        "max_qps" => quota.max_qps = value as u32,
        "max_vector_dim" => quota.max_vector_dim = value as u32,
        "max_graph_depth" => quota.max_graph_depth = value as u32,
        "deactivated_collection_retention_days" => {
            quota.deactivated_collection_retention_days = Some(value as u32);
        }
        other => {
            return Err(ddl_err(
                "42601",
                format!(
                    "unknown quota field: {other}. Valid: max_memory_bytes, max_storage_bytes, max_concurrent_requests, max_qps, max_vector_dim, max_graph_depth, deactivated_collection_retention_days"
                ),
            ));
        }
    }
    tenants.set_quota(tenant_id, quota);

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("altered tenant {tenant_id}: set {field} = {value}"),
    );

    Ok(status("ALTER TENANT"))
}
