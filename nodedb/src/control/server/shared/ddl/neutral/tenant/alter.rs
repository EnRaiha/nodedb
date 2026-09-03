// SPDX-License-Identifier: BUSL-1.1

//! `ALTER TENANT <id|name> SET QUOTA <field> = <value>` handler.
//!
//! The unqualified form targets the session's current database, matching every
//! other unqualified DDL. It writes the same `_system.tenant_quotas` row the
//! `IN DATABASE` form writes, through the same
//! `CatalogEntry::PutTenantQuota` propose, so the cap survives a restart and
//! reaches every node.
//!
//! The tenant reference accepts either a numeric id or a tenant name
//! (single-quoted optional), parallel to `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>`.
//!
//! The neutral string-prefix dispatch that reaches this handler must guard
//! against the `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)` typed
//! form (handled by [`super::alter_quota::handle_alter_tenant_quota`]) — that
//! guard lives at the call site in `neutral::router`, not here, because the
//! typed AST claims the `IN DATABASE` form first.

use nodedb_types::QuotaRecord;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::replicate::propose_and_apply;
use super::support::{ddl_err, resolve_tenant_ref, status, tenant_exists};

pub fn alter_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
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

    let catalog = state.credentials.catalog();

    // Start from the stored record so an unset field keeps the value the
    // operator set on an earlier statement.
    let before = catalog
        .get_tenant_quota(database_id, tenant_id)
        .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);
    let mut record = before.clone();
    match field.as_str() {
        "max_memory_bytes" => record.max_memory_bytes = value,
        "max_storage_bytes" => record.max_storage_bytes = value,
        "max_concurrent_requests" => record.max_concurrent_requests = value as u32,
        "max_qps" => record.max_qps = value as u32,
        "max_vector_dim" => record.max_vector_dim = value as u32,
        "max_graph_depth" => record.max_graph_depth = value as u32,
        "deactivated_collection_retention_days" => {
            record.deactivated_collection_retention_days = Some(value as u32);
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

    // The catalog enforces the sum-of-tenant-quotas ≤ database-quota ceiling.
    catalog
        .check_tenant_quota(database_id, tenant_id, &record)
        .map_err(|e| ddl_err("53400", format!("{e}")))?;

    // Replicated: every node writes the row and installs the cap in its live
    // enforcement components via post-apply.
    propose_and_apply(
        state,
        &CatalogEntry::PutTenantQuota {
            db_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            record: Box::new(record.clone()),
        },
        || {
            catalog
                .write_tenant_quota(database_id, tenant_id, &record)
                .map_err(|e| ddl_err("53400", format!("{e}")))?;
            crate::control::catalog_entry::post_apply::quota::put_tenant(
                database_id,
                tenant_id,
                &record,
                state,
            );
            Ok(())
        },
    )?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "altered tenant {tenant_id}: set {field} = {value} — before: [{}] — after: [{}]",
            before.audit_summary(),
            record.audit_summary()
        ),
    );

    Ok(status("ALTER TENANT"))
}
