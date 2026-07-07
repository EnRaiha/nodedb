// SPDX-License-Identifier: BUSL-1.1

//! Handlers for `SHOW TENANT QUOTA FOR <name> IN DATABASE <db>` and
//! `SHOW TENANT USAGE FOR <name> IN DATABASE <db>`.
//!
//! Ported verbatim from the pgwire `ddl::tenant::show_in_database` handlers.
//! Both are 100% `text_field` schemas in the original, so every column stays
//! `DdlColType::Text` via [`super::support::text_rows`]. `require_tenant_admin`
//! is byte-identical to the pgwire gate used here originally, so it is reused
//! from `neutral::database::gate` rather than duplicated. `format_percent` is
//! reused from `neutral::database::show_usage`, matching the repoint already
//! done at the call site before this migration.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::QuotaRecord;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::database::gate::require_tenant_admin;
use super::super::database::show_usage::format_percent;
use super::support::{ddl_err, text_rows};

/// Handle `SHOW TENANT QUOTA FOR <name> IN DATABASE <db>`.
///
/// Returns one row per quota dimension showing the stored limit.
pub fn handle_show_tenant_quota_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show tenant quota")?;

    let (_db_id, _tenant_id, record) = resolve_tenant_quota(state, name, database)?;

    let columns = vec![
        "tenant".to_string(),
        "database".to_string(),
        "quota_name".to_string(),
        "limit".to_string(),
        "priority_class".to_string(),
        "cache_weight".to_string(),
        "maintenance_cpu_pct".to_string(),
    ];

    let dims: &[(&str, u64)] = &[
        ("max_memory_bytes", record.max_memory_bytes),
        ("max_storage_bytes", record.max_storage_bytes),
        ("max_qps", record.max_qps as u64),
        ("max_connections", record.max_connections as u64),
    ];

    let priority_str = format!("{:?}", record.priority_class).to_lowercase();

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
    for &(quota_name, limit) in dims {
        let limit_str = if limit == 0 {
            "unlimited".to_string()
        } else {
            limit.to_string()
        };
        let mut row = Map::new();
        row.insert("tenant".to_string(), JsonValue::String(name.to_string()));
        row.insert(
            "database".to_string(),
            JsonValue::String(database.to_string()),
        );
        row.insert(
            "quota_name".to_string(),
            JsonValue::String(quota_name.to_string()),
        );
        row.insert("limit".to_string(), JsonValue::String(limit_str));
        row.insert(
            "priority_class".to_string(),
            JsonValue::String(priority_str.clone()),
        );
        row.insert(
            "cache_weight".to_string(),
            JsonValue::String(record.cache_weight.to_string()),
        );
        row.insert(
            "maintenance_cpu_pct".to_string(),
            JsonValue::String(record.maintenance_cpu_pct.to_string()),
        );
        rows.push(row);
    }

    Ok(text_rows(columns, rows))
}

/// Handle `SHOW TENANT USAGE FOR <name> IN DATABASE <db>`.
///
/// Returns quota dimensions with current-usage columns. Per-tenant accounting
/// gauges are not yet emitted by any subsystem (memory governor, compaction,
/// query path), so `current` is reported as `0` — the value such a gauge
/// would actually hold — and `percent_used` is computed accordingly via
/// [`format_percent`]. When per-tenant gauges land they wire in here without
/// changing the column shape.
pub fn handle_show_tenant_usage_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show tenant usage")?;

    let (_db_id, _tenant_id, record) = resolve_tenant_quota(state, name, database)?;

    let columns = vec![
        "tenant".to_string(),
        "database".to_string(),
        "quota_name".to_string(),
        "limit".to_string(),
        "current".to_string(),
        "percent_used".to_string(),
    ];

    // Per-tenant accounting gauges are not yet emitted; every dimension reports
    // 0 until they land. Keeping the same `(limit, current)` shape as the
    // database handler so percent rendering stays uniform across both forms.
    let dims: &[(&str, u64, u64)] = &[
        ("max_memory_bytes", record.max_memory_bytes, 0),
        ("max_storage_bytes", record.max_storage_bytes, 0),
        ("max_qps", record.max_qps as u64, 0),
        ("max_connections", record.max_connections as u64, 0),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
    for &(quota_name, limit, current) in dims {
        let limit_str = if limit == 0 {
            "unlimited".to_string()
        } else {
            limit.to_string()
        };
        let pct_str = format_percent(limit, current);
        let mut row = Map::new();
        row.insert("tenant".to_string(), JsonValue::String(name.to_string()));
        row.insert(
            "database".to_string(),
            JsonValue::String(database.to_string()),
        );
        row.insert(
            "quota_name".to_string(),
            JsonValue::String(quota_name.to_string()),
        );
        row.insert("limit".to_string(), JsonValue::String(limit_str));
        row.insert(
            "current".to_string(),
            JsonValue::String(current.to_string()),
        );
        row.insert("percent_used".to_string(), JsonValue::String(pct_str));
        rows.push(row);
    }

    Ok(text_rows(columns, rows))
}

// ── shared helpers ────────────────────────────────────────────────────────────

/// Resolve tenant name + database name to IDs and load the tenant's quota record.
/// Returns `(db_id, tenant_id, record)`.
fn resolve_tenant_quota(
    state: &SharedState,
    name: &str,
    database: &str,
) -> Result<(nodedb_types::DatabaseId, TenantId, QuotaRecord), DdlError> {
    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(database)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{database}' does not exist")))?;

    let tenants = catalog
        .load_all_tenants()
        .map_err(|e| ddl_err("XX000", format!("tenant load failed: {e}")))?;
    let tenant_id = tenants
        .iter()
        .find(|t| t.name == name)
        .map(|t| TenantId::new(t.tenant_id))
        .ok_or_else(|| ddl_err("42704", format!("tenant '{name}' does not exist")))?;

    let record = catalog
        .get_tenant_quota(db_id, tenant_id)
        .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);

    Ok((db_id, tenant_id, record))
}
