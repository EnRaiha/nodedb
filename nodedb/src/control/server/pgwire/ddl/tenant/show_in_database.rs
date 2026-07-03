// SPDX-License-Identifier: BUSL-1.1

//! Handlers for `SHOW TENANT QUOTA FOR <name> IN DATABASE <db>` and
//! `SHOW TENANT USAGE FOR <name> IN DATABASE <db>`.

use std::sync::Arc;

use futures::stream;
use nodedb_types::QuotaRecord;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::{require_tenant_admin, sqlstate_error, text_field};

/// Handle `SHOW TENANT QUOTA FOR <name> IN DATABASE <db>`.
///
/// Returns one row per quota dimension showing the stored limit.
pub fn handle_show_tenant_quota_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database: &str,
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "show tenant quota")?;

    let (db_id, tenant_id, record) = resolve_tenant_quota(state, name, database)?;
    let _ = db_id; // used only for resolution

    let schema = Arc::new(vec![
        text_field("tenant"),
        text_field("database"),
        text_field("quota_name"),
        text_field("limit"),
        text_field("priority_class"),
        text_field("cache_weight"),
        text_field("maintenance_cpu_pct"),
    ]);

    let dims: &[(&str, u64)] = &[
        ("max_memory_bytes", record.max_memory_bytes),
        ("max_storage_bytes", record.max_storage_bytes),
        ("max_qps", record.max_qps as u64),
        ("max_connections", record.max_connections as u64),
    ];

    let priority_str = format!("{:?}", record.priority_class).to_lowercase();
    let _ = tenant_id;

    let mut rows = Vec::new();
    for &(quota_name, limit) in dims {
        let limit_str = if limit == 0 {
            "unlimited".to_string()
        } else {
            limit.to_string()
        };
        let mut enc = DataRowEncoder::new(schema.clone());
        enc.encode_field(&name.to_string())?;
        enc.encode_field(&database.to_string())?;
        enc.encode_field(&quota_name.to_string())?;
        enc.encode_field(&limit_str)?;
        enc.encode_field(&priority_str)?;
        enc.encode_field(&record.cache_weight.to_string())?;
        enc.encode_field(&record.maintenance_cpu_pct.to_string())?;
        rows.push(Ok(enc.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Handle `SHOW TENANT USAGE FOR <name> IN DATABASE <db>`.
///
/// Returns quota dimensions with current-usage columns. Per-tenant accounting
/// gauges are not yet emitted by any subsystem (memory governor, compaction,
/// query path), so `current` is reported as `0` — the value such a gauge
/// would actually hold — and `percent_used` is computed accordingly via
/// [`crate::control::server::shared::ddl::neutral::database::show_usage::format_percent`].
/// When per-tenant gauges land they wire in here without changing the column
/// shape.
pub fn handle_show_tenant_usage_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database: &str,
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "show tenant usage")?;

    let (db_id, tenant_id, record) = resolve_tenant_quota(state, name, database)?;
    let _ = (db_id, tenant_id);

    let schema = Arc::new(vec![
        text_field("tenant"),
        text_field("database"),
        text_field("quota_name"),
        text_field("limit"),
        text_field("current"),
        text_field("percent_used"),
    ]);

    // Per-tenant accounting gauges are not yet emitted; every dimension reports
    // 0 until they land. Keeping the same `(limit, current)` shape as the
    // database handler so percent rendering stays uniform across both forms.
    let dims: &[(&str, u64, u64)] = &[
        ("max_memory_bytes", record.max_memory_bytes, 0),
        ("max_storage_bytes", record.max_storage_bytes, 0),
        ("max_qps", record.max_qps as u64, 0),
        ("max_connections", record.max_connections as u64, 0),
    ];

    let mut rows = Vec::new();
    for &(quota_name, limit, current) in dims {
        let limit_str = if limit == 0 {
            "unlimited".to_string()
        } else {
            limit.to_string()
        };
        let pct_str =
            crate::control::server::shared::ddl::neutral::database::show_usage::format_percent(
                limit, current,
            );
        let mut enc = DataRowEncoder::new(schema.clone());
        enc.encode_field(&name.to_string())?;
        enc.encode_field(&database.to_string())?;
        enc.encode_field(&quota_name.to_string())?;
        enc.encode_field(&limit_str)?;
        enc.encode_field(&current.to_string())?;
        enc.encode_field(&pct_str)?;
        rows.push(Ok(enc.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

// ── shared helpers ────────────────────────────────────────────────────────────

/// Resolve tenant name + database name to IDs and load the tenant's quota record.
/// Returns `(db_id, tenant_id, record)`.
fn resolve_tenant_quota(
    state: &SharedState,
    name: &str,
    database: &str,
) -> PgWireResult<(nodedb_types::DatabaseId, TenantId, QuotaRecord)> {
    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => return Err(sqlstate_error("XX000", "system catalog unavailable")),
    };

    let db_id = catalog
        .get_database_id_by_name(database)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| sqlstate_error("3D000", &format!("database '{database}' does not exist")))?;

    let tenants = catalog
        .load_all_tenants()
        .map_err(|e| sqlstate_error("XX000", &format!("tenant load failed: {e}")))?;
    let tenant_id = tenants
        .iter()
        .find(|t| t.name == name)
        .map(|t| TenantId::new(t.tenant_id))
        .ok_or_else(|| sqlstate_error("42704", &format!("tenant '{name}' does not exist")))?;

    let record = catalog
        .get_tenant_quota(db_id, tenant_id)
        .map_err(|e| sqlstate_error("XX000", &format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);

    Ok((db_id, tenant_id, record))
}
