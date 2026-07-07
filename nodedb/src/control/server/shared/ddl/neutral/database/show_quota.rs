// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASE QUOTA FOR <name>`.
//!
//! Ported from the pgwire `ddl::database::show_quota` handler. The tenant-admin
//! gate, catalog lookup, quota-record fallback to `QuotaRecord::DEFAULT`, and
//! per-dimension row rendering (including the `unlimited` special-case) are
//! preserved verbatim; only the result construction changed from pgwire
//! `QueryResponse` to the protocol-neutral [`DdlResult`] over `ShapedRows`.
//! Every column is a `text_field` in the original, so all columns stay `Text`.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::QuotaRecord;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_tenant_admin;
use super::support::{ddl_err, text_rows};

/// Handle `SHOW DATABASE QUOTA FOR <name>`.
pub fn show_database_quota(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show database quota")?;

    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    let record = catalog
        .get_database_quota(db_id)
        .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);

    let columns = vec![
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
        row.insert("database".to_string(), JsonValue::String(name.to_string()));
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
