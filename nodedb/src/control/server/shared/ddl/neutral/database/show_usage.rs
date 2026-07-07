// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASE USAGE FOR <name>`.
//!
//! Ported from the pgwire `ddl::database::show_usage` handler. The tenant-admin
//! gate, catalog lookup, live-gauge reads from `SystemMetrics`, and per-dimension
//! row rendering (`unlimited` limit + `percent_used`) are preserved verbatim;
//! only the result construction changed from pgwire `QueryResponse` to the
//! protocol-neutral [`DdlResult`] over `ShapedRows`. Every column is a
//! `text_field` in the original, so all columns stay `Text`.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::QuotaRecord;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_tenant_admin;
use super::support::{ddl_err, text_rows};

/// Handle `SHOW DATABASE USAGE FOR <name>`.
pub fn show_database_usage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show database usage")?;

    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    let record = catalog
        .get_database_quota(db_id)
        .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);

    // Pull live gauges from the system metrics registry. Dimensions without a
    // per-database accounting source land as `0`, which is the gauge's actual
    // value, not a fabricated placeholder.
    let (cur_memory, cur_storage, cur_queries) = match &state.system_metrics {
        Some(m) => (
            m.database_memory_bytes(name),
            m.database_storage_bytes(name),
            m.database_queries_total(name),
        ),
        None => (0, 0, 0),
    };
    // No per-database connection gauge yet — `connections_accepted` /
    // `connections_rejected` on SharedState are process-global and would
    // mislead under a per-database column. Report `0` until per-database
    // connection accounting lands.
    let cur_connections: u64 = 0;

    let columns = vec![
        "database".to_string(),
        "quota_name".to_string(),
        "limit".to_string(),
        "current".to_string(),
        "percent_used".to_string(),
    ];

    let dims: &[(&str, u64, u64)] = &[
        ("max_memory_bytes", record.max_memory_bytes, cur_memory),
        ("max_storage_bytes", record.max_storage_bytes, cur_storage),
        ("max_qps", record.max_qps as u64, cur_queries),
        (
            "max_connections",
            record.max_connections as u64,
            cur_connections,
        ),
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
        row.insert("database".to_string(), JsonValue::String(name.to_string()));
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

/// Render `current / limit` as a `"<n>%"` string.
///
/// `limit == 0` means "unlimited" and renders as `"n/a"` (percentage of
/// infinity is undefined). Otherwise the result is `(current * 100 / limit)`
/// floored, with the divisor pre-promoted to `u128` so the multiply can never
/// overflow even when both inputs are near `u64::MAX`.
pub(crate) fn format_percent(limit: u64, current: u64) -> String {
    if limit == 0 {
        return "n/a".to_string();
    }
    let pct = (u128::from(current) * 100) / u128::from(limit);
    format!("{pct}%")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_unlimited_renders_na() {
        assert_eq!(format_percent(0, 100), "n/a");
        assert_eq!(format_percent(0, 0), "n/a");
    }

    #[test]
    fn percent_basic_arithmetic() {
        assert_eq!(format_percent(100, 25), "25%");
        assert_eq!(format_percent(100, 100), "100%");
        assert_eq!(format_percent(100, 200), "200%"); // over-budget surfaces, never silently clamped
        assert_eq!(format_percent(1000, 0), "0%");
    }

    #[test]
    fn percent_does_not_overflow_on_max() {
        // Both at u64::MAX: 100 * MAX / MAX = 100. Pre-u64 arithmetic would overflow.
        assert_eq!(format_percent(u64::MAX, u64::MAX), "100%");
    }
}
