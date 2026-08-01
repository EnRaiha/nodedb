// SPDX-License-Identifier: BUSL-1.1

//! `ALTER TIMESERIES <name> SET (key = 'value', ...)`

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::persist_collection_replicated;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{ddl_err, parse_with_clause};

/// ALTER TIMESERIES <name> SET (key = 'value', ...)
pub fn alter_timeseries(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 5 || parts[3].to_uppercase() != "SET" {
        return Err(ddl_err(
            "42601",
            "syntax: ALTER TIMESERIES <name> SET (key = 'value', ...)",
        ));
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id;

    {
        let catalog = state.credentials.catalog();
        let mut coll = catalog
            .get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &name)
            .map_err(|e| ddl_err("XX000", e.to_string()))?
            .ok_or_else(|| ddl_err("42P01", format!("collection '{name}' does not exist")))?;

        if !coll.collection_type.is_timeseries() {
            return Err(ddl_err(
                "42809",
                format!("'{name}' is not a timeseries collection"),
            ));
        }

        let new_config = parse_with_clause(parts);
        if let Some(cfg) = new_config {
            coll.timeseries_config = Some(cfg);
        }

        // Update partition registry interval if partition_by changed.
        if let Some(registries) = state.timeseries_registries() {
            let key = format!("{}:{}", tenant_id.as_u64(), name);
            let mut regs =
                crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
            if let Some(registry) = regs.get_mut(&key)
                && let Some(config) = coll.get_timeseries_config()
                && let Some(partition_by) = config.get("partition_by").and_then(|v| v.as_str())
                && let Ok(interval) =
                    nodedb_types::timeseries::PartitionInterval::parse(partition_by)
            {
                registry.set_partition_interval(interval);
            }
        }

        persist_collection_replicated(state, DatabaseId::DEFAULT, &coll)
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
    }

    tracing::info!(collection = name, "timeseries config updated");

    Ok(vec![DdlResult::Status {
        command: "ALTER TIMESERIES".to_string(),
        rows_affected: None,
    }])
}
