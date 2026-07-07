// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASE MIRROR STATUS [FOR <name>]`.
//!
//! Ported from the pgwire `ddl::database::mirror::show` handler. The tenant-admin
//! gate, catalog list, mirror-only filtering, `FOR <name>` filter, status /
//! mode / lag rendering, `mirror_lag` fallback reads, and the not-found error
//! for a specific name are preserved verbatim; only the result construction
//! changed from pgwire `QueryResponse` to the protocol-neutral [`DdlResult`]
//! over `ShapedRows`. Every column is a `text_field` in the original, so all
//! columns stay `Text`.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::{MirrorMode, MirrorStatus};

use crate::control::security::catalog::database_types::DatabaseStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_tenant_admin;
use super::super::support::{ddl_err, text_rows};

/// Handle `SHOW DATABASE MIRROR STATUS [FOR <name>]`.
pub fn show_database_mirror_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show database mirror status")?;

    let catalog = state.credentials.catalog();

    let all_databases = catalog
        .list_databases()
        .map_err(|e| ddl_err("XX000", format!("catalog list failed: {e}")))?;

    let columns = vec![
        "name".to_string(),
        "source_cluster".to_string(),
        "source_database".to_string(),
        "mode".to_string(),
        "status".to_string(),
        "bytes_done".to_string(),
        "bytes_total".to_string(),
        "lag_ms".to_string(),
        "last_applied_lsn".to_string(),
        "last_apply_ms".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();

    for db in &all_databases {
        // Filter: only mirror databases (Mirroring status or Active with Promoted origin).
        let origin = match &db.mirror_origin {
            Some(o) => o,
            None => continue,
        };

        // Apply FOR <name> filter if specified.
        if let Some(filter) = name
            && !db.name.eq_ignore_ascii_case(filter)
        {
            continue;
        }

        // Check that the database status is consistent with a mirror lifecycle.
        match db.status {
            DatabaseStatus::Mirroring | DatabaseStatus::Active => {}
            DatabaseStatus::Deactivated | DatabaseStatus::Cloning => continue,
        }

        let mode_str = match origin.mode {
            MirrorMode::Sync => "sync",
            MirrorMode::Async => "async",
        };

        let (status_str, bytes_done, bytes_total, lag_ms) = match &origin.status {
            MirrorStatus::Bootstrapping {
                bytes_done,
                bytes_total,
            } => ("bootstrapping", *bytes_done, *bytes_total, 0u64),
            MirrorStatus::Following => ("following", 0u64, 0u64, 0u64),
            MirrorStatus::Degraded { lag_ms } => ("degraded", 0, 0, *lag_ms),
            MirrorStatus::Disconnected => ("disconnected", 0, 0, 0),
            MirrorStatus::Promoted => ("promoted", 0, 0, 0),
        };

        // Load lag record from _system.mirror_lag for precise LSN / ms values.
        let (last_applied_lsn, last_apply_ms) = match catalog.get_mirror_lag(db.id) {
            Ok(Some(lag)) => (lag.last_applied_lsn.as_u64(), lag.last_apply_ms),
            Ok(None) => (origin.last_applied.as_u64(), 0u64),
            Err(_) => (origin.last_applied.as_u64(), 0u64),
        };

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(db.name.clone()));
        row.insert(
            "source_cluster".to_string(),
            JsonValue::String(origin.source_cluster.clone()),
        );
        row.insert(
            "source_database".to_string(),
            JsonValue::String(origin.source_database.as_u64().to_string()),
        );
        row.insert("mode".to_string(), JsonValue::String(mode_str.to_string()));
        row.insert(
            "status".to_string(),
            JsonValue::String(status_str.to_string()),
        );
        row.insert(
            "bytes_done".to_string(),
            JsonValue::String(bytes_done.to_string()),
        );
        row.insert(
            "bytes_total".to_string(),
            JsonValue::String(bytes_total.to_string()),
        );
        row.insert("lag_ms".to_string(), JsonValue::String(lag_ms.to_string()));
        row.insert(
            "last_applied_lsn".to_string(),
            JsonValue::String(last_applied_lsn.to_string()),
        );
        row.insert(
            "last_apply_ms".to_string(),
            JsonValue::String(last_apply_ms.to_string()),
        );
        rows.push(row);
    }

    // When a specific name was requested and no rows were found, return an error.
    if let Some(filter) = name
        && rows.is_empty()
    {
        return Err(ddl_err(
            "42P01",
            format!("mirror database '{filter}' not found"),
        ));
    }

    Ok(text_rows(columns, rows))
}
