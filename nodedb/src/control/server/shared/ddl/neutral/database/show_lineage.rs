// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASE LINEAGE FOR <name>`.
//!
//! Ported from the pgwire `ddl::database::show_lineage` handler. The tenant-admin
//! gate, bounded `parent_clone` chain walk, and per-ancestor row rendering are
//! preserved verbatim; only the result construction changed from pgwire
//! `QueryResponse` to the protocol-neutral [`DdlResult`] over `ShapedRows`.
//! Every column is a `text_field` in the original, so all columns stay `Text`.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_tenant_admin;
use super::support::{ddl_err, text_rows};

/// One row in the lineage result set.
struct LineageRow {
    database_id: DatabaseId,
    name: String,
    /// `as_of_lsn` for this clone (the LSN boundary inherited from its parent).
    /// Zero for the root database (which has no parent clone reference).
    as_of_lsn: u64,
    /// LSN at which this clone was created.  Zero for the root database.
    clone_created_at_lsn: u64,
}

/// Handle `SHOW DATABASE LINEAGE FOR <name>`.
pub fn show_database_lineage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show database lineage")?;

    let catalog = state.credentials.catalog();

    let start_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    // Walk the parent_clone chain, bounded by MAX_CLONE_DEPTH to prevent
    // infinite loops from corrupt catalog state.
    let mut lineage: Vec<LineageRow> = Vec::new();
    let mut current_id = start_id;
    let max_hops = nodedb_types::MAX_CLONE_DEPTH + 2; // +2 for safety headroom

    for _ in 0..max_hops {
        let desc = catalog
            .get_database(current_id)
            .map_err(|e| ddl_err("XX000", format!("catalog read failed: {e}")))?
            .ok_or_else(|| {
                ddl_err(
                    "XX000",
                    format!("database id {} descriptor missing", current_id.as_u64()),
                )
            })?;

        let (as_of_lsn, clone_created_at_lsn) = match &desc.parent_clone {
            Some(p) => (p.as_of_lsn, desc.created_at_lsn),
            None => (0u64, 0u64),
        };

        lineage.push(LineageRow {
            database_id: current_id,
            name: desc.name.clone(),
            as_of_lsn,
            clone_created_at_lsn,
        });

        match desc.parent_clone {
            Some(p) => {
                current_id = p.source_db_id;
            }
            None => break,
        }
    }

    let columns = vec![
        "database_id".to_string(),
        "name".to_string(),
        "as_of_lsn".to_string(),
        "clone_created_at_lsn".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
    for row in lineage {
        let mut m = Map::new();
        m.insert(
            "database_id".to_string(),
            JsonValue::String(row.database_id.as_u64().to_string()),
        );
        m.insert("name".to_string(), JsonValue::String(row.name));
        m.insert(
            "as_of_lsn".to_string(),
            JsonValue::String(row.as_of_lsn.to_string()),
        );
        m.insert(
            "clone_created_at_lsn".to_string(),
            JsonValue::String(row.clone_created_at_lsn.to_string()),
        );
        rows.push(m);
    }

    Ok(text_rows(columns, rows))
}
