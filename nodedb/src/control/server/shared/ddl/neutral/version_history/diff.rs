// SPDX-License-Identifier: BUSL-1.1

//! SELECT DIFF(collection, 'doc-id', version_a, version_b)

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// SELECT DIFF(collection, 'doc-id', 'version_a', 'version_b')
///
/// Returns the delta bytes between two versions. The `version_a` and
/// `version_b` parameters can be checkpoint names or raw VV JSON.
///
/// The result is the raw Loro delta (binary) encoded as hex, plus size info.
/// Application-level diff rendering (field-level diffs) will be added
/// with the Field-Level Change Events feature (3.4).
pub async fn select_diff(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_diff_args(sql)?;
    if args.len() < 4 {
        return Err(err(
            "42601",
            "syntax: SELECT DIFF('collection', 'doc_id', 'version_a', 'version_b')".to_string(),
        ));
    }

    let collection = &args[0];
    let doc_id = &args[1];
    let version_a_name = &args[2];
    let version_b_name = &args[3];
    let tenant_id = identity.tenant_id;

    // Resolve version names to VV JSON.
    let from_vv = super::at_version::resolve_checkpoint_vv(
        state,
        tenant_id.as_u64(),
        collection,
        doc_id,
        version_a_name,
    )?;

    // Export delta from version_a to current via Data Plane.
    let plan = PhysicalPlan::Crdt(CrdtOp::ExportDelta {
        collection: collection.clone(),
        from_version_json: from_vv,
    });
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let delta_bytes = dispatch_system(
        state,
        SystemTask::new(
            SystemReason::CatalogMaintenance,
            tenant_id,
            database_id,
            collection,
            plan,
        ),
        timeout,
    )
    .await
    .map_err(|e| err("XX000", format!("dispatch: {e}")))?;

    let columns = vec![
        "from_version".to_string(),
        "to_version".to_string(),
        "delta_size_bytes".to_string(),
        "delta_hex".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    // Encode delta as hex for SQL-safe transport.
    let hex: String = delta_bytes.iter().map(|b| format!("{b:02x}")).collect();

    let mut row = Map::new();
    row.insert(
        "from_version".to_string(),
        JsonValue::String(version_a_name.clone()),
    );
    row.insert(
        "to_version".to_string(),
        JsonValue::String(version_b_name.clone()),
    );
    row.insert(
        "delta_size_bytes".to_string(),
        JsonValue::String((delta_bytes.len() as i64).to_string()),
    );
    row.insert("delta_hex".to_string(), JsonValue::String(hex));

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows: vec![row],
        notice: None,
    })])
}

/// Parse function arguments from `SELECT DIFF('a', 'b', 'c', 'd')`.
fn parse_diff_args(sql: &str) -> Result<Vec<String>, DdlError> {
    let start = sql
        .find('(')
        .ok_or_else(|| err("42601", "expected '(' in DIFF call".to_string()))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| err("42601", "expected ')' in DIFF call".to_string()))?;
    if start >= end {
        return Err(err("42601", "empty DIFF arguments".to_string()));
    }
    let args_str = &sql[start + 1..end];
    Ok(args_str
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .collect())
}
