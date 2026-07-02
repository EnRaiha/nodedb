// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `LAST_VALUE` and `LAST_VALUES` query handlers.
//!
//! Syntax:
//! ```sql
//! SELECT LAST_VALUES('<collection>')
//! SELECT LAST_VALUE('<collection>', <series_id>)
//! ```
//!
//! These dispatch `MetaOp::QueryLastValues` / `QueryLastValue` to the Data Plane
//! and return results as protocol-neutral rows.

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::result::{DdlError, DdlResult};

/// `SELECT LAST_VALUES('<collection>')` — returns all cached last values.
pub async fn query_last_values(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let plan = PhysicalPlan::Meta(MetaOp::QueryLastValues {
        collection: collection.to_string(),
    });

    let payload = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        Duration::from_secs(5),
    )
    .await
    .map_err(|e| ddl_err("XX000", format!("dispatch failed: {e}")))?;

    let entries: Vec<(u64, i64, f64)> = sonic_rs::from_slice(&payload).unwrap_or_default();

    let mut rows = Vec::with_capacity(entries.len());
    for (series_id, ts, value) in &entries {
        let mut row = Map::new();
        row.insert(
            "series_id".to_string(),
            JsonValue::String((*series_id as i64).to_string()),
        );
        row.insert(
            "timestamp_ms".to_string(),
            JsonValue::String(ts.to_string()),
        );
        row.insert(
            "value".to_string(),
            JsonValue::String(format!("{value:.6}")),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec![
            "series_id".to_string(),
            "timestamp_ms".to_string(),
            "value".to_string(),
        ],
        column_types: vec![DdlColType::Int8, DdlColType::Int8, DdlColType::Text],
        rows,
        notice: None,
    })])
}

/// `SELECT LAST_VALUE('<collection>', <series_id>)` — returns single series value.
pub async fn query_last_value(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    series_id: u64,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let plan = PhysicalPlan::Meta(MetaOp::QueryLastValue {
        collection: collection.to_string(),
        series_id,
    });

    let payload = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        Duration::from_secs(5),
    )
    .await
    .map_err(|e| ddl_err("XX000", format!("dispatch failed: {e}")))?;

    let entry: Option<(i64, f64)> = sonic_rs::from_slice(&payload).unwrap_or_default();

    let mut rows = Vec::new();
    if let Some((ts, value)) = entry {
        let mut row = Map::new();
        row.insert(
            "timestamp_ms".to_string(),
            JsonValue::String(ts.to_string()),
        );
        row.insert(
            "value".to_string(),
            JsonValue::String(format!("{value:.6}")),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["timestamp_ms".to_string(), "value".to_string()],
        column_types: vec![DdlColType::Int8, DdlColType::Text],
        rows,
        notice: None,
    })])
}

fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
