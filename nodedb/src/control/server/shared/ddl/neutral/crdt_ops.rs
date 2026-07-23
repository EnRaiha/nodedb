// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral CRDT operations exposed as SQL-like functions.
//!
//! - `SELECT crdt_state('collection', 'doc_id')` → read CRDT snapshot
//! - `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')` → apply CRDT delta
//!
//! Handlers build [`DdlResult`](super::super::result::DdlResult) directly and
//! carry no pgwire types.

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::hex_decode;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::result::{DdlError, DdlResult};

/// Parse function arguments from SQL like `SELECT func('arg1', 'arg2')`.
fn parse_function_args(sql: &str) -> Vec<String> {
    // Find content between first '(' and last ')'.
    let start = match sql.find('(') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let end = match sql.rfind(')') {
        Some(i) => i,
        None => return Vec::new(),
    };
    if start >= end {
        return Vec::new();
    }

    let args_str = &sql[start..end];
    args_str
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .collect()
}

/// `SELECT crdt_state('collection', 'doc_id')`
///
/// Returns the CRDT document snapshot as a text result.
pub async fn crdt_state(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql);
    if args.len() < 2 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: SELECT crdt_state('collection', 'doc_id')".to_string(),
        });
    }

    let collection = &args[0];
    let document_id = &args[1];
    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Crdt(CrdtOp::Read {
        collection: collection.clone(),
        document_id: document_id.clone(),
    });

    // Synchronous dispatch via the blocking bridge.
    let result = crate::control::server::shared::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
    )
    .await
    .map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: e.to_string(),
    })?;

    let columns = vec!["crdt_state".to_string()];
    let column_types = vec![DdlColType::Text];

    if result.is_empty() {
        return Ok(vec![DdlResult::Rows(ShapedRows {
            columns,
            column_types,
            rows: Vec::new(),
            notice: None,
        })]);
    }

    let text = String::from_utf8_lossy(&result).into_owned();
    let mut row = Map::new();
    row.insert("crdt_state".to_string(), JsonValue::String(text));

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows: vec![row],
        notice: None,
    })])
}

/// `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')`
///
/// Applies a CRDT delta and returns the result.
pub async fn crdt_apply(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql);
    if args.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: SELECT crdt_apply('collection', 'doc_id', 'delta_hex_or_base64')"
                .to_string(),
        });
    }

    let collection = &args[0];
    let document_id = &args[1];
    let delta_str = &args[2];

    // Try hex decode first, then treat as raw bytes.
    let delta = hex_decode(delta_str).unwrap_or_else(|| delta_str.as_bytes().to_vec());

    let tenant_id = identity.tenant_id;

    let surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, collection, document_id.as_bytes())
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.clone(),
        document_id: document_id.clone(),
        delta,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate,
        provenance: None,
        // Local pgwire write, not a replicated peer sync — no constraint fence.
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });

    // Route through the Raft proposer gate so the delta is quorum-durable under
    // replication. A local-only dispatch would land the delta on the receiving
    // node only — it would be lost to every follower and entirely on failover.
    crate::control::server::sync::raft_dispatch::dispatch_write_replicated(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
        crate::event::EventSource::User,
    )
    .await
    .map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: e.to_string(),
    })?;

    let columns = vec!["result".to_string()];
    let column_types = vec![DdlColType::Text];
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String("OK".to_string()));

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows: vec![row],
        notice: None,
    })])
}
