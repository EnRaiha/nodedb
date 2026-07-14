// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral handlers for atomic KV SQL functions: KV_INCR, KV_DECR,
//! KV_INCR_FLOAT, KV_CAS, KV_GETSET.
//!
//! These are side-effecting operations that dispatch to the Data Plane via the
//! SPSC bridge, so they cannot be pure DataFusion UDFs. Instead they're
//! intercepted in the DDL router before DataFusion parsing. Each builds a
//! single-text-column [`DdlResult`] carrying the Data Plane's JSON payload.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::result::{DdlError, DdlResult};

/// Handle `SELECT KV_INCR(collection, key, delta [, TTL => seconds])`
///
/// Returns `{"value": <new_value>}` as a single text column.
pub async fn kv_incr(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    negate: bool,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let func_name = if negate { "KV_DECR" } else { "KV_INCR" };
    let args = parse_function_args(sql, func_name)?;

    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            format!("{func_name} requires at least 3 arguments: (collection, key, delta)"),
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let delta: i64 = parse_i64(&args[2], func_name)?;
    let delta = if negate {
        delta
            .checked_neg()
            .ok_or_else(|| ddl_err("22003", format!("{func_name}: delta overflow on negation")))?
    } else {
        delta
    };

    let ttl_ms = parse_optional_ttl(&args[3..])?;

    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
    let surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            key.as_bytes(),
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection,
        key: key.as_bytes().to_vec(),
        delta,
        ttl_ms,
        surrogate,
    });

    dispatch_and_respond(state, identity, vshard, plan, func_name, txn_ctx).await
}

/// Handle `SELECT KV_INCR_FLOAT(collection, key, delta)`
///
/// Returns `{"value": <new_value>}` as a single text column.
pub async fn kv_incr_float(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "KV_INCR_FLOAT")?;

    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            "KV_INCR_FLOAT requires 3 arguments: (collection, key, delta)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let delta: f64 = args[2].trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("KV_INCR_FLOAT: delta must be a float, got '{}'", args[2]),
        )
    })?;

    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
    let surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            key.as_bytes(),
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
        collection,
        key: key.as_bytes().to_vec(),
        delta,
        surrogate,
    });

    dispatch_and_respond(state, identity, vshard, plan, "KV_INCR_FLOAT", txn_ctx).await
}

/// Handle `SELECT KV_CAS(collection, key, expected, new_value)`
///
/// Returns `{"success": bool, "current_value": "<base64>"}` as a single text column.
pub async fn kv_cas(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "KV_CAS")?;

    if args.len() < 4 {
        return Err(ddl_err(
            "42601",
            "KV_CAS requires 4 arguments: (collection, key, expected, new_value)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let expected = unquote(&args[2]);
    let new_value = unquote(&args[3]);

    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
    let surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            key.as_bytes(),
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let plan = PhysicalPlan::Kv(KvOp::Cas {
        collection,
        key: key.as_bytes().to_vec(),
        expected: expected.into_bytes(),
        new_value: new_value.into_bytes(),
        surrogate,
    });

    dispatch_and_respond(state, identity, vshard, plan, "KV_CAS", txn_ctx).await
}

/// Handle `SELECT KV_GETSET(collection, key, new_value)`
///
/// Returns `{"old_value": "<base64>"}` as a single text column.
pub async fn kv_getset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "KV_GETSET")?;

    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            "KV_GETSET requires 3 arguments: (collection, key, new_value)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let new_value = unquote(&args[2]);

    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
    let surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            key.as_bytes(),
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let plan = PhysicalPlan::Kv(KvOp::GetSet {
        collection,
        key: key.as_bytes().to_vec(),
        new_value: new_value.into_bytes(),
        surrogate,
    });

    dispatch_and_respond(state, identity, vshard, plan, "KV_GETSET", txn_ctx).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Dispatch a KvOp through the protocol-neutral in-transaction staging gate
/// and return the JSON response as a single text-column row keyed by the
/// lower-cased function name.
///
/// Outside a transaction block (or for the read half of the gate, which
/// never applies here since every `KvOp` this module builds is a write),
/// `route_in_tx_write` dispatches immediately -- byte-identical to the
/// pre-staging behavior. Inside a transaction, `KvOp::Incr` / `IncrFloat` /
/// `Cas` / `GetSet` are staged into the per-transaction overlay
/// (`is_stageable_write`) and this function reads the computed value back
/// from `StagedWriteOutcome::payload` (forwarded verbatim by the staging
/// gate for `StagedTagKind::RawPayload`), so a `SELECT KV_INCR(...)` inside
/// `BEGIN..COMMIT` returns the same value the staged overlay now holds, and
/// a following `SELECT KV_INCR(...)` on the same key in the same
/// transaction chains off it.
///
/// `pub(super)` so the sibling `transfer.rs` module reuses the identical
/// in-transaction routing for `TRANSFER` / `TRANSFER_ITEM` instead of the
/// direct `dispatch_to_data_plane` call it used before those two `KvOp`s
/// became stageable.
pub(super) async fn dispatch_and_respond(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    vshard: VShardId,
    plan: PhysicalPlan,
    func_name: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    use crate::control::server::shared::session::staging_gate::{
        InTxnRoute, StagingGateError, route_in_tx_write,
    };

    let tenant_id = identity.tenant_id;
    let database_id = DatabaseId::DEFAULT;

    let task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };

    let routed = route_in_tx_write(state, txn_ctx.sessions, txn_ctx.addr, task, |staged| {
        crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
            state,
            staged.tenant_id,
            staged.database_id,
            staged.vshard_id,
            staged.plan,
            TraceId::ZERO,
            staged.txn_id,
        )
    })
    .await;

    let payload = match routed {
        Ok(InTxnRoute::Read(task)) => {
            let task = *task;
            match crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                state,
                task.tenant_id,
                task.database_id,
                task.vshard_id,
                task.plan,
                TraceId::ZERO,
                task.txn_id,
            )
            .await
            {
                Ok(resp) => resp.payload.as_ref().to_vec(),
                Err(e) => return Err(ddl_err("XX000", e.to_string())),
            }
        }
        // Every `KvOp` this module builds is stageable once in a
        // transaction (`is_stageable_write`), so `Buffered` never occurs;
        // handled defensively with an empty payload rather than a panic.
        Ok(InTxnRoute::Buffered) => Vec::new(),
        Ok(InTxnRoute::Staged(outcome)) => outcome.payload,
        Err(StagingGateError::Dispatch(e)) => return Err(ddl_err("XX000", e.to_string())),
        Err(StagingGateError::Rejected { code }) => {
            let (_, sqlstate, message) = match code {
                Some(code) => {
                    crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code)
                }
                None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
            };
            return Err(ddl_err(sqlstate, message));
        }
    };

    let payload_text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    let col_name = func_name.to_lowercase();
    Ok(vec![single_text_col(&col_name, payload_text)])
}

/// Build a single-text-column row set carrying `text` under `col`.
pub(super) fn single_text_col(col: &str, text: String) -> DdlResult {
    let mut row = Map::new();
    row.insert(col.to_string(), JsonValue::String(text));
    DdlResult::Rows(ShapedRows {
        columns: vec![col.to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![row],
        notice: None,
    })
}

/// Parse function arguments from `SELECT FUNC_NAME(arg1, arg2, ...)`.
///
/// Handles quoted strings with commas inside them.
pub(super) fn parse_function_args(sql: &str, _func_name: &str) -> Result<Vec<String>, DdlError> {
    let start = sql
        .find('(')
        .ok_or_else(|| ddl_err("42601", "expected '(' in function call"))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| ddl_err("42601", "expected ')' in function call"))?;
    if start >= end {
        return Ok(Vec::new());
    }

    let inner = &sql[start + 1..end];
    Ok(split_args(inner))
}

/// Split comma-separated arguments, respecting single-quoted strings.
///
/// Handles SQL-standard escaped quotes: `''` inside a quoted string becomes `'`.
/// Example: `'O''Reilly'` → `O'Reilly`.
pub(super) fn split_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                // Check if next char is also ' (escaped quote).
                if chars.peek() == Some(&'\'') {
                    chars.next(); // Consume the second '.
                    current.push('\''); // Keep one ' in the output.
                    current.push('\'');
                } else {
                    in_quote = false;
                    current.push(ch);
                }
            }
            ',' if !in_quote => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        args.push(trimmed);
    }
    args
}

/// Remove surrounding single quotes from a string argument.
pub(super) fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// Parse an i64 from a string argument.
fn parse_i64(s: &str, func_name: &str) -> Result<i64, DdlError> {
    s.trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("{func_name}: delta must be an integer, got '{}'", s.trim()),
        )
    })
}

/// Parse optional `TTL => seconds` from remaining args.
///
/// Supports: `TTL => 86400` or just a bare number as the 4th arg.
fn parse_optional_ttl(args: &[String]) -> Result<u64, DdlError> {
    if args.is_empty() {
        return Ok(0);
    }

    // Check for `TTL => value` pattern.
    for (i, arg) in args.iter().enumerate() {
        let upper = arg.trim().to_uppercase();
        if upper.starts_with("TTL") {
            // Formats: "TTL => 86400" or split across args: "TTL", "=>", "86400"
            if let Some(val_str) = upper
                .strip_prefix("TTL")
                .map(|r| r.trim_start_matches("=>").trim_start_matches('=').trim())
                && !val_str.is_empty()
            {
                return parse_ttl_seconds(val_str);
            }
            // Look at next arg(s) for the value.
            let remaining: Vec<&str> = args[i + 1..].iter().map(|s| s.trim()).collect();
            for r in &remaining {
                let cleaned = r.trim_start_matches("=>").trim_start_matches('=').trim();
                if !cleaned.is_empty() {
                    return parse_ttl_seconds(cleaned);
                }
            }
        }
    }

    Ok(0)
}

/// Parse TTL seconds → milliseconds.
fn parse_ttl_seconds(s: &str) -> Result<u64, DdlError> {
    let secs: u64 = s.parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("TTL must be a positive integer (seconds), got '{s}'"),
        )
    })?;
    Ok(secs * 1000)
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
