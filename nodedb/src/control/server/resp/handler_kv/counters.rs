// SPDX-License-Identifier: BUSL-1.1

//! Atomic counter commands: INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;

use super::super::codec::RespValue;
use super::super::command::RespCommand;
use super::super::handler::dispatch_kv_write;
use super::super::payload::{payload_field_i64, payload_json};
use super::super::redaction::resp_redaction;
use super::super::session::RespSession;
use super::surrogate::resp_kv_surrogate;

/// Refuse a counter command whose result a redaction rule covers.
///
/// `INCR` / `DECR` / `INCRBY` / `DECRBY` / `INCRBYFLOAT` answer with the row's
/// new stored value, which the KV engine holds as the single-value form — the
/// column every SQL-side read of that row calls `value`. Masking the answer
/// would report a number the key does not hold, so the command is refused
/// instead, on the same fail-closed principle the planner applies to an
/// aggregate over a redacted column. The refusal happens BEFORE dispatch, so
/// the increment the caller could not observe is never performed either.
fn refuse_if_counter_is_redacted(state: &SharedState, session: &RespSession) -> Option<RespValue> {
    let redaction = resp_redaction(state, session)?;
    redaction
        .field_has_rule(&state.redaction, "value")
        .then(|| RespValue::err("ERR the counter value is redacted for this role"))
}

/// INCR key / DECR key — increment/decrement by 1.
///
/// `default_delta` is +1 for INCR, -1 for DECR.
pub(in crate::control::server::resp) async fn handle_incr(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    default_delta: i64,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'incr' command");
    };
    dispatch_incr(state, session, key.to_vec(), default_delta).await
}

/// INCRBY key delta
pub(in crate::control::server::resp) async fn handle_incrby(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'incrby' command");
    }

    let key = cmd.args[0].clone();
    let Some(delta) = cmd.arg_i64(1) else {
        return RespValue::err("ERR value is not an integer or out of range");
    };
    dispatch_incr(state, session, key, delta).await
}

/// DECRBY key delta
pub(in crate::control::server::resp) async fn handle_decrby(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'decrby' command");
    }

    let key = cmd.args[0].clone();
    let Some(delta) = cmd.arg_i64(1) else {
        return RespValue::err("ERR value is not an integer or out of range");
    };
    let Some(neg_delta) = delta.checked_neg() else {
        return RespValue::err("ERR value is not an integer or out of range");
    };
    dispatch_incr(state, session, key, neg_delta).await
}

/// Shared body for every integer counter command.
async fn dispatch_incr(
    state: &SharedState,
    session: &RespSession,
    key: Vec<u8>,
    delta: i64,
) -> RespValue {
    if let Some(refusal) = refuse_if_counter_is_redacted(state, session) {
        return refusal;
    }
    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: session.collection.clone(),
        key,
        delta,
        ttl_ms: 0,
        surrogate,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => match payload_field_i64(&resp.payload, "value") {
            Some(new_val) => RespValue::integer(new_val),
            // The counter did change; a response we cannot read means we do
            // not know its new value, and echoing 0 would report a value the
            // key does not hold.
            None => RespValue::err("ERR counter response could not be decoded"),
        },
        Err(e) => RespValue::from_error(&e),
    }
}

/// INCRBYFLOAT key delta
pub(in crate::control::server::resp) async fn handle_incrbyfloat(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'incrbyfloat' command");
    }

    let key = cmd.args[0].clone();
    let delta_str = match cmd.arg_str(1) {
        Some(s) => s,
        None => return RespValue::err("ERR value is not a valid float"),
    };
    let delta: f64 = match delta_str.parse() {
        Ok(v) => v,
        Err(_) => return RespValue::err("ERR value is not a valid float"),
    };

    if let Some(refusal) = refuse_if_counter_is_redacted(state, session) {
        return refusal;
    }

    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: session.collection.clone(),
        key,
        delta,
        surrogate,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            // Return the new value as a bulk string (Redis convention).
            match payload_json(&resp.payload).get("value") {
                Some(v) => RespValue::bulk(v.to_string().into_bytes()),
                None => RespValue::err("ERR counter response could not be decoded"),
            }
        }
        Err(e) => RespValue::from_error(&e),
    }
}
