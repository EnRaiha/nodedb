// SPDX-License-Identifier: BUSL-1.1

//! The four public `SELECT KV_*(...)` entry points: `KV_INCR` / `KV_DECR`
//! (one function, `negate`-switched), `KV_INCR_FLOAT`, `KV_CAS`, and
//! `KV_GETSET`. Each parses its SQL-text arguments, resolves a surrogate for
//! the target key, builds the matching [`KvOp`], and hands off to
//! [`dispatch_and_respond`](super::dispatch::dispatch_and_respond) for
//! authorization, in-transaction routing, and response shaping.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::super::super::result::{DdlError, DdlResult};
use super::dispatch::{
    ddl_err, dispatch_and_respond, parse_function_args, parse_i64, parse_optional_ttl, unquote,
};

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
        collection: collection.clone(),
        key: key.as_bytes().to_vec(),
        delta,
        ttl_ms,
        surrogate,
        // Filled by `dispatch_and_respond`, which runs the same RLS injection
        // pass the planner-driven path runs.
        rls_write_check: Vec::new(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard,
        plan,
        func_name,
        &[collection.as_str()],
        txn_ctx,
    )
    .await
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
        collection: collection.clone(),
        key: key.as_bytes().to_vec(),
        delta,
        surrogate,
        // Filled by `dispatch_and_respond` — see `kv_incr`.
        rls_write_check: Vec::new(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard,
        plan,
        "KV_INCR_FLOAT",
        &[collection.as_str()],
        txn_ctx,
    )
    .await
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
        collection: collection.clone(),
        key: key.as_bytes().to_vec(),
        expected: expected.into_bytes(),
        new_value: new_value.into_bytes(),
        surrogate,
        // Filled by `dispatch_and_respond` — see `kv_incr`.
        rls_write_check: Vec::new(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard,
        plan,
        "KV_CAS",
        &[collection.as_str()],
        txn_ctx,
    )
    .await
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
        collection: collection.clone(),
        key: key.as_bytes().to_vec(),
        new_value: new_value.into_bytes(),
        surrogate,
        // Both filled by `dispatch_and_respond`: the read half gates the old
        // value this function returns, the write half the value it stores.
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard,
        plan,
        "KV_GETSET",
        &[collection.as_str()],
        txn_ctx,
    )
    .await
}
