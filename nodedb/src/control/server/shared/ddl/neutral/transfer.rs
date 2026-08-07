// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral atomic transfer SQL functions: TRANSFER (fungible) and
//! TRANSFER_ITEM (non-fungible).
//!
//! `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
//!   — Atomically: source.field -= amount, dest.field += amount.
//!   — Fails with INSUFFICIENT_BALANCE if source.field < amount.
//!   — Returns: `{ source_key, dest_key, field, amount, source_balance, dest_balance }`.
//!
//! `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
//!   — Atomically: remove item from source owner, add to dest owner.
//!   — Fails with NOT_FOUND if source doesn't own the item.
//!   — Returns: `{ item_key, dest_key, source_collection, dest_collection }`.
//!
//! Both dispatch to the Data Plane as dedicated KvOp variants. The entire
//! read-validate-write executes in a single TPC core pass — no TOCTOU race.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::super::result::{DdlError, DdlResult};
use super::kv_atomic::{dispatch_and_respond, parse_function_args, unquote};

/// Handle `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
pub async fn transfer(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "TRANSFER")?;
    if args.len() < 5 {
        return Err(ddl_err(
            "42601",
            "TRANSFER requires 5 arguments: (collection, source_key, dest_key, field, amount)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let source_key = unquote(&args[1]);
    let dest_key = unquote(&args[2]);
    let field = unquote(&args[3]);
    let amount_str = args[4].trim().to_string();
    let amount: f64 = amount_str.parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("TRANSFER: amount must be a number, got '{amount_str}'"),
        )
    })?;

    if amount <= 0.0 {
        return Err(ddl_err("42601", "TRANSFER: amount must be positive"));
    }

    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);

    // Dispatch to Data Plane — entire read+validate+write is atomic (single TPC
    // core). Routed through the protocol-neutral in-transaction staging gate
    // (`dispatch_and_respond`, shared with `KV_INCR` et al.): outside a
    // transaction it dispatches immediately, byte-identical to before;
    // inside a `BEGIN..COMMIT` block `KvOp::Transfer` is staged into the
    // per-transaction overlay so a same-transaction read observes both
    // updated balances and COMMIT durably replays the same op.
    // Content-addressed cross-engine identity per key: the debited (source)
    // row and the credited (dest) row each keep the surrogate their original
    // insert assigned. Distinct keys → distinct surrogates, so the two rows
    // never collapse onto one identity.
    let source_bytes = source_key.into_bytes();
    let dest_bytes = dest_key.into_bytes();
    let debit_surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            &source_bytes,
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let credit_surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &collection,
            &dest_bytes,
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let plan = PhysicalPlan::Kv(KvOp::Transfer {
        collection: collection.clone(),
        source_key: source_bytes,
        dest_key: dest_bytes,
        field,
        amount,
        debit_surrogate,
        credit_surrogate,
        // Filled by `dispatch_and_respond`, which runs the same RLS injection
        // pass the planner-driven path runs.
        rls_write_check: Vec::new(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard,
        plan,
        "TRANSFER",
        &[collection.as_str()],
        txn_ctx,
    )
    .await
}

/// Handle `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
pub async fn transfer_item(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "TRANSFER_ITEM")?;
    if args.len() < 5 {
        return Err(ddl_err(
            "42601",
            "TRANSFER_ITEM requires 5 arguments: (source_collection, dest_collection, item_id, source_owner, dest_owner)",
        ));
    }

    let source_collection = unquote(&args[0]).to_lowercase();
    let dest_collection = unquote(&args[1]).to_lowercase();
    let item_id = unquote(&args[2]);
    let source_owner = unquote(&args[3]);
    let dest_owner = unquote(&args[4]);

    // Cross-collection transfers must be on the same vshard.
    // Validate this upfront to prevent silent failures.
    let vshard_src = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &source_collection);
    let vshard_dst = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &dest_collection);
    if source_collection != dest_collection && vshard_src != vshard_dst {
        return Err(ddl_err(
            "0A000",
            format!(
                "TRANSFER_ITEM: cross-shard transfer not supported \
                 (source '{}' and dest '{}' map to different vShards)",
                source_collection, dest_collection
            ),
        ));
    }

    let item_key = format!("{source_owner}:{item_id}");
    let dest_key = format!("{dest_owner}:{item_id}");

    // The moved row's identity is content-addressed at its DESTINATION
    // `(dest_collection, dest_key)`, matching the engine write-back.
    let dest_bytes = dest_key.into_bytes();
    let surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            identity.tenant_id,
            &dest_collection,
            &dest_bytes,
        )
        .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // Dispatch to Data Plane — verify + delete + insert is atomic. Routed
    // through the same in-transaction staging gate as `TRANSFER` (see above).
    let plan = PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection: source_collection.clone(),
        dest_collection: dest_collection.clone(),
        item_key: item_key.into_bytes(),
        dest_key: dest_bytes,
        surrogate,
        // One predicate per side, both filled by `dispatch_and_respond`: the
        // two collections carry independent policies.
        source_rls_write_check: Vec::new(),
        dest_rls_write_check: Vec::new(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard_src,
        plan,
        "TRANSFER_ITEM",
        &[source_collection.as_str(), dest_collection.as_str()],
        txn_ctx,
    )
    .await
}

// ── Helpers ────────────────────────────────────────────────────────────

fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
