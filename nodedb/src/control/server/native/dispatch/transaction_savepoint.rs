// SPDX-License-Identifier: BUSL-1.1

//! Savepoint control for the native protocol: SAVEPOINT, RELEASE SAVEPOINT,
//! and ROLLBACK TO SAVEPOINT.
//!
//! Mirrors the pgwire savepoint handler: both protocols drive the same
//! protocol-neutral `SessionStore` savepoint stack and dispatch the same
//! `MetaOp::MarkSavepoint` / `MetaOp::RollbackToSavepoint` overlay meta-ops to
//! the transaction's home vShard. Only the transport differs — pgwire returns
//! `Response` tags and `PgWireError`, native returns `NativeResponse` status
//! rows and `NativeResponse::error` with the same SQLSTATE codes.

use nodedb_types::TraceId;
use nodedb_types::id::DatabaseId;
use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils;
use crate::control::server::shared::session::TransactionState;
use nodedb_physical::physical_plan::MetaOp;

use super::DispatchCtx;

/// Reject a savepoint command issued outside a transaction block with
/// SQLSTATE 25P01 (no_active_sql_transaction), matching PostgreSQL and the
/// pgwire path. Returns `Some(error)` when there is no active transaction.
fn require_active_txn(ctx: &DispatchCtx<'_>, seq: u64) -> Option<NativeResponse> {
    if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::Idle {
        return Some(NativeResponse::error(
            seq,
            "25P01",
            "SAVEPOINT can only be used in transaction blocks",
        ));
    }
    None
}

/// Dispatch a savepoint overlay meta-op to the transaction's home vShard and
/// return the raw response payload bytes, or `None` when no staged write has
/// homed a vShard yet (the overlay — and its journal — is empty, so there is
/// nothing on the Data Plane to mark or rewind).
async fn dispatch_overlay_savepoint(ctx: &DispatchCtx<'_>, op: MetaOp) -> Option<Vec<u8>> {
    let (_txn_id, vshard) = ctx.sessions.txn_identity(ctx.peer_addr);
    let vshard_id = vshard?;
    match dispatch_utils::dispatch_to_data_plane(
        ctx.state,
        ctx.tenant_id(),
        DatabaseId::DEFAULT,
        vshard_id,
        PhysicalPlan::Meta(op),
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) => Some(resp.payload.to_vec()),
        Err(e) => {
            tracing::warn!(error = %e, "savepoint overlay meta-op dispatch failed");
            None
        }
    }
}

/// Decode the 16-byte composite savepoint marker payload: two LE u64s carrying
/// the value/TTL overlay journal marker followed by the graph overlay marker.
/// A missing or short payload means empty journals → `(0, 0)`.
fn decode_markers(payload: Option<Vec<u8>>) -> (usize, usize) {
    payload
        .filter(|bytes| bytes.len() == 16)
        .map(|bytes| {
            let mut value = [0u8; 8];
            value.copy_from_slice(&bytes[..8]);
            let mut graph = [0u8; 8];
            graph.copy_from_slice(&bytes[8..16]);
            (
                u64::from_le_bytes(value) as usize,
                u64::from_le_bytes(graph) as usize,
            )
        })
        .unwrap_or((0, 0))
}

/// Handle SAVEPOINT <name>.
pub(crate) async fn handle_savepoint(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql_trimmed: &str,
) -> NativeResponse {
    if let Some(err) = require_active_txn(ctx, seq) {
        return err;
    }
    let sp_name = sql_trimmed
        .split_whitespace()
        .nth(1)
        .unwrap_or("sp")
        .to_string();
    // Capture the composite overlay undo-journal marker on the txn's home
    // vShard so a later ROLLBACK TO reverts staged value/TTL AND graph state to
    // exactly here.
    let (value_marker, graph_marker) = match ctx.sessions.tx_id(ctx.peer_addr) {
        Some(txn_id) => {
            let payload = dispatch_overlay_savepoint(ctx, MetaOp::MarkSavepoint { txn_id }).await;
            decode_markers(payload)
        }
        None => (0, 0),
    };
    ctx.sessions
        .create_savepoint(ctx.peer_addr, sp_name, value_marker, graph_marker);
    NativeResponse::status_row(seq, "SAVEPOINT")
}

/// Handle RELEASE SAVEPOINT <name>.
///
/// RELEASE only pops the Control-Plane savepoint stack; the overlay journal
/// entries are retained (they merge into the enclosing scope), so no
/// Data-Plane meta-op is dispatched.
pub(crate) fn handle_release_savepoint(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql_trimmed: &str,
) -> NativeResponse {
    if let Some(err) = require_active_txn(ctx, seq) {
        return err;
    }
    let sp_name = sql_trimmed
        .split_whitespace()
        .last()
        .unwrap_or("sp")
        .to_string();
    match ctx.sessions.release_savepoint(ctx.peer_addr, &sp_name) {
        Ok(()) => NativeResponse::status_row(seq, "RELEASE"),
        Err(e) => NativeResponse::error(seq, "3B001", e.to_string()),
    }
}

/// Handle ROLLBACK TO SAVEPOINT <name>.
pub(crate) async fn handle_rollback_to_savepoint(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql_trimmed: &str,
) -> NativeResponse {
    if let Some(err) = require_active_txn(ctx, seq) {
        return err;
    }
    let sp_name = sql_trimmed
        .split_whitespace()
        .last()
        .unwrap_or("sp")
        .to_string();
    let (value_marker, graph_marker) =
        match ctx.sessions.rollback_to_savepoint(ctx.peer_addr, &sp_name) {
            Ok(markers) => markers,
            Err(e) => return NativeResponse::error(seq, "3B001", e.to_string()),
        };
    // Rewind BOTH the value/TTL overlay and the graph overlay to the marked
    // journal points.
    if let Some(txn_id) = ctx.sessions.tx_id(ctx.peer_addr) {
        dispatch_overlay_savepoint(
            ctx,
            MetaOp::RollbackToSavepoint {
                txn_id,
                value_marker: value_marker as u64,
                graph_marker: graph_marker as u64,
            },
        )
        .await;
    }
    NativeResponse::status_row(seq, "ROLLBACK")
}
