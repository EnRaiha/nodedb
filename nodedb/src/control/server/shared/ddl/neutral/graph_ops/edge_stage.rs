// SPDX-License-Identifier: BUSL-1.1

//! In-transaction staging for single-home graph-edge writes issued through the
//! `GRAPH ... EDGE` DSL.
//!
//! The `GRAPH INSERT EDGE` / `GRAPH DELETE EDGE` handlers dispatch a single
//! `GraphOp::EdgePut` / `EdgeDelete` directly to the Data Plane in autocommit.
//! Inside an explicit `BEGIN..COMMIT` block that direct dispatch would apply
//! the write DURABLY at statement time, so an in-transaction `MATCH` / `GRAPH
//! NEIGHBORS` would not observe it as staged (breaking read-your-own-writes)
//! and a ROLLBACK could not undo it. This helper instead routes the write
//! through the protocol-neutral staging gate
//! ([`route_in_tx_write`](crate::control::server::shared::session::staging_gate::route_in_tx_write)),
//! exactly like every other in-transaction point write: the Data Plane stages
//! the edge into the per-transaction `GraphTxnOverlay` (merged by Neighbors /
//! Hop for RYOW), the plan is buffered for COMMIT's durable replay, and
//! ROLLBACK drops the overlay.
//!
//! Only a SINGLE-HOME edge (both endpoints on one vShard, or single-node) is
//! stageable this way — the buffered `GraphOp` commits through the single-shard
//! WAL + `TransactionBatch` path. A cross-shard (dual-home) edge inside an
//! explicit transaction needs the cross-shard-commit machinery and is rejected
//! by the caller before reaching here.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// Stage a single-home graph-edge write (`GraphOp::EdgePut` / `EdgeDelete`)
/// into the active transaction's overlay through the neutral staging gate.
///
/// Caller invariant: the session is `InBlock` and `plan` is a stageable
/// single-home `GraphOp` write. Returns `Ok(())` once the write is staged +
/// buffered; a staging rejection or dispatch failure maps to a [`DdlError`].
pub(super) async fn stage_edge_write_in_txn(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard: VShardId,
    plan: PhysicalPlan,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<(), DdlError> {
    let task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };

    let routed = route_in_tx_write(txn_ctx.sessions, txn_ctx.addr, task, |staged| {
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

    match routed {
        // Edge writes are stageable (`is_stageable_write`), so inside a
        // transaction block the gate always returns `Staged`. `Read` (not in a
        // block) and `Buffered` (non-stageable write) cannot occur for a
        // caller that already checked `InBlock`; treat them as a successful
        // no-op tag rather than panicking.
        Ok(InTxnRoute::Staged(_)) | Ok(InTxnRoute::Read(_)) | Ok(InTxnRoute::Buffered) => Ok(()),
        Err(StagingGateError::Dispatch(e)) => Err(ddl_err("XX000", e.to_string())),
        Err(StagingGateError::Rejected { code }) => {
            let (_, sqlstate, message) = match code {
                Some(code) => {
                    crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code)
                }
                None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
            };
            Err(ddl_err(sqlstate, message))
        }
    }
}
