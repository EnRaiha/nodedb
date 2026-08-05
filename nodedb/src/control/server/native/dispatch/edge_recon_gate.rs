// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge OLLP/Calvin routing gate for the native protocol.
//!
//! A dependent predicate (`BulkDelete`/`BulkUpdate`) on an edge-bearing
//! schemaless collection MUST route through the OLLP/Calvin coordinator so the
//! implicit edge tasks are derived from a pre-exec reconnaissance scan and
//! committed ATOMICALLY with the document write. Without this gate the native
//! path classifies such a delete as `SingleShard` and dispatches directly to
//! the Data Plane — leaving mirrored CSR edges dangling.
//!
//! This module exposes a single entry point: [`try_edge_recon_dispatch`], which
//! implements the same three-guard check as the pgwire `execute.rs` gate and
//! returns the native protocol outcome when the gate fires.

use nodedb_types::protocol::NativeResponse;

use crate::control::planner::calvin::{
    dispatch_authorized_dependent_edge_recon, plan_needs_implicit_edge_recon,
};
use crate::control::server::shared::authorization::AuthorizedTaskSet;
use crate::control::server::shared::session::TransactionState;
use nodedb_physical::physical_task::PhysicalTask;

use super::{DispatchCtx, SqlOutcome, error_to_native};

/// Attempt to route `tasks` through the implicit-edge OLLP/Calvin path.
///
/// Returns `Some(outcome)` when the gate fires (the task set contains a
/// `BulkDelete`/`BulkUpdate` on an edge-bearing collection that is not inside
/// an explicit transaction block and the Calvin sequencer registry is up). The
/// caller MUST return this outcome immediately — the tasks have been consumed.
///
/// Returns `None` when the gate does not fire; the caller proceeds with the
/// normal classify/dispatch path.
///
/// A genuine catalog I/O error propagates as `Some(Err-shaped SqlOutcome)` so
/// the caller surfaces it correctly — misrouting on a real I/O fault would
/// silently skip edge cleanup (dangling edges).
pub(super) async fn try_edge_recon_dispatch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
) -> EdgeReconResult {
    // Guard 1: not inside an explicit transaction block.  The native
    // `handle_begin`/`handle_commit`/`handle_rollback` helpers drive the SAME
    // `SessionStore` transaction-state machine that pgwire uses, so this guard
    // is identical across both protocol paths.  Edge-bearing predicate writes
    // inside an explicit native transaction block are NOT recon-routed (same
    // limitation as pgwire — buffering a multi-step OLLP inside an explicit txn
    // would require full two-phase commit across the outer txn boundary).
    if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock {
        return EdgeReconResult::NotFired(tasks, authorized);
    }

    // Guard 2: Calvin completion registry available (sequencer is up).
    if ctx.state.calvin_completion_registry.get().is_none() {
        return EdgeReconResult::NotFired(tasks, authorized);
    }

    // Guard 3: at least one BulkDelete/BulkUpdate targets an edge-bearing
    // collection.  A genuine catalog I/O error propagates rather than falling
    // through — misrouting on a real fault would skip edge cleanup.
    let (_coll, database_id) =
        match plan_needs_implicit_edge_recon(ctx.state, &tasks, ctx.tenant_id()) {
            Err(e) => return EdgeReconResult::Outcome(resp(error_to_native(seq, &e))),
            Ok(None) => return EdgeReconResult::NotFired(tasks, authorized),
            Ok(Some(pair)) => pair,
        };

    // Capture the batch's plans before `tasks` is moved into the recon call, so
    // the response can shape a RETURNING dependent delete/update's rows and read
    // a plain write's affected count from the applied Response the coordinator
    // drains.
    let plans: Vec<_> = tasks.iter().map(|t| t.plan.clone()).collect();

    // All three guards passed — run the OLLP/Calvin coordinator. This is the
    // normal multi-shard OLLP path (NOT the contended single-shard route from
    // `route_write_to_calvin`), so it stays on the strict multi-vshard
    // dependent `TxClass` builder (`allow_single_vshard: false`).
    let outcome = dispatch_authorized_dependent_edge_recon(
        ctx.state,
        authorized,
        ctx.identity,
        ctx.tenant_id(),
        database_id,
        false,
    )
    .await;

    EdgeReconResult::Outcome(match outcome {
        Ok(recon) => {
            // A RETURNING dependent write surfaces its deleted/updated rows; a
            // plain write reports the affected count its own mutation returned.
            resp(super::conversion::calvin_native_response(
                seq,
                recon.apply_result,
                &plans,
                ctx.state,
                database_id,
                ctx.tenant_id(),
                ctx.auth_context(),
            ))
        }
        Err(e) => resp(error_to_native(seq, &e)),
    })
}

/// Result returned by [`try_edge_recon_dispatch`].
pub(super) enum EdgeReconResult {
    /// Gate did not fire; caller receives the task list back and continues
    /// normal dispatch.
    NotFired(Vec<PhysicalTask>, AuthorizedTaskSet),
    /// Gate fired; caller must return this outcome immediately.
    Outcome(SqlOutcome),
}

fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
}
