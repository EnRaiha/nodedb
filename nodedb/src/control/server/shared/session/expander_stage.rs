// SPDX-License-Identifier: BUSL-1.1

//! Statement-time expansion + staging of an in-transaction `MERGE`.
//!
//! Autocommit `MERGE` is intercepted before this seam and driven by
//! [`crate::control::merge_orchestrator::run_merge`]; only a `MERGE` executed
//! INSIDE an explicit transaction block reaches here. For those, the raw
//! `DocumentOp::Merge` plan is NOT buffered for COMMIT-time replay. Instead the
//! merge is resolved NOW — against base ∪ overlay, so it sees rows this
//! transaction staged in earlier statements — and the concrete
//! `PointInsert` / `PointPut` / `PointDelete` ops it expands to are staged into
//! the transaction's overlay (and buffered for COMMIT) through the exact same
//! statement-time staging path a plain in-transaction point write uses
//! ([`stage_write`]).
//!
//! Doing this at statement time (rather than at COMMIT) makes base == overlay
//! universally: a LATER statement in the same transaction (e.g. an `UPDATE` of a
//! row the merge inserted) resolves against an overlay that already holds the
//! merge's post-image, and an in-transaction `SELECT` after the `MERGE` reads
//! the merge-inserted rows (read-your-own-writes) — neither of which the
//! COMMIT-time expander could offer.
//!
//! This is a NEW seam, called from the two SQL-planned dispatch loops BEFORE the
//! protocol-neutral [`route_in_tx_write`](super::staging_gate::route_in_tx_write):
//! resolve-and-stage needs `SharedState` (dispatcher / surrogate assigner /
//! catalog) that `route_in_tx_write` deliberately does not hold. Non-MERGE tasks
//! (and everything in autocommit) come back as [`ExpanderOutcome::Passthrough`]
//! and fall through to `route_in_tx_write` unchanged.

use std::future::Future;
use std::net::SocketAddr;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::merge_orchestrator::resolve_and_emit_merge_ops;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::PhysicalTask;

use super::staging_gate::{
    InTxnRoute, StagedTagKind, StagedWriteOutcome, StagingGateError, stage_write,
};
use super::state::TransactionState;
use super::store::SessionStore;

/// Outcome of [`route_in_tx_expander`].
pub(crate) enum ExpanderOutcome {
    /// `task` was a not-yet-resolved in-transaction `MERGE`: resolved,
    /// staged, and buffered. Carries the aggregate command tag.
    Handled(InTxnRoute),
    /// Autocommit, an already-resolved `MERGE`, or any non-`MERGE` plan.
    /// Hands the original task back — unmodified, no clone taken — for the
    /// caller to route through [`route_in_tx_write`](
    /// super::staging_gate::route_in_tx_write). Boxed so the common
    /// passthrough variant does not bloat this enum to a full `PhysicalTask`.
    Passthrough(Box<PhysicalTask>),
}

/// Intercept an in-transaction `MERGE` for statement-time resolution + staging.
///
/// Takes `task` by value and hands it back via [`ExpanderOutcome::Passthrough`]
/// for every case that isn't a not-yet-resolved in-transaction `MERGE`, so
/// callers never need to clone `task` just to probe whether this seam applies.
///
/// `dispatch` is invoked once per emitted point op (hence `Fn`, not `FnOnce`),
/// with a `MetaOp::StageWrite` task wrapping that op — the same closure the
/// caller passes to `route_in_tx_write`.
pub(crate) async fn route_in_tx_expander<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    addr: &SocketAddr,
    task: PhysicalTask,
    dispatch: F,
) -> Result<ExpanderOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    // Only in-transaction MERGE is handled here (U2). Autocommit and non-MERGE
    // fall through (`Passthrough`) to the neutral staging gate.
    if sessions.transaction_state(addr) != TransactionState::InBlock {
        return Ok(ExpanderOutcome::Passthrough(Box::new(task)));
    }
    match &task.plan {
        PhysicalPlan::Document(DocumentOp::Merge {
            resolve_only: false,
            resolved_inserts: None,
            ..
        }) => Ok(ExpanderOutcome::Handled(
            resolve_and_stage_merge(state, sessions, addr, task, dispatch).await?,
        )),
        _ => Ok(ExpanderOutcome::Passthrough(Box::new(task))),
    }
}

/// Resolve the MERGE at statement time and stage + buffer each concrete point
/// op it expands to, aggregating the per-arm affected counts into one staged
/// outcome for the whole statement.
async fn resolve_and_stage_merge<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    addr: &SocketAddr,
    mut task: PhysicalTask,
    dispatch: F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    // Stamp the active transaction id so the RESOLVE pass (and its source scan)
    // fold this transaction's staging overlay: a MERGE matches — and reuses the
    // surrogate of — a row an earlier statement in the same transaction staged.
    task.txn_id = sessions.tx_id(addr);

    // Resolve the merge and derive the concrete point ops. A resolve /
    // surrogate-assignment failure is a genuine dispatch error; map it into the
    // gate's `Dispatch` variant so the caller renders it exactly like any other
    // in-transaction write failure.
    let ops = resolve_and_emit_merge_ops(state, task.tenant_id, &task)
        .await
        .map_err(StagingGateError::Dispatch)?;

    // Stage + buffer each point op through the shared statement-time path. Each
    // `stage_write` dispatches a `MetaOp::StageWrite` into the overlay (real
    // statement-time constraint errors) AND buffers the concrete op for COMMIT's
    // durable replay — the raw `Merge` is never buffered.
    let mut affected = 0usize;
    for op in ops {
        // `stage_write` only ever returns `Staged` (or propagates an `Err`
        // before returning at all) -- `Read` / `Buffered` are its callers'
        // OTHER return paths in `route_in_tx_write`, never `stage_write`'s
        // own. Panic loudly rather than silently guess an affected count if
        // that invariant is ever broken.
        let InTxnRoute::Staged(outcome) = stage_write(sessions, addr, op, &dispatch).await? else {
            unreachable!("stage_write returned a non-Staged InTxnRoute");
        };
        affected += outcome.affected;
    }

    Ok(InTxnRoute::Staged(StagedWriteOutcome {
        kind: StagedTagKind::Merge,
        affected,
        payload: Vec::new(),
    }))
}
