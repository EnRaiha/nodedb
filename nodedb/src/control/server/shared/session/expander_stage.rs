// SPDX-License-Identifier: BUSL-1.1

//! Statement-time expansion + staging of an in-transaction `MERGE`,
//! `UPDATE ... FROM <source>`, or `INSERT ... SELECT`.
//!
//! Autocommit `MERGE` / `UPDATE ... FROM` / `INSERT ... SELECT` is intercepted
//! before this seam and driven by
//! [`crate::control::merge_orchestrator::run_merge`] /
//! [`crate::control::update_from_join_orchestrator::run_update_from_join`] /
//! [`crate::control::insert_select::run_insert_select`]; only such a DML executed
//! INSIDE an explicit transaction block reaches here. For those, the raw
//! `DocumentOp::Merge` / `DocumentOp::UpdateFromJoin` / `DocumentOp::InsertSelect`
//! plan is NOT buffered for COMMIT-time replay. Instead it is resolved NOW —
//! against base ∪ overlay, so it sees rows this transaction staged in earlier
//! statements — and the concrete `PointInsert` / `PointPut` / `PointDelete` ops it
//! expands to are staged into the transaction's overlay (and buffered for COMMIT)
//! through the exact same statement-time staging path a plain in-transaction point
//! write uses ([`stage_write`]). (`UPDATE ... FROM` only ever emits `PointPut`
//! ops; `INSERT ... SELECT` only ever emits fresh-surrogate `PointInsert` ops.)
//!
//! Doing this at statement time (rather than at COMMIT) makes base == overlay
//! universally: a LATER statement in the same transaction (e.g. an `UPDATE` of a
//! row the merge inserted) resolves against an overlay that already holds the
//! post-image, and an in-transaction `SELECT` after the DML reads its effect
//! (read-your-own-writes) — neither of which the COMMIT-time expander could offer.
//!
//! This is a seam called from the two SQL-planned dispatch loops BEFORE the
//! protocol-neutral [`route_in_tx_write`](super::staging_gate::route_in_tx_write):
//! resolve-and-stage needs `SharedState` (dispatcher / surrogate assigner /
//! catalog) that `route_in_tx_write` deliberately does not hold. Every other task
//! (and everything in autocommit) comes back as [`ExpanderOutcome::Passthrough`]
//! and falls through to `route_in_tx_write` unchanged.

use std::future::Future;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::insert_select::resolve_and_emit_insert_select_ops;
use crate::control::merge_orchestrator::resolve_and_emit_merge_ops;
use crate::control::state::SharedState;
use crate::control::update_from_join_orchestrator::resolve_and_emit_update_from_join_ops;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::Surrogate;

use super::connection::SessionId;
use super::staging_gate::{
    InTxnRoute, StagedTagKind, StagedWriteOutcome, StagingGateError, stage_write,
};
use super::state::TransactionState;
use super::store::SessionStore;

/// Outcome of [`route_in_tx_expander`].
pub(crate) enum ExpanderOutcome {
    /// `task` was a not-yet-resolved in-transaction `MERGE`, `UPDATE ... FROM`,
    /// or `INSERT ... SELECT`: resolved, staged, and buffered. Carries the
    /// aggregate command tag.
    Handled(InTxnRoute),
    /// Autocommit, an already-resolved `MERGE` / `UPDATE ... FROM` /
    /// `INSERT ... SELECT`, or any other plan.
    /// Hands the original task back — unmodified, no clone taken — for the
    /// caller to route through [`route_in_tx_write`](
    /// super::staging_gate::route_in_tx_write). Boxed so the common
    /// passthrough variant does not bloat this enum to a full `PhysicalTask`.
    Passthrough(Box<PhysicalTask>),
}

/// Intercept an in-transaction `MERGE` / `UPDATE ... FROM` / `INSERT ... SELECT`
/// for statement-time resolution + staging.
///
/// Takes `task` by value and hands it back via [`ExpanderOutcome::Passthrough`]
/// for every case that isn't a not-yet-resolved in-transaction join-expanding
/// DML, so callers never need to clone `task` just to probe whether this seam
/// applies.
///
/// `dispatch` is invoked once per emitted point op (hence `Fn`, not `FnOnce`),
/// with a `MetaOp::StageWrite` task wrapping that op — the same closure the
/// caller passes to `route_in_tx_write`.
pub(crate) async fn route_in_tx_expander<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    mut task: PhysicalTask,
    dispatch: F,
) -> Result<ExpanderOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    // Only an in-transaction `MERGE` / `UPDATE ... FROM` / `INSERT ... SELECT`
    // is handled here. Autocommit and every other plan fall through
    // (`Passthrough`) to the neutral staging gate.
    if sessions.transaction_state(session_id) != TransactionState::InBlock {
        return Ok(ExpanderOutcome::Passthrough(Box::new(task)));
    }
    let (ops, kind) = match &task.plan {
        PhysicalPlan::Document(DocumentOp::Merge {
            resolve_only: false,
            resolved_inserts: None,
            ..
        }) => {
            // Stamp the active transaction id so the RESOLVE pass (and its
            // source scan) fold this transaction's staging overlay: a MERGE
            // matches — and reuses the surrogate of — a row an earlier
            // statement in the same transaction staged.
            task.txn_id = sessions.tx_id(session_id);
            // Resolve the merge and derive the concrete point ops. A resolve
            // / surrogate-assignment failure is a genuine dispatch error; map
            // it into the gate's `Dispatch` variant so the caller renders it
            // exactly like any other in-transaction write failure.
            let ops = resolve_and_emit_merge_ops(state, task.tenant_id, &task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::Merge)
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            resolve_only: false,
            ..
        }) => {
            // Stamp the active transaction id so the RESOLVE pass (and its
            // source scan) fold this transaction's staging overlay: an
            // `UPDATE ... FROM` matches — and reuses the surrogate of — a row
            // an earlier statement in the same transaction staged.
            task.txn_id = sessions.tx_id(session_id);
            // Resolve the update and derive the concrete point ops. A resolve
            // failure is a genuine dispatch error; map it into the gate's
            // `Dispatch` variant so the caller renders it exactly like any
            // other in-transaction write failure.
            let ops = resolve_and_emit_update_from_join_ops(state, task.tenant_id, &task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::UpdateFromJoin)
        }
        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => {
            // Stamp the active transaction id so the source scan folds this
            // transaction's staging overlay: an `INSERT ... SELECT` copies a
            // row an earlier statement in the same transaction staged.
            task.txn_id = sessions.tx_id(session_id);
            // Resolve the copy and derive the concrete, fresh-surrogate
            // `PointInsert` ops. A resolve / surrogate-assignment failure is a
            // genuine dispatch error; map it into the gate's `Dispatch` variant
            // so the caller renders it exactly like any other in-transaction
            // write failure. `INSERT ... SELECT` renders the `INSERT n` tag, so
            // it reuses `StagedTagKind::Insert`.
            let ops = resolve_and_emit_insert_select_ops(state, task.tenant_id, &task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::Insert)
        }
        // A `BatchInsert` page is an AUTOCOMMIT shape. It exists so that a
        // multi-row INSERT into a collection with a statement-scoped constraint
        // (BALANCED) is ONE Data-Plane request and therefore one boundary,
        // instead of one request per row. Inside a transaction the enclosing
        // COMMIT batch already IS that boundary — entries accumulate across
        // statements — so the page buys nothing and costs what only point ops
        // have: an overlay post-image (read-your-own-writes for later
        // statements in the same transaction), a per-row undo entry (so a
        // constraint refused at COMMIT actually rolls the rows back), and a
        // row-level redo shape (`classify_document_op` rejects a page outright
        // — it has no staged post-image).
        //
        // So the page is expanded back into its constituent point inserts here,
        // exactly as `INSERT ... SELECT` above is: same seam, same staging
        // path, same reason.
        PhysicalPlan::Document(DocumentOp::BatchInsert { .. }) => {
            (expand_batch_insert(&task), StagedTagKind::Insert)
        }
        _ => return Ok(ExpanderOutcome::Passthrough(Box::new(task))),
    };
    Ok(ExpanderOutcome::Handled(
        stage_and_aggregate(state, sessions, session_id, ops, kind, dispatch).await?,
    ))
}

/// Expand a `BatchInsert` page into one `PointInsert` op per row.
///
/// Nothing is resolved: the page already carries each row's document id, body
/// and surrogate, so this is a pure reshaping of work the planner already did.
/// Every op carries the page's whole materialized-sum resolution — a row folds
/// against the entry its own join value selects — and its `deferred_sum_targets`,
/// which name target COLLECTIONS and so apply to every row alike.
///
/// A plan that is not a page comes back empty, which stages nothing; the caller
/// only reaches this on the arm that matched one.
fn expand_batch_insert(task: &PhysicalTask) -> Vec<PhysicalTask> {
    let PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection,
        documents,
        surrogates,
        returning,
        rls_filters,
        resolved_sum_targets,
        deferred_sum_targets,
        ..
    }) = &task.plan
    else {
        return Vec::new();
    };
    documents
        .iter()
        .enumerate()
        .map(|(i, (document_id, value))| PhysicalTask {
            tenant_id: task.tenant_id,
            // The page and its rows are the same collection, so they home to
            // the same vShard the page was routed to.
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                // A page carries no per-row conflict behaviour, so neither does
                // any op it expands to.
                if_absent: false,
                // Parallel to `documents` when present. A page whose producer
                // filled no surrogates carries the documented `ZERO` sentinel,
                // which leaves the row's identity to the Data Plane exactly as
                // the page itself would have.
                surrogate: surrogates.get(i).copied().unwrap_or(Surrogate::ZERO),
                returning: returning.clone(),
                rls_filters: rls_filters.clone(),
                resolved_sum_targets: resolved_sum_targets.clone(),
                deferred_sum_targets: deferred_sum_targets.clone(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        })
        .collect()
}

/// Stage + buffer each concrete point op a resolved `MERGE` / `UPDATE ...
/// FROM` / `INSERT ... SELECT` expands to, aggregating the per-op affected
/// counts into one staged outcome for the whole statement. Shared tail of
/// [`route_in_tx_expander`]'s resolve arms — they differ only in which
/// `resolve_and_emit_*` fn produced `ops` and which [`StagedTagKind`] the result
/// carries.
async fn stage_and_aggregate<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    ops: Vec<PhysicalTask>,
    kind: StagedTagKind,
    dispatch: F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    // Stage + buffer each point op through the shared statement-time path. Each
    // `stage_write` dispatches a `MetaOp::StageWrite` into the overlay (real
    // statement-time constraint errors) AND buffers the concrete op for COMMIT's
    // durable replay — the raw `Merge` / `UpdateFromJoin` / `InsertSelect` is
    // never buffered.
    let mut affected = 0usize;
    for op in ops {
        let outcome = stage_write(state, sessions, session_id, op, &dispatch).await?;
        affected += outcome.affected;
    }

    Ok(InTxnRoute::Staged(StagedWriteOutcome {
        kind,
        affected,
        payload: Vec::new(),
    }))
}
