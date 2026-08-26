// SPDX-License-Identifier: BUSL-1.1

//! Statement-time expansion + staging of an in-transaction `MERGE`,
//! `UPDATE ... FROM <source>`, or `INSERT ... SELECT`.
//!
//! Resolved against base ∪ overlay and staged through [`stage_write`], so
//! read-your-own-writes holds. Everything else is [`ExpanderOutcome::Passthrough`].

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
    /// A not-yet-resolved in-transaction `MERGE`/`UPDATE ... FROM`/`INSERT ... SELECT`:
    /// resolved, staged, and buffered. Carries the aggregate command tag.
    Handled(InTxnRoute),
    /// Autocommit or any other plan. Hands the original task back unmodified
    /// for [`route_in_tx_write`](super::staging_gate::route_in_tx_write).
    /// Boxed so this common variant doesn't bloat the enum.
    Passthrough(Box<PhysicalTask>),
}

/// Intercept an in-transaction `MERGE`/`UPDATE ... FROM`/`INSERT ... SELECT` for
/// statement-time resolution + staging. Hands `task` back via
/// [`ExpanderOutcome::Passthrough`] otherwise, no clone needed.
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
    // Only in-transaction join-expanding DML is handled here; everything else falls through.
    if sessions.transaction_state(session_id) != TransactionState::InBlock {
        return Ok(ExpanderOutcome::Passthrough(Box::new(task)));
    }
    let (ops, kind) = match &task.plan {
        PhysicalPlan::Document(DocumentOp::Merge {
            resolved_inserts: None,
            ..
        }) => {
            // Stamp txn_id so the resolve pass folds this transaction's staging overlay.
            task.txn_id = sessions.tx_id(session_id);
            // A resolve/surrogate-assignment failure maps to the gate's `Dispatch` variant.
            let ops = resolve_and_emit_merge_ops(state, task.tenant_id, &task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::Merge)
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { .. }) => {
            // Stamp txn_id so the resolve pass folds this transaction's staging overlay.
            task.txn_id = sessions.tx_id(session_id);
            // A resolve failure maps to the gate's `Dispatch` variant.
            let ops = resolve_and_emit_update_from_join_ops(state, task.tenant_id, &task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::UpdateFromJoin)
        }
        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => {
            // Stamp txn_id so the source scan folds this transaction's staging overlay.
            task.txn_id = sessions.tx_id(session_id);
            // Reuses `StagedTagKind::Insert` — `INSERT ... SELECT` renders the `INSERT n` tag.
            let ops = resolve_and_emit_insert_select_ops(state, task.tenant_id, &task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::Insert)
        }
        // A `BatchInsert` page is autocommit-shaped; only point ops give an overlay
        // post-image, per-row undo, and row-level redo, so expand back to points.
        PhysicalPlan::Document(DocumentOp::BatchInsert { .. }) => {
            (expand_batch_insert(&task), StagedTagKind::Insert)
        }
        _ => return Ok(ExpanderOutcome::Passthrough(Box::new(task))),
    };
    Ok(ExpanderOutcome::Handled(
        stage_and_aggregate(state, sessions, session_id, ops, kind, dispatch).await?,
    ))
}

/// Expand a `BatchInsert` page into one `PointInsert` op per row. Nothing is
/// resolved — pure reshaping. A non-page plan comes back empty.
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
            // Rows home to the same vShard the page was routed to.
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                // A page carries no per-row conflict behaviour.
                if_absent: false,
                // No surrogate filled falls back to `ZERO`, leaving identity to the Data Plane.
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

/// Stage + buffer each concrete point op a resolved DML expands to,
/// aggregating affected counts into one outcome. Shared tail of
/// [`route_in_tx_expander`]'s resolve arms.
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
    // Each `stage_write` dispatches into the overlay AND buffers the concrete op
    // for COMMIT replay — the raw Merge/UpdateFromJoin/InsertSelect is never buffered.
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
