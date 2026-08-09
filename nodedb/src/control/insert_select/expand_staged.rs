// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction
//! `INSERT ... SELECT`.
//!
//! Autocommit `INSERT ... SELECT` is intercepted before the transaction path
//! and driven by [`crate::control::insert_select::run_insert_select`] (scan →
//! fresh registered surrogate per row → atomic `BatchInsert`); only an
//! `INSERT ... SELECT` executed INSIDE an explicit transaction block reaches
//! here. For those, the raw `DocumentOp::InsertSelect` plan is NOT buffered for
//! COMMIT-time replay. Instead it is resolved NOW — the source is scanned
//! against base ∪ overlay (so it folds rows this transaction staged in earlier
//! statements), and each copied row is assigned its OWN fresh, catalog-REGISTERED
//! surrogate. The concrete `DocumentOp::PointInsert` ops it expands to are staged
//! into the transaction's overlay (and buffered for COMMIT) through the exact
//! same statement-time staging path a plain in-transaction point write uses
//! ([`stage_write`](crate::control::server::shared::session::staging_gate::
//! stage_write)).
//!
//! Doing this at statement time (rather than at COMMIT) gives read-your-own-
//! writes: an in-transaction `SELECT` after the `INSERT ... SELECT` reads the
//! copied rows, and a LATER statement in the same transaction resolves against an
//! overlay that already holds them. Assigning each copied row a fresh registered
//! surrogate (surrogate registration is Control-Plane-only, under the registry
//! lock and WAL-durable) is what gives the target rows their OWN
//! `(target_collection, surrogate)→pk` catalog binding, so cross-engine (vector /
//! FTS) hits on the target resolve back to the target row's own primary key —
//! fixing the stale-source-surrogate copy the COMMIT-time expander produced.
//!
//! `PointInsert` (not `BatchInsert`) is emitted deliberately: only `PointPut` /
//! `PointInsert` / `PointDelete` have an undo-tracked arm in the transactional
//! replay path; a `BatchInsert` there falls through to the passthrough handler
//! with no undo capture, which would survive an atomic-rollback of a sibling op
//! (partial commit). Mirrors
//! [`crate::control::merge_orchestrator::expand_staged_merge`] and
//! [`crate::control::update_from_join_orchestrator::expand_staged_update_from_join`].

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::maintenance::clone_materializer::scan_source_page;
use crate::control::state::SharedState;
use crate::types::{TxnId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Resolve one in-transaction `DocumentOp::InsertSelect` task into the concrete,
/// fresh-surrogate `PointInsert` tasks its copied source rows expand to.
///
/// `task` must be an `InsertSelect` plan with `task.txn_id` set to the active
/// transaction, so the source scan folds rows staged by earlier statements in the
/// same transaction. Each copied row is assigned its OWN fresh, catalog-registered
/// surrogate. The emitted point ops carry the same `txn_id` and target the TARGET
/// collection's vShard (recomputed here so dispatch classification stays honest,
/// exactly as the MERGE / `UPDATE ... FROM` expanders do). The caller stages +
/// buffers each returned op.
///
/// Signature mirrors
/// [`resolve_and_emit_merge_ops`](crate::control::merge_orchestrator::
/// resolve_and_emit_merge_ops) /
/// [`resolve_and_emit_update_from_join_ops`](crate::control::
/// update_from_join_orchestrator::resolve_and_emit_update_from_join_ops).
pub(crate) async fn resolve_and_emit_insert_select_ops(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection,
        source_collection,
        source_filters,
        source_limit,
    }) = &task.plan
    else {
        // Callers only pass an `InsertSelect` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_insert_select_ops: non-INSERT-SELECT task".into(),
        });
    };

    // Scan the source (base ∪ overlay via the threaded `task.txn_id`) and assign
    // each copied row its OWN fresh, catalog-registered surrogate.
    let rows = materialize_copy(
        state,
        MaterializeCopy {
            tenant_id,
            database_id: task.database_id,
            target_collection,
            source_collection,
            source_filters,
            source_limit: *source_limit,
            txn_id: task.txn_id,
        },
    )
    .await?;

    // Concrete writes land on the TARGET collection's vShard — that is where the
    // copied rows live. Recomputing it (rather than reusing the staged task's
    // vShard) keeps dispatch classification honest, exactly as the MERGE /
    // `UPDATE ... FROM` expanders do.
    let vshard_id = VShardId::from_collection_in_database(task.database_id, target_collection);

    // Resolve the materialized-sum targets these copied rows credit. The point
    // ops this expansion emits are staged directly and never pass through the
    // statement-level resolution pass, so without this an in-transaction
    // `INSERT ... SELECT` into a bound collection would fold against an empty
    // resolution. Every op carries the whole resolution — a row is folded
    // against the entry its own join value selects.
    let sum_bodies: Vec<&[u8]> = rows.iter().map(|(_, value, _)| value.as_slice()).collect();
    let resolved_sum_targets =
        crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies(
            state,
            &sum_bodies,
            target_collection,
            tenant_id,
            task.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?;

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(rows.len());
    for (document_id, value, surrogate) in rows {
        out.push(PhysicalTask {
            tenant_id: task.tenant_id,
            vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: target_collection.clone(),
                document_id,
                value,
                if_absent: false,
                surrogate,
                // Expanded internal writes answer no client — see the
                // orchestrator's paged batch insert.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: resolved_sum_targets.clone(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        });
    }
    Ok(out)
}

/// Inputs for [`materialize_copy`], bundled to keep the copy pipeline within
/// argument limits.
struct MaterializeCopy<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &'a str,
    source_collection: &'a str,
    source_filters: &'a [u8],
    source_limit: usize,
    txn_id: Option<TxnId>,
}

/// Scan the source page-by-page and produce the concrete target rows:
/// `(target_document_id, msgpack_value, fresh_surrogate)`, one per surviving
/// source row. Reuses the shared [`resolve_copy_spec`] / [`assign_page_rows`]
/// pipeline (scan → normalize → filter → assign) so the strict-source
/// normalization and identity derivation stay identical to the autocommit path.
async fn materialize_copy(
    state: &SharedState,
    args: MaterializeCopy<'_>,
) -> crate::Result<Vec<(String, Vec<u8>, Surrogate)>> {
    let MaterializeCopy {
        tenant_id,
        database_id,
        target_collection,
        source_collection,
        source_filters,
        source_limit,
        txn_id,
    } = args;
    let spec = resolve_copy_spec(
        state,
        tenant_id,
        database_id,
        target_collection,
        source_collection,
        source_filters,
    )?;

    let mut cursor: Vec<u8> = Vec::new();
    let mut remaining = source_limit;
    let mut rows: Vec<(String, Vec<u8>, Surrogate)> = Vec::new();

    while remaining > 0 {
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            database_id,
            source_collection,
            &cursor,
            None,
            txn_id,
        )
        .await?;

        let page = assign_page_rows(
            state,
            tenant_id,
            database_id,
            target_collection,
            &spec,
            entries,
            &mut remaining,
        )?;
        rows.extend(page);

        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }

    Ok(rows)
}
