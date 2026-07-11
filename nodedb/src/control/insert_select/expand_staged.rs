// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time expansion of a staged `INSERT ... SELECT`.
//!
//! A transactional `BEGIN; INSERT ... SELECT ...; COMMIT` buffers the copy as a
//! single `DocumentOp::InsertSelect` plan. Left intact, COMMIT's buffered-plan
//! replay re-scans the source on the Data Plane and writes each target row under
//! the SOURCE row's surrogate — which has no `(target_collection, surrogate)→pk`
//! catalog binding, so cross-engine (vector / FTS) hits on the target can never
//! resolve back to the target row's own primary key.
//!
//! This expander rewrites every staged `InsertSelect` into concrete, per-row
//! `DocumentOp::PointInsert` writes BEFORE dispatch, exactly as the autocommit
//! orchestrator does: it scans the source, normalizes each row to msgpack, and
//! assigns each target row its OWN fresh, catalog-REGISTERED surrogate
//! (surrogate registration is Control-Plane-only, under the registry lock and
//! WAL-durable). Because the concrete writes replace the `InsertSelect` in the
//! buffered list, they commit atomically with sibling ops, ride the undo-tracked
//! transactional `PointInsert` path (so rollback of any sibling still unwinds
//! them), and replicate as concrete writes — replicas never re-derive the copy
//! and so cannot diverge on surrogate assignment.
//!
//! `PointInsert` (not `BatchInsert`) is emitted deliberately: only `PointPut` /
//! `PointInsert` / `PointDelete` have an undo-tracked arm in the transactional
//! `exec_tx_document` replay path; `BatchInsert` there falls through to the
//! passthrough handler with no undo capture, which would survive an
//! atomic-rollback of a sibling op (partial commit).

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::maintenance::clone_materializer::scan_source_page;
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Expand every staged `DocumentOp::InsertSelect` in `buffered` into concrete,
/// fresh-surrogate `PointInsert` tasks, preserving each `InsertSelect`'s
/// position and passing every other task through untouched.
///
/// Runs in the Control-Plane COMMIT path just after the buffered plans are
/// obtained and before dispatch classification, so the transaction commits
/// concrete writes rather than a re-scanned `InsertSelect`.
pub(crate) async fn expand_staged_insert_selects(
    state: &SharedState,
    tenant_id: TenantId,
    buffered: Vec<PhysicalTask>,
) -> crate::Result<Vec<PhysicalTask>> {
    // Fast path: no staged INSERT ... SELECT to expand — return the buffer as-is.
    if !buffered.iter().any(|t| {
        matches!(
            &t.plan,
            PhysicalPlan::Document(DocumentOp::InsertSelect { .. })
        )
    }) {
        return Ok(buffered);
    }

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(buffered.len());
    for task in buffered {
        let PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        }) = &task.plan
        else {
            out.push(task);
            continue;
        };

        let rows = materialize_copy(
            state,
            tenant_id,
            task.database_id,
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        )
        .await?;

        // Concrete writes land on the TARGET collection's vShard — that is where
        // the copied rows live. Recomputing it (rather than reusing the staged
        // task's vShard) keeps dispatch classification honest: `classify_dispatch`
        // runs AFTER expansion, so a copy whose target differs from a sibling op's
        // shard is correctly classified single- vs multi-shard by the real vShards.
        let vshard_id = VShardId::from_collection_in_database(task.database_id, target_collection);
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
                }),
                post_set_op: PostSetOp::None,
                txn_id: task.txn_id,
            });
        }
    }
    Ok(out)
}

/// Scan the source page-by-page and produce the concrete target rows:
/// `(target_document_id, msgpack_value, fresh_surrogate)`, one per surviving
/// source row. Reuses the shared [`resolve_copy_spec`] / [`assign_page_rows`]
/// pipeline (scan → normalize → filter → assign) so the strict-source
/// normalization and identity derivation stay identical to the autocommit path.
async fn materialize_copy(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
    source_limit: usize,
) -> crate::Result<Vec<(String, Vec<u8>, Surrogate)>> {
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
