// SPDX-License-Identifier: BUSL-1.1

//! Reshape an autocommit-shaped document upsert page or vector-primary page
//! into the point writes a transaction stages.
//!
//! A page writes every row of one statement as one request. Inside a
//! transaction each row needs its own overlay post-image, undo entry, redo
//! entry and row-trigger firing, so the page expands back into one point
//! write per row. Nothing is resolved here: the rows are the page's rows.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, VectorDirectWriteIntent, VectorOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::staging_gate::StagedTagKind;

/// The point writes of a vector-primary `DirectBatchWrite` page, in page
/// order, and the tag the statement renders. `None` for any other plan.
pub fn expand_vector_page(task: &PhysicalTask) -> Option<(Vec<PhysicalTask>, StagedTagKind)> {
    let PhysicalPlan::Vector(
        op @ VectorOp::DirectBatchWrite {
            intent,
            on_conflict_updates,
            ..
        },
    ) = &task.plan
    else {
        return None;
    };
    let kind = match intent {
        VectorDirectWriteIntent::Insert | VectorDirectWriteIntent::InsertIfAbsent => {
            StagedTagKind::Insert
        }
        // Each row decides its verb; the rows fold to one tag.
        VectorDirectWriteIntent::Upsert if !on_conflict_updates.is_empty() => {
            StagedTagKind::KvUpsert { updated: true }
        }
        VectorDirectWriteIntent::Upsert => StagedTagKind::Upsert,
    };
    let tasks = op
        .direct_batch_point_writes()?
        .into_iter()
        .map(|point| PhysicalTask {
            tenant_id: task.tenant_id,
            // Rows home to the same vShard the page was routed to.
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Vector(point),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        })
        .collect();
    Some((tasks, kind))
}

/// One `Upsert` per row of a `BatchUpsert` page, in page order. A non-page
/// plan comes back empty. A page whose rows and surrogates differ in count
/// is refused: every row carries its own bound surrogate.
pub fn expand_batch_upsert(task: &PhysicalTask) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::BatchUpsert {
        collection,
        documents,
        surrogates,
        on_conflict_updates,
        rls_write_check,
        returning,
        rls_filters,
        resolved_sum_targets,
    }) = &task.plan
    else {
        return Ok(Vec::new());
    };
    if documents.len() != surrogates.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "batch upsert into '{}' carries {} documents but {} surrogates; every row \
                 must carry its own surrogate",
                collection.as_str(),
                documents.len(),
                surrogates.len(),
            ),
        });
    }
    Ok(documents
        .iter()
        .zip(surrogates.iter().copied())
        .map(|((document_id, value), surrogate)| PhysicalTask {
            tenant_id: task.tenant_id,
            // Rows home to the same vShard the page was routed to.
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::Upsert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                on_conflict_updates: on_conflict_updates.clone(),
                surrogate,
                // A key the page names twice is present in the overlay when
                // its later row stages, so that row merges as the page does.
                rls_write_check: rls_write_check.clone(),
                returning: returning.clone(),
                rls_filters: rls_filters.clone(),
                resolved_sum_targets: resolved_sum_targets.clone(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        })
        .collect())
}
