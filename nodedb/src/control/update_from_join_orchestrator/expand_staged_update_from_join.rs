// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time expansion of a staged in-transaction `UPDATE ... FROM <source>`.
//!
//! A transactional `BEGIN; UPDATE t SET ... FROM s WHERE t.col = s.col; COMMIT`
//! buffers the update as a single `DocumentOp::UpdateFromJoin` plan. Left intact,
//! COMMIT's buffered-plan replay runs it through the legacy Data-Plane
//! passthrough, whose `execute_update_from_join` writes each matched row via a
//! raw `sparse.put` in its OWN redb transaction — OUTSIDE the COMMIT batch's undo
//! log (not atomic with sibling ops / ROLLBACK) and minting no batch-tracked op
//! (so a vector/FTS-indexed target is reindexed live but the write does not ride
//! the replicated, undo-tracked point-write path).
//!
//! This expander rewrites every staged `UpdateFromJoin` into concrete,
//! surrogate-carrying `PointPut` writes BEFORE dispatch, exactly as
//! [`super::orchestrator::run_update_from_join`] does for autocommit: it ships
//! the source rows to the source's own core, dispatches the shared Data-Plane
//! RESOLVE pass (the single classifier — never re-derived here), and reuses each
//! EXISTING target row's registered surrogate. Because the concrete `PointPut`
//! ops replace the `UpdateFromJoin` in the buffered list, they commit atomically
//! with sibling ops (undo-tracked `tx_point_*` arms), ride the replicated
//! point-write path, and index into every cross-engine index.
//!
//! Unlike the MERGE expander this is UPDATE-only: `UPDATE ... FROM` never inserts
//! or deletes, so there is no fresh-surrogate assignment and only a `PointPut`
//! arm. Mirrors [`crate::control::merge_orchestrator::expand_staged_merge`].

use nodedb_types::TenantId;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::merge_orchestrator::target_surrogate::{
    bare_collection_name, derive_document_id, require_surrogate, resolve_target_pk,
};
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// One resolved UPDATE row from the RESOLVE pass: `(target storage doc_id, its
/// registered surrogate — `None` only for a legacy non-surrogate-keyed row, the
/// post-image body)`.
type ResolvedUpdateArm = (String, Option<u32>, Vec<u8>);

/// Expand every staged `DocumentOp::UpdateFromJoin` in `buffered` into concrete,
/// surrogate-carrying `PointPut` tasks, preserving each update's position and
/// passing every other task through untouched.
///
/// Runs in the Control-Plane COMMIT path just after [`MERGE`
/// expansion](crate::control::merge_orchestrator::expand_staged_merges) and
/// before dispatch classification, so the transaction commits concrete point
/// writes rather than a re-played `UpdateFromJoin` through the legacy passthrough.
pub(crate) async fn expand_staged_update_from_joins(
    state: &SharedState,
    tenant_id: TenantId,
    buffered: Vec<PhysicalTask>,
) -> crate::Result<Vec<PhysicalTask>> {
    // Fast path: no staged UPDATE ... FROM to expand — return the buffer as-is.
    if !buffered.iter().any(|t| {
        matches!(
            &t.plan,
            PhysicalPlan::Document(DocumentOp::UpdateFromJoin { .. })
        )
    }) {
        return Ok(buffered);
    }

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(buffered.len());
    for task in buffered {
        let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection, ..
        }) = &task.plan
        else {
            out.push(task);
            continue;
        };
        let target_collection = target_collection.clone();

        let resolved = resolve_update_rows(state, tenant_id, &task).await?;

        let catalog = state.credentials.catalog();
        let target_bare = bare_collection_name(task.database_id, &target_collection);
        let target = catalog
            .get_collection(task.database_id, tenant_id.as_u64(), &target_bare)?
            .ok_or_else(|| crate::Error::CollectionNotFound {
                tenant_id,
                collection: target_collection.clone(),
            })?;
        let target_pk = resolve_target_pk(&target)?;

        // Concrete writes land on the TARGET collection's vShard — that is where
        // the updated rows live. Recomputing it (rather than reusing the staged
        // task's vShard) keeps dispatch classification honest, exactly as the
        // MERGE / `INSERT ... SELECT` expanders do.
        let vshard_id = VShardId::from_collection_in_database(task.database_id, &target_collection);

        for (doc_id, surrogate_u32, body) in resolved {
            let surrogate = require_surrogate(surrogate_u32, &doc_id, "UPDATE ... FROM")?;
            let document_id = derive_document_id(&target_pk, &body, surrogate);
            let pk_bytes = document_id.clone().into_bytes();
            out.push(PhysicalTask {
                tenant_id: task.tenant_id,
                vshard_id,
                database_id: task.database_id,
                plan: PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: target_collection.clone(),
                    document_id,
                    value: body,
                    surrogate,
                    pk_bytes,
                }),
                post_set_op: PostSetOp::None,
                txn_id: task.txn_id,
            });
        }
    }
    Ok(out)
}

/// Ship the source rows and dispatch the shared Data-Plane RESOLVE pass for one
/// staged update, decoding the matched target rows. Never re-derives the join /
/// assignment locally — `collect_update_from_join_rows` on the Data Plane is the
/// single shared classifier for both this path and the write path.
async fn resolve_update_rows(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<ResolvedUpdateArm>> {
    let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        updates,
        target_filters,
        ..
    }) = &task.plan
    else {
        // Callers only pass an `UpdateFromJoin` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "expand_staged_update_from_joins: resolve on non-UPDATE-FROM task".into(),
        });
    };

    // Phase 0: read the SOURCE where it lives (its vShard can map to a different
    // Data-Plane core than the target's) and ship the raw rows into the plan.
    // Threading the staged transaction's id folds the source's own staging
    // overlay, so a source row inserted/updated earlier in this transaction is
    // shipped too.
    let source_rows = read_all_source_rows(
        state,
        tenant_id,
        task.database_id,
        source_collection,
        task.txn_id,
    )
    .await?;

    // Phase 1: dispatch the read-only RESOLVE pass against the target's core.
    let resolve_plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection: target_collection.clone(),
        source_collection: source_collection.clone(),
        source_alias: source_alias.clone(),
        target_join_col: target_join_col.clone(),
        source_join_col: source_join_col.clone(),
        updates: updates.clone(),
        target_filters: target_filters.clone(),
        returning: None,
        resolve_only: true,
        source_rows: Some(source_rows),
    });
    // The RESOLVE pass reads the TARGET as base ∪ overlay: passing the staged
    // transaction's id lets the target scan fold rows this transaction staged
    // earlier, so an `UPDATE ... FROM` affects a row a prior statement in the
    // same transaction inserted.
    let resolve_resp = dispatch_local(
        state,
        tenant_id,
        task.database_id,
        target_collection,
        resolve_plan,
        task.txn_id,
    )
    .await?;
    if resolve_resp.status != Status::Ok {
        return Err(crate::Error::Dispatch {
            detail: format!(
                "in-transaction UPDATE ... FROM resolve failed: {:?}",
                resolve_resp.error_code
            ),
        });
    }
    decode_resolved_update_rows(&resolve_resp.payload)
}

/// Decode the RESOLVE pass payload (a msgpack `Vec<(doc_id, Option<surrogate>,
/// post_image_body)>`; see `encode_resolved_update_rows`).
fn decode_resolved_update_rows(payload: &[u8]) -> crate::Result<Vec<ResolvedUpdateArm>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("update-from-join resolve rows: {e}"),
    })
}
