// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction
//! `UPDATE ... FROM <source>`.
//!
//! A transactional `BEGIN; UPDATE t SET ... FROM s WHERE t.col = s.col; COMMIT`
//! must not replay the raw `UpdateFromJoin` plan through the legacy Data-Plane
//! passthrough, whose `execute_update_from_join` writes each matched row via a
//! raw `sparse.put` in its OWN redb transaction — OUTSIDE the COMMIT batch's undo
//! log (not atomic with sibling ops / ROLLBACK) and minting no batch-tracked op
//! (so a vector/FTS-indexed target is reindexed live but the write does not ride
//! the replicated, undo-tracked point-write path).
//!
//! Instead the update is resolved at STATEMENT time (`session::expander_stage`):
//! this module ships the source rows to the source's own core, dispatches the
//! shared Data-Plane RESOLVE pass (the single classifier — never re-derived
//! here), and reuses each EXISTING target row's registered surrogate. It returns
//! the resulting per-row `PointPut` tasks; the expander then stages + buffers each
//! through the normal statement-time staging path, so they land in the
//! transaction's overlay immediately (read-your-own-writes for later statements)
//! and commit as indexed, replicated, undo-tracked point writes.
//!
//! Because the RESOLVE pass reads the TARGET as base ∪ overlay (the staged
//! transaction's id is threaded through), an `UPDATE ... FROM` affects — and
//! reuses the surrogate of — a row a prior statement in the same transaction
//! staged.
//!
//! Unlike the MERGE expander this is UPDATE-only: `UPDATE ... FROM` never inserts
//! or deletes, so there is no fresh-surrogate assignment and only a `PointPut`
//! arm. Mirrors [`crate::control::merge_orchestrator::expand_staged_merge`].

use nodedb_types::TenantId;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use crate::control::target_identity::{
    bare_collection_name, derive_document_id, require_surrogate, resolve_target_pk,
};
use crate::types::VShardId;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// One resolved UPDATE row from the RESOLVE pass: `(target storage doc_id, its
/// registered surrogate — `None` only for a legacy non-surrogate-keyed row, the
/// post-image body, the PRE-image body)`.
///
/// The pre-image is what lets the Control Plane resolve BOTH sides of a
/// materialized-sum join-key rewrite; see `encode_resolved_update_rows`.
pub(crate) type ResolvedUpdateArm = (String, Option<u32>, Vec<u8>, Vec<u8>);

/// Resolve one in-transaction `DocumentOp::UpdateFromJoin` task into the
/// concrete, surrogate-carrying `PointPut` tasks its matched target rows expand
/// to.
///
/// `task` must be an `UpdateFromJoin` plan with `task.txn_id` set to the active
/// transaction, so the RESOLVE pass (and its source scan) fold rows staged by
/// earlier statements in the same transaction. The emitted point ops carry the
/// same `txn_id` and target the TARGET collection's vShard (recomputed here so
/// dispatch classification stays honest, exactly as the MERGE / `INSERT ...
/// SELECT` expanders do). The caller stages + buffers each returned op.
pub(crate) async fn resolve_and_emit_update_from_join_ops(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection,
        rls_write_check,
        ..
    }) = &task.plan
    else {
        // Callers only pass an `UpdateFromJoin` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_update_from_join_ops: non-UPDATE-FROM task".into(),
        });
    };
    let target_collection = target_collection.clone();
    let rls_write_check = rls_write_check.clone();

    let resolved = resolve_update_rows(state, tenant_id, task).await?;

    // Gate every matched row's post-image on the target's write policy. This
    // expansion rewrites the statement into concrete `PointPut` ops the RLS
    // injection pass has already run past, so the predicate compiled into the
    // `UpdateFromJoin` plan is the last thing that can decide these rows;
    // without the check here, expanding a governed update would launder it into
    // ungoverned point writes.
    if !rls_write_check.is_empty() {
        for (_, _, body, _) in &resolved {
            crate::control::security::rls::admit_compiled_write_image(
                &rls_write_check,
                body,
                tenant_id.as_u64(),
                &target_collection,
            )?;
        }
    }

    let catalog = state.credentials.catalog();
    let target_bare = bare_collection_name(task.database_id, &target_collection);
    let target = catalog
        .get_collection(task.database_id, tenant_id.as_u64(), &target_bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id,
            collection: target_collection.clone(),
        })?;
    let target_pk = resolve_target_pk(&target, "UPDATE ... FROM")?;

    // Concrete writes land on the TARGET collection's vShard — that is where the
    // updated rows live. Recomputing it (rather than reusing the staged task's
    // vShard) keeps dispatch classification honest, exactly as the MERGE /
    // `INSERT ... SELECT` expanders do.
    let vshard_id = VShardId::from_collection_in_database(task.database_id, &target_collection);

    // Resolve this expansion's materialized-sum targets from the arms the
    // RESOLVE pass classified. BOTH images of every matched row contribute a
    // join key: an update that rewrites the join column debits the target the
    // row leaves and credits the one it joins, so resolving the post-images
    // alone would leave the abandoned target permanently overstated. Every
    // emitted point op carries the whole resolution — a row is folded against
    // the entry its own join value selects, and an entry no op needs costs one
    // unused surrogate.
    let sum_bodies: Vec<&[u8]> = resolved
        .iter()
        .flat_map(|(_, _, body, old_body)| [body.as_slice(), old_body.as_slice()])
        .collect();
    let resolved_sum_targets =
        crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies(
            state,
            &sum_bodies,
            &target_collection,
            tenant_id,
            task.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?;

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(resolved.len());
    for (doc_id, surrogate_u32, body, _old_body) in resolved {
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
                // The `UPDATE ... FROM` op owns the statement's projection; the
                // puts it expands into answer no client.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: resolved_sum_targets.clone(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        });
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
            detail: "resolve_update_rows: resolve on non-UPDATE-FROM task".into(),
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
        // Read-only resolve pass: it emits no rows to the client and writes
        // nothing, so neither policy has anything to gate here. The caller
        // decides the resolved post-images against the statement's write
        // predicate before any of them becomes a point op.
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
        // The RESOLVE pass writes nothing, so it folds no materialized-sum
        // delta. The point ops this expansion emits carry their own resolution.
        resolved_sum_targets: Vec::new(),
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
pub(crate) fn decode_resolved_update_rows(payload: &[u8]) -> crate::Result<Vec<ResolvedUpdateArm>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("update-from-join resolve rows: {e}"),
    })
}
