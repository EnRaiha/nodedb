// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction `MERGE`.
//!
//! A transactional `BEGIN; MERGE INTO t USING s ...; ...; COMMIT` must not
//! replay the raw `Merge` plan through the legacy Data-Plane passthrough, which
//! writes the NOT-MATCHED inserts under a raw `sparse.put` with NO surrogate
//! (never indexed — invisible to vector/FTS search), whose `to_replicated_entry`
//! returns `None` (the whole row is lost on a WAL-only restart), and outside the
//! COMMIT batch's undo log (not atomic with sibling ops).
//!
//! Instead the MERGE is resolved at STATEMENT time (`session::expander_stage`):
//! this module ships the source rows to the source's own core, dispatches the
//! shared Data-Plane RESOLVE pass (the single classifier — never re-derived
//! here), assigns each inserted row its OWN fresh, catalog-REGISTERED surrogate,
//! and reuses the EXISTING target row's registered surrogate for updates/deletes.
//! It returns the resulting per-row `PointInsert` / `PointPut` / `PointDelete`
//! tasks; the expander then stages + buffers each through the normal
//! statement-time staging path, so they land in the transaction's overlay
//! immediately (read-your-own-writes for later statements) and commit as
//! indexed, replicated, undo-tracked point writes.
//!
//! Because the RESOLVE pass reads the TARGET as base ∪ overlay (the staged
//! transaction's id is threaded through), a MERGE matches — and reuses the
//! surrogate of — a row a prior statement in the same transaction staged.
//!
//! Mirrors [`super::orchestrator::run_merge`]'s identity derivation for
//! autocommit; the two drivers share [`crate::control::target_identity`].

use nodedb_types::TenantId;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::resolve_arms::{ResolvedMergeArms, decode_resolve};
use crate::control::target_identity::{
    TargetPk, assign_target_surrogate, bare_collection_name, derive_document_id, require_surrogate,
    resolve_target_pk,
};

/// Resolve one in-transaction `DocumentOp::Merge` task into the concrete,
/// surrogate-carrying `PointInsert` / `PointPut` / `PointDelete` tasks its three
/// arms expand to.
///
/// `task` must be a `Merge` plan with `task.txn_id` set to the active
/// transaction, so the RESOLVE pass (and its source scan) fold rows staged by
/// earlier statements in the same transaction. The emitted point ops carry the
/// same `txn_id` and target the TARGET collection's vShard (recomputed here so
/// dispatch classification stays honest, exactly as the `INSERT ... SELECT`
/// expander does). The caller stages + buffers each returned op.
pub(crate) async fn resolve_and_emit_merge_ops(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection,
        rls_write_check,
        ..
    }) = &task.plan
    else {
        // Callers only pass a `Merge` task; a mismatch is a programmer error.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_merge_ops: non-MERGE task".into(),
        });
    };
    let target_collection = target_collection.clone();
    let rls_write_check = rls_write_check.clone();

    let arms = resolve_merge_arms(state, tenant_id, task).await?;

    // Gate every resolved arm on the target's write policy, against the image
    // that arm stores — the post-image for an UPDATE or INSERT arm, the
    // pre-image for a DELETE arm. This expansion rewrites the statement into
    // concrete point ops the RLS injection pass has already run past, so the
    // predicate compiled into the MERGE plan is the last thing that can decide
    // these rows; without the check here, expanding a governed MERGE would
    // launder it into ungoverned point writes.
    if !rls_write_check.is_empty() {
        let bodies = arms
            .updates
            .iter()
            .map(|(_, _, body, _)| body)
            .chain(arms.deletes.iter().map(|(_, _, body)| body))
            .chain(arms.inserts.iter().map(|(_, body)| body));
        for body in bodies {
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
    let target_pk = resolve_target_pk(&target, "MERGE")?;

    let vshard_id = VShardId::from_collection_in_database(task.database_id, &target_collection);
    let mut out: Vec<PhysicalTask> = Vec::new();
    emit_arms(
        state,
        task,
        &target_collection,
        &target_pk,
        vshard_id,
        arms,
        &mut out,
    )?;
    Ok(out)
}

/// Ship the source rows and dispatch the shared Data-Plane RESOLVE pass for one
/// staged merge, decoding all three resolved arms. Never re-derives the
/// classification locally — `collect_merge_plan` on the Data Plane is the single
/// shared classifier for both this path and autocommit `run_merge`.
async fn resolve_merge_arms(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<ResolvedMergeArms> {
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        clauses,
        ..
    }) = &task.plan
    else {
        // Callers only pass a `Merge` task; a mismatch is a programmer error.
        return Err(crate::Error::PlanError {
            detail: "resolve_merge_arms: resolve on non-MERGE task".into(),
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
    let resolve_plan = PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: target_collection.clone(),
        source_collection: source_collection.clone(),
        source_alias: source_alias.clone(),
        target_join_col: target_join_col.clone(),
        source_join_col: source_join_col.clone(),
        clauses: clauses.clone(),
        returning: None,
        resolve_only: true,
        resolved_inserts: None,
        source_rows: Some(source_rows),
        // Read-only classification pass: it emits no rows to the client and
        // writes nothing, so neither policy has anything to gate here. The
        // caller decides the resolved arms against the statement's write
        // predicate before any of them becomes a point op.
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
        // The RESOLVE pass writes nothing, so it folds no materialized-sum
        // delta. The point ops this expansion emits carry their own resolution.
        resolved_sum_targets: Vec::new(),
    });
    // The RESOLVE pass reads the TARGET as base ∪ overlay: passing the staged
    // transaction's id lets `collect_target_docs` fold rows this transaction
    // staged earlier, so a MERGE matches (and reuses the surrogate of) a row a
    // prior statement inserted, instead of resolving against base and inserting
    // a duplicate.
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
                "in-transaction MERGE resolve failed: {:?}",
                resolve_resp.error_code
            ),
        });
    }
    decode_resolve(&resolve_resp.payload)
}

/// Rewrite the three resolved arms into concrete point-write tasks appended to
/// `out`. An UPDATE/DELETE arm with no registered surrogate is a hard error: a
/// non-surrogate-keyed target row is unreachable for any surrogate-keyed
/// collection, and emitting a degraded raw op would reproduce the indexing /
/// durability defect this expansion fixes.
fn emit_arms(
    state: &SharedState,
    task: &PhysicalTask,
    target_collection: &str,
    target_pk: &TargetPk,
    vshard_id: VShardId,
    arms: ResolvedMergeArms,
    out: &mut Vec<PhysicalTask>,
) -> crate::Result<()> {
    for (_join_key, body) in arms.inserts {
        let surrogate = assign_target_surrogate(
            state,
            task.database_id,
            task.tenant_id,
            target_collection,
            target_pk,
            &body,
        )?;
        let document_id = derive_document_id(target_pk, &body, surrogate);
        out.push(point_task(
            task,
            vshard_id,
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: target_collection.to_string(),
                document_id,
                value: body,
                if_absent: false,
                surrogate,
                // The MERGE itself owns the statement's projection; the ops it
                // expands into are internal writes that answer no client.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
        ));
    }

    for (doc_id, surrogate_u32, body, _old_body) in arms.updates {
        let surrogate = require_surrogate(surrogate_u32, &doc_id, "MERGE")?;
        let document_id = derive_document_id(target_pk, &body, surrogate);
        let pk_bytes = document_id.clone().into_bytes();
        out.push(point_task(
            task,
            vshard_id,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: target_collection.to_string(),
                document_id,
                value: body,
                surrogate,
                pk_bytes,
                // See the insert arm above.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        ));
    }

    for (doc_id, surrogate_u32, body) in arms.deletes {
        let surrogate = require_surrogate(surrogate_u32, &doc_id, "MERGE")?;
        let document_id = derive_document_id(target_pk, &body, surrogate);
        let pk_bytes = document_id.clone().into_bytes();
        out.push(point_task(
            task,
            vshard_id,
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: target_collection.to_string(),
                document_id,
                surrogate,
                pk_bytes,
                returning: None,
                rls_filters: Vec::new(),
                // The arm's pre-image was already decided against the merge's
                // write predicate before this op was emitted, so re-checking it
                // in the staging path would only re-run the same test.
                rls_write_check: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        ));
    }
    Ok(())
}

/// Build a concrete point-write task carrying the staged transaction's identity
/// (`txn_id`) so it commits inside the same COMMIT batch as its siblings.
fn point_task(task: &PhysicalTask, vshard_id: VShardId, plan: PhysicalPlan) -> PhysicalTask {
    PhysicalTask {
        tenant_id: task.tenant_id,
        vshard_id,
        database_id: task.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: task.txn_id,
    }
}
