// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time expansion of a staged in-transaction `MERGE`.
//!
//! A transactional `BEGIN; MERGE INTO t USING s ...; COMMIT` buffers the merge
//! as a single `DocumentOp::Merge` plan. Left intact, COMMIT's buffered-plan
//! replay runs it through the legacy Data-Plane passthrough, which writes the
//! NOT-MATCHED inserts under a raw `sparse.put` with NO surrogate (never indexed
//! — invisible to vector/FTS search), whose `to_replicated_entry` returns `None`
//! (the whole row is lost on a WAL-only restart), and outside the COMMIT batch's
//! undo log (not atomic with sibling ops).
//!
//! This expander rewrites every staged `Merge` into concrete, per-row
//! `PointInsert` / `PointPut` / `PointDelete` writes BEFORE dispatch, exactly as
//! [`super::orchestrator::run_merge`] does for autocommit: it ships the source
//! rows to the source's own core, dispatches the shared Data-Plane RESOLVE pass
//! (the single classifier — never re-derived here), assigns each inserted row its
//! OWN fresh, catalog-REGISTERED surrogate, and reuses the EXISTING target row's
//! registered surrogate for updates/deletes. Because the concrete point ops
//! replace the `Merge` in the buffered list, they commit atomically with sibling
//! ops (undo-tracked `tx_point_*` arms), ride the replicated point-write path
//! (durable across a WAL-only restart), and index into every cross-engine index —
//! so all three defects of the legacy path vanish with no MERGE-specific undo,
//! replication, or minting code.
//!
//! Mirrors [`crate::control::insert_select::expand_staged`]; see its module doc
//! for why concrete `PointInsert` (not `BatchInsert`) is emitted.

use nodedb_types::{Surrogate, TenantId};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::target_surrogate::{
    ResolvedMergeArms, TargetPk, assign_target_surrogate, bare_collection_name, decode_resolve,
    derive_document_id, resolve_target_pk,
};

/// Expand every staged `DocumentOp::Merge` in `buffered` into concrete,
/// surrogate-carrying `PointInsert` / `PointPut` / `PointDelete` tasks,
/// preserving each `Merge`'s position and passing every other task through
/// untouched.
///
/// Runs in the Control-Plane COMMIT path just after [`InsertSelect`
/// expansion](crate::control::insert_select::expand_staged) and before dispatch
/// classification, so the transaction commits concrete point writes rather than
/// a re-played `Merge` through the legacy passthrough.
pub(crate) async fn expand_staged_merges(
    state: &SharedState,
    tenant_id: TenantId,
    buffered: Vec<PhysicalTask>,
) -> crate::Result<Vec<PhysicalTask>> {
    // Fast path: no staged MERGE to expand — return the buffer as-is.
    if !buffered
        .iter()
        .any(|t| matches!(&t.plan, PhysicalPlan::Document(DocumentOp::Merge { .. })))
    {
        return Ok(buffered);
    }

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(buffered.len());
    for task in buffered {
        let PhysicalPlan::Document(DocumentOp::Merge {
            target_collection, ..
        }) = &task.plan
        else {
            out.push(task);
            continue;
        };
        let target_collection = target_collection.clone();

        let arms = resolve_merge_arms(state, tenant_id, &task).await?;

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
        // the merged rows live. Recomputing it (rather than reusing the staged
        // task's vShard) keeps dispatch classification honest, exactly as the
        // `INSERT ... SELECT` expander does.
        let vshard_id = VShardId::from_collection_in_database(task.database_id, &target_collection);
        emit_arms(
            state,
            &task,
            &target_collection,
            &target_pk,
            vshard_id,
            arms,
            &mut out,
        )?;
    }
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
            detail: "expand_staged_merges: resolve on non-MERGE task".into(),
        });
    };

    // Phase 0: read the SOURCE where it lives (its vShard can map to a different
    // Data-Plane core than the target's) and ship the raw rows into the plan.
    let source_rows =
        read_all_source_rows(state, tenant_id, task.database_id, source_collection).await?;

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
    });
    let resolve_resp = dispatch_local(
        state,
        tenant_id,
        task.database_id,
        target_collection,
        resolve_plan,
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
            }),
        ));
    }

    for (doc_id, surrogate_u32, body) in arms.updates {
        let surrogate = require_surrogate(surrogate_u32, &doc_id)?;
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
            }),
        ));
    }

    for (doc_id, surrogate_u32, body) in arms.deletes {
        let surrogate = require_surrogate(surrogate_u32, &doc_id)?;
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
            }),
        ));
    }
    Ok(())
}

/// A resolved UPDATE/DELETE arm must carry the target row's registered
/// surrogate. `None` means a non-surrogate-keyed row — unreachable for every
/// current (and every vector-indexed) collection — so fail the commit loudly
/// rather than emit a degraded, unindexed, unreplicated write.
fn require_surrogate(surrogate_u32: Option<u32>, doc_id: &str) -> crate::Result<Surrogate> {
    match surrogate_u32 {
        Some(s) => Ok(Surrogate::new(s)),
        None => Err(crate::Error::PlanError {
            detail: format!(
                "MERGE target row '{doc_id}' lacks a surrogate; collection is not surrogate-keyed"
            ),
        }),
    }
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
