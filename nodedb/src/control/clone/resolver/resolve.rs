// SPDX-License-Identifier: BUSL-1.1

//! `resolve_read`: walk the clone chain for ONE physical task and build its
//! source-side twins.

use nodedb_types::{CloneStatus, Lsn, TenantId};

use crate::control::server::shared::plan_util::extract_collection;
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::metadata::ClonePredicatesNote;
use super::refusal::SourceRewrite;
use super::rewrite::rewrite_plan_for_source;

/// Parameters for the clone read resolver.
pub struct CloneReadParams {
    /// The LSN at which the query runs (T_lsn).
    pub query_lsn: Lsn,
    /// Wall-clock milliseconds corresponding to `query_lsn` (for engine
    /// `system_as_of_ms` fields that work in millisecond space).
    pub query_ms: Option<i64>,
}

/// Outcome of attempting to resolve a clone read for one task.
pub enum ResolveOutcome {
    /// The query time predates the clone's creation — return empty + note.
    PreDatesClone(ClonePredicatesNote),
    /// `target_task` plus every source-side task the chain walk produced.
    Augmented {
        /// Boxed: `PhysicalTask` embeds `PhysicalPlan`, the crate's largest
        /// enum, which would otherwise blow this variant's size far past
        /// `PreDatesClone`'s.
        target_task: Box<PhysicalTask>,
        source_tasks: Vec<PhysicalTask>,
        /// Collection key for tombstone lookups, e.g. `"1/users"`.
        target_collection_key: String,
        /// Clone predation note, `None` unless `T_lsn < clone_created_at`.
        note: Option<ClonePredicatesNote>,
    },
}

/// Attempt to resolve `task` against a cloned collection.
///
/// Returns `None` when the collection has no clone origin (fast path: zero
/// overhead). Returns `Some(ResolveOutcome)` when resolution is required.
pub fn resolve_read(
    state: &SharedState,
    task: PhysicalTask,
    tenant_id: TenantId,
    params: &CloneReadParams,
) -> crate::Result<Option<ResolveOutcome>> {
    let db_id = task.database_id;
    let catalog = state.credentials.catalog();

    // The shared extractor sees through the `Exchange` / `PostProcess`
    // wrappers the converter puts over every sharded read; a clone-local
    // copy would drift out of sync and misread those as "not a clone".
    let Some(raw_coll) = extract_collection(&task.plan) else {
        return Ok(None);
    };
    // Strip the database prefix that db_qualified() prepends, e.g. "1/users" → "users".
    let coll_name = super::rewrite::strip_db_prefix(db_id, raw_coll);

    // Look up the stored collection descriptor.
    let Some(desc) = catalog
        .get_collection(db_id, tenant_id.as_u64(), coll_name)
        .map_err(|e| crate::Error::Storage {
            engine: "catalog".into(),
            detail: format!("clone resolver: get_collection failed: {e}"),
        })?
    else {
        return Ok(None);
    };

    // Short-circuit: not a clone or fully materialized.
    let Some(ref origin) = desc.cloned_from else {
        return Ok(None);
    };
    match desc.clone_status {
        CloneStatus::Materialized => return Ok(None),
        CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
    }

    // UNION DISTINCT/INTERSECT/EXCEPT dedup/subtract this task's response
    // against siblings' by exact row match — unsound without proven parity.
    if !matches!(task.post_set_op, PostSetOp::None) {
        return Err(crate::Error::PlanError {
            detail: format!(
                "a set operation over '{coll_name}' cannot be read through an unmaterialized \
                 clone; run ALTER DATABASE <clone> MATERIALIZE first"
            ),
        });
    }

    // Bitemporal correctness: check if T_lsn < clone_created_at.
    if params.query_lsn < origin.clone_created_at {
        return Ok(Some(ResolveOutcome::PreDatesClone(
            ClonePredicatesNote::new(params.query_lsn, origin.clone_created_at),
        )));
    }

    // Compute effective source LSN: min(T_lsn, as_of_lsn).
    let effective_source_lsn = if params.query_lsn > origin.as_of_lsn {
        origin.as_of_lsn
    } else {
        params.query_lsn
    };

    // Convert effective_source_lsn to wall-ms for the engine.
    let effective_source_ms = state.ms_to_lsn_inverse(effective_source_lsn);

    // Walk source-side tasks up the clone chain until `cloned_from = None`
    // or `Materialized`. `MAX_CLONE_DEPTH` bounds the chain at create time;
    // the loop still caps at 8 as a guard against catalog corruption.
    let mut source_tasks: Vec<PhysicalTask> = Vec::new();

    // Current "target" level for this iteration.
    let mut cur_db_id = db_id;
    let mut cur_coll_name_owned = coll_name.to_string();
    let mut cur_origin = origin.clone();
    let mut cur_effective_ms = effective_source_ms;

    // Template for the next rewrite; after each level, updated to the task
    // just pushed so the next iteration rewrites the correct per-level
    // qualified name rather than the original target task.
    let mut prev_level_tasks: Vec<PhysicalTask> = vec![task.clone()];

    const MAX_WALK: u32 = 8;
    let mut depth = 0u32;

    loop {
        if depth >= MAX_WALK {
            break;
        }
        depth += 1;

        let src_db_id = cur_origin.source_database;
        let src_coll_name = cur_origin.source_collection.as_str();
        let cur_coll_str = cur_coll_name_owned.as_str();

        let mut this_level_tasks: Vec<PhysicalTask> = Vec::new();

        for level_task in &prev_level_tasks {
            // An unsupported read shape over the cloned collection returns an
            // error here, not `NoSourceTask` — it propagates to the client
            // instead of quietly producing a target-only answer.
            let rewritten = rewrite_plan_for_source(super::rewrite::RewriteForSourceParams {
                plan: &level_task.plan,
                target_db_id: cur_db_id,
                source_db_id: src_db_id,
                tenant_id,
                target_coll: cur_coll_str,
                source_coll: src_coll_name,
                effective_source_ms: cur_effective_ms,
                kv_surrogate_ceiling: cur_origin.kv_surrogate_ceiling,
                state,
            })?;
            let SourceRewrite::Task(source_plan) = rewritten else {
                continue;
            };
            let source_vshard = VShardId::from_collection_in_database(
                src_db_id,
                &crate::control::planner::sql_plan_convert::convert::db_qualified(
                    src_db_id,
                    src_coll_name,
                ),
            );
            this_level_tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: source_vshard,
                database_id: src_db_id,
                plan: *source_plan,
                post_set_op: PostSetOp::None,
                txn_id: None,
            });
        }

        source_tasks.extend(this_level_tasks.iter().cloned());
        prev_level_tasks = this_level_tasks;

        // Check whether `src_db_id / src_coll_name` is itself a clone so we
        // can continue the walk.
        let ancestor_desc = catalog
            .get_collection(src_db_id, tenant_id.as_u64(), src_coll_name)
            .map_err(|e| crate::Error::Storage {
                engine: "catalog".into(),
                detail: format!("clone resolver: ancestor get_collection failed: {e}"),
            })?;

        let Some(ancestor) = ancestor_desc else { break };

        // Materialized ancestor — data is fully self-contained; stop.
        match ancestor.clone_status {
            CloneStatus::Materialized => break,
            CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
        }

        let Some(ancestor_origin) = ancestor.cloned_from else {
            break;
        };

        // Compute effective LSN for this ancestor level.
        let ancestor_effective_lsn = if params.query_lsn > ancestor_origin.as_of_lsn {
            ancestor_origin.as_of_lsn
        } else {
            params.query_lsn
        };
        cur_effective_ms = state.ms_to_lsn_inverse(ancestor_effective_lsn);

        cur_db_id = src_db_id;
        cur_coll_name_owned = src_coll_name.to_string();
        cur_origin = ancestor_origin;
    }

    let target_collection_key =
        crate::control::planner::sql_plan_convert::convert::db_qualified(db_id, coll_name);

    Ok(Some(ResolveOutcome::Augmented {
        target_task: Box::new(task),
        source_tasks,
        target_collection_key,
        note: None,
    }))
}
