// SPDX-License-Identifier: BUSL-1.1

//! `resolve_read`: walk the clone chain and build augmented source tasks.

use std::sync::Arc;

use nodedb_types::{CloneOrigin, CloneStatus, Lsn, TenantId};

use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::metadata::ClonePredicatesNote;
use super::rewrite::rewrite_plan_for_source;

/// Parameters for the clone read resolver.
pub struct CloneReadParams {
    /// The LSN at which the query runs (T_lsn).
    pub query_lsn: Lsn,
    /// Wall-clock milliseconds corresponding to `query_lsn` (for engine
    /// `system_as_of_ms` fields that work in millisecond space).
    pub query_ms: Option<i64>,
}

/// Outcome of attempting to resolve clone reads for a set of tasks.
pub enum ResolveOutcome {
    /// Tasks were not modified — either the collection is not a clone, or
    /// it is fully `Materialized`.
    Passthrough(Vec<PhysicalTask>),
    /// The query time predates the clone's creation — return empty + note.
    PreDatesClone(ClonePredicatesNote),
    /// Augmented tasks: [target_tasks..., source_tasks...].
    ///
    /// The caller dispatches all tasks and then calls `merge_source_into_target`
    /// to filter tombstoned rows before returning results to the client.
    Augmented {
        tasks: Vec<PhysicalTask>,
        /// Index into `tasks` where source tasks begin.
        source_start_idx: usize,
        /// Clone metadata for result filtering.
        origin: CloneOrigin,
        /// Collection key for tombstone lookups, e.g. `"1/users"`.
        target_collection_key: String,
        /// Clone predation note, `None` unless `T_lsn < clone_created_at`.
        note: Option<ClonePredicatesNote>,
    },
}

/// Attempt to resolve `tasks` for a cloned collection.
///
/// Returns `None` when the collection has no clone origin (fast path: zero
/// overhead). Returns `Some(ResolveOutcome)` when resolution is required.
pub fn resolve_read(
    state: &Arc<SharedState>,
    tasks: Vec<PhysicalTask>,
    tenant_id: TenantId,
    params: &CloneReadParams,
) -> crate::Result<Option<ResolveOutcome>> {
    // Quick-check: do any tasks target a database other than the default?
    // All tasks in a statement share the same database_id (single-database
    // statements); use the first task's database_id.
    let Some(first_task) = tasks.first() else {
        return Ok(None);
    };
    let db_id = first_task.database_id;

    // Retrieve catalog for lookup.
    let catalog = state.credentials.catalog();

    // Extract the collection name from the first read-type task.
    let Some(raw_coll) = super::rewrite::extract_collection_from_plan(&first_task.plan) else {
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

    // Build source-side tasks for every ancestor in the clone chain.
    //
    // Walk from the immediate source upward until `cloned_from = None` or the
    // collection is `Materialized` (at which point the stored data is complete
    // and no further indirection is needed). MAX_CLONE_DEPTH is enforced at
    // clone-create time so the chain is bounded; the loop still caps at 8 as
    // a belt-and-suspenders guard against catalog corruption.
    let mut augmented_tasks = tasks.clone();
    let source_start_idx = augmented_tasks.len();

    // Current "target" level for this iteration.
    let mut cur_db_id = db_id;
    let mut cur_coll_name_owned = coll_name.to_string();
    let mut cur_origin = origin.clone();
    let mut cur_effective_ms = effective_source_ms;

    // Tasks from the previous level that serve as templates for the next
    // rewrite.  Initialized to the original target tasks; after each level is
    // added, updated to the tasks that were just pushed so the next iteration
    // rewrites those (which carry the correct collection qualified name for
    // that level) rather than always rewriting the original target tasks.
    let mut prev_level_tasks: Vec<PhysicalTask> = tasks.clone();

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

        for task in &prev_level_tasks {
            if let Some(source_plan) =
                rewrite_plan_for_source(super::rewrite::RewriteForSourceParams {
                    plan: &task.plan,
                    target_db_id: cur_db_id,
                    source_db_id: src_db_id,
                    tenant_id,
                    target_coll: cur_coll_str,
                    source_coll: src_coll_name,
                    effective_source_ms: cur_effective_ms,
                    kv_surrogate_ceiling: cur_origin.kv_surrogate_ceiling,
                    state,
                })?
            {
                let source_vshard = VShardId::from_collection_in_database(
                    src_db_id,
                    &crate::control::planner::sql_plan_convert::convert::db_qualified(
                        src_db_id,
                        src_coll_name,
                    ),
                );
                let task = PhysicalTask {
                    tenant_id,
                    vshard_id: source_vshard,
                    database_id: src_db_id,
                    plan: source_plan,
                    post_set_op: nodedb_physical::physical_task::PostSetOp::None,
                    txn_id: None,
                };
                this_level_tasks.push(task);
            }
        }

        for task in this_level_tasks.iter().cloned() {
            augmented_tasks.push(task);
        }
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
        tasks: augmented_tasks,
        source_start_idx,
        origin: origin.clone(),
        target_collection_key,
        note: None,
    }))
}
