// SPDX-License-Identifier: BUSL-1.1

//! Append a task of its own for every materialized-sum target that does NOT
//! share its source collection's vShard.
//!
//! A collection homes to one vShard, so a binding's source and target are
//! generally served by different cores — co-residency is the exception. The
//! co-resident case works because one core owns both rows: the derived write
//! rides the source write's transaction and is atomic for free. Cross-shard it
//! cannot, so the balance is shipped as a separate
//! [`DocumentOp::ApplyBalanceDelta`] task homed on the target's vShard.
//!
//! # This mirrors the implicit graph edge, deliberately
//!
//! [`append_implicit_edge_tasks`](crate::control::planner::implicit_edges::append_implicit_edge_tasks)
//! already solves exactly this shape: a cross-collection write derived from a
//! document write, resolved on the Control Plane at plan time, appended as its
//! own task with its own home, and left to the downstream classifier to
//! single-home or Calvin dual-home. Nothing here invents a second mechanism —
//! the pair becomes multi-shard by the two tasks' own `vshard_id`s, exactly as
//! an edge whose endpoints hash apart does, and
//! [`classify_dispatch`](crate::control::planner::calvin::dispatch::classify_dispatch)
//! routes it through the Calvin sequencer so the source row and the balance
//! commit together or not at all.
//!
//! # Only the shapes whose delta the plan already settles
//!
//! The appended task carries a NUMBER, so this pass can only run where the plan
//! determines that number by itself. `PointInsert` and `BatchInsert` do: their
//! rows are new by construction — a duplicate primary key fails the statement —
//! so the whole of each row's value is credited and there is no pre-image to
//! subtract. Every other write shape's delta is a difference between two images,
//! at least one of which the plan does not carry, and a difference guessed at
//! plan time is a wrong balance. Those keep folding on the Data Plane from the
//! real images, and their cross-shard targets are NOT deferred here.
//!
//! `if_absent` inserts are excluded for the same reason: a row the handler
//! silently skips owes its target nothing, and the plan cannot know which rows
//! will be skipped.

use rust_decimal::Decimal;

use nodedb_physical::physical_plan::{
    DocumentOp, PhysicalPlan, ResolvedSumTarget, resolved_sum_surrogate,
};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::state::SharedState;
use crate::query::sum_target_is_co_resident;
use crate::types::{DatabaseId, TenantId};

/// One balance write this pass decided to ship on its own task.
struct AppendedDelta {
    binding_target: String,
    task: PhysicalTask,
}

/// Append an `ApplyBalanceDelta` task per cross-shard target, and record the
/// deferral on the source op so the Data Plane does not also apply it.
///
/// Runs AFTER [`resolve_materialized_sum_targets`](super::resolve) — it consumes
/// that pass's `resolved_sum_targets`, and issues no lookup of its own. A
/// collection that drives no binding costs one cached index probe and nothing
/// else.
pub fn append_cross_shard_balance_tasks(
    state: &SharedState,
    tasks: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<()> {
    let schema_version = state.schema_version.current();
    let catalog = state.credentials.catalog();

    // Collected first so the immutable walk of `tasks` does not borrow-conflict
    // with the `&mut Vec` pushed into below — the same two-phase shape
    // `append_implicit_edge_tasks` uses.
    let mut appended: Vec<(usize, Vec<AppendedDelta>)> = Vec::new();
    for (index, task) in tasks.iter().enumerate() {
        let PhysicalPlan::Document(op) = &task.plan else {
            continue;
        };
        let Some(SettleableInsert {
            collection,
            docs,
            resolved,
        }) = settleable_insert(op)?
        else {
            continue;
        };
        let Some(bindings) = state.materialized_sum_index.bindings_for_source(
            catalog,
            schema_version,
            database_id,
            tenant_id,
            strip_db_prefix(database_id, collection),
        )?
        else {
            continue;
        };

        let mut for_task = Vec::new();
        for binding in bindings.iter() {
            if sum_target_is_co_resident(database_id, collection, &binding.target_collection) {
                continue;
            }
            for (join_value, delta) in crate::query::binding_insert_deltas(binding, &docs)? {
                // A zero net delta leaves the stored total unchanged, so the
                // read-modify-write on the target would rewrite the row
                // byte-for-byte. Shipping a task for it would also make an
                // otherwise single-shard statement multi-shard for nothing.
                if delta == Decimal::ZERO {
                    continue;
                }
                let surrogate =
                    resolved_sum_surrogate(resolved, &binding.target_collection, &join_value)
                        .ok_or_else(|| crate::Error::MaterializedSumTargetNotFound {
                            target_collection: binding.target_collection.clone(),
                            join_column: binding.join_column.clone(),
                            join_value: join_value.clone(),
                        })?;
                for_task.push(AppendedDelta {
                    binding_target: binding.target_collection.clone(),
                    task: super::settle::balance_task(super::settle::BalanceTaskSpec {
                        txn_id: task.txn_id,
                        database_id,
                        tenant_id,
                        binding,
                        surrogate,
                        join_value,
                        delta,
                    }),
                });
            }
        }
        if !for_task.is_empty() {
            appended.push((index, for_task));
        }
    }

    for (index, deltas) in appended {
        for delta in deltas {
            // The deferral is recorded on the SOURCE op before its sibling is
            // pushed, so a plan can never carry the appended task without the
            // instruction that stops the source core applying it too.
            defer_binding(&mut tasks[index].plan, delta.binding_target);
            tasks.push(delta.task);
        }
    }

    Ok(())
}

/// A write whose materialized-sum delta the PLAN already determines: its source
/// collection, the row images it will store, and the resolve pass's join-value →
/// surrogate table.
struct SettleableInsert<'a> {
    collection: &'a str,
    docs: Vec<serde_json::Value>,
    resolved: &'a [ResolvedSumTarget],
}

/// The settleable shape of `op`, or `None` for every other op.
///
/// The match is exhaustive so a new `DocumentOp` variant must state which side
/// it is on: a variant that silently fell through would either lose its
/// cross-shard balance or, worse, have one guessed for it.
fn settleable_insert(op: &DocumentOp) -> crate::Result<Option<SettleableInsert<'_>>> {
    match op {
        DocumentOp::PointInsert {
            collection,
            value,
            if_absent,
            resolved_sum_targets,
            ..
        } => {
            // A skipped conflict inserts nothing and owes its target nothing,
            // and the plan cannot tell which rows will be skipped.
            if *if_absent {
                return Ok(None);
            }
            Ok(Some(SettleableInsert {
                collection: collection.as_str(),
                docs: decode_bodies(std::slice::from_ref(value)),
                resolved: resolved_sum_targets.as_slice(),
            }))
        }
        DocumentOp::BatchInsert {
            collection,
            documents,
            resolved_sum_targets,
            ..
        } => {
            let bodies: Vec<&[u8]> = documents.iter().map(|(_, v)| v.as_slice()).collect();
            Ok(Some(SettleableInsert {
                collection: collection.as_str(),
                docs: decode_bodies(&bodies),
                resolved: resolved_sum_targets.as_slice(),
            }))
        }
        // Every other write's delta is a difference between two images, at
        // least one of which the plan does not carry — an UPDATE's pre-image,
        // a DELETE's removed row, a `PointPut`/`Upsert`'s stored row when one
        // is already there. They fold on the Data Plane from the real images
        // and their cross-shard targets are not deferred here.
        DocumentOp::PointPut { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        // Reads, index DDL, and the balance task this pass itself appends.
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => Ok(None),
    }
}

/// Record that one binding's delta travels on its own task. Exhaustive for the
/// same reason [`settleable_insert`] is.
fn defer_binding(plan: &mut PhysicalPlan, target_collection: String) {
    let PhysicalPlan::Document(op) = plan else {
        return;
    };
    let deferred = match op {
        DocumentOp::PointInsert {
            deferred_sum_targets,
            ..
        }
        | DocumentOp::BatchInsert {
            deferred_sum_targets,
            ..
        } => deferred_sum_targets,
        DocumentOp::PointPut { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => return,
    };
    if !deferred.contains(&target_collection) {
        deferred.push(target_collection);
    }
}

/// Decode each MessagePack row body into a document.
///
/// A body that will not decode carries no column any binding can read, so it
/// contributes no delta — the same conclusion the Data-Plane hook reaches for a
/// submitted body it cannot decode.
fn decode_bodies<B: AsRef<[u8]>>(bodies: &[B]) -> Vec<serde_json::Value> {
    bodies
        .iter()
        .filter_map(|body| nodedb_types::json_from_msgpack(body.as_ref()).ok())
        .collect()
}

/// Strip the `"<db_id>/"` prefix a planned collection name carries, yielding the
/// catalog name the binding index is keyed on.
fn strip_db_prefix(database_id: DatabaseId, qualified: &str) -> &str {
    if database_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", database_id.as_u64());
    qualified.strip_prefix(prefix.as_str()).unwrap_or(qualified)
}
