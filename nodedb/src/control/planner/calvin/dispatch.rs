// SPDX-License-Identifier: BUSL-1.1

//! Calvin dispatch classification and routing for cross-shard writes.
//!
//! This module is the single chokepoint for deciding whether a set of
//! [`PhysicalTask`]s should be dispatched via:
//!
//! - The single-shard fast path (existing path, no Calvin involvement).
//! - Calvin static dispatch (all write keys known upfront).
//! - Calvin dependent-read dispatch (OLLP) (write keys depend on a pre-read).
//! - Best-effort non-atomic dispatch (each vshard independently, no atomicity).
//!
//! `TxClass` construction lives in the sibling [`tx_class`](super::tx_class)
//! module; this module classifies and routes.
//!
//! # Note on predicate_class
//!
//! The ideal implementation of `predicate_class` would serialize the `Filter`
//! AST via zerompk and normalize bound parameter values to their type tags.
//! However, `nodedb_sql::types::Filter` does not derive `zerompk::ToMessagePack`
//! or `zerompk::FromMessagePack`. As a declared fallback, `predicate_class`
//! accepts the canonical SQL text string (post-parse-canonicalization) and
//! normalizes numeric and string literals to their type tags before hashing.
//! This is a degraded path relative to AST-level hashing.

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::calvin::sequencer::inbox::Inbox;
use nodedb_types::TenantId;

use crate::Error;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
use crate::control::planner::calvin::tx_class::build_static_tx_class;
use crate::control::planner::calvin::types::{DispatchClass, DispatchOutcome};
use crate::control::server::shared::session::TransactionState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};
use nodedb_physical::physical_task::PhysicalTask;

pub use crate::control::planner::calvin::predicate::predicate_class;

// ── is_write_plan ─────────────────────────────────────────────────────────────

/// Returns `true` if the plan is a write operation.
///
/// Centralizing this avoids scattered `match` arms when new write variants
/// are added. Reads, scans, and query operators return `false`.
pub fn is_write_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        // Document writes
        PhysicalPlan::Document(op) => matches!(
            op,
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::BatchInsert { .. }
                | DocumentOp::InsertSelect { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
                | DocumentOp::UpdateFromJoin { .. }
        ),
        // KV writes
        PhysicalPlan::Kv(op) => matches!(
            op,
            KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::BatchPut { .. }
        ),
        // Vector writes
        PhysicalPlan::Vector(op) => matches!(
            op,
            VectorOp::Insert { .. }
                | VectorOp::BatchInsert { .. }
                | VectorOp::Delete { .. }
                | VectorOp::DeleteBySurrogate { .. }
                | VectorOp::SparseInsert { .. }
                | VectorOp::SparseDelete { .. }
                | VectorOp::MultiVectorInsert { .. }
        ),
        // Graph writes
        PhysicalPlan::Graph(op) => {
            matches!(op, GraphOp::EdgePut { .. } | GraphOp::EdgeDelete { .. })
        }
        // Timeseries writes
        PhysicalPlan::Timeseries(op) => matches!(op, TimeseriesOp::Ingest { .. }),
        // Columnar writes
        PhysicalPlan::Columnar(op) => {
            use nodedb_physical::physical_plan::ColumnarOp;
            matches!(op, ColumnarOp::Insert { .. })
        }
        // CRDT writes
        PhysicalPlan::Crdt(op) => {
            use nodedb_physical::physical_plan::CrdtOp;
            matches!(op, CrdtOp::ListInsert { .. } | CrdtOp::ListDelete { .. })
        }
        // Array writes
        PhysicalPlan::Array(op) => {
            use nodedb_physical::physical_plan::ArrayOp;
            matches!(
                op,
                ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. }
            )
        }
        // Everything else: reads, scans, queries, meta, spatial, text
        PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_) => false,
    }
}

// ── is_dependent_predicate ────────────────────────────────────────────────────

/// Returns `true` if the plan contains a value-dependent predicate that
/// requires OLLP dependent-read dispatch instead of static Calvin dispatch.
///
/// The detection criterion: the plan is a `BulkUpdate` or `BulkDelete`
/// (predicate is not a point-equality on the collection's primary key).
/// Point-equality writes (`PointPut`, `PointInsert`, `PointDelete`,
/// `PointUpdate`) have their write keys statically known and are routed
/// via the static Calvin path.
pub fn is_dependent_predicate(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(DocumentOp::BulkUpdate { .. })
            | PhysicalPlan::Document(DocumentOp::BulkDelete { .. })
    )
}

// ── classify_dispatch ─────────────────────────────────────────────────────────

/// Classify the dispatch class of a task slice by collecting the unique set of
/// write vShards.
///
/// 0 or 1 unique write vShards → `SingleShard`.
/// 2+ unique write vShards → `MultiShard` with the full `BTreeSet<u32>`.
pub fn classify_dispatch(tasks: &[PhysicalTask]) -> DispatchClass {
    let mut vshards: BTreeSet<u32> = BTreeSet::new();
    let mut last_vshard = None;

    for task in tasks {
        if is_write_plan(&task.plan) {
            let id = task.vshard_id.as_u32();
            vshards.insert(id);
            last_vshard = Some(task.vshard_id);
        }
    }

    match vshards.len() {
        0 => DispatchClass::SingleShard {
            vshard: tasks
                .first()
                .map(|t| t.vshard_id)
                .unwrap_or(VShardId::new(0)),
        },
        1 => DispatchClass::SingleShard {
            // Invariant: vshards.len() == 1 guarantees the loop ran at least
            // once and set last_vshard. The unwrap_or_else is a defensive
            // fallback that upholds the no-panic contract for library code.
            vshard: last_vshard.unwrap_or_else(|| VShardId::new(0)),
        },
        _ => DispatchClass::MultiShard { vshards },
    }
}

// ── dispatch_calvin_or_fast ───────────────────────────────────────────────────

/// Route a set of tasks to the appropriate dispatch path.
///
/// Decision tree:
/// 1. `InBlock` + `MultiShard` → `Err(CrossShardInExplicitTransaction)`.
/// 2. `MultiShard` + `Strict` + no inbox → `Err(SequencerUnavailable)`.
/// 3. `MultiShard` + `Strict` → Calvin static path via inbox.
/// 4. `MultiShard` + `BestEffortNonAtomic` → independent per-vshard dispatch.
/// 5. `SingleShard` → existing single-shard fast path.
///
/// The single-shard and best-effort paths are modeled here as outcomes only —
/// the caller is responsible for the actual Data Plane dispatch, since this
/// module lives in the Control Plane and has no direct Data Plane handle.
pub async fn dispatch_calvin_or_fast(
    tasks: &[PhysicalTask],
    mode: CrossShardTxnMode,
    tx_state: TransactionState,
    inbox: Option<&Inbox>,
    _orchestrator: Option<&Arc<OllpOrchestrator>>,
    tenant_id: TenantId,
) -> crate::Result<DispatchOutcome> {
    let class = classify_dispatch(tasks);

    match &class {
        DispatchClass::MultiShard { .. } => {
            // Reject cross-shard writes inside explicit transaction blocks.
            if tx_state == TransactionState::InBlock {
                return Err(Error::CrossShardInExplicitTransaction);
            }

            match mode {
                CrossShardTxnMode::Strict => {
                    let inbox = inbox.ok_or(Error::SequencerUnavailable)?;
                    // Autocommit cross-shard write: no session read-set is
                    // accumulated (interactive COMMIT carries one later).
                    let tx_class = build_static_tx_class(tasks, tenant_id, &[])?;
                    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
                        detail: format!("Calvin sequencer rejected transaction: {e}"),
                    })?;
                    Ok(DispatchOutcome::CalvinStatic { inbox_seq })
                }
                CrossShardTxnMode::BestEffortNonAtomic => Ok(DispatchOutcome::BestEffortNonAtomic),
            }
        }
        DispatchClass::SingleShard { .. } => Ok(DispatchOutcome::SingleShard),
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
