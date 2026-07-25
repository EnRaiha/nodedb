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
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use nodedb_cluster::calvin::sequencer::inbox::Inbox;
#[cfg(test)]
use nodedb_types::TenantId;

#[cfg(test)]
use crate::Error;
#[cfg(test)]
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
#[cfg(test)]
use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
#[cfg(test)]
use crate::control::planner::calvin::tx_class::build_static_tx_class;
use crate::control::planner::calvin::types::DispatchClass;
#[cfg(test)]
use crate::control::planner::calvin::types::DispatchOutcome;
#[cfg(test)]
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

pub use crate::control::planner::calvin::predicate::predicate_class;
pub use crate::control::planner::calvin::write_class::is_write_plan;

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

/// Derive the set of vShards a transaction's session read-set touches.
///
/// Each [`ReadSetEntry`] homes to its collection's vShard using the SAME
/// collection→vShard map `ReadWriteSet::participating_vshards` uses to derive the
/// `TxClass` read_set's participants. Each read retains its session database so
/// classification and the database-scoped transaction class agree. A read with
/// no extractable collection contributes nothing.
pub fn read_vshards_of(reads: &[ReadSetEntry]) -> BTreeSet<u32> {
    reads
        .iter()
        .filter(|e| !e.collection.is_empty())
        .map(|e| VShardId::from_collection_in_database(e.database_id, &e.collection).as_u32())
        .collect()
}

/// Classify the dispatch class of a task slice from the union of its write
/// vShards and the session read-set's vShards (`read_vshards`).
///
/// 0 or 1 unique vShards → `SingleShard`.
/// 2+ unique vShards → `MultiShard` with the full `BTreeSet<u32>`.
///
/// A txn that writes shard X but READS shard Y participates in `{X, Y}` and must
/// route through Calvin with Y as a participant, so the read vShards widen the
/// class exactly as the write vShards do. Autocommit callers pass an empty
/// `read_vshards` (no session read-set is captured outside an explicit
/// transaction block), preserving write-only classification for them.
pub fn classify_dispatch(tasks: &[PhysicalTask], read_vshards: &BTreeSet<u32>) -> DispatchClass {
    let mut vshards: BTreeSet<u32> = BTreeSet::new();
    let mut last_vshard = None;

    for task in tasks {
        if is_write_plan(&task.plan) {
            let id = task.vshard_id.as_u32();
            vshards.insert(id);
            last_vshard = Some(task.vshard_id);
        }
    }

    // Union the session read-set's vShards into the participant candidate set.
    vshards.extend(read_vshards.iter().copied());

    match vshards.len() {
        0 => DispatchClass::SingleShard {
            vshard: tasks
                .first()
                .map(|t| t.vshard_id)
                .unwrap_or(VShardId::new(0)),
        },
        1 => DispatchClass::SingleShard {
            // The single vShard is a write shard whenever any write ran (the
            // common case: `last_vshard` is set). It could instead be a lone
            // read shard with no writes — unreachable via the COMMIT path, which
            // only classifies a non-empty write buffer — so the `unwrap_or_else`
            // is a defensive fallback upholding the no-panic contract.
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
#[cfg(test)]
pub(crate) async fn dispatch_calvin_or_fast(
    tasks: &[PhysicalTask],
    mode: CrossShardTxnMode,
    tx_state: TransactionState,
    inbox: Option<&Inbox>,
    _orchestrator: Option<&Arc<OllpOrchestrator>>,
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<DispatchOutcome> {
    // Interactive COMMIT threads its session read-set here; autocommit passes an
    // empty slice. The read vShards widen both the classification (below) and the
    // TxClass read_set participants (in `build_static_tx_class`) in lockstep.
    let read_vshards = read_vshards_of(reads);
    let class = classify_dispatch(tasks, &read_vshards);

    match &class {
        DispatchClass::MultiShard { .. } => {
            // Reject cross-shard writes inside explicit transaction blocks.
            if tx_state == TransactionState::InBlock {
                return Err(Error::CrossShardInExplicitTransaction);
            }

            match mode {
                CrossShardTxnMode::Strict => {
                    let inbox = inbox.ok_or(Error::SequencerUnavailable)?;
                    // Populate the TxClass read_set from the session reads so the
                    // read shards are enumerated as Calvin participants.
                    let tx_class = build_static_tx_class(tasks, tenant_id, reads)?;
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
