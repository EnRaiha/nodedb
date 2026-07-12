// SPDX-License-Identifier: BUSL-1.1

//! Fail-closed co-location guard for cross-collection write ops.
//!
//! One write op still reads a SOURCE collection and writes a TARGET collection
//! from a single Data-Plane handler that assumes co-residence:
//! `KvOp::TransferItem`. The handler reads the source from the LOCAL core's
//! store, assuming source and target are co-resident on ONE core. But every
//! collection name hashes to its own vShard independently, and on a multi-core
//! node two vShards can map to DIFFERENT Data-Plane cores. When they do, the
//! source read hits an empty store on the target's core and the op returns a
//! silently wrong (empty) result.
//!
//! Until cross-core source-shipping lands for that op, it is refused with a
//! loud, typed error whenever source and target resolve to different cores. On a
//! single-core node every vShard maps to core 0, so co-location always holds and
//! the guard never fires. This is a safety FLOOR, not the final fix.
//!
//! `MERGE` and `UPDATE ... FROM` are NOT guarded: they already ship their source
//! across cores. Autocommit `MERGE` is driven by `control::merge_orchestrator`
//! and autocommit `UPDATE ... FROM` by `control::update_from_join_orchestrator`;
//! each scans the source on its own core and ships the rows into the plan, so no
//! raw `DocumentOp::Merge` / `DocumentOp::UpdateFromJoin` reaches this guard.
//! Their in-transaction forms never route raw through this guard: in-transaction
//! `MERGE` and `UPDATE ... FROM` are both resolved + staged at STATEMENT time
//! into concrete point ops (which target the target's own vShard) by
//! `control::server::shared::session::expander_stage`.
//!
//! This guards only the one remaining cross-collection WRITE op.
//! Cross-collection READ joins are untouched — they scan each side independently
//! and never assume co-residence.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::DatabaseId;

use crate::control::router::vshard::VShardRouter;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// Resolve the Data-Plane core that owns `collection`'s vShard on THIS node,
/// using the same `VShardRouter` the dispatch path uses so the guard can never
/// drift from the real vShard→core mapping.
fn owning_core(router: &VShardRouter, database_id: DatabaseId, collection: &str) -> Option<usize> {
    router.resolve(VShardId::from_collection_in_database(
        database_id,
        collection,
    ))
}

/// True when `coll_a` and `coll_b` resolve to DIFFERENT cores on `router`.
fn cores_diverge(
    router: &VShardRouter,
    database_id: DatabaseId,
    coll_a: &str,
    coll_b: &str,
) -> bool {
    match (
        owning_core(router, database_id, coll_a),
        owning_core(router, database_id, coll_b),
    ) {
        (Some(a), Some(b)) => a != b,
        // An unresolvable vShard can never be proven co-resident, so fail closed
        // (refuse) rather than dispatch a possibly-wrong read. Unreachable with
        // the round-robin router, which resolves every vShard.
        _ => true,
    }
}

/// True when two collections' vShards resolve to DIFFERENT Data-Plane cores on
/// this node. On a single-core node all vShards map to core 0, so this is always
/// `false` (the collections are co-resident) and the caller must not block.
pub(crate) fn cross_collection_cores_diverge(
    state: &SharedState,
    database_id: DatabaseId,
    coll_a: &str,
    coll_b: &str,
) -> bool {
    let dispatcher = match state.dispatcher.lock() {
        Ok(d) => d,
        Err(poisoned) => poisoned.into_inner(),
    };
    cores_diverge(dispatcher.router(), database_id, coll_a, coll_b)
}

/// Fail-closed guard for one cross-collection write op: refuse with a typed
/// error when source and target are not co-resident on the same core.
pub(crate) fn ensure_cross_collection_colocated(
    state: &SharedState,
    database_id: DatabaseId,
    op: &'static str,
    source: &str,
    target: &str,
) -> crate::Result<()> {
    if cross_collection_cores_diverge(state, database_id, source, target) {
        return Err(crate::Error::CrossCollectionNotColocated {
            op,
            source_collection: source.to_string(),
            target_collection: target.to_string(),
        });
    }
    Ok(())
}

/// Apply the co-location guard to a plan before routing. Only the one remaining
/// cross-collection WRITE op is guarded; every other plan — including
/// cross-collection READ joins — passes through untouched.
pub(crate) fn guard_cross_collection_write(
    state: &SharedState,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    use nodedb_physical::physical_plan::KvOp;

    match plan {
        // `MERGE` and `UPDATE ... FROM` are NOT guarded here: they work
        // cross-core. Autocommit forms are intercepted at every dispatch entry
        // point and driven by their orchestrators (`control::merge_orchestrator`
        // / `control::update_from_join_orchestrator`), which scan the source on
        // its own core and ship the rows into the plan — so a raw
        // `DocumentOp::Merge` / `DocumentOp::UpdateFromJoin` never reaches this
        // router. In-transaction forms are buffered and expanded at COMMIT into
        // concrete point ops by their expanders, so they never route here either.
        PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        }) => ensure_cross_collection_colocated(
            state,
            database_id,
            "TRANSFER",
            source_collection,
            dest_collection,
        ),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Brute-force a collection name whose vShard maps to `core` under `router`.
    fn collection_on_core(router: &VShardRouter, core: usize) -> String {
        for i in 0u64.. {
            let name = format!("col_{i}");
            if owning_core(router, DatabaseId::DEFAULT, &name) == Some(core) {
                return name;
            }
        }
        unreachable!("router covers all cores")
    }

    #[test]
    fn single_core_never_diverges() {
        // Single-core node: every vShard maps to core 0, so ANY two collections
        // are co-resident and the guard must never fire.
        let router = VShardRouter::round_robin(1);
        for i in 0u64..64 {
            for j in 0u64..64 {
                let a = format!("col_{i}");
                let b = format!("col_{j}");
                assert!(
                    !cores_diverge(&router, DatabaseId::DEFAULT, &a, &b),
                    "single-core node must treat all collections as co-resident ({a}, {b})"
                );
            }
        }
    }

    #[test]
    fn same_core_does_not_diverge() {
        let router = VShardRouter::round_robin(4);
        let a = collection_on_core(&router, 2);
        // A collection trivially shares a core with itself.
        assert!(!cores_diverge(&router, DatabaseId::DEFAULT, &a, &a));
    }

    #[test]
    fn different_cores_diverge() {
        let router = VShardRouter::round_robin(4);
        let a = collection_on_core(&router, 0);
        let b = collection_on_core(&router, 1);
        assert_ne!(a, b);
        assert!(
            cores_diverge(&router, DatabaseId::DEFAULT, &a, &b),
            "collections on cores 0 and 1 must be reported as divergent"
        );
    }
}
