// SPDX-License-Identifier: BUSL-1.1

//! Root-level `Shuffle` and `ShuffleAggregate` exchange resolution: thin
//! delegation into the cross-node grace-hash-join and distributed-GROUP-BY
//! orchestrators.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::control::server::exchange::resolve::{shuffle, shuffle_aggregate};
use crate::control::state::SharedState;

use super::dispatch::ResolveCtx;
use super::entry::Resolved;

/// Resolve a root-level `Exchange{Shuffle}` node: orchestrate a real
/// cross-node grace hash join. The child must be a `QueryOp::HashJoin`
/// (shuffle wraps a complete hash join); `shuffle::resolve_shuffle_join`
/// validates that, fans producers + consumers, and returns the merged join
/// rows as `Resolved::Gathered`.
pub(super) async fn resolve_shuffle(
    state: &SharedState,
    ctx: ResolveCtx,
    child: PhysicalPlan,
    keys: Vec<(String, String)>,
    num_parts: usize,
) -> crate::Result<Resolved> {
    shuffle::resolve_shuffle_join(
        state,
        ctx.database_id,
        ctx.tenant_id,
        child,
        keys,
        num_parts,
        ctx.trace_id,
    )
    .await
}

/// Resolve a root-level `Exchange{ShuffleAggregate}` node: orchestrate a real
/// cross-node distributed GROUP BY shuffle. The child must be a
/// `QueryOp::Aggregate` (shuffle wraps a complete aggregate);
/// `shuffle_aggregate::resolve_shuffle_aggregate` validates that, fans the
/// partial-state producers + per-part consumers, and returns the merged
/// finalized rows as `Resolved::Gathered`.
pub(super) async fn resolve_shuffle_aggregate(
    state: &SharedState,
    ctx: ResolveCtx,
    child: PhysicalPlan,
    keys: Vec<String>,
    num_parts: usize,
) -> crate::Result<Resolved> {
    shuffle_aggregate::resolve_shuffle_aggregate(
        state,
        ctx.database_id,
        ctx.tenant_id,
        child,
        keys,
        num_parts,
        ctx.trace_id,
    )
    .await
}
