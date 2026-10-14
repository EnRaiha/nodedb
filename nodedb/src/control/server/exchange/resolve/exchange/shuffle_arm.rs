// SPDX-License-Identifier: BUSL-1.1

//! Root-level `Shuffle` and `ShuffleAggregate` exchange resolution: thin
//! delegation into the cross-node grace-hash-join and distributed-GROUP-BY
//! orchestrators.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::exchange::resolve::peers::ShuffleRead;
use crate::control::server::exchange::resolve::{shuffle, shuffle_aggregate};
use crate::control::state::SharedState;

use super::entry::Resolved;
use crate::control::server::exchange::read_scope::ReadScope;

/// Resolve a root-level `Exchange{Shuffle}` node: orchestrate a real
/// cross-node grace hash join. The child must be a `QueryOp::HashJoin`
/// (shuffle wraps a complete hash join); `shuffle::resolve_shuffle_join`
/// validates that, fans producers + consumers, and returns the merged join
/// rows as `Resolved::Gathered`.
///
/// Shuffle producers read committed rows only. Inside a transaction the join
/// resolves as a gathered hash join instead, whose sides read through the
/// engine scans that fold the transaction's staged writes.
pub(super) async fn resolve_shuffle(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    child: PhysicalPlan,
    keys: Vec<(String, String)>,
    num_parts: usize,
) -> crate::Result<Resolved> {
    if ctx.txn_id.is_some() {
        return super::gather_arm::resolve_gather(state, ctx, child, false, captures).await;
    }
    shuffle::resolve_shuffle_join(
        state,
        ctx.database_id,
        ctx.tenant_id,
        child,
        keys,
        num_parts,
        ShuffleRead {
            trace_id: ctx.trace_id,
            linearizable: ctx.linearizable,
        },
    )
    .await
}

/// Resolve a root-level `Exchange{ShuffleAggregate}` node: orchestrate a real
/// cross-node distributed GROUP BY shuffle. The child must be a
/// `QueryOp::Aggregate` (shuffle wraps a complete aggregate);
/// `shuffle_aggregate::resolve_shuffle_aggregate` validates that, fans the
/// partial-state producers + per-part consumers, and returns the merged
/// finalized rows as `Resolved::Gathered`.
///
/// Inside a transaction the aggregate resolves as a gathered aggregate over
/// the collection's engine scan, which folds the transaction's staged
/// writes; the shuffle producers read committed rows only.
pub(super) async fn resolve_shuffle_aggregate(
    state: &SharedState,
    ctx: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    child: PhysicalPlan,
    keys: Vec<String>,
    num_parts: usize,
) -> crate::Result<Resolved> {
    if ctx.txn_id.is_some() {
        return super::gather_arm::resolve_gather(state, ctx, child, true, captures).await;
    }
    shuffle_aggregate::resolve_shuffle_aggregate(
        state,
        ctx.database_id,
        ctx.tenant_id,
        child,
        keys,
        num_parts,
        ShuffleRead {
            trace_id: ctx.trace_id,
            linearizable: ctx.linearizable,
        },
    )
    .await
}
