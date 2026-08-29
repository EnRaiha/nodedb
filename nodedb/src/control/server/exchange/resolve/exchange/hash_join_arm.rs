// SPDX-License-Identifier: BUSL-1.1

//! `HashJoin` exchange resolution: resolve `Broadcast` children embedded in
//! `left_input` / `right_input`, and cross-node build-side gather.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::server::exchange::full_scan::ScanSide;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::exchange::resolve::join_input::{
    gather_join_build_side, resolve_join_input,
};
use crate::control::state::SharedState;

use super::dispatch::ResolveCtx;
use super::entry::Resolved;

/// Fields of a `QueryOp::HashJoin` plan node, carried through resolution as
/// one value instead of as individually threaded arguments.
pub(super) struct HashJoinFields {
    pub left_collection: String,
    pub right_collection: String,
    pub left_alias: Option<String>,
    pub right_alias: Option<String>,
    pub on: Vec<(String, String)>,
    pub join_type: String,
    pub limit: usize,
    pub post_group_by: Vec<String>,
    pub post_aggregates: Vec<(String, String)>,
    pub projection: Vec<nodedb_physical::physical_plan::JoinProjection>,
    pub computed_projection: Vec<u8>,
    pub join_filters: Vec<u8>,
    pub post_filters: Vec<u8>,
    pub left_input: Option<Box<PhysicalPlan>>,
    pub right_input: Option<Box<PhysicalPlan>>,
    pub left_bitmap: Option<Box<PhysicalPlan>>,
    pub right_bitmap: Option<Box<PhysicalPlan>>,
    pub left_rls_filters: Vec<u8>,
    pub right_rls_filters: Vec<u8>,
}

/// Resolve a `QueryOp::HashJoin` node: resolve `Broadcast` children embedded
/// in `left_input` / `right_input`, then cross-node gather the build side.
pub(super) async fn resolve_hash_join(
    state: &SharedState,
    ctx: ResolveCtx,
    captures: &mut Vec<DistributedReadCapture>,
    fields: HashJoinFields,
) -> crate::Result<Resolved> {
    let ResolveCtx {
        database_id,
        tenant_id,
        trace_id,
        txn_id,
    } = ctx;
    let HashJoinFields {
        left_collection,
        right_collection,
        left_alias,
        right_alias,
        on,
        join_type,
        limit,
        post_group_by,
        post_aggregates,
        projection,
        computed_projection,
        join_filters,
        post_filters,
        left_input,
        mut right_input,
        left_bitmap,
        right_bitmap,
        left_rls_filters,
        right_rls_filters,
    } = fields;

    let left_input = resolve_join_input(
        state,
        database_id,
        tenant_id,
        left_input,
        trace_id,
        txn_id,
        captures,
    )
    .await?;
    right_input = resolve_join_input(
        state,
        database_id,
        tenant_id,
        right_input,
        trace_id,
        txn_id,
        captures,
    )
    .await?;

    // Cross-node build-side gather.
    //
    // The HashJoin task routes to the LEFT (probe) collection's owning
    // vShard, where the LEFT side is scanned locally. The RIGHT (build)
    // collection is otherwise scanned BY NAME from that same node — but
    // a single-vShard-homed build collection may live on a DIFFERENT
    // node, so the by-name scan returns nothing and the join drops rows.
    //
    // When a gateway is installed (it always is, single node included),
    // and the build side has not already been materialized by
    // `resolve_join_input` (`right_input` still `None`), and
    // `right_collection` names a real user collection (catalog sides
    // carry an empty name and are already embedded as a
    // ProviderScan), gather the build collection
    // across all vShards on the coordinator and inline it as a
    // `ProviderScan`. The HashJoin shipped to the probe node is then
    // self-contained. Only the RIGHT/build side is gathered; the
    // LEFT/probe side stays local to the routed vShard.
    if state.gateway.get().is_some() && right_input.is_none() && !right_collection.is_empty() {
        right_input = gather_join_build_side(
            state,
            database_id,
            tenant_id,
            // The side's own collection and its own injected policy,
            // taken as one value: a planner that swaps build and probe
            // swaps both together, never one without the other.
            ScanSide::join_side(&right_collection, &right_rls_filters),
            trace_id,
            txn_id,
            captures,
        )
        .await?;
    }

    Ok(Resolved::Plan(Box::new(PhysicalPlan::Query(
        QueryOp::HashJoin {
            left_collection: nodedb_types::QualifiedCollection::from_stored(left_collection),
            right_collection: nodedb_types::QualifiedCollection::from_stored(right_collection),
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            computed_projection,
            join_filters,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            left_rls_filters,
            right_rls_filters,
        },
    ))))
}
