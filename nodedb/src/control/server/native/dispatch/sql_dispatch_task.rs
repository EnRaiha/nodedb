// SPDX-License-Identifier: BUSL-1.1

//! The single-task dispatch primitive the native SQL loop calls.
//!
//! Split out of `sql_loop.rs` so that file stays within the file-size limit;
//! behavior is unchanged. Everything here decides HOW one already-planned task
//! reaches an engine — Control-Plane orchestrators for the multi-arm writes,
//! Exchange resolution, then the gateway — while `sql_loop.rs` decides what to
//! do with the answers.

use nodedb_types::TraceId;

use crate::bridge::envelope::Response;
use crate::control::server::exchange::DistributedReadCapture;
use crate::control::server::exchange::resolve::{Resolved, resolve_and_materialize};
use crate::types::{Lsn, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;
use super::sql_gateway::dispatch_task_via_gateway;

/// Dispatch a single `PhysicalTask`, returning the response plus the per-shard
/// watermark LSNs a single-node fan gather observed (one `(vshard, watermark)`
/// per responding core).
///
/// `INSERT ... SELECT`, autocommit `MERGE` and autocommit `UPDATE ... FROM` are
/// intercepted here and run by their Control-Plane orchestrators; `DROP ARRAY`
/// fans out to every core. All other tasks flow through
/// `dispatch_task_via_gateway`, which routes via the gateway when available or
/// falls back to the local SPSC path on single-node boot.
///
/// The watermark list is empty for a non-gathered dispatch; the transactional
/// read-recording seam in the caller's loop then falls back to the single
/// response watermark.
pub(super) async fn dispatch_task(
    ctx: &DispatchCtx<'_>,
    mut task: PhysicalTask,
) -> crate::Result<(Response, Vec<(VShardId, Lsn)>, Vec<DistributedReadCapture>)> {
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. },
    ) = &task.plan
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp =
            crate::control::insert_select::run_authorized_insert_select(ctx.state, authorized)
                .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Autocommit `MERGE` is orchestrated on the Control Plane
    // (`control::merge_orchestrator`): each NOT-MATCHED insert row gets its OWN
    // fresh, registered surrogate and all arms apply atomically.
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::Merge {
            target_collection: _,
            source_collection: _,
            source_alias: _,
            target_join_col: _,
            source_join_col: _,
            clauses: _,
            returning: _,
            resolve_only: false,
            resolved_inserts: None,
            source_rows: _,
            rls_filters: _,
            rls_write_check: _,
            resolved_sum_targets: _,
        },
    ) = &task.plan
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp =
            crate::control::merge_orchestrator::run_authorized_merge(ctx.state, authorized).await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Autocommit `UPDATE ... FROM <source>` is orchestrated on the Control Plane
    // (`control::update_from_join_orchestrator`): the source is scanned on its
    // OWN core and shipped into the plan so the target-core handler joins
    // against it instead of a local read (the source's vShard can live on a
    // different core).
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
            target_collection: _,
            source_collection: _,
            source_alias: _,
            target_join_col: _,
            source_join_col: _,
            updates: _,
            target_filters: _,
            returning: _,
            resolve_only: false,
            source_rows: None,
            rls_filters: _,
            rls_write_check: _,
            resolved_sum_targets: _,
        },
    ) = &task.plan
    {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let resp = crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
            ctx.state, authorized,
        )
        .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Native DROP uses the same authorization and reversible all-core
    // protocol as pgwire; it must never bypass the catalog transition.
    if matches!(
        task.plan,
        crate::bridge::envelope::PhysicalPlan::Array(
            nodedb_physical::physical_plan::ArrayOp::DropArray { .. }
        )
    ) {
        let authorized = super::sql_gateway::authorize_native_task(ctx, &task)?;
        let task = authorized.into_physical_task();
        let resp = crate::control::array_catalog::ddl::run_authorized_drop(
            ctx.state,
            task.tenant_id,
            task.database_id,
            task.plan,
            TraceId::ZERO,
        )
        .await?;
        return Ok((resp, Vec::new(), Vec::new()));
    }

    // Exchange resolution: materialize catalog providers and resolve any
    // Exchange nodes (Gather/Broadcast) before dispatch.
    match resolve_and_materialize(
        ctx.state,
        ctx.identity,
        task.database_id,
        task.tenant_id,
        task.plan,
        TraceId::ZERO,
        task.txn_id,
    )
    .await?
    {
        Resolved::Gathered(resp, shard_watermarks, dist_reads) => {
            return Ok((resp, shard_watermarks, dist_reads));
        }
        Resolved::Plan(resolved_plan) => {
            let resolved_plan = *resolved_plan;
            task.plan = resolved_plan;
        }
        // Native path materializes the stream into a Response (it streams later
        // in its own effort); preserves the existing gather-then-return shape.
        Resolved::Stream(s) => {
            let resp = crate::control::server::exchange::gather::stream_to_response(s).await?;
            return Ok((resp, Vec::new(), Vec::new()));
        }
    }

    // All other tasks — point ops, writes, Raft-replicated writes — route
    // through the gateway when available (cluster-aware routing + retry),
    // or via the local SPSC path when the gateway is not yet wired.
    let resp = dispatch_task_via_gateway(ctx, task).await?;
    Ok((resp, Vec::new(), Vec::new()))
}
