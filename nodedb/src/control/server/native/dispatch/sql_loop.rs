// SPDX-License-Identifier: BUSL-1.1

//! Per-task dispatch loop for the DataFusion-planned SQL path, plus the
//! single-task dispatch helper it calls. Split out of `sql.rs` to keep
//! that file under the file-size limit; behavior is unchanged — this is
//! the same code that used to run inline in `execute_planned`.

use std::sync::Arc;

use nodedb_types::TraceId;
use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::session::expander_stage::{
    ExpanderOutcome, route_in_tx_expander,
};
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagedTagKind, StagingGateError, route_in_tx_write,
};
use crate::types::{DatabaseId, Lsn, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::sql_gateway::dispatch_task_via_gateway;
use super::streaming::SqlOutcome;
use super::{DispatchCtx, error_to_native, shape_error_to_native, to_native_columns_rows};
use crate::control::server::exchange::DistributedReadCapture;
use crate::control::server::exchange::resolve::{Resolved, resolve_and_materialize};

/// Wrap a materialized response as a non-streaming [`SqlOutcome`].
#[inline]
fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
}

/// Run the per-task dispatch loop for a planned, non-streamed task set,
/// materializing all rows/columns/affected-count into a single
/// [`SqlOutcome::Response`].
///
/// Called from `execute_planned` after the streaming fast path has been
/// ruled out (or declined). Buffers writes when in an explicit transaction
/// block, exactly like the pgwire dispatch loop.
pub(super) async fn run_dispatch_loop(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tasks: Vec<PhysicalTask>,
    output_schema: Option<&OutputSchema>,
    database_id: DatabaseId,
    plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
) -> SqlOutcome {
    let mut all_columns: Option<Vec<String>> = None;
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut last_lsn = 0u64;
    let mut total_affected = 0u64;
    // Checked once rather than per task — metering is disabled by default, so
    // this keeps the per-task extraction below (which clones the collection
    // name) a true no-op on the hot path for every deployment that hasn't
    // turned it on.
    let metering_enabled = ctx.state.metering_config.enabled;

    for task in tasks {
        if task.tenant_id != ctx.tenant_id() {
            return resp(NativeResponse::error(
                seq,
                "42501",
                "tenant isolation violation",
            ));
        }

        // Cloned before `route_in_tx_write` consumes `task`, so a staged
        // write whose outcome carries a computed payload (KV `Incr` /
        // `IncrFloat` / `Cas` / `GetSet` -- see `StagedTagKind::RawPayload`)
        // can be shaped into the response exactly like the non-staged branch
        // below shapes `task_resp.payload`.
        let plan_for_staged_response = task.plan.clone();
        // Extracted from the same clone above, before `task` is moved into
        // the routing call below — metering needs the collection/engine
        // shape after this task's dispatch succeeds. Only covers the direct
        // dispatch below (`InTxnRoute::Read`, i.e. autocommit writes/reads
        // and in-transaction reads); `Buffered`/`Staged` tasks `continue`
        // before reaching the metering call and are not billed here — a
        // `Buffered` task performs no dispatch yet (replayed at COMMIT), and
        // a `Staged` task's dispatch happens inside `route_in_tx_write`'s
        // closure, whose response is consumed before returning `Staged` and
        // is not observable at this loop level.
        let plan_metering_info =
            metering_enabled.then(|| PlanMeteringInfo::extract(&plan_for_staged_response));

        // In transaction: route through the protocol-neutral staging gate.
        // Reads (including in-transaction reads) come back as `Read` with
        // `txn_id` stamped for read-your-own-writes; non-stageable writes are
        // buffered for COMMIT-time replay; stageable writes are applied to
        // the per-transaction overlay immediately for a real affected count
        // and statement-time constraint errors. Outside a transaction block,
        // `route_in_tx_write` always returns `Read(task)` unchanged, so the
        // autocommit path is untouched.
        // In-transaction `MERGE` and `UPDATE ... FROM` are resolved + staged at
        // STATEMENT time by the expander (read-your-own-writes for later
        // statements in the same txn); every other task falls through to the
        // neutral staging gate.
        let buffer_start = ctx.sessions.buffered_task_count(ctx.peer_addr);
        let routed = match route_in_tx_expander(
            ctx.state,
            ctx.sessions,
            ctx.peer_addr.into(),
            task,
            |stage_task| async move {
                dispatch_task(ctx, stage_task)
                    .await
                    .map(|(resp, _, _)| resp)
            },
        )
        .await
        {
            Ok(ExpanderOutcome::Handled(route)) => Ok(route),
            Ok(ExpanderOutcome::Passthrough(task)) => {
                route_in_tx_write(
                    ctx.state,
                    ctx.sessions,
                    ctx.peer_addr.into(),
                    *task,
                    |stage_task| async move {
                        dispatch_task(ctx, stage_task)
                            .await
                            .map(|(resp, _, _)| resp)
                    },
                )
                .await
            }
            Err(e) => Err(e),
        };
        if ctx.sessions.buffered_task_count(ctx.peer_addr) > buffer_start
            && !ctx.sessions.attach_tx_lease_scope_since(
                ctx.peer_addr,
                buffer_start,
                Arc::clone(&plan_lease_scope),
            )
        {
            return resp(NativeResponse::error(
                seq,
                "XX000",
                "internal error: failed to retain descriptor leases for buffered transaction tasks",
            ));
        }
        let task = match routed {
            Ok(InTxnRoute::Read(routed_task)) => *routed_task,
            Ok(InTxnRoute::Buffered) => {
                total_affected += 1;
                continue;
            }
            Ok(InTxnRoute::Staged(outcome)) => {
                if matches!(outcome.kind, StagedTagKind::RawPayload) && !outcome.payload.is_empty()
                {
                    let plan_kind = describe_plan(&plan_for_staged_response);
                    let redaction = QueryRedaction::for_plan(
                        ctx.tenant_id(),
                        ctx.auth_context(),
                        &plan_for_staged_response,
                    );
                    match shape_response_materialized(MaterializedShapeRequest {
                        payload: &outcome.payload,
                        plan: &plan_for_staged_response,
                        plan_kind,
                        projection: output_schema,
                        state: ctx.state,
                        database_id,
                        tenant_id: ctx.tenant_id(),
                        redaction: Some(redaction.ctx(&ctx.state.redaction)),
                    }) {
                        Ok(ShapeOutcome::Rows(mut shaped)) => {
                            if let Some(notice) = shaped.notice.take() {
                                warnings.push(notice);
                            }
                            let (cols, rows) = to_native_columns_rows(&shaped);
                            if !cols.is_empty() && all_columns.is_none() {
                                all_columns = Some(cols);
                            }
                            all_rows.extend(rows);
                        }
                        Ok(ShapeOutcome::Passthrough) => {
                            total_affected += 1;
                        }
                        Err(e) => return resp(shape_error_to_native(seq, &e)),
                    }
                } else {
                    total_affected += outcome.affected as u64;
                }
                continue;
            }
            Err(StagingGateError::Dispatch(e)) => return resp(error_to_native(seq, &e)),
            Err(StagingGateError::Rejected { code }) => {
                let (_, sqlstate, message) = match code {
                    Some(code) => error_code_to_sqlstate(&code),
                    None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
                };
                return resp(NativeResponse::error(seq, sqlstate, message));
            }
        };

        let plan_for_response = task.plan.clone();
        let task_vshard = task.vshard_id;
        let (task_resp, shard_watermarks, dist_reads) = match dispatch_task(ctx, task).await {
            Ok(r) => r,
            Err(e) => return resp(error_to_native(seq, &e)),
        };

        // Track reads for snapshot-isolation / cross-shard conflict detection at
        // the protocol-neutral layer — the native (canonical) transport records
        // identically to pgwire. Recorded BEFORE the error short-circuit so an
        // absent-key point read (a `NotFound` from the Data Plane) is captured
        // too; a "not found" is a validatable phantom observation. A multi-core
        // fan read records one entry per participating shard from the gather's
        // per-shard watermarks; a single read falls back to its one watermark.
        let records_read = task_resp.status == Status::Ok
            || task_resp.error_code.as_deref()
                == Some(&crate::bridge::envelope::ErrorCode::NotFound);
        if records_read
            && ctx.sessions.transaction_state(ctx.peer_addr)
                == crate::control::server::shared::session::TransactionState::InBlock
        {
            let watermarks = if shard_watermarks.is_empty() {
                vec![(task_vshard, task_resp.watermark_lsn)]
            } else {
                shard_watermarks
            };
            crate::control::server::shared::session::record_reads_for_response(
                ctx.state,
                ctx.sessions,
                ctx.peer_addr.into(),
                ctx.tenant_id(),
                crate::control::server::shared::session::ResponseReads {
                    plan: &plan_for_response,
                    watermarks: &watermarks,
                    read_version_lsn: task_resp.read_version_lsn,
                    found: task_resp.status == Status::Ok,
                    distributed_reads: &dist_reads,
                    read_lsn_vshard: task_vshard,
                },
            )
            .await;
        }

        if task_resp.status == Status::Error {
            let msg = if task_resp.payload.is_empty() {
                task_resp
                    .error_code
                    .as_ref()
                    .map(|c| format!("{c:?}"))
                    .unwrap_or_else(|| "unknown error".into())
            } else {
                String::from_utf8_lossy(&task_resp.payload).into_owned()
            };
            return resp(NativeResponse::error(seq, "XX000", msg));
        }

        last_lsn = task_resp.watermark_lsn.as_u64();

        // This task's own row count, for metering below — distinct from
        // `total_affected`/`all_rows`, which accumulate across every task in
        // the loop.
        let mut task_rows: Option<u64> = None;
        let plan_kind = describe_plan(&plan_for_response);
        if let crate::control::server::response_shape::types::PlanKind::DmlResult(_) = plan_kind {
            // A count-bearing write reports the rows it actually touched. Adding
            // 1 per dispatched task instead — which is what the empty-payload
            // branch below used to do — reported a row for a delete that removed
            // nothing and for an `ON CONFLICT DO NOTHING` insert that skipped.
            match crate::control::server::shared::sql::staging_predicates::require_affected_count(
                &task_resp.payload,
            ) {
                Ok(n) => {
                    total_affected += n;
                    task_rows = Some(n);
                }
                Err(e) => return resp(error_to_native(seq, &e)),
            }
        } else if task_resp.payload.is_empty() {
            // Not a count-bearing plan (graph / vector / index write): one unit
            // of work per dispatched task, as before.
            total_affected += 1;
        } else {
            let redaction =
                QueryRedaction::for_plan(ctx.tenant_id(), ctx.auth_context(), &plan_for_response);
            match shape_response_materialized(MaterializedShapeRequest {
                payload: &task_resp.payload,
                plan: &plan_for_response,
                plan_kind,
                projection: output_schema,
                state: ctx.state,
                database_id,
                tenant_id: ctx.tenant_id(),
                redaction: Some(redaction.ctx(&ctx.state.redaction)),
            }) {
                Ok(ShapeOutcome::Rows(mut shaped)) => {
                    if let Some(notice) = shaped.notice.take() {
                        warnings.push(notice);
                    }
                    let (cols, rows) = to_native_columns_rows(&shaped);
                    if !cols.is_empty() && all_columns.is_none() {
                        all_columns = Some(cols);
                    }
                    task_rows = Some(rows.len() as u64);
                    all_rows.extend(rows);
                }
                Ok(ShapeOutcome::Passthrough) => {
                    total_affected += 1;
                }
                Err(e) => return resp(shape_error_to_native(seq, &e)),
            }
        }

        // Metered here, once per successfully dispatched task (see the scope
        // note on `plan_metering_info` above for the tasks this does not
        // cover) — `task_resp.status == Status::Error` returned above, so
        // every path reaching here is the success path.
        if let Some(info) = &plan_metering_info {
            meter_dispatch(ctx.state, &ctx.scope, info, task_rows);
        }
    }

    if all_rows.is_empty() {
        let mut r = NativeResponse::ok(seq);
        r.rows_affected = Some(total_affected);
        r.watermark_lsn = last_lsn;
        r.warnings = warnings;
        resp(r)
    } else {
        resp(NativeResponse {
            seq,
            status: nodedb_types::protocol::ResponseStatus::Ok,
            columns: all_columns,
            rows: Some(all_rows),
            rows_affected: Some(total_affected),
            watermark_lsn: last_lsn,
            error: None,
            auth: None,
            warnings,
        })
    }
}

/// Dispatch a single PhysicalTask.
///
/// `INSERT ... SELECT` is intercepted here and run by the Control-Plane
/// orchestrator (`control::insert_select`); `DROP ARRAY` fans out to every
/// core. All other tasks flow through `dispatch_task_via_gateway` which routes
/// via the gateway when available, or falls back to the local SPSC path on
/// single-node boot.
/// Dispatch a single PhysicalTask, returning the response plus the per-shard
/// watermark LSNs a single-node fan gather observed (one `(vshard, watermark)`
/// per responding core). The list is empty for a non-gathered dispatch; the
/// transactional read-recording seam in [`run_dispatch_loop`] then falls back
/// to the single response watermark.
async fn dispatch_task(
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
