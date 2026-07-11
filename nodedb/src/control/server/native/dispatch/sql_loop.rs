// SPDX-License-Identifier: BUSL-1.1

//! Per-task dispatch loop for the DataFusion-planned SQL path, plus the
//! single-task dispatch helper it calls. Split out of `sql.rs` to keep
//! that file under the file-size limit; behavior is unchanged — this is
//! the same code that used to run inline in `execute_planned`.

use nodedb_types::TraceId;
use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagedTagKind, StagingGateError, route_in_tx_write,
};
use crate::types::{DatabaseId, Lsn, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::sql_gateway::dispatch_task_via_gateway;
use super::streaming::SqlOutcome;
use super::{DispatchCtx, error_to_native, shape_error_to_native, to_native_columns_rows};
use crate::control::server::broadcast::broadcast_count_to_all_cores;
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
) -> SqlOutcome {
    let mut all_columns: Option<Vec<String>> = None;
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut last_lsn = 0u64;
    let mut total_affected = 0u64;

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

        // In transaction: route through the protocol-neutral staging gate.
        // Reads (including in-transaction reads) come back as `Read` with
        // `txn_id` stamped for read-your-own-writes; non-stageable writes are
        // buffered for COMMIT-time replay; stageable writes are applied to
        // the per-transaction overlay immediately for a real affected count
        // and statement-time constraint errors. Outside a transaction block,
        // `route_in_tx_write` always returns `Read(task)` unchanged, so the
        // autocommit path is untouched.
        let task =
            match route_in_tx_write(ctx.sessions, ctx.peer_addr, task, |stage_task| async move {
                dispatch_task(ctx, stage_task).await.map(|(resp, _)| resp)
            })
            .await
            {
                Ok(InTxnRoute::Read(routed_task)) => *routed_task,
                Ok(InTxnRoute::Buffered) => {
                    total_affected += 1;
                    continue;
                }
                Ok(InTxnRoute::Staged(outcome)) => {
                    if matches!(outcome.kind, StagedTagKind::RawPayload)
                        && !outcome.payload.is_empty()
                    {
                        let plan_kind = describe_plan(&plan_for_staged_response);
                        match shape_response_materialized(
                            &outcome.payload,
                            &plan_for_staged_response,
                            plan_kind,
                            output_schema,
                            ctx.state,
                            database_id,
                            ctx.tenant_id(),
                        ) {
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
        let (task_resp, shard_watermarks) = match dispatch_task(ctx, task).await {
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
            || task_resp.error_code == Some(crate::bridge::envelope::ErrorCode::NotFound);
        if records_read
            && ctx.sessions.transaction_state(ctx.peer_addr)
                == crate::control::server::shared::session::TransactionState::InBlock
        {
            let watermarks = if shard_watermarks.is_empty() {
                vec![(task_vshard, task_resp.watermark_lsn)]
            } else {
                shard_watermarks
            };
            crate::control::server::shared::session::record_read_set(
                ctx.sessions,
                ctx.peer_addr,
                ctx.tenant_id(),
                &plan_for_response,
                &watermarks,
            );
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

        if task_resp.payload.is_empty() {
            total_affected += 1;
        } else {
            let plan_kind = describe_plan(&plan_for_response);
            match shape_response_materialized(
                &task_resp.payload,
                &plan_for_response,
                plan_kind,
                output_schema,
                ctx.state,
                database_id,
                ctx.tenant_id(),
            ) {
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
) -> crate::Result<(Response, Vec<(VShardId, Lsn)>)> {
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        },
    ) = &task.plan
    {
        let resp = crate::control::insert_select::run_insert_select(
            ctx.state,
            task.tenant_id,
            task.database_id,
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        )
        .await?;
        return Ok((resp, Vec::new()));
    }

    // `DROP ARRAY` fans out to every core so per-core stores are released.
    if matches!(
        task.plan,
        crate::bridge::envelope::PhysicalPlan::Array(
            nodedb_physical::physical_plan::ArrayOp::DropArray { .. }
        )
    ) {
        let resp = broadcast_count_to_all_cores(
            ctx.state,
            task.tenant_id,
            task.database_id,
            task.plan,
            TraceId::ZERO,
            "dropped",
        )
        .await?;
        return Ok((resp, Vec::new()));
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
        Resolved::Gathered(resp, shard_watermarks) => return Ok((resp, shard_watermarks)),
        Resolved::Plan(resolved_plan) => {
            task.plan = resolved_plan;
        }
        // Native path materializes the stream into a Response (it streams later
        // in its own effort); preserves the existing gather-then-return shape.
        Resolved::Stream(s) => {
            let resp = crate::control::server::exchange::gather::stream_to_response(s).await?;
            return Ok((resp, Vec::new()));
        }
    }

    // All other tasks — point ops, writes, Raft-replicated writes — route
    // through the gateway when available (cluster-aware routing + retry),
    // or via the local SPSC path when the gateway is not yet wired.
    let resp = dispatch_task_via_gateway(ctx, task).await?;
    Ok((resp, Vec::new()))
}
