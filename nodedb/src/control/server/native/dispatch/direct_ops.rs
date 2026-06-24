// SPDX-License-Identifier: BUSL-1.1

//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, classify_dispatch, dispatch_tasks_to_calvin,
};
use crate::data::executor::response_codec;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::server::wal_dispatch;

use super::super::super::dispatch_utils;
use super::{DispatchCtx, error_to_native};

/// Dispatch a direct Data Plane operation by opcode.
pub(crate) async fn handle_direct_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    // Per-operation cap enforcement (vector dim, top_k, batch size, etc.).
    if let Err(e) = super::limits::check_op_limits(ctx.state, fields) {
        return NativeResponse::error(seq, "0A000", e.to_string());
    }

    // Quota enforcement — reject before planning or dispatch.
    if let Err(e) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &e);
    }

    let mut plan = match super::plan_builder::build_plan(ctx, op, fields, &collection) {
        Ok(p) => p,
        Err(e) => return NativeResponse::error(seq, "42601", e.to_string()),
    };

    // Inject RLS filters from auth context (same as pgwire planner).
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context,
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    // Implicit graph-edge extraction (pgwire / native-SQL parity): a schemaless
    // document carrying `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task.
    // The common no-edge case leaves `tasks` at length 1 and runs the existing
    // single-dispatch path byte-identically below; an edge-bearing insert
    // augments the vec and routes through classify/Calvin like every other
    // write surface.
    let mut tasks = vec![PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: DatabaseId::DEFAULT,
        plan,
        post_set_op: PostSetOp::None,
    }];
    if let Err(e) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        ctx.state,
        &mut tasks,
        tenant_id,
        DatabaseId::DEFAULT,
        TraceId::ZERO,
    )
    .await
    {
        return error_to_native(seq, &e);
    }

    if tasks.len() == 1
        && let Some(task) = tasks.pop()
    {
        // No-edge fast path — behaviorally identical to the pre-migration
        // single-plan dispatch. The local-path WAL append now lives inside
        // `dispatch_single_task` so it is shared with the single-shard edge loop.
        ctx.state.tenant_request_start(tenant_id);
        let result = dispatch_single_task(ctx, seq, tenant_id, vshard_id, task.plan).await;
        ctx.state.tenant_request_end(tenant_id);
        return result;
    }

    // Edge-bearing insert: route the augmented task set the same way native SQL
    // does. A cross-shard set goes through the Calvin sequencer atomically (which
    // owns its own replicated durability); a single-shard set dispatches each
    // task sequentially (matching pgwire / native-SQL single-shard multi-task),
    // returning the document task's response. Local WAL durability for the
    // single-shard path is handled inside `dispatch_single_task`.
    ctx.state.tenant_request_start(tenant_id);
    let result = match classify_dispatch(&tasks) {
        DispatchClass::MultiShard { .. } => {
            match dispatch_tasks_to_calvin(
                ctx.state,
                &tasks,
                tenant_id,
                CrossShardTxnMode::Strict,
                false,
            )
            .await
            {
                Ok(()) => {
                    let mut r = NativeResponse::ok(seq);
                    r.rows_affected = Some(tasks.len() as u64);
                    r
                }
                Err(e) => error_to_native(seq, &e),
            }
        }
        DispatchClass::SingleShard { .. } => {
            // The document task is first; its response is the one returned to
            // the caller. Edge tasks dispatch after it in order.
            let mut doc_response: Option<NativeResponse> = None;
            let mut error: Option<NativeResponse> = None;
            for task in tasks {
                let task_vshard = task.vshard_id;
                let resp = dispatch_single_task(ctx, seq, tenant_id, task_vshard, task.plan).await;
                if resp.status == nodedb_types::protocol::ResponseStatus::Error {
                    error = Some(resp);
                    break;
                }
                if doc_response.is_none() {
                    doc_response = Some(resp);
                }
            }
            error
                .or(doc_response)
                .unwrap_or_else(|| NativeResponse::ok(seq))
        }
    };
    ctx.state.tenant_request_end(tenant_id);
    result
}

/// Dispatch a native `GraphMatch` op, unwrapping the DP `{rows, frontier}`
/// envelope into a bare rows array before native conversion.
///
/// MATCH responses are enveloped on the DP→CP hop (see
/// `data::executor::handlers::graph_match`). The native row decoder expects a
/// bare msgpack array, so this handler unwraps the envelope here. In B1
/// `cluster_mode` is always `false`, so the frontier is empty and the rows
/// payload is byte-identical to the prior bare-array native MATCH response.
/// (B2 will consume the frontier for cross-shard continuation.)
pub(crate) async fn handle_graph_match(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    if let Err(e) = super::limits::check_op_limits(ctx.state, fields) {
        return NativeResponse::error(seq, "0A000", e.to_string());
    }
    if let Err(e) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &e);
    }

    let mut plan =
        match super::plan_builder::build_plan(ctx, OpCode::GraphMatch, fields, &collection) {
            Ok(p) => p,
            Err(e) => return NativeResponse::error(seq, "42601", e.to_string()),
        };
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context,
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    ctx.state.tenant_request_start(tenant_id);
    let raw = dispatch_single_task_raw(ctx, tenant_id, vshard_id, plan).await;
    ctx.state.tenant_request_end(tenant_id);

    let resp = match raw {
        Ok(r) => r,
        Err(e) => return error_to_native(seq, &e),
    };

    if resp.status == Status::Error {
        return data_plane_response_to_native(seq, &resp);
    }

    // Unwrap the `{rows, frontier, resume}` envelope into a bare rows array. The
    // frontier is discarded here (B2 consumes it for cross-shard dispatch); the
    // resume cursor is likewise not acted on on this single-shard direct-op
    // path — the frame's `partial` flag already marks a truncated result.
    let unwrapped =
        match crate::control::server::graph_dispatch::unwrap_match_envelope(&resp.payload) {
            Ok(u) => Response {
                payload: u.rows_payload,
                ..resp
            },
            Err(e) => return error_to_native(seq, &e),
        };
    data_plane_response_to_native(seq, &unwrapped)
}

/// Dispatch one plan via the gateway (when wired) or the local SPSC path,
/// converting the Data-Plane response into a `NativeResponse`.
///
/// This is the exact single-plan dispatch the direct-op handler used before
/// implicit-edge extraction; it is factored out so the no-edge fast path and
/// the single-shard edge loop share one code path.
async fn dispatch_single_task(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> NativeResponse {
    match dispatch_single_task_raw(ctx, tenant_id, vshard_id, plan).await {
        Ok(resp) => data_plane_response_to_native(seq, &resp),
        Err(e) => error_to_native(seq, &e),
    }
}

/// Dispatch one plan via the gateway (when wired) or the local SPSC path and
/// return the raw Data-Plane [`Response`] without native conversion.
///
/// Factored out of [`dispatch_single_task`] so MATCH dispatch can unwrap the
/// `{rows, frontier}` envelope before native conversion while every other
/// direct op keeps its prior convert-in-place behaviour.
async fn dispatch_single_task_raw(
    ctx: &DispatchCtx<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    match ctx.state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id: DatabaseId::DEFAULT,
            };
            match gw.execute(&gw_ctx, plan).await {
                Ok(payloads) => Ok(gateway_payloads_to_response(payloads)),
                Err(e) => {
                    let (_code, msg) = GatewayErrorMap::to_native(&e);
                    Err(crate::Error::Dispatch { detail: msg })
                }
            }
        }
        None => {
            // Local SPSC path (single-node boot, before the gateway is wired):
            // the gateway would otherwise own WAL durability on the target node,
            // so we must append locally before dispatching. Doing it here covers
            // every local dispatch — the no-edge fast path AND each task of a
            // single-shard edge bundle — so an implicit edge written on the boot
            // path is durable. (Cross-shard bundles route via Calvin, which owns
            // its own replicated durability and never reaches this branch.)
            wal_dispatch::wal_append_if_write(
                &ctx.state.wal,
                tenant_id,
                vshard_id,
                DatabaseId::DEFAULT,
                &plan,
            )?;
            dispatch_utils::dispatch_to_data_plane(
                ctx.state,
                tenant_id,
                DatabaseId::DEFAULT,
                vshard_id,
                plan,
                TraceId::ZERO,
            )
            .await
        }
    }
}

/// Convert gateway `Vec<Vec<u8>>` payloads into a synthetic `Response`.
fn gateway_payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
    let payload = payloads
        .into_iter()
        .next()
        .map(Payload::from_vec)
        .unwrap_or_else(Payload::empty);
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 0,
        partial: false,
        payload,
        watermark_lsn: Lsn::new(0),
        error_code: None,
    }
}

fn data_plane_response_to_native(seq: u64, resp: &Response) -> NativeResponse {
    if resp.status == Status::Error {
        let msg = if resp.payload.is_empty() {
            resp.error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown error".into())
        } else {
            String::from_utf8_lossy(&resp.payload).into_owned()
        };
        return NativeResponse::error(seq, "XX000", msg);
    }

    if resp.payload.is_empty() {
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        return r;
    }

    let json_text = response_codec::decode_payload_to_json(&resp.payload);
    let (columns, rows) = super::parse_json_to_columns_rows(&json_text);
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(columns),
        rows: Some(rows),
        rows_affected: None,
        watermark_lsn: resp.watermark_lsn.as_u64(),
        error: None,
        auth: None,
        warnings: Vec::new(),
    }
}
