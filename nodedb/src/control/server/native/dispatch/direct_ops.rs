// SPDX-License-Identifier: BUSL-1.1

//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin,
};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::types::{Lsn, RequestId, TenantId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::raw_dispatch::{authorize_single_task, dispatch_authorized_single_task};
use super::response::data_plane_response_to_native;
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

    // CRDT Apply allocates a surrogate while planning, so authorize the exact
    // collection before any planner-side state or admission preview is touched.
    if matches!(op, OpCode::CrdtApply) {
        let audit = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &ctx.state.audit,
        ));
        if let Err(error) = crate::control::server::shared::authorization::authorize_collection(
            ctx.identity,
            ctx.database_id(),
            &collection,
            crate::control::security::identity::Permission::Write,
            &ctx.state.permissions,
            &ctx.state.roles,
            &audit,
        ) {
            return error_to_native(seq, &crate::Error::from(error));
        }
    }

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

    // Apply RLS before any special Control-Plane orchestration can observe the plan.
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context,
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    // `INSERT ... SELECT` is orchestrated on the Control Plane (fresh, registered
    // surrogate per target row + atomic `BatchInsert`); it never reaches the
    // Data Plane as a single op.
    if matches!(
        &plan,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. })
    ) {
        let task = PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: ctx.database_id(),
            plan: plan.clone(),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
            Ok(authorized) => authorized,
            Err(error) => return error_to_native(seq, &error),
        };
        let _request = ctx.state.tenant_request_guard(tenant_id);
        let result =
            crate::control::insert_select::run_authorized_insert_select(ctx.state, authorized)
                .await;
        return match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
            Err(e) => error_to_native(seq, &e),
        };
    }

    // Autocommit `MERGE` is orchestrated on the Control Plane (fresh, registered
    // surrogate per NOT-MATCHED insert row + atomic apply); it never reaches the
    // Data Plane as a single op.
    if matches!(
        &plan,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Merge {
            resolve_only: false,
            resolved_inserts: None,
            ..
        })
    ) {
        let task = PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: ctx.database_id(),
            plan: plan.clone(),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
            Ok(authorized) => authorized,
            Err(error) => return error_to_native(seq, &error),
        };
        let _request = ctx.state.tenant_request_guard(tenant_id);
        let result =
            crate::control::merge_orchestrator::run_authorized_merge(ctx.state, authorized).await;
        return match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
            Err(e) => error_to_native(seq, &e),
        };
    }

    // Autocommit `UPDATE ... FROM <source>` is orchestrated on the Control Plane
    // (source scanned on its own core + shipped into the plan); it never reaches
    // the Data Plane as a single op reading a possibly-non-resident source.
    if matches!(
        &plan,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
            resolve_only: false,
            source_rows: None,
            ..
        })
    ) {
        let task = PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: ctx.database_id(),
            plan: plan.clone(),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
            Ok(authorized) => authorized,
            Err(error) => return error_to_native(seq, &error),
        };
        let _request = ctx.state.tenant_request_guard(tenant_id);
        let result =
            crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                ctx.state, authorized,
            )
            .await;
        return match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
            Err(e) => error_to_native(seq, &e),
        };
    }

    // Stamp the connection's active transaction id (as the SQL path's
    // `route_in_tx_write` does for in-transaction reads — see
    // `staging_gate.rs::route_in_tx_write`) so the Data Plane can resolve this
    // transaction's staging overlay for read-your-own-writes on direct-op
    // reads (PointGet / RangeScan / VectorSearch) and give direct-op writes
    // (KvBatchPut) a real transaction identity. `tx_id` is `None` outside a
    // transaction block, so autocommit behavior is unchanged.
    let txn_id = ctx.sessions.tx_id(ctx.peer_addr);

    // Implicit graph-edge extraction (pgwire / native-SQL parity): a schemaless
    // document carrying `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task.
    // The common no-edge case leaves `tasks` at length 1 and runs the existing
    // single-dispatch path byte-identically below; an edge-bearing insert
    // augments the vec and routes through classify/Calvin like every other
    // write surface.
    let mut tasks = vec![PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: ctx.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id,
    }];
    if let Err(e) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        ctx.state,
        &mut tasks,
        tenant_id,
        ctx.database_id(),
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
        let _request = ctx.state.tenant_request_guard(tenant_id);
        return dispatch_single_task(ctx, seq, tenant_id, vshard_id, task.plan, task.txn_id).await;
    }

    // Edge-bearing insert: route the augmented task set the same way native SQL
    // does. A cross-shard set goes through the Calvin sequencer atomically (which
    // owns its own replicated durability); a single-shard set dispatches each
    // task sequentially (matching pgwire / native-SQL single-shard multi-task),
    // returning the document task's response. Local WAL durability for the
    // single-shard path is handled inside `dispatch_single_task`.
    let _request = ctx.state.tenant_request_guard(tenant_id);
    // Autocommit direct-ops dispatch: no session read-set to widen with.
    match classify_dispatch(&tasks, &std::collections::BTreeSet::new()) {
        DispatchClass::MultiShard { .. } => {
            let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
                &ctx.state.audit,
            ));
            let authorized = match crate::control::server::shared::authorization::authorize_task_set(
                ctx.identity,
                &tasks,
                &ctx.state.permissions,
                &ctx.state.roles,
                &emitter,
            ) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &crate::Error::from(error)),
            };
            match dispatch_authorized_tasks_to_calvin(
                ctx.state,
                authorized,
                tenant_id,
                CrossShardTxnMode::Strict,
                TxnDispatchPosition::Autocommit,
                &[],
                None,
            )
            .await
            {
                // Edge-bearing INSERT: no RETURNING clause is possible here, so
                // the applied Response (if any) carries no rows — report one
                // row-affected per task.
                Ok(_apply) => {
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
                let task_txn_id = task.txn_id;
                let resp =
                    dispatch_single_task(ctx, seq, tenant_id, task_vshard, task.plan, task_txn_id)
                        .await;
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
    }
}

/// Dispatch one plan via the gateway (when wired) or the local SPSC path,
/// converting the Data-Plane response into a `NativeResponse`.
///
/// This is the exact single-plan dispatch the direct-op handler used before
/// implicit-edge extraction; it is factored out so the no-edge fast path and
/// the single-shard edge loop share one code path.
///
/// Routes through the same protocol-neutral in-transaction staging gate
/// (`route_in_tx_write`) the SQL-planned dispatch loops (`sql_loop.rs`,
/// pgwire's `execute_dml_hooks.rs`) already use. Outside a transaction block
/// this is a no-op passthrough (`InTxnRoute::Read` with the task unchanged),
/// so autocommit direct ops (including `KvBatchPut`) dispatch exactly as
/// before. Inside a transaction block, a stageable write (e.g. `KvBatchPut`)
/// is applied to the per-transaction overlay at statement time instead of
/// hitting durable storage directly -- fixing the atomicity gap where a
/// native direct-op write inside `BEGIN...COMMIT` used to commit immediately
/// and survive `ROLLBACK`. A non-stageable write is buffered for COMMIT-time
/// replay, matching the SQL path's deferral for the same plan shapes.
async fn dispatch_single_task(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> NativeResponse {
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: ctx.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id,
    };

    // Authorization must precede the staging decision. Non-stageable writes
    // are buffered without invoking the stage-dispatch closure, so authorizing
    // only inside that closure would let an ungranted task reach trusted
    // COMMIT replay. Consuming the exact-task capability here makes every
    // branch below originate from a successful authorization decision.
    let task = match authorize_single_task(ctx, task) {
        Ok(authorized) => authorized.into_staging_task(),
        Err(error) => return error_to_native(seq, &error),
    };

    // Cloned before `route_in_tx_write` consumes `task`, so a staged write
    // whose outcome carries a real affected-count/computed-value payload
    // (e.g. `KvBatchPut`'s `{"inserted": n}`) can be shaped into the
    // response the same way the non-staged branch below shapes it.
    let plan_for_staged_response = task.plan.clone();

    let task = match route_in_tx_write(
        ctx.state,
        ctx.sessions,
        ctx.peer_addr.into(),
        task,
        |stage_task| {
            dispatch_authorized_single_task(
                ctx,
                stage_task.tenant_id,
                stage_task.vshard_id,
                stage_task.plan,
                stage_task.txn_id,
            )
        },
    )
    .await
    {
        Ok(InTxnRoute::Read(routed_task)) => *routed_task,
        Ok(InTxnRoute::Buffered) => {
            let mut r = NativeResponse::ok(seq);
            r.rows_affected = Some(1);
            return r;
        }
        Ok(InTxnRoute::Staged(outcome)) => {
            let synthetic = Response {
                request_id: RequestId::new(0),
                status: Status::Ok,
                attempt: 0,
                partial: false,
                payload: Payload::from_vec(outcome.payload),
                watermark_lsn: Lsn::new(0),
                error_code: None,
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            };
            return data_plane_response_to_native(ctx, seq, &plan_for_staged_response, &synthetic);
        }
        Err(StagingGateError::Dispatch(e)) => return error_to_native(seq, &e),
        Err(StagingGateError::Rejected { code }) => {
            let (_, sqlstate, message) = match code {
                Some(code) => error_code_to_sqlstate(&code),
                None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
            };
            return NativeResponse::error(seq, sqlstate, message);
        }
    };

    let plan_for_response = task.plan.clone();
    let task_vshard = task.vshard_id;
    match dispatch_authorized_single_task(
        ctx,
        task.tenant_id,
        task.vshard_id,
        task.plan,
        task.txn_id,
    )
    .await
    {
        Ok(resp) => {
            // Track direct-op reads, including NotFound phantom observations,
            // identically to native SQL and pgwire conflict detection.
            let records_read = resp.status == Status::Ok
                || resp.error_code.as_deref()
                    == Some(&crate::bridge::envelope::ErrorCode::NotFound);
            if records_read
                && ctx.sessions.transaction_state(ctx.peer_addr)
                    == crate::control::server::shared::session::TransactionState::InBlock
            {
                crate::control::server::shared::session::record_reads_for_response(
                    ctx.state,
                    ctx.sessions,
                    ctx.peer_addr.into(),
                    ctx.tenant_id(),
                    crate::control::server::shared::session::ResponseReads {
                        plan: &plan_for_response,
                        watermarks: &[(task_vshard, resp.watermark_lsn)],
                        read_version_lsn: resp.read_version_lsn,
                        found: resp.status == Status::Ok,
                        distributed_reads: &[],
                        read_lsn_vshard: task_vshard,
                    },
                )
                .await;
            }
            data_plane_response_to_native(ctx, seq, &plan_for_response, &resp)
        }
        Err(e) => error_to_native(seq, &e),
    }
}
