// SPDX-License-Identifier: BUSL-1.1

//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin,
};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::types::TraceId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::response::data_plane_response_to_native;
use super::single_task::dispatch_single_task;
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
        ctx.auth_context(),
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    // Refuse what column redaction cannot cover (an aggregate over a redacted
    // column, a graph traversal), before any orchestration observes the plan.
    if let Err(e) = crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        ctx.auth_context(),
        &ctx.state.redaction,
    ) {
        return NativeResponse::error(seq, "0A000", e.to_string());
    }

    // Extracted before `plan` is moved/cloned into any of the branches below
    // — metering needs the collection/engine shape after dispatch succeeds,
    // and only when metering is enabled (the default is disabled, so this is
    // a no-op on the hot path for every caller that hasn't turned it on).
    let plan_metering_info = ctx
        .state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));

    // A spent hard quota refuses the op before it runs; the charges below are
    // all on the success path and so can never refuse anything themselves.
    // The branches that route through `dispatch_single_task` are gated there
    // too, on the plan actually dispatched — harmless, since a scope already
    // over its cap refuses either way.
    if let Some(info) = &plan_metering_info
        && let Err(e) = admit_quota_for_dispatch(ctx.state, &ctx.scope, info)
    {
        return NativeResponse::error(seq, "53400", e.to_string());
    }

    // Whether the blanket metering call below (after the block) still needs
    // to run. It does for every branch that dispatches directly (Control-
    // Plane-orchestrated INSERT SELECT / MERGE / UPDATE FROM, and the
    // implicit-edge MultiShard Calvin batch) — those never touch the
    // in-transaction staging gate. It does NOT for a branch that routes
    // through `dispatch_single_task`: that function now meters itself,
    // correctly distinguishing a `Read`/`Staged` dispatch (real work,
    // metered now) from a `Buffered` one (no dispatch yet, metered at COMMIT
    // replay instead — see `session::commit::run_commit`); re-metering its
    // response here with the ORIGINAL top-level plan would double-bill the
    // former and wrongly bill the latter before its COMMIT/ROLLBACK is even
    // known.
    let mut needs_top_level_metering = true;
    // The rest of the dispatch logic has several early-return branches
    // (Control-Plane-orchestrated INSERT SELECT / MERGE / UPDATE FROM, the
    // no-edge fast path, and the multi/single-shard implicit-edge paths).
    // Wrapped in an async block (not the outer fn) so `return` inside each
    // branch exits only this block, letting the metering call below run
    // exactly once, after whichever branch actually dispatched, regardless
    // of which one it was.
    let response: NativeResponse = async {
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
                crate::control::merge_orchestrator::run_authorized_merge(ctx.state, authorized)
                    .await;
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
        // Implicit-edge extraction marks catalog state and allocates surrogates.
        // Authorize the original direct-op task before those side effects.
        let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &ctx.state.audit,
        ));
        if let Err(error) = crate::control::server::shared::authorization::authorize_task_set(
            ctx.identity,
            &tasks,
            &ctx.state.permissions,
            &ctx.state.roles,
            &emitter,
        ) {
            return error_to_native(seq, &crate::Error::from(error));
        }

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

        // The entries cover the row images every cross-shard balance this pass
        // settled was folded from; they travel on the dispatch read-set so
        // Calvin's OCC check aborts rather than committing a total folded from
        // an image that has since moved.
        let sum_target_reads =
            match crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
                ctx.state,
                &mut tasks,
                tenant_id,
                ctx.database_id(),
                TraceId::ZERO,
            )
            .await
            {
                Ok(reads) => reads,
                Err(e) => return error_to_native(seq, &e),
            };

        // Follows the resolution: it consumes the surrogates that pass bound,
        // and issues no lookup of its own.
        if let Err(e) = crate::control::planner::materialized_sum::append_cross_shard_balance_tasks(
            ctx.state,
            &mut tasks,
            tenant_id,
            ctx.database_id(),
        ) {
            return error_to_native(seq, &e);
        }

        // The expanded set is the dispatch authorization boundary. The no-edge
        // path retains its existing per-task capability consumption below.
        let authorized_tasks =
            match crate::control::server::shared::authorization::authorize_task_set(
                ctx.identity,
                &tasks,
                &ctx.state.permissions,
                &ctx.state.roles,
                &emitter,
            ) {
                Ok(authorized) => authorized,
                Err(error) => return error_to_native(seq, &crate::Error::from(error)),
            };

        if tasks.len() == 1 {
            // No-edge fast path — behaviorally identical to the pre-migration
            // single-plan dispatch. The local-path WAL append now lives inside
            // `dispatch_single_task` so it is shared with the single-shard edge loop.
            let task = match authorized_tasks.into_tasks().into_iter().next() {
                Some(task) => task,
                None => {
                    return NativeResponse::error(
                        seq,
                        "XX000",
                        "authorization returned no task capability",
                    );
                }
            };
            let _request = ctx.state.tenant_request_guard(tenant_id);
            needs_top_level_metering = false;
            return dispatch_single_task(ctx, seq, task).await;
        }

        // Edge-bearing insert: route the augmented task set the same way native SQL
        // does. A cross-shard set goes through the Calvin sequencer atomically (which
        // owns its own replicated durability); a single-shard set dispatches each
        // task sequentially (matching pgwire / native-SQL single-shard multi-task),
        // returning the document task's response. Local WAL durability for the
        // single-shard path is handled inside `dispatch_single_task`.
        let _request = ctx.state.tenant_request_guard(tenant_id);
        // Autocommit direct-ops dispatch: the only reads to widen with are the
        // ones the materialized-sum settlement stamped on the source rows its
        // shipped balances were folded from.
        match classify_dispatch(
            &tasks,
            &crate::control::planner::calvin::read_vshards_of(&sum_target_reads),
        ) {
            DispatchClass::MultiShard { .. } => {
                match dispatch_authorized_tasks_to_calvin(
                    ctx.state,
                    authorized_tasks,
                    tenant_id,
                    CrossShardTxnMode::Strict,
                    TxnDispatchPosition::Autocommit,
                    &sum_target_reads,
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
                needs_top_level_metering = false;
                let mut doc_response: Option<NativeResponse> = None;
                let mut error: Option<NativeResponse> = None;
                for task in authorized_tasks.into_tasks() {
                    let resp = dispatch_single_task(ctx, seq, task).await;
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
    .await;

    // Metered only on the success path, once per call, and only for a branch
    // that dispatched directly rather than through `dispatch_single_task`
    // (`needs_top_level_metering` — see its declaration above for why: that
    // function now meters its own Read/Staged/Buffered routing correctly).
    // `response.rows`/`rows_affected` are already computed by the branch
    // above, so this adds no extra decode.
    if needs_top_level_metering
        && response.status != nodedb_types::protocol::ResponseStatus::Error
        && let Some(info) = &plan_metering_info
    {
        let rows = response
            .rows
            .as_ref()
            .map(|rows| rows.len() as u64)
            .or(response.rows_affected);
        meter_dispatch(ctx.state, &ctx.scope, info, rows);
    }
    response
}
