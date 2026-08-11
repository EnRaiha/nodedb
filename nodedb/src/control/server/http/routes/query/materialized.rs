// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::Status;
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;

use super::super::super::auth::{ApiError, AppState, build_request_scope, resolve_auth_parts};
use super::super::super::peer::PeerAddr;
use super::super::super::transport::ClientTransport;
use super::super::super::types::{HttpQueryRequest, HttpQueryResponse};
use super::super::result_shape::{
    HttpShaped, ddl_results_to_json, passthrough_json_row, shape_http_payload,
};
use super::{DatabaseQueryParam, resolve_database_id};

/// POST /v1/query — execute a SQL/DDL statement.
///
/// Request body: `{ "sql": "..." }`
/// Response: `{ "status": "ok", "rows": [...] }` or `{ "error": "..." }`
///
/// Database context (optional):
/// - `X-NodeDB-Database: <name>` header (highest priority)
/// - `?database=<name>` query parameter (fallback)
pub async fn query(
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<HttpQueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, verified_jwt) =
        resolve_auth_parts(&headers, &state, peer.as_str(), transport.security()).await?;
    let database_id = resolve_database_id(&headers, &db_param, &state)?;
    let trace_id = crate::control::trace_context::extract_from_headers(&headers);
    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    authorize_database(&identity, database_id, &emitter).map_err(crate::Error::from)?;

    let sql = body.sql.as_str();

    // The request-selected database is authoritative for RLS variables while
    // retaining verified JWT/session enrichment from authentication: passing
    // it as the session database makes `scope.database_id()` resolve to
    // exactly `database_id`, and the verified JWT (when this request
    // authenticated via JWT bearer) reproduces the same claim-derived
    // enrichment `resolve_auth` would have given an `AuthContext`.
    let request = build_request_scope(
        &identity,
        verified_jwt.as_ref(),
        &headers,
        &state,
        database_id,
        peer.as_str(),
    );

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, then rate limit — run exactly once per request, here, before it
    // can branch to `shared::ddl::dispatch` below or fall through to
    // DataFusion planning, so both DDL/DSL text and ordinary DML/SELECT
    // statements are covered by this one call. `Some(result)` carries the
    // rate-limit outcome this handler surfaces as `X-RateLimit-*` response
    // headers below. The accepted socket's address is what makes the
    // IP-blacklist and risk halves of that gate live on this route.
    let rate_limit_result = crate::control::server::session_auth::check_request_admission(
        &state.shared,
        &request,
        "sql",
    )?;
    let scope = request.into_scope();
    let rate_limit_headers =
        super::super::super::rate_limit_headers::rate_limit_headers(&rate_limit_result);

    // HTTP is stateless — there is no BEGIN/COMMIT session concept over this
    // transport, so a session-less scope satisfies the DDL dispatch signature.
    // A fresh store reports "not in a transaction block" for any address, so
    // the staging gate inside `plan_and_dispatch` always takes the immediate
    // autocommit branch here, unchanged from before the gate existed.
    let http_scope = crate::control::server::shared::session::DetachedTxnScope::new();
    let txn_ctx = http_scope.ctx();

    // Try DDL commands first (same as pgwire handler). Now reached only after
    // the single admission call above, so `shared::ddl::user_dispatch` (the
    // DSL/DDL dispatch door some DDL/DSL statements fall through to) must not
    // admit this request a second time.
    if let Some(result) = crate::control::server::shared::ddl::dispatch(
        &state.shared,
        &identity,
        sql.trim(),
        database_id,
        &txn_ctx,
    )
    .await
    {
        return match result {
            Ok(results) => {
                let json_rows = ddl_results_to_json(results);
                Ok((
                    rate_limit_headers,
                    axum::Json(HttpQueryResponse::ok(json_rows)),
                ))
            }
            Err(e) => Err(ddl_error_to_api(e)),
        };
    }

    // Extract per-query ON DENY override + plan SQL with RLS injection.
    let tenant_id = identity.tenant_id;

    // Quota enforcement — reject before any planning or dispatch.
    state
        .shared
        .check_tenant_quota(tenant_id)
        .map_err(|e| ApiError::RateLimited {
            message: e.to_string(),
            retry_after_secs: 1,
        })?;

    let (clean_sql, scope) =
        crate::control::server::session_auth::apply_per_query_on_deny(sql, scope);
    // Planning and lease admission run as one retried unit so a descriptor
    // drain starting between them is absorbed rather than surfaced. The scope
    // is retained through every dispatch and response-shaping operation below.
    let admission = plan_authorize_and_admit(PlanAdmissionRequest {
        state: &state.shared,
        query_ctx: &state.query_ctx,
        scope: &scope,
        sql: &clean_sql,
        trace_id: crate::types::TraceId::ZERO,
    })
    .await
    .map_err(ApiError::from)?;
    let tasks = admission.tasks;
    let output_schema = admission.output_schema;
    let authorized_tasks = admission.authorized_tasks.into_tasks();
    let _lease_scope = admission.lease_scope;

    if tasks.is_empty() {
        return Ok((
            rate_limit_headers,
            axum::Json(HttpQueryResponse::ok(vec![])),
        ));
    }

    // Track active request for quota accounting.
    let _request = state.shared.tenant_request_guard(tenant_id);

    // Execute each task via the SPSC bridge.
    let mut result_rows = Vec::new();
    // Checked once rather than per task — metering is disabled by default,
    // so this keeps the per-task extraction below (which clones the
    // collection name) a true no-op on the hot path for every deployment
    // that hasn't turned it on.
    let metering_enabled = state.shared.metering_config.enabled;

    async {
        for (task, authorized_task) in tasks.into_iter().zip(authorized_tasks) {
            // Extracted from `task.plan` before it's cloned/moved into any
            // branch below — metering needs the collection/engine shape
            // after this task's dispatch succeeds.
            let plan_metering_info =
                metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
            // A spent hard quota refuses the task before it runs; the
            // charging calls below are all on the success path and so can
            // never refuse anything themselves.
            if let Some(info) = &plan_metering_info {
                admit_quota_for_dispatch(&state.shared, &scope, info).map_err(gateway_error)?;
            }
            let rows_before = result_rows.len();
            // `INSERT ... SELECT` is orchestrated on the Control Plane: the
            // source is scanned, each target row gets its OWN fresh, registered
            // surrogate, and the rows are written via an atomic `BatchInsert`.
            // The orchestrator issues its own WAL-backed writes, so the outer
            // per-task WAL append below is skipped for it.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let resp = crate::control::insert_select::run_authorized_insert_select(
                    &state.shared,
                    authorized_task,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // Autocommit `MERGE` is orchestrated on the Control Plane: each
            // NOT-MATCHED insert row gets its OWN fresh, registered surrogate
            // and all arms apply atomically. The orchestrator issues its own
            // writes, so the per-task WAL append below is skipped for it.
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
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let resp = crate::control::merge_orchestrator::run_authorized_merge(
                    &state.shared,
                    authorized_task,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // Autocommit `UPDATE ... FROM <source>` is orchestrated on the
            // Control Plane: the source is scanned on its OWN core and shipped
            // into the plan so the target-core handler joins against it instead
            // of a local read. The orchestrator issues its own write, so the
            // per-task WAL append below is skipped for it.
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
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let resp = crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                    &state.shared,
                    authorized_task,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // Captured before dispatch moves `task.plan` — needed by the
            // protocol-neutral shaping core below.
            let plan_kind = describe_plan(&task.plan);
            let plan_for_shape = task.plan.clone();
            // Resolved once for this task and reused for every payload it
            // produced, rather than per payload.
            let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape);

            // Dispatch: prefer gateway when available (cluster-aware routing —
            // the gateway owns WAL durability on the target node), fall back to
            // direct local SPSC dispatch on single-node boot. On the local path
            // the WAL append is performed inside the dispatch core, under the
            // write-admission guard and just before the enqueue, so LSN order
            // matches apply order.
            let payloads = match state.shared.gateway.get() {
                Some(gw) => {
                    let gw_ctx = QueryContext {
                        tenant_id: task.tenant_id,
                        trace_id,
                        database_id,
                        txn_id: None,
                    };
                    gw.execute(&gw_ctx, authorized_task)
                        .await
                        .map_err(gateway_error)?
                }
                None => {
                    // Single-node boot: gateway not yet initialised — dispatch locally.
                    let response = crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write(
                        &state.shared,
                        authorized_task,
                        trace_id,
                    )
                        .await
                        .map_err(gateway_error)?;
                    if response.status != Status::Ok {
                        return Err(response_error(&response));
                    }
                    vec![response.payload.to_vec()]
                }
            };

            for payload in &payloads {
                if payload.is_empty() {
                    continue;
                }
                match shape_http_payload(MaterializedShapeRequest {
                    payload,
                    plan: &plan_for_shape,
                    plan_kind,
                    projection: Some(&output_schema),
                    state: &state.shared,
                    database_id,
                    tenant_id,
                    redaction: Some(redaction.ctx(&state.shared.redaction)),
                }) {
                    Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
                    Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(payload)),
                    Err(e) => return Err(ApiError::Internal(e.message().to_string())),
                }
            }
            meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
        }

        Ok((rate_limit_headers, axum::Json(HttpQueryResponse::ok(result_rows))))
    }
    .await
}

fn ddl_error_to_api(error: crate::control::server::shared::ddl::DdlError) -> ApiError {
    if error.sqlstate == "42501" {
        ApiError::Forbidden(error.message)
    } else {
        ApiError::BadRequest(error.message)
    }
}

fn gateway_error(error: crate::Error) -> ApiError {
    let (status, msg) = GatewayErrorMap::to_http(&error);
    ApiError::HttpStatus(status, msg)
}

fn response_error(response: &crate::bridge::envelope::Response) -> ApiError {
    let detail = response
        .error_code
        .as_ref()
        .map(|code| format!("{code:?}"))
        .unwrap_or_else(|| "unknown error".into());
    ApiError::Internal(detail)
}

/// Meter one task's dispatch, once its rows (if any) have already been
/// appended to `result_rows` — the row count is the delta since
/// `rows_before`, so this must run after every append point for the task,
/// never before.
fn meter_task_dispatch(
    state: &crate::control::state::SharedState,
    scope: &crate::control::security::request_scope::RequestAuthScope<'_>,
    info: &Option<PlanMeteringInfo>,
    rows_before: usize,
    result_rows: &[serde_json::Value],
) {
    if let Some(info) = info {
        let task_rows = (result_rows.len() - rows_before) as u64;
        meter_dispatch(state, scope, info, Some(task_rows));
    }
}

/// Everything one orchestrated task's response needs to be shaped and
/// appended. Grouped so the append helper stays within the argument budget as
/// it gained the per-statement redaction resolution.
struct ShapedAppend<'a> {
    plan: &'a crate::bridge::envelope::PhysicalPlan,
    plan_kind: crate::control::server::response_shape::types::PlanKind,
    output_schema: &'a crate::control::server::response_shape::schema::OutputSchema,
    state: &'a AppState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: crate::types::TenantId,
    redaction: &'a QueryRedaction,
}

fn append_response(
    result_rows: &mut Vec<serde_json::Value>,
    response: crate::bridge::envelope::Response,
    append: ShapedAppend<'_>,
) -> Result<(), ApiError> {
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    let payload = response.payload.to_vec();
    if payload.is_empty() {
        return Ok(());
    }
    match shape_http_payload(MaterializedShapeRequest {
        payload: &payload,
        plan: append.plan,
        plan_kind: append.plan_kind,
        projection: Some(append.output_schema),
        state: &append.state.shared,
        database_id: append.database_id,
        tenant_id: append.tenant_id,
        redaction: Some(append.redaction.ctx(&append.state.shared.redaction)),
    }) {
        Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
        Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(&payload)),
        Err(e) => return Err(ApiError::Internal(e.message().to_string())),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_insufficient_privilege_maps_to_forbidden() {
        let error = crate::control::server::shared::ddl::DdlError {
            sqlstate: "42501".into(),
            message: "write permission denied".into(),
        };

        assert!(matches!(
            ddl_error_to_api(error),
            ApiError::Forbidden(message) if message == "write permission denied"
        ));
    }
}
