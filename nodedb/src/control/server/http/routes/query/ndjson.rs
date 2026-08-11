// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::metering::{
    DetachedMeterGuard, PlanMeteringInfo, meter_dispatch,
};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;

use super::super::super::auth::{ApiError, AppState, build_request_scope, resolve_auth_parts};
use super::super::super::peer::PeerAddr;
use super::super::super::transport::ClientTransport;
use super::super::query_stream::{NdjsonBody, ndjson_body_stream, try_open_stream};
use super::super::result_shape::{HttpShaped, passthrough_to_ndjson, shape_http_payload};
use super::{DatabaseQueryParam, resolve_database_id};

/// POST /v1/query/stream — execute SQL and return results as NDJSON (newline-delimited JSON).
///
/// Each result row is a separate JSON line terminated by `\n`.
/// Content-Type: application/x-ndjson
///
/// This is suitable for streaming large result sets without buffering
/// the entire response. Clients can process each line as it arrives.
pub async fn query_ndjson(
    State(state): State<AppState>,
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    axum::Json(body): axum::Json<crate::control::server::http::types::HttpQueryStreamRequest>,
) -> impl IntoResponse {
    use axum::response::Response;

    let (identity, verified_jwt) =
        match resolve_auth_parts(&headers, &state, peer.as_str(), transport.security()).await {
            Ok(auth) => auth,
            Err(e) => return e.into_response(),
        };
    let database_id = match resolve_database_id(&headers, &db_param, &state) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    if let Err(error) = authorize_database(&identity, database_id, &emitter) {
        return ApiError::from(crate::Error::from(error)).into_response();
    }

    let sql = body.sql.trim();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty SQL").into_response();
    }

    let tenant_id = identity.tenant_id;

    // Quota enforcement — reject before any planning or dispatch.
    if let Err(e) = state.shared.check_tenant_quota(tenant_id) {
        let body = serde_json::json!({ "error": e.to_string() });
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header("Retry-After", "1")
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
            });
    }

    let query_ctx = &state.query_ctx;

    // The request-selected database is authoritative for RLS variables while
    // retaining verified JWT/session enrichment from authentication: passing
    // it as the session database makes `scope.database_id()` resolve to
    // exactly `database_id`, and the verified JWT (when this request
    // authenticated via JWT bearer) reproduces the same claim-derived
    // enrichment `resolve_auth` would have given an `AuthContext`.
    //
    // NDJSON does not extract a per-query `ON DENY` clause (unlike the
    // materialized `/v1/query` route) — that is pre-existing behavior.
    let request = build_request_scope(
        &identity,
        verified_jwt.as_ref(),
        &headers,
        &state,
        database_id,
        peer.as_str(),
    );

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, then rate limit — before any planning/dispatch, so load is
    // shed before it is spent. `Some(result)` carries the rate-limit outcome
    // this handler surfaces as `X-RateLimit-*` response headers below. The
    // accepted socket's address is what makes the IP-blacklist and risk
    // halves of that gate live on this route.
    let rate_limit_result = match crate::control::server::session_auth::check_request_admission(
        &state.shared,
        &request,
        "sql",
    ) {
        Ok(result) => result,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let scope = request.into_scope();
    let rate_limit_headers =
        super::super::super::rate_limit_headers::rate_limit_headers(&rate_limit_result);

    // Planning and lease admission run as one retried unit so a descriptor
    // drain starting between them is absorbed rather than surfaced. Admission
    // still follows authorization inside the unit, so denied requests never
    // acquire a descriptor lease. A lazy body takes ownership of the scope
    // below; the materialized path retains it lexically through all dispatch
    // and NDJSON shaping.
    let admission = match plan_authorize_and_admit(PlanAdmissionRequest {
        state: &state.shared,
        query_ctx,
        scope: &scope,
        sql,
        trace_id: crate::types::TraceId::ZERO,
    })
    .await
    {
        Ok(admission) => admission,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let tasks = admission.tasks;
    let output_schema = admission.output_schema;
    let authorized_tasks = admission.authorized_tasks.into_tasks();
    let mut lease_scope = Some(admission.lease_scope);

    let trace_id = crate::control::trace_context::generate_trace_id();

    let _request = state.shared.tenant_request_guard(tenant_id);

    // Authorization and admission above intentionally precede stream dispatch.
    // `Body::from_stream` then polls the data-plane stream under normal HTTP
    // backpressure while its captured lease scope remains alive until body
    // completion or client disconnect.
    match try_open_stream(&state, &tasks, &identity, database_id, trace_id).await {
        Ok(Some((stream, limit))) => {
            let Some(lease_scope) = lease_scope.take() else {
                return ApiError::from(crate::Error::Internal {
                    detail: "query lease scope missing before NDJSON stream dispatch".into(),
                })
                .into_response();
            };
            // Built only once the streaming path is confirmed taken — `tasks`
            // is a single-task slice here (`try_open_stream` requires exactly
            // one task to return `Some`). The streaming body below owns the
            // resulting guard for its whole polling lifetime, so rows
            // actually sent to the client (not rows planned) are what gets
            // billed; see `DetachedMeterGuard`'s docs. `None` when metering
            // is disabled (the default).
            let stream_meter_guard = if state.shared.metering_config.enabled
                && let [task] = tasks.as_slice()
            {
                let info = PlanMeteringInfo::extract(&task.plan);
                DetachedMeterGuard::new(&state.shared, &scope, &info)
            } else {
                None
            };
            let mut response = Response::builder()
                .header("Content-Type", "application/x-ndjson")
                .body(axum::body::Body::from_stream(ndjson_body_stream(
                    NdjsonBody {
                        stream,
                        limit,
                        projection: Some(output_schema.clone()),
                        // `try_open_stream` only returns `Some` for a
                        // single-task plan, so the first task IS the stream's
                        // source; resolved here, once, before any line ships.
                        redaction: tasks.first().map(|task| {
                            QueryRedaction::for_plan(tenant_id, scope.auth(), &task.plan)
                        }),
                        state: Arc::clone(&state.shared),
                        lease_scope,
                        meter_guard: stream_meter_guard,
                    },
                )))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
                });
            response.headers_mut().extend(rate_limit_headers);
            return response;
        }
        Ok(None) => {}
        Err(error) => return ApiError::from(error).into_response(),
    }

    let _lease_scope = lease_scope;
    let mut ndjson = String::new();
    // Checked once rather than per task — metering is disabled by default,
    // so this keeps the per-task extraction below (which clones the
    // collection name) a true no-op on the hot path for every deployment
    // that hasn't turned it on. This fallback path fully materializes the
    // NDJSON body before returning it (unlike the true streaming path
    // above), so there is no early-client-disconnect case to account for
    // here — it meters like the materialized `/v1/query` route.
    let metering_enabled = state.shared.metering_config.enabled;
    for (task, authorized_task) in tasks.into_iter().zip(authorized_tasks) {
        // Captured before dispatch moves `task.plan` — needed by the
        // protocol-neutral shaping core below.
        let plan_kind = describe_plan(&task.plan);
        let plan_for_shape = task.plan.clone();
        // Resolved once per task, reused for every payload it produced.
        let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape);
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));

        // A spent hard quota refuses the task before it runs; the charging
        // call at the end of this loop is on the success path and so can
        // never refuse anything itself. Reported as an error line and the
        // task skipped, matching how this stream reports a dispatch error.
        if let Some(info) = &plan_metering_info
            && let Err(e) = admit_quota_for_dispatch(&state.shared, &scope, info)
        {
            ndjson.push_str(&serde_json::json!({"error": e.to_string()}).to_string());
            ndjson.push('\n');
            continue;
        }

        let dispatch_result: crate::Result<Vec<Vec<u8>>> = if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
            )
        ) {
            crate::control::insert_select::run_authorized_insert_select(
                &state.shared,
                authorized_task,
            )
            .await
            .map(|response| vec![response.payload.to_vec()])
        } else if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Merge {
                    resolve_only: false,
                    resolved_inserts: None,
                    ..
                }
            )
        ) {
            crate::control::merge_orchestrator::run_authorized_merge(&state.shared, authorized_task)
                .await
                .map(|response| vec![response.payload.to_vec()])
        } else if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                    resolve_only: false,
                    source_rows: None,
                    ..
                }
            )
        ) {
            crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                &state.shared,
                authorized_task,
            )
            .await
            .map(|response| vec![response.payload.to_vec()])
        } else {
            match state.shared.gateway.get() {
                Some(gw) => {
                    let gw_ctx = QueryContext {
                        tenant_id: task.tenant_id,
                        trace_id,
                        database_id,
                        txn_id: None,
                    };
                    gw.execute(&gw_ctx, authorized_task).await
                }
                None => crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                    &state.shared,
                    authorized_task,
                    trace_id,
                )
                .await
                .map(|response| vec![response.payload.to_vec()]),
            }
        };

        match dispatch_result {
            Ok(payloads) => {
                // This task's own row count, for metering below — the
                // dispatch itself already succeeded here (that is this
                // `match` arm), so a per-row shaping error doesn't change
                // whether the task is billed, only how many rows it counts.
                let mut task_rows: u64 = 0;
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
                        Ok(HttpShaped::Rows(rows)) => {
                            task_rows += rows.len() as u64;
                            for row in rows {
                                ndjson.push_str(&row.to_string());
                                ndjson.push('\n');
                            }
                        }
                        Ok(HttpShaped::Passthrough) => {
                            task_rows += 1;
                            passthrough_to_ndjson(payload, &mut ndjson);
                        }
                        Err(e) => {
                            ndjson.push_str(&serde_json::json!({"error": e.message()}).to_string());
                            ndjson.push('\n');
                        }
                    }
                }
                if let Some(info) = &plan_metering_info {
                    meter_dispatch(&state.shared, &scope, info, Some(task_rows));
                }
            }
            Err(e) => {
                let (_status, msg) = GatewayErrorMap::to_http(&e);
                ndjson.push_str(&serde_json::json!({"error": msg}).to_string());
                ndjson.push('\n');
            }
        }
    }

    let mut response = Response::builder()
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from(ndjson))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response());
    response.headers_mut().extend(rate_limit_headers);
    response
}
