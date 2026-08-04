// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};

use super::super::super::auth::{ApiError, AppState, build_request_scope, resolve_auth_parts};
use super::super::query_stream::{ndjson_body_stream, try_open_stream};
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
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    axum::Json(body): axum::Json<crate::control::server::http::types::HttpQueryStreamRequest>,
) -> impl IntoResponse {
    use axum::response::Response;

    let (identity, verified_jwt) = match resolve_auth_parts(&headers, &state, "http") {
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
    let scope = build_request_scope(
        &identity,
        verified_jwt.as_ref(),
        &headers,
        &state,
        database_id,
    );
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
            return Response::builder()
                .header("Content-Type", "application/x-ndjson")
                .body(axum::body::Body::from_stream(ndjson_body_stream(
                    stream,
                    limit,
                    Some(output_schema.clone()),
                    lease_scope,
                )))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
                });
        }
        Ok(None) => {}
        Err(error) => return ApiError::from(error).into_response(),
    }

    let _lease_scope = lease_scope;
    let mut ndjson = String::new();
    for (task, authorized_task) in tasks.into_iter().zip(authorized_tasks) {
        // Captured before dispatch moves `task.plan` — needed by the
        // protocol-neutral shaping core below.
        let plan_kind = describe_plan(&task.plan);
        let plan_for_shape = task.plan.clone();

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
                for payload in &payloads {
                    if payload.is_empty() {
                        continue;
                    }
                    match shape_http_payload(
                        payload,
                        &plan_for_shape,
                        plan_kind,
                        Some(&output_schema),
                        &state.shared,
                        database_id,
                        tenant_id,
                    ) {
                        Ok(HttpShaped::Rows(rows)) => {
                            for row in rows {
                                ndjson.push_str(&row.to_string());
                                ndjson.push('\n');
                            }
                        }
                        Ok(HttpShaped::Passthrough) => {
                            passthrough_to_ndjson(payload, &mut ndjson);
                        }
                        Err(e) => {
                            ndjson.push_str(&serde_json::json!({"error": e.message()}).to_string());
                            ndjson.push('\n');
                        }
                    }
                }
            }
            Err(e) => {
                let (_status, msg) = GatewayErrorMap::to_http(&e);
                ndjson.push_str(&serde_json::json!({"error": msg}).to_string());
                ndjson.push('\n');
            }
        }
    }

    Response::builder()
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from(ndjson))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response())
}
