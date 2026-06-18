// SPDX-License-Identifier: BUSL-1.1

//! Query endpoint — execute SQL/DDL via HTTP POST.
//!
//! POST /v1/query { "sql": "SELECT * FROM users LIMIT 10" }
//! Authorization: Bearer ndb_...
//!
//! Supports both DDL commands (SHOW USERS, CREATE COLLECTION, etc.) and
//! full SQL queries (SELECT, INSERT, UPDATE, DELETE) via DataFusion.

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;
use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::identity::{required_permission, role_grants_permission};
use crate::types::{TraceId, VShardId};

use super::super::auth::{ApiError, AppState, resolve_identity};
use super::super::types::HttpQueryRequest;
use super::super::types::HttpQueryResponse;

/// Query string parameters for `/v1/query` and `/v1/query/stream`.
///
/// `?database=<name>` is the fallback when `X-NodeDB-Database` is absent.
#[derive(Debug, Default, Deserialize)]
pub struct DatabaseQueryParam {
    pub database: Option<String>,
}

/// Resolve the active `DatabaseId` from an HTTP request.
///
/// Priority: `X-NodeDB-Database` header > `?database=` query param > DEFAULT.
///
/// When a name is supplied but does not resolve to an existing database,
/// returns `ApiError::BadRequest` with SQLSTATE-style detail
/// (`3D000 database '<name>' does not exist`). Silently falling back to
/// DEFAULT would mask client mistakes and run queries against the wrong
/// database; that is a correctness bug, not a usability convenience.
fn resolve_database_id(
    headers: &HeaderMap,
    param: &DatabaseQueryParam,
    state: &AppState,
) -> Result<nodedb_types::DatabaseId, ApiError> {
    let name = headers
        .get("x-nodedb-database")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| param.database.clone().filter(|s| !s.is_empty()));

    let Some(db_name) = name else {
        return Ok(nodedb_types::DatabaseId::DEFAULT);
    };

    let catalog = state.shared.credentials.catalog();
    let catalog = catalog
        .as_ref()
        .ok_or_else(|| ApiError::Internal("system catalog unavailable".to_string()))?;

    match catalog.get_database_id_by_name(&db_name) {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err(ApiError::BadRequest(format!(
            "3D000 database '{db_name}' does not exist"
        ))),
        Err(e) => Err(ApiError::Internal(format!("catalog lookup failed: {e}"))),
    }
}

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
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<HttpQueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;
    let database_id = resolve_database_id(&headers, &db_param, &state)?;
    let trace_id = crate::control::trace_context::extract_from_headers(&headers);

    let sql = body.sql.as_str();

    // Try DDL commands first (same as pgwire handler).
    if let Some(result) = crate::control::server::pgwire::ddl::dispatch(
        &state.shared,
        &identity,
        sql.trim(),
        database_id,
    )
    .await
    {
        return match result {
            Ok(responses) => {
                let json_rows = responses_to_json(responses);
                Ok(axum::Json(HttpQueryResponse::ok(json_rows)))
            }
            Err(e) => Err(ApiError::BadRequest(e.to_string())),
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

    let mut auth_ctx = crate::control::server::session_auth::build_auth_context(&identity);
    let clean_sql =
        crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);
    let perm_cache = state.shared.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: &identity,
        auth: &auth_ctx,
        rls_store: &state.shared.rls,
        permissions: &state.shared.permissions,
        roles: &state.shared.roles,
        permission_cache: Some(&*perm_cache),
    };
    let tasks = state
        .query_ctx
        .plan_sql_with_rls(&clean_sql, tenant_id, database_id, &sec)
        .await
        .map_err(|e| ApiError::BadRequest(format!("SQL planning failed: {e}")))?;

    if tasks.is_empty() {
        return Ok(axum::Json(HttpQueryResponse::ok(vec![])));
    }

    // Track active request for quota accounting.
    state.shared.tenant_request_start(tenant_id);

    // Execute each task via the SPSC bridge.
    let mut result_rows = Vec::new();

    let result = async {
        for task in tasks {
            // Permission check.
            let required = required_permission(&task.plan);
            if !identity.is_superuser
                && !identity
                    .roles
                    .iter()
                    .any(|r| role_grants_permission(r, required))
            {
                return Err(ApiError::Forbidden(format!(
                    "insufficient permissions for this operation (requires {required:?})"
                )));
            }

            // Tenant isolation check.
            if task.tenant_id != tenant_id {
                return Err(ApiError::Forbidden("tenant isolation violation".into()));
            }

            // WAL append for write operations.
            wal_append_if_write(&state, &task)?;

            // Dispatch: prefer gateway when available (cluster-aware routing),
            // fall back to direct local SPSC dispatch on single-node boot.
            let payloads = match state.shared.gateway.as_ref() {
                Some(gw) => {
                    let gw_ctx = QueryContext {
                        tenant_id: task.tenant_id,
                        trace_id,
                        database_id,
                    };
                    gw.execute(&gw_ctx, task.plan).await.map_err(|e| {
                        let (status, msg) = GatewayErrorMap::to_http(&e);
                        ApiError::HttpStatus(status, msg)
                    })?
                }
                None => {
                    // Single-node boot: gateway not yet initialised — dispatch locally.
                    let response = dispatch_to_data_plane(
                        &state,
                        task.tenant_id,
                        task.database_id,
                        task.vshard_id,
                        task.plan,
                        trace_id,
                    )
                    .await
                    .map_err(|e| {
                        let (status, msg) = GatewayErrorMap::to_http(&e);
                        ApiError::HttpStatus(status, msg)
                    })?;
                    if response.status != Status::Ok {
                        let detail = response
                            .error_code
                            .as_ref()
                            .map(|c| format!("{c:?}"))
                            .unwrap_or_else(|| "unknown error".into());
                        return Err(ApiError::Internal(detail));
                    }
                    vec![response.payload.to_vec()]
                }
            };

            for payload in &payloads {
                if !payload.is_empty() {
                    match decode_payload_to_json(payload) {
                        Ok(value) => result_rows.push(value),
                        Err(_) => {
                            // Binary payload — base64 encode.
                            use base64::Engine;
                            let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
                            result_rows.push(serde_json::json!({ "data": encoded }));
                        }
                    }
                }
            }
        }

        Ok(axum::Json(HttpQueryResponse::ok(result_rows)))
    }
    .await;

    state.shared.tenant_request_end(tenant_id);
    result
}

/// Append write operations to WAL before dispatch (single-node durability).
fn wal_append_if_write(
    state: &AppState,
    task: &nodedb_physical::physical_task::PhysicalTask,
) -> Result<(), ApiError> {
    crate::control::server::wal_dispatch::wal_append_if_write(
        &state.shared.wal,
        task.tenant_id,
        task.vshard_id,
        task.database_id,
        &task.plan,
    )
    .map_err(|e| ApiError::Internal(format!("WAL append: {e}")))
}

/// Dispatch a physical plan locally (single-node fallback path).
///
/// Called only when `shared.gateway` is `None` (pre-cluster-init boot).
async fn dispatch_to_data_plane(
    state: &AppState,
    tenant_id: crate::types::TenantId,
    database_id: nodedb_types::DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<crate::bridge::envelope::Response> {
    crate::control::server::dispatch_utils::dispatch_to_data_plane(
        &state.shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
    )
    .await
}

/// Decode a Data Plane response payload to JSON.
///
/// Tries MessagePack first (primary format), then JSON passthrough.
fn decode_payload_to_json(payload: &[u8]) -> Result<serde_json::Value, ()> {
    // Try MessagePack.
    if let Ok(val) = nodedb_types::json_from_msgpack(payload) {
        return Ok(val);
    }

    // Try JSON passthrough.
    if let Ok(val) = sonic_rs::from_slice::<serde_json::Value>(payload) {
        return Ok(val);
    }

    Err(())
}

/// Convert pgwire Response vec to JSON rows (for DDL results).
fn responses_to_json(responses: Vec<pgwire::api::results::Response>) -> Vec<serde_json::Value> {
    use pgwire::api::results::Response;

    let mut rows = Vec::new();
    for resp in responses {
        match resp {
            Response::Execution(tag) => {
                rows.push(serde_json::json!({
                    "type": "execution",
                    "tag": format!("{:?}", tag),
                }));
            }
            Response::Query(_) => {
                rows.push(serde_json::json!({
                    "type": "query",
                    "note": "query results available via pgwire protocol",
                }));
            }
            Response::EmptyQuery => {
                rows.push(serde_json::json!({ "type": "empty" }));
            }
            _ => {}
        }
    }
    rows
}

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

    let identity = match resolve_identity(&headers, &state, "http") {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let database_id = match resolve_database_id(&headers, &db_param, &state) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

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

    let auth_ctx = crate::control::server::session_auth::build_auth_context(&identity);
    let perm_cache = state.shared.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: &identity,
        auth: &auth_ctx,
        rls_store: &state.shared.rls,
        permissions: &state.shared.permissions,
        roles: &state.shared.roles,
        permission_cache: Some(&*perm_cache),
    };
    let tasks = match query_ctx
        .plan_sql_with_rls(sql, tenant_id, database_id, &sec)
        .await
    {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let trace_id = crate::control::trace_context::generate_trace_id();

    // Lazy fast path: an eligible single-task, unordered, multi-row SELECT
    // streams its rows straight off a `ResultStream` as NDJSON lines instead
    // of materializing the whole result first. HTTP is stateless, so there is
    // no autocommit / transaction-block gate (cf. native + pgwire). The
    // streaming body outlives this handler, so request-accounting is not
    // bracketed around it — admission was already gated by `check_tenant_quota`
    // above, matching the pgwire lazy path which also does not request-account
    // the streamed `QueryResponse`.
    match super::query_stream::try_open_stream(&state, &tasks, database_id, trace_id).await {
        Ok(Some((stream, limit))) => {
            let body = axum::body::Body::from_stream(super::query_stream::ndjson_body_stream(
                stream, limit,
            ));
            return Response::builder()
                .header("Content-Type", "application/x-ndjson")
                .body(body)
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
                });
        }
        Ok(None) => {
            // Not streamable — fall through to the materialized path below.
        }
        Err(e) => {
            let (_status, msg) = GatewayErrorMap::to_http(&e);
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
    }

    state.shared.tenant_request_start(tenant_id);

    let mut ndjson = String::new();
    for task in tasks {
        let dispatch_result: crate::Result<Vec<Vec<u8>>> = match state.shared.gateway.as_ref() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id: task.tenant_id,
                    trace_id,
                    database_id,
                };
                gw.execute(&gw_ctx, task.plan).await
            }
            None => {
                // Single-node boot: gateway not yet initialised — dispatch locally.
                crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    &state.shared,
                    task.tenant_id,
                    task.database_id,
                    task.vshard_id,
                    task.plan,
                    trace_id,
                )
                .await
                .map(|r| vec![r.payload.to_vec()])
            }
        };

        match dispatch_result {
            Ok(payloads) => {
                for payload in &payloads {
                    if !payload.is_empty() {
                        let json_str =
                            crate::data::executor::response_codec::decode_payload_to_json(payload);
                        // Emit each array element as its own NDJSON line.
                        // `to_array_iter` yields raw JSON slices without
                        // deserializing into a Value tree — no per-item heap
                        // alloc and no re-serialization. Non-array payloads
                        // (DDL acks, error text, etc.) fall back to emitting
                        // the whole payload as one line.
                        if json_str.trim_start().starts_with('[') {
                            for lv in sonic_rs::to_array_iter(json_str.as_str()).flatten() {
                                ndjson.push_str(lv.as_raw_str());
                                ndjson.push('\n');
                            }
                        } else {
                            ndjson.push_str(&json_str);
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

    state.shared.tenant_request_end(tenant_id);

    Response::builder()
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from(ndjson))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response())
}
