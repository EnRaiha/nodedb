// SPDX-License-Identifier: BUSL-1.1

//! HTTP poll endpoint for change stream consumption.
//!
//! `GET /v1/streams/{stream}/poll?group={group}&limit=100&partition=3`
//!
//! Returns a JSON batch of events from the stream buffer, starting after
//! the consumer group's committed offsets. Does NOT auto-commit.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use super::super::admission::admit;
use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use super::super::peer::PeerAddr;
use super::query::{DatabaseQueryParam, resolve_database_id};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::Permission;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::authorization::authorize_collection;
use crate::event::cdc::CdcSubscriberScope;
use crate::event::cdc::consume::{ConsumeError, ConsumeParams, ConsumeResult, consume_stream};

/// Query parameters.
#[derive(Deserialize, Default)]
pub struct PollParams {
    /// Consumer group name (required).
    pub group: Option<String>,
    /// Maximum events to return. Default: 100.
    pub limit: Option<usize>,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u32>,
    /// Detected and rejected — callers must not supply `tenant_id` as a
    /// query parameter. Tenant is always sourced from the bearer token.
    pub tenant_id: Option<u64>,
    /// Optional database selector. The header takes precedence when present.
    pub database: Option<String>,
}

/// Response body.
///
/// All fields that were present before v1.0 remain. `evicted_since_last_poll`
/// and `oldest_available_offset` are additive — HTTP clients and ORMs that ignore
/// unknown JSON fields will not break.
#[derive(Serialize)]
pub struct PollResponse {
    /// Events in this batch.
    pub events: Vec<serde_json::Value>,
    /// Per-partition latest canonical `<lsn>:<sequence>` offset in this batch.
    pub partition_offsets: std::collections::BTreeMap<String, String>,
    /// Total events returned.
    pub count: usize,
    /// Events dropped from this stream's buffer since the previous poll for
    /// this consumer group. Zero on the first poll or when the buffer has not
    /// overflowed. A non-zero value means the consumer has a gap: events
    /// between the last committed offset and `oldest_available_offset` are gone.
    pub evicted_since_last_poll: u64,
    /// Oldest canonical offset still available in the stream buffer. If
    /// `evicted_since_last_poll > 0`, seek here to resume consumption.
    pub oldest_available_offset: String,
}

/// `GET /v1/streams/{stream}/poll`
pub async fn poll_stream(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    Path(stream_name): Path<String>,
    Query(params): Query<PollParams>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Reject any attempt to override the caller's tenant via query string.
    if params.tenant_id.is_some() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "tenant_id must not be supplied as a query parameter; \
                          tenant is determined from the bearer token"
            })),
        )
            .into_response();
    }

    let group = match params.group {
        Some(g) => g.to_lowercase(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing 'group' query parameter"})),
            )
                .into_response();
        }
    };

    let tenant_id = identity.tenant_id().as_u64();
    let database_id = match resolve_database_id(
        &headers,
        &DatabaseQueryParam {
            database: params.database.clone(),
        },
        &state,
    ) {
        Ok(database_id) => database_id,
        Err(error) => return error.into_response(),
    };
    // Full gate before any registry lookup, authorization, or buffer read: a
    // poll is a discrete per-request read of the caller's stream data, exactly
    // the shape the rate limiter's cost table models.
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        database_id,
        peer.as_str(),
        "stream_poll",
    ) {
        Ok(headers) => headers,
        Err(error) => return error.into_response(),
    };
    let limit = params.limit.unwrap_or(100).min(10_000);
    let stream_name = stream_name.to_lowercase();

    // A change stream exposes events from its source collection. Resolve the
    // definition in the caller's selected database and tenant before either a
    // local consume or remote forwarding can expose those events. Durable
    // topics are protected by the corresponding logical topic resource.
    if let Some(topic_name) = stream_name.strip_prefix("topic:") {
        let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.shared.audit));
        if let Err(error) = authorize_collection(
            &identity.0,
            database_id,
            &format!("topic:{topic_name}"),
            Permission::Read,
            &state.shared.permissions,
            &state.shared.roles,
            &emitter,
        ) {
            return ApiError::from(crate::Error::from(error)).into_response();
        }
    } else if let Some(stream_def) =
        state
            .shared
            .stream_registry
            .get(database_id, tenant_id, &stream_name)
    {
        let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.shared.audit));
        if let Err(error) = authorize_collection(
            &identity.0,
            database_id,
            &stream_def.collection,
            Permission::Read,
            &state.shared.permissions,
            &state.shared.roles,
            &emitter,
        ) {
            return ApiError::from(crate::Error::from(error)).into_response();
        }
    }

    let consume_params = ConsumeParams {
        database_id,
        tenant_id,
        stream_name: &stream_name,
        group_name: &group,
        partition: params.partition,
        limit,
    };

    let mut result = match consume_stream(&state.shared, &consume_params) {
        Ok(r) => r,
        Err(ConsumeError::RemotePartition { leader_node, .. }) => {
            // Forward to remote node.
            match crate::event::cdc::consume::consume_remote(
                &state.shared,
                &consume_params,
                leader_node,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            }
        }
        Err(ConsumeError::BufferEmpty(_)) => ConsumeResult {
            events: Vec::new(),
            partition_offsets: Vec::new(),
            evicted_since_last_poll: 0,
            oldest_available_offset: crate::event::cdc::CdcOffset::ZERO,
        },
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Events carry the written row, so the poller's column redaction rules
    // apply to them exactly as they do to a SELECT of the source collection.
    let mut subscriber = CdcSubscriberScope::new(
        identity.tenant_id(),
        RequestAuthScope::for_database(&identity.0, state.shared.auth_stores(), database_id)
            .auth()
            .roles
            .clone(),
    );
    subscriber.retain_deliverable(&state.shared.redaction, &mut result.events);

    let events: Vec<serde_json::Value> = result
        .events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap_or_default())
        .collect();
    let count = events.len();
    let partition_offsets: std::collections::BTreeMap<String, String> = result
        .partition_offsets
        .into_iter()
        .map(|(pid, offset)| (pid.to_string(), offset.token()))
        .collect();

    (
        rate_limit_headers,
        Json(PollResponse {
            events,
            partition_offsets,
            count,
            evicted_since_last_poll: result.evicted_since_last_poll,
            oldest_available_offset: result.oldest_available_offset.token(),
        }),
    )
        .into_response()
}
