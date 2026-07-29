// SPDX-License-Identifier: BUSL-1.1

//! Change Data Capture (CDC) API endpoints.
//!
//! Two modes:
//!
//! 1. **SSE streaming** (`GET /cdc/{collection}`) — Server-Sent Events stream
//!    that pushes changes in real-time. Connection stays open. Supports
//!    `Last-Event-ID` header for reconnection replay.
//!
//! 2. **Pull-based polling** (`GET /cdc/{collection}/poll`) — Returns a batch
//!    of changes since a cursor. Non-streaming, returns immediately. For
//!    Kafka Connect / Debezium-style integration.

use std::convert::Infallible;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use serde::Deserialize;

use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use crate::control::change_stream::ChangeEvent;
use crate::types::TenantId;

/// Query parameters for the SSE endpoint.
///
/// `tenant_id` is intentionally removed — callers must not control which
/// tenant they act as via a query-string parameter.
#[derive(Deserialize, Default)]
pub struct SseParams {
    /// Start streaming from this timestamp (epoch ms). Default: now.
    pub since_ms: Option<u64>,
    /// If a caller passes `tenant_id`, we detect and reject it to prevent
    /// cross-tenant escalation. The field is present only for rejection.
    pub tenant_id: Option<u64>,
}

/// Query parameters for the poll endpoint.
#[derive(Deserialize, Default)]
pub struct PollParams {
    /// Return changes since this timestamp (epoch ms). Default: 0 (all).
    pub since_ms: Option<u64>,
    /// Return changes since this LSN. Overrides since_ms if set.
    pub since_lsn: Option<u64>,
    /// Maximum changes to return. Default: 100.
    pub limit: Option<usize>,
    /// Detected and rejected — never honoured.
    pub tenant_id: Option<u64>,
}

/// SSE streaming endpoint: `GET /cdc/{collection}`
pub async fn sse_stream(
    identity: ResolvedIdentity,
    Path(collection): Path<String>,
    Query(params): Query<SseParams>,
    State(state): State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Reject any attempt to override the caller's tenant via query string.
    if params.tenant_id.is_some() {
        return Err(ApiError::Forbidden(
            "tenant_id must not be supplied as a query parameter; \
             tenant is determined from the bearer token"
                .into(),
        ));
    }

    let collection = collection.to_lowercase();
    let tenant_id = identity.tenant_id();
    let since_ms = params.since_ms.unwrap_or(0);
    let shared = Arc::clone(&state.shared);

    // First, replay any missed events from the ring buffer. The ring is shared
    // by every tenant on the node, so the query is tenant-scoped at the source.
    let backlog =
        shared
            .change_stream
            .query_changes(tenant_id, Some(&collection), since_ms, 10_000);

    // Then subscribe for new events going forward.
    let mut subscription = shared
        .change_stream
        .subscribe(Some(collection.clone()), Some(tenant_id));

    let stream = async_stream::stream! {
        // Phase 1: replay backlog.
        for event in backlog {
            yield Ok(format_sse_event(&event));
        }

        // Phase 2: stream new events as they arrive.
        loop {
            match subscription.recv_filtered().await {
                Ok(event) => {
                    yield Ok(format_sse_event(&event));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(Event::default()
                        .event("warning")
                        .data(format!("lagged {n} events")));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Poll endpoint: `GET /cdc/{collection}/poll`
pub async fn poll_changes(
    identity: ResolvedIdentity,
    Path(collection): Path<String>,
    Query(params): Query<PollParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Reject any attempt to override the caller's tenant via query string.
    if params.tenant_id.is_some() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "tenant_id must not be supplied as a query parameter"
            })),
        )
            .into_response();
    }

    let tenant_id: TenantId = identity.tenant_id();
    let collection = collection.to_lowercase();
    let since_ms = params.since_ms.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(10_000);

    // A change row exposes the collection's document ids and post-images, so
    // reading the stream needs the same grant reading the collection needs.
    if let Err(error) = crate::control::server::shared::authorization::authorize_collection(
        &identity.0,
        crate::types::DatabaseId::DEFAULT,
        &collection,
        crate::control::security::identity::Permission::Read,
        &state.shared.permissions,
        &state.shared.roles,
        &crate::control::security::audit::NoopAuditEmitter,
    ) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            format!("permission denied: {}", error.resource()),
        )
            .into_response();
    }

    let changes =
        state
            .shared
            .change_stream
            .query_changes(tenant_id, Some(&collection), since_ms, limit);

    let change_json: Vec<serde_json::Value> = changes
        .iter()
        .map(|e| {
            serde_json::json!({
                "operation": e.operation.as_str(),
                "document_id": e.document_id,
                "timestamp_ms": e.timestamp_ms,
                "lsn": e.lsn.as_u64(),
                "collection": e.collection,
            })
        })
        .collect();

    let next_cursor = changes.last().map(|last| {
        serde_json::json!({
            "since_ms": last.timestamp_ms + 1,
            "since_lsn": last.lsn.as_u64() + 1,
        })
    });

    let has_more = changes.len() >= limit;

    Json(serde_json::json!({
        "changes": change_json,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "count": changes.len(),
    }))
    .into_response()
}

/// Format a ChangeEvent as an SSE Event.
fn format_sse_event(event: &ChangeEvent) -> Event {
    let data = serde_json::json!({
        "collection": event.collection,
        "operation": event.operation.as_str(),
        "document_id": event.document_id,
        "timestamp_ms": event.timestamp_ms,
        "lsn": event.lsn.as_u64(),
    });

    Event::default()
        .id(event.lsn.as_u64().to_string())
        .event(event.operation.as_str().to_lowercase())
        .data(data.to_string())
}
