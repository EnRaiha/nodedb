// SPDX-License-Identifier: BUSL-1.1

//! Health check endpoints.
//!
//! | Endpoint          | Method | Purpose                     | k8s probe     |
//! |-------------------|--------|-----------------------------|---------------|
//! | `/healthz`        | GET    | Ready to serve traffic      | readiness     |
//! | `/health/live`    | GET    | Process alive (always 200)  | liveness      |
//! | `/health/ready`   | GET    | WAL recovered               | readiness alt |
//! | `/health/drain`   | POST   | Trigger graceful drain      | preStop hook  |

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

use super::super::admission::{admit_without_rate_limit, identity_database};
use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use super::super::peer::PeerAddr;

/// GET /health/live — unconditional liveness probe.
///
/// Always returns 200. If this endpoint fails to respond, the
/// process is dead and should be restarted. No internal state is
/// checked — the mere ability to respond proves the event loop and
/// HTTP listener are alive.
pub async fn live() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(json!({ "status": "alive" })))
}

/// GET /healthz — k8s-style readiness probe.
///
/// Returns `200 OK` when the node has reached `GatewayEnable`, is
/// serving traffic, and is NOT draining/decommissioned. Returns
/// `503 Service Unavailable` during startup, after startup failure,
/// or when the node is being decommissioned.
pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    // The coordinator signals this canonical watch before progressing drain
    // phases, so readiness must fail immediately even before lifecycle state
    // has been updated by other shutdown participants.
    if state.shared.shutdown.is_shutdown() {
        let body = json!({
            "status": "draining",
            "reason": "shutdown_signaled",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    // Check decommission state via the cluster observer (if present).
    if let Some(obs) = state.shared.cluster_observer.get() {
        let snap = obs.snapshot();
        let label = snap.lifecycle_label();
        if label == "draining" || label == "decommissioned" || label == "failed" {
            let body = json!({
                "status": "draining",
                "lifecycle": label,
                "node_id": state.shared.node_id,
            });
            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
        }
    }
    // A permanently wedged metadata applier is invisible to the startup gate:
    // the node booted cleanly and only stopped making progress afterwards. It
    // must fail readiness anyway, or it keeps taking traffic that can only end
    // in a descriptor-lease timeout naming nothing about the real cause.
    if let Some(report) = state.shared.metadata_apply_wedge.report() {
        let body = json!({
            "status": "failed",
            "reason": "metadata_apply_wedged",
            "node_id": state.shared.node_id,
            "raft_index": report.raft_index,
            "last_applied_watermark": report.last_applied_watermark,
            "entry_kind": report.entry_kind,
            "error": report.error,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    // A halted sequencer leaves the node serving everything that does not route
    // through Calvin, so the startup gate and every read path still look fine.
    // Report it anyway: silently dropping a whole write class is exactly what an
    // operator needs told, and it is the reason nothing takes this node out of
    // rotation on its own.
    if let Some(halt) = state.shared.sequencer_halt.report() {
        let body = json!({
            "status": "degraded",
            "reason": "sequencer_halted",
            "node_id": state.shared.node_id,
            "expected_epoch": halt.expected_epoch,
            "found_epoch": halt.found_epoch,
            "txns_in_batch": halt.txns_in_batch,
            "raft_index": halt.raft_index,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    let health = crate::control::startup::health::observe(&state.shared.startup);
    let (status, body) = crate::control::startup::health::to_http_response(&health);
    (status, axum::Json(body))
}

/// GET /health/ready — readiness check (WAL recovered, cores initialized).
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let wal_ready = state.shared.wal.next_lsn().as_u64() > 0;
    let status = if wal_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = json!({
        "status": if wal_ready { "ready" } else { "not_ready" },
        "wal_lsn": state.shared.wal.next_lsn().as_u64(),
        "node_id": state.shared.node_id,
    });
    (status, axum::Json(body))
}

/// POST /health/drain — trigger graceful connection drain.
///
/// Initiates the shared phased shutdown coordinator. It signals the canonical
/// `ShutdownWatch` and then drives every registered drain phase. Subsequent
/// `/healthz` calls return 503, which causes the k8s readiness probe to fail
/// and the service mesh to stop routing new connections to this node.
///
/// Designed for use as an authenticated Kubernetes `preStop` hook. In
/// password mode, inject `NODEDB_DRAIN_TOKEN` from a Secret containing a
/// superuser credential:
///
/// ```yaml
/// lifecycle:
///   preStop:
///     exec:
///       command:
///         - /bin/sh
///         - -c
///         - >-
///           curl -fsS -X POST
///           -H "Authorization: Bearer ${NODEDB_DRAIN_TOKEN}"
///           http://127.0.0.1:8080/health/drain
/// ```
pub async fn drain(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    // Blacklist + account status, no rate limit: a drain is a one-shot
    // lifecycle action a preStop hook must never see throttled, but a
    // blacklisted IP or suspended/banned account must not be able to take a
    // node out of rotation. Runs before the role check so the refusal is on
    // identity alone, as on every other transport.
    admit_without_rate_limit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
    )?;

    // State-changing administrative health actions require authenticated superuser authority.
    if !identity.0.is_superuser() {
        return Err(ApiError::Forbidden("superuser role required".into()));
    }

    tracing::info!(node_id = state.shared.node_id, "drain requested via HTTP");
    // Dropping a Tokio JoinHandle detaches the coordinator task; shutdown
    // progress remains observable through the shared bus.
    drop(state.shutdown_bus.initiate());
    Ok((
        StatusCode::OK,
        axum::Json(json!({
            "status": "draining",
            "node_id": state.shared.node_id,
        })),
    ))
}
