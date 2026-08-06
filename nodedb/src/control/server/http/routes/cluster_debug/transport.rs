// SPDX-License-Identifier: BUSL-1.1

//! `GET /v1/cluster/debug/transport` — dump the QUIC transport's
//! connection cache + per-peer circuit-breaker state.

use axum::extract::State;
use axum::response::Response;

use nodedb_cluster::{BreakerSnapshot, TransportPeerSnapshot};

use super::super::super::auth::{AppState, ResolvedIdentity};
use super::super::super::peer::PeerAddr;
use super::guard::{cluster_disabled, ensure_debug_access, ok_json};

#[derive(serde::Serialize)]
struct TransportDebugResponse {
    node_id: u64,
    peers: Vec<TransportPeerSnapshot>,
    breakers: Vec<BreakerSnapshot>,
}

pub async fn transport_debug(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Response {
    if let Some(resp) = ensure_debug_access(&state, &identity, peer.as_str()) {
        return resp;
    }
    let Some(transport) = state.shared.cluster_transport.as_ref() else {
        return cluster_disabled();
    };
    let response = TransportDebugResponse {
        node_id: state.shared.node_id,
        peers: transport.peer_snapshot(),
        breakers: transport.circuit_breaker().snapshot(),
    };
    ok_json(&response)
}
