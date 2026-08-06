// SPDX-License-Identifier: BUSL-1.1

//! `GET /v1/cluster/debug/raft/{group_id}` — dump the Raft state for one
//! group from the node-local `raft_status_fn`.
//!
//! The status closure is published by `control::cluster::start_raft`
//! at startup. It enumerates every group this node participates in;
//! we filter to the caller's requested group so the response stays
//! bounded even on nodes hosting hundreds of groups.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;

use super::super::super::auth::{AppState, ResolvedIdentity};
use super::super::super::peer::PeerAddr;
use super::guard::{cluster_disabled, ensure_debug_access, json_response, ok_json};

#[derive(serde::Serialize)]
struct RaftDebugResponse {
    node_id: u64,
    group_id: u64,
    status: nodedb_cluster::GroupStatus,
}

pub async fn raft_debug(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Path(group_id): Path<u64>,
) -> Response {
    if let Some(resp) = ensure_debug_access(&state, &identity, peer.as_str()) {
        return resp;
    }
    let Some(status_fn) = state.shared.raft_status_fn.get() else {
        return cluster_disabled();
    };
    let statuses = status_fn();
    match statuses.into_iter().find(|s| s.group_id == group_id) {
        Some(status) => ok_json(&RaftDebugResponse {
            node_id: state.shared.node_id,
            group_id,
            status,
        }),
        None => json_response(
            StatusCode::NOT_FOUND,
            format!(r#"{{"error":"group not found on this node","group_id":{group_id}}}"#),
        ),
    }
}
