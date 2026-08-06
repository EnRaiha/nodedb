// SPDX-License-Identifier: BUSL-1.1

//! `GET /v1/cluster/debug/leases` — dump live descriptor leases + the
//! local drain tracker.

use axum::extract::State;
use axum::response::Response;

use super::super::super::auth::{AppState, ResolvedIdentity};
use super::super::super::peer::PeerAddr;
use super::guard::{ensure_debug_access, ok_json};

#[derive(serde::Serialize)]
struct LeaseRow {
    descriptor_id: String,
    node_id: u64,
    version: u64,
    expires_at: String,
}

#[derive(serde::Serialize)]
struct DrainRow {
    descriptor_id: String,
    up_to_version: u64,
    expires_at: String,
}

#[derive(serde::Serialize)]
struct LeasesDebugResponse {
    node_id: u64,
    leases: Vec<LeaseRow>,
    active_drains: Vec<DrainRow>,
}

pub async fn leases_debug(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Response {
    if let Some(resp) = ensure_debug_access(&state, &identity, peer.as_str()) {
        return resp;
    }

    let leases: Vec<LeaseRow> = {
        let cache = state
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let mut rows: Vec<LeaseRow> = cache
            .leases
            .iter()
            .map(|((descriptor_id, node_id), lease)| LeaseRow {
                descriptor_id: format!("{descriptor_id:?}"),
                node_id: *node_id,
                version: lease.version,
                expires_at: format!("{:?}", lease.expires_at),
            })
            .collect();
        rows.sort_by(|a, b| {
            a.descriptor_id
                .cmp(&b.descriptor_id)
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        rows
    };

    let active_drains: Vec<DrainRow> = {
        let mut rows: Vec<DrainRow> = state
            .shared
            .lease_drain
            .snapshot()
            .into_iter()
            .map(|(descriptor_id, entry)| DrainRow {
                descriptor_id: format!("{descriptor_id:?}"),
                up_to_version: entry.up_to_version,
                expires_at: format!("{:?}", entry.expires_at),
            })
            .collect();
        rows.sort_by(|a, b| a.descriptor_id.cmp(&b.descriptor_id));
        rows
    };

    ok_json(&LeasesDebugResponse {
        node_id: state.shared.node_id,
        leases,
        active_drains,
    })
}
