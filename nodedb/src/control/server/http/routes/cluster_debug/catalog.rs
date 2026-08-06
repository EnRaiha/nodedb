// SPDX-License-Identifier: BUSL-1.1

//! `GET /v1/cluster/debug/catalog/descriptors` — snapshot of the
//! replicated `MetadataCache`.
//!
//! The cache itself isn't `Serialize` (it owns `HashMap<..., Lease>`
//! keyed by tuple types), so we project into a local flat response
//! shape that's cheap to encode and useful for dev/ops.

use axum::extract::State;
use axum::response::Response;

use super::super::super::auth::{AppState, ResolvedIdentity};
use super::super::super::peer::PeerAddr;
use super::guard::{ensure_debug_access, ok_json};

#[derive(serde::Serialize)]
struct CatalogDebugResponse {
    node_id: u64,
    applied_index: u64,
    last_applied_hlc: String,
    cluster_version: u16,
    catalog_entries_applied: u64,
    topology_log_len: usize,
    routing_log_len: usize,
    lease_count: usize,
}

pub async fn catalog_debug(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Response {
    if let Some(resp) = ensure_debug_access(&state, &identity, peer.as_str()) {
        return resp;
    }
    let cache = state
        .shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let response = CatalogDebugResponse {
        node_id: state.shared.node_id,
        applied_index: cache.applied_index,
        last_applied_hlc: format!("{:?}", cache.last_applied_hlc),
        cluster_version: cache.cluster_version,
        catalog_entries_applied: cache.catalog_entries_applied,
        topology_log_len: cache.topology_log.len(),
        routing_log_len: cache.routing_log.len(),
        lease_count: cache.leases.len(),
    };
    ok_json(&response)
}
