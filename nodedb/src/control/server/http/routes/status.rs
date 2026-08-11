// SPDX-License-Identifier: BUSL-1.1

//! Cluster/node status endpoint.
//!
//! GET /v1/status — returns node and cluster health information.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::control::server::http::admission::{admit, identity_database};
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};
use crate::control::server::http::peer::PeerAddr;
use crate::control::server::http::transport::ClientTransport;

/// GET /v1/status
///
/// Returns node status, cluster topology (if clustered), and resource usage.
/// Requires authentication (any valid identity).
///
/// Response:
/// ```json
/// {
///   "node_id": 1,
///   "wal_next_lsn": 42,
///   "active_sessions": 3,
///   "users": 5,
///   "tenants": 2,
///   "cluster": { "nodes": [...], "raft_groups": [...] }  // if clustered
/// }
/// ```
pub async fn status(
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, peer.as_str(), transport.security()).await?;

    // Request-admission gate before any state is read: this endpoint reports
    // user, tenant, and cluster counts on behalf of the caller, so a
    // blacklisted IP or suspended/banned account must not reach it.
    let rate_limit_headers = admit(
        &state,
        &identity,
        identity_database(&identity),
        peer.as_str(),
        "status",
    )?;

    let wal_next_lsn = state.shared.wal.next_lsn().as_u64();
    let node_id = state.shared.node_id;
    let user_count = state.shared.credentials.list_user_details().len();

    // Tenant count from known users.
    let user_details = state.shared.credentials.list_user_details();
    let mut tenant_ids = std::collections::HashSet::new();
    for user in &user_details {
        tenant_ids.insert(user.tenant_id.as_u64());
    }
    let tenant_count = tenant_ids.len();

    // Audit log size.
    let audit_entries = match state.shared.audit.lock() {
        Ok(log) => log.all().len(),
        Err(p) => p.into_inner().all().len(),
    };

    let mut result = serde_json::json!({
        "node_id": node_id,
        "wal_next_lsn": wal_next_lsn,
        "users": user_count,
        "tenants": tenant_count,
        "audit_entries": audit_entries,
    });

    // Add cluster info if available.
    if let Some(ref topo) = state.shared.cluster_topology {
        let topo = topo.read().unwrap_or_else(|p| p.into_inner());
        let nodes: Vec<serde_json::Value> = topo
            .all_nodes()
            .map(|n| {
                serde_json::json!({
                    "node_id": n.node_id,
                    "addr": n.addr,
                    "state": format!("{:?}", n.state),
                })
            })
            .collect();

        let raft_groups = if let Some(ref routing) = state.shared.cluster_routing {
            let routing = routing.read().unwrap_or_else(|p| p.into_inner());
            routing
                .group_ids()
                .iter()
                .filter_map(|&gid| {
                    routing.group_info(gid).map(|info| {
                        serde_json::json!({
                            "group_id": gid,
                            "leader": info.leader,
                            "members": info.members,
                            "vshard_count": routing.vshards_for_group(gid).len(),
                        })
                    })
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        result["cluster"] = serde_json::json!({
            "nodes": nodes,
            "raft_groups": raft_groups,
        });
    }

    // Add migration info if available.
    if let Some(ref tracker) = state.shared.migration_tracker {
        let snapshots = tracker.snapshot();
        result["migrations"] = serde_json::json!(
            snapshots
                .iter()
                .map(|s| serde_json::json!({
                    "vshard_id": s.vshard_id,
                    "phase": s.phase,
                    "elapsed_ms": s.elapsed_ms,
                    "is_active": s.is_active,
                }))
                .collect::<Vec<_>>()
        );
    }

    Ok((rate_limit_headers, axum::Json(result)))
}
