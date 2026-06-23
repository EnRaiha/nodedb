// SPDX-License-Identifier: BUSL-1.1

//! Shared cluster-routing helpers for graph scatter paths (`match_scatter` and
//! `bsp_pagerank`): resolve a vShard to a live `RouteDecision`, and fetch the
//! gateway `Arc<SharedState>` used for remote dispatch.
//!
//! Both helpers resolve against LIVE Raft leadership where available so a stale
//! routing-table hint cannot misdirect a scatter. Factored here so the MATCH
//! scatter and the BSP PageRank coordinator share one implementation instead of
//! duplicating the routing-lock + live-leader plumbing.

use std::sync::Arc;

use crate::control::gateway::RouteDecision;
use crate::control::gateway::router::resolve_decision;
use crate::control::state::SharedState;

/// Resolve a vShard to a `RouteDecision` against live Raft leadership, falling
/// back to the routing-table hint when no live snapshot is available.
pub(super) fn resolve_for_vshard(state: &SharedState, vshard_id: u32) -> RouteDecision {
    let routing_guard = state
        .cluster_routing
        .as_ref()
        .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
    let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
        state.raft_status_fn.get().map(|f| f()).unwrap_or_default();
    let live_leader = move |group_id: u64| -> u64 {
        raft_snapshot
            .iter()
            .find(|gs| gs.group_id == group_id)
            .map(|gs| gs.leader_id)
            .unwrap_or(0)
    };
    let live_lookup: Option<&dyn Fn(u64) -> u64> = if state.raft_status_fn.get().is_some() {
        Some(&live_leader)
    } else {
        None
    };
    resolve_decision(
        vshard_id,
        state.node_id,
        routing_guard.as_deref(),
        live_lookup,
    )
}

/// The gateway's `Arc<SharedState>` for the remote dispatch path. In cluster
/// mode the gateway is always wired; failing loudly here beats silently
/// degrading to a local-only (partial) scatter.
pub(super) fn gateway_shared(state: &SharedState) -> crate::Result<&Arc<SharedState>> {
    state
        .gateway
        .as_ref()
        .map(|g| &g.shared)
        .ok_or_else(|| crate::Error::Internal {
            detail: "graph scatter: cluster routing present but gateway unavailable for \
                     remote dispatch"
                .into(),
        })
}
