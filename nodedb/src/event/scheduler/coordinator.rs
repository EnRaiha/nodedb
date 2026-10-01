// SPDX-License-Identifier: BUSL-1.1

//! The `_system` coordinator: the one node that fires cross-collection
//! schedules and scheduled backups.
//!
//! The coordinator is the node that leads vShard 0's Raft group under a
//! leader lease valid now. A leader cut off from its peers keeps its Raft
//! role until it hears a higher term, but its lease lapses first, and the
//! lease lapses before any other node can win an election. So at most one
//! node is the coordinator at a time.

use crate::control::state::SharedState;

/// Whether this node is the `_system` coordinator now. A node with no
/// routing table is a single node and always is. A node with a routing
/// table but no Raft read gate yet is not.
pub fn is_system_coordinator(state: &SharedState) -> bool {
    let Some(ref routing_lock) = state.cluster_routing else {
        return true;
    };
    let group_id = {
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        routing.group_for_vshard(0)
    };
    let Ok(group_id) = group_id else {
        return false;
    };
    state
        .raft_read_gate
        .get()
        .is_some_and(|gate| gate.holds_leader_lease(group_id))
}

/// Fail unless this node is the `_system` coordinator now. A scheduled step
/// calls this right before each effect another node must not repeat.
pub fn ensure_system_coordinator(state: &SharedState) -> crate::Result<()> {
    if is_system_coordinator(state) {
        return Ok(());
    }
    Err(crate::Error::Dispatch {
        detail: format!(
            "node {} no longer holds the vShard 0 leader lease; the scheduled step stops \
             before its next effect",
            state.node_id
        ),
    })
}
