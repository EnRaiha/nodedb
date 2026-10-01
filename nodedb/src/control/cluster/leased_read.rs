// SPDX-License-Identifier: BUSL-1.1

//! Serving a read on a group's leader under its leader lease.
//!
//! A read answered by a node that lost leadership can miss writes a newer
//! leader committed. A node answers a leased read of a group only while it
//! holds the group's leader lease, and only once it has applied the group
//! through the lease read index. The lease lapses before any other node can
//! win an election, so no newer leader has committed anything the read
//! misses. A group whose lease this node does not hold is refused, with the
//! leader and term its routing table names.

use std::time::Instant;

use crate::control::state::SharedState;

use super::linearizable_read::wait_applied_through;

/// A group this node does not serve a leased read of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRefusal {
    pub group_id: u64,
    /// The leader this node's routing table names, `0` when it names none or
    /// names this node.
    pub leader_node: u64,
    /// The term `leader_node` is known at, `0` when unknown.
    pub leader_term: u64,
}

/// Make a read of `groups` safe to serve on this node under its leader
/// leases.
///
/// Returns the groups whose lease this node does not hold. Every other group
/// is applied here through its lease read index before this returns. A node
/// with no routing table serves every group: without a cluster there is one
/// copy and nothing to prove.
pub async fn confirm_leased_read(
    state: &SharedState,
    groups: &[u64],
    deadline: Instant,
) -> crate::Result<Vec<LeaseRefusal>> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(gate) = state.raft_read_gate.get() else {
        // A routing table without a gate: `start_raft` has not published it.
        return Ok(groups
            .iter()
            .map(|&group_id| LeaseRefusal {
                group_id,
                leader_node: 0,
                leader_term: 0,
            })
            .collect());
    };
    let mut refusals = Vec::new();
    let mut leased: Vec<(u64, u64)> = Vec::new();
    {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        for &group_id in groups {
            match gate.lease_read_index(group_id) {
                Some(read_index) => leased.push((group_id, read_index)),
                None => {
                    let (leader, term) = routing
                        .group_info(group_id)
                        .map(|info| (info.leader, info.leader_term))
                        .unwrap_or((0, 0));
                    refusals.push(LeaseRefusal {
                        group_id,
                        leader_node: if leader == state.node_id { 0 } else { leader },
                        leader_term: term,
                    });
                }
            }
        }
    }
    let waits = leased
        .into_iter()
        .map(|(group_id, read_index)| wait_applied_through(state, group_id, read_index, deadline));
    futures::future::try_join_all(waits).await?;
    Ok(refusals)
}
