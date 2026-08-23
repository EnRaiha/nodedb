// SPDX-License-Identifier: BUSL-1.1

//! Where a task set runs: here, or at the leader that owns it.
//!
//! Serving a linearizable read on this node is two separate claims — that
//! the routing table names this node leader, and that the node still is one.
//! A partition does not notify a deposed leader, so the second claim is
//! proven against a quorum rather than assumed from the first.
//!
//! A bounded-staleness read makes a weaker claim, and it is checked the same
//! way: being a member of the group says nothing about how far behind the
//! replica has fallen, so the bound is measured against the leader rather
//! than assumed from membership.

use crate::types::ReadConsistency;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;

/// Where a set of tasks should execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskPlacement {
    /// Run here. Either the deployment has no Raft routing at all, or the
    /// read accepts this replica.
    Local,
    /// Run here once a quorum confirms this node still leads `group_id`.
    LocalLeader { group_id: u64 },
    /// One remote leader owns every task — forward through the gateway.
    Gateway,
    /// The read must reach a leader, and this node knows of none to send it
    /// to. Serving it here would answer from a replica that may be arbitrarily
    /// far behind.
    NoLeader,
}

/// Per-task outcome of [`placement_for_group`], folded across a task set by
/// `placement_for_tasks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupPlacement {
    /// This node leads the group and the read may be served without proof.
    Local,
    /// This node leads the group by the routing table's account, but the
    /// read needs that confirmed against a quorum first. The caller attaches
    /// `group_id` — this function is never given one to keep it plain-value.
    LocalLeader,
    /// A remote node leads the group — forward to it.
    RemoteLeader { leader: u64 },
    /// The read needs a leader or a freshness guarantee neither a known
    /// leader nor an unproven local replica can give, and none is known.
    NoLeader,
}

/// Decide how a single task's group should be served, given plain facts
/// about it — no routing-table or gate lookups, so this is unit-testable in
/// isolation from `SharedState`.
///
/// `needs_confirmed_leader` gates the `leader == my_node` case: a linearizable
/// read leading here still has to prove that against a quorum, a write does
/// not (Raft proves it by accepting the proposal). `replica_fresh` is this
/// node's answer to "is my replica within the read's bound", pre-computed by
/// the caller since it requires the staleness gate; it is only consulted when
/// this node is a member and not the leader.
fn placement_for_group(
    leader: u64,
    my_node: u64,
    is_member: bool,
    consistency: ReadConsistency,
    needs_confirmed_leader: bool,
    replica_fresh: bool,
) -> GroupPlacement {
    if leader == my_node {
        // Leading by the routing table's account. A linearizable read still
        // has to prove it against a quorum before it is served.
        if needs_confirmed_leader {
            return GroupPlacement::LocalLeader;
        }
        return GroupPlacement::Local;
    }
    // A replica here may serve a read that does not need the leader — but a
    // bounded-staleness read only if the replica is actually within its
    // bound. Too far behind, and it falls through to the leader, which
    // satisfies any bound by definition.
    if !consistency.requires_leader() && is_member && replica_fresh {
        return GroupPlacement::Local;
    }
    if leader == 0 {
        // No leader is known — mid-election, or this node's view is stale.
        // A read that needs a confirmed leader, or a fresher replica than
        // this one just proved it has, waits for one rather than being
        // answered from whatever is local. A write still runs locally: it
        // is proposed through Raft, which refuses it on a non-leader and
        // redirects, so refusing here would only break writes during the
        // seconds an election takes.
        let needs_leader_or_fresh = needs_confirmed_leader || consistency.max_staleness().is_some();
        if needs_leader_or_fresh {
            return GroupPlacement::NoLeader;
        }
        return GroupPlacement::Local;
    }
    GroupPlacement::RemoteLeader { leader }
}

impl NodeDbPgHandler {
    /// Decide where `tasks` run.
    ///
    /// `needs_confirmed_leader` is false for a write: it reaches the leader by
    /// being proposed through Raft, which establishes leadership on its own, so
    /// a read-index round in front of it would only add a round trip.
    pub(super) fn placement_for_tasks(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
        needs_confirmed_leader: bool,
    ) -> TaskPlacement {
        if self.state.gateway.get().is_none() {
            return TaskPlacement::Local;
        }
        let Some(routing) = self.state.cluster_routing.as_ref() else {
            return TaskPlacement::Local;
        };
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;
        let mut local_group: Option<u64> = None;
        for task in tasks {
            let vshard_id = task.vshard_id.as_u32();
            let Ok(group_id) = routing.group_for_vshard(vshard_id) else {
                return TaskPlacement::Local;
            };
            let Some(info) = routing.group_info(group_id) else {
                return TaskPlacement::Local;
            };
            let leader = info.leader;
            let is_member = info.members.contains(&my_node);
            // Only cheap when consistency carries no bound: `max_staleness()`
            // returns `None` and the gate is never touched.
            let replica_fresh = self.replica_satisfies(group_id, consistency);

            match placement_for_group(
                leader,
                my_node,
                is_member,
                consistency,
                needs_confirmed_leader,
                replica_fresh,
            ) {
                GroupPlacement::Local => return TaskPlacement::Local,
                GroupPlacement::LocalLeader => {
                    local_group = Some(group_id);
                }
                GroupPlacement::NoLeader => return TaskPlacement::NoLeader,
                GroupPlacement::RemoteLeader { leader } => match remote_leader {
                    None => remote_leader = Some(leader),
                    // Tasks fan out across leaders — the gateway forwards to
                    // one node, so this set runs locally instead.
                    Some(prev) if prev != leader => return TaskPlacement::Local,
                    _ => {}
                },
            }
        }

        match (local_group, remote_leader) {
            // Some tasks lead here and others lead elsewhere: no single node
            // can serve the set, so it runs locally as it always has.
            (Some(_), Some(_)) => TaskPlacement::Local,
            (Some(group_id), None) => TaskPlacement::LocalLeader { group_id },
            (None, Some(_)) => TaskPlacement::Gateway,
            (None, None) => TaskPlacement::Local,
        }
    }

    /// Whether this node's replica of `group_id` meets `consistency`.
    ///
    /// `Eventual` asks for no freshness at all, so any replica satisfies it.
    /// `BoundedStaleness` asks how far behind the leader this replica is,
    /// which only Raft can answer. With no gate installed there is no cluster,
    /// and the local copy is the only copy.
    fn replica_satisfies(&self, group_id: u64, consistency: ReadConsistency) -> bool {
        let Some(max_staleness) = consistency.max_staleness() else {
            return true;
        };
        match self.state.raft_read_gate.get() {
            Some(gate) => gate.within_staleness_bound(group_id, max_staleness),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{GroupPlacement, placement_for_group};
    use crate::types::ReadConsistency;

    const LEADER: u64 = 1;
    const ME: u64 = 1;
    const OTHER: u64 = 2;
    const NO_LEADER: u64 = 0;

    fn bounded() -> ReadConsistency {
        ReadConsistency::BoundedStaleness(Duration::from_secs(5))
    }

    #[test]
    fn write_runs_locally_when_no_leader_is_known() {
        let placement =
            placement_for_group(NO_LEADER, ME, true, ReadConsistency::Strong, false, false);
        assert_eq!(placement, GroupPlacement::Local);
    }

    #[test]
    fn eventual_read_always_runs_locally() {
        let placement = placement_for_group(
            NO_LEADER,
            ME,
            false,
            ReadConsistency::Eventual,
            false,
            false,
        );
        assert_eq!(placement, GroupPlacement::Local);
    }

    #[test]
    fn fresh_bounded_staleness_replica_runs_locally() {
        let placement = placement_for_group(OTHER, ME, true, bounded(), false, true);
        assert_eq!(placement, GroupPlacement::Local);
    }

    #[test]
    fn stale_bounded_staleness_replica_forwards_to_known_leader() {
        let placement = placement_for_group(OTHER, ME, true, bounded(), false, false);
        assert_eq!(placement, GroupPlacement::RemoteLeader { leader: OTHER });
    }

    #[test]
    fn stale_bounded_staleness_replica_waits_for_unknown_leader() {
        let placement = placement_for_group(NO_LEADER, ME, true, bounded(), false, false);
        assert_eq!(placement, GroupPlacement::NoLeader);
    }

    #[test]
    fn strong_read_waits_for_unknown_leader() {
        let placement =
            placement_for_group(NO_LEADER, ME, true, ReadConsistency::Strong, true, false);
        assert_eq!(placement, GroupPlacement::NoLeader);
    }

    #[test]
    fn confirmed_local_leader_is_used_when_leadership_needs_proof() {
        let placement = placement_for_group(LEADER, ME, true, ReadConsistency::Strong, true, false);
        assert_eq!(placement, GroupPlacement::LocalLeader);
    }
}
