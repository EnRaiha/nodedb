// SPDX-License-Identifier: BUSL-1.1

//! Where a task set runs: here, or at the leader that owns it.
//!
//! Serving a linearizable read on this node is two separate claims — that
//! the routing table names this node leader, and that the node still is one.
//! A partition does not notify a deposed leader, so the second claim is
//! proven against a quorum rather than assumed from the first.

use crate::types::ReadConsistency;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;

/// Where a set of tasks should execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskPlacement {
    /// Run here. Either the deployment has no Raft routing at all, or the
    /// read accepts a local replica.
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

            if leader == my_node {
                // Leading by the routing table's account. A linearizable read
                // still has to prove it against a quorum before it is served.
                if needs_confirmed_leader {
                    local_group = Some(group_id);
                    continue;
                }
                return TaskPlacement::Local;
            }
            // A replica here is good enough for a non-linearizable read.
            if !consistency.requires_leader() && info.members.contains(&my_node) {
                return TaskPlacement::Local;
            }
            if leader == 0 {
                // No leader is known — mid-election, or this node's view is
                // stale. A linearizable read waits for one rather than being
                // answered from whatever is local. A write still runs locally:
                // it is proposed through Raft, which refuses it on a non-leader
                // and redirects, so refusing here would only break writes
                // during the seconds an election takes.
                if needs_confirmed_leader {
                    return TaskPlacement::NoLeader;
                }
                return TaskPlacement::Local;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                // Tasks fan out across leaders — the gateway forwards to one
                // node, so this set runs locally instead.
                Some(prev) if prev != leader => return TaskPlacement::Local,
                _ => {}
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
}
