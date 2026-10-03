// SPDX-License-Identifier: BUSL-1.1

//! Where a task set runs: here, or at the leader that owns it.
//!
//! Serving a linearizable read on this node is two separate claims — that
//! the routing table names this node leader, and that the node still is one.
//! A partition does not notify a deposed leader, so the second claim is
//! proven against a quorum rather than assumed from the first.
//!
//! A linearizable read that runs here over groups led elsewhere gets the same
//! proof per group: each read confirms its group where it is served (see
//! `control::cluster::linearizable_read`). A read of a group with no replica
//! here cannot be served here with that proof, so such a set is refused.
//!
//! A bounded-staleness read makes a weaker claim, and it is checked the same
//! way: being a member of the group says nothing about how far behind the
//! replica has fallen, so the bound is measured against the leader rather
//! than assumed from membership.

use crate::types::ReadConsistency;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;

/// Where a set of tasks must execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TaskPlacement {
    /// Run here. Either the deployment has no Raft routing at all, or the
    /// task set needs no confirmed leader.
    Local,
    /// Run here as a linearizable read. Every read confirms its group where it
    /// is served, and this node holds a replica of every group read here.
    LocalConfirmed,
    /// One remote leader owns every task — forward through the gateway.
    Gateway,
    /// The read must reach a leader, and this node knows of none to send it
    /// to. Serving it here will answer from a replica that can be arbitrarily
    /// far behind.
    NoLeader,
    /// A linearizable read spans groups led by different nodes, and this node
    /// holds no replica of at least one of them. No node can serve the whole
    /// set with proof.
    Unconfirmable,
}

/// Per-task outcome of [`placement_for_group`], folded across a task set by
/// `placement_for_tasks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupPlacement {
    /// This node leads the group and the read can be served without proof.
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

/// One task's group, as `placement_for_tasks` resolves it from the routing
/// table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskGroup {
    /// The routing table maps the task's vShard to no known group.
    Unmapped,
    Placed {
        placement: GroupPlacement,
        /// This node holds a replica of the group, as a voter or a learner.
        hosts_replica: bool,
    },
}

/// One task's routing facts, copied out of the routing table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutedTask {
    group_id: u64,
    /// The routing table's leader hint for the group.
    leader: u64,
    /// This node is a voter of the group.
    is_member: bool,
    /// This node holds a replica of the group, as a voter or a learner.
    hosts_replica: bool,
}

/// Decide how a single task's group must be served, given plain facts
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
    // A hint naming this node is stale once this node is no voter of the
    // group: it left the group, so it leads nothing there.
    let leader = if leader == my_node && !is_member {
        0
    } else {
        leader
    };
    if leader == my_node {
        // Leading by the routing table's account. A linearizable read still
        // has to prove it against a quorum before it is served.
        if needs_confirmed_leader {
            return GroupPlacement::LocalLeader;
        }
        return GroupPlacement::Local;
    }
    // A replica here can serve a read that does not need the leader — but a
    // bounded-staleness read only if the replica is actually within its
    // bound. Too far behind, and it falls through to the leader, which
    // satisfies any bound by definition.
    if !consistency.requires_leader() && is_member && replica_fresh {
        return GroupPlacement::Local;
    }
    if leader == 0 {
        // No leader is known — mid-election, or this node's view is stale.
        // A read that needs a confirmed leader, or a fresher replica than
        // this one proved it has, waits for one rather than being
        // answered from whatever is local. A write still runs locally: it
        // is proposed through Raft, which refuses it on a non-leader and
        // redirects, so refusing here will only break writes during the
        // seconds an election takes.
        let needs_leader_or_fresh = needs_confirmed_leader || consistency.max_staleness().is_some();
        if needs_leader_or_fresh {
            return GroupPlacement::NoLeader;
        }
        return GroupPlacement::Local;
    }
    GroupPlacement::RemoteLeader { leader }
}

/// Fold the per-task groups of a task set into one placement.
///
/// `needs_confirmed_leader` is true for a linearizable read. Such a read
/// never gets `Local`: it runs here only as `LocalConfirmed`, or goes to the
/// one remote leader, or is refused. Every other
/// task set keeps the plain rules: one remote leader forwards, anything else
/// runs here.
fn fold_task_groups(
    groups: impl IntoIterator<Item = TaskGroup>,
    needs_confirmed_leader: bool,
) -> TaskPlacement {
    let mut led_here = false;
    let mut remote_unhosted = false;
    let mut remote_leader: Option<u64> = None;
    let mut split_leaders = false;

    for group in groups {
        let TaskGroup::Placed {
            placement,
            hosts_replica,
        } = group
        else {
            // No group means no leader to prove anything against.
            if needs_confirmed_leader {
                return TaskPlacement::NoLeader;
            }
            return TaskPlacement::Local;
        };
        match placement {
            GroupPlacement::Local if !needs_confirmed_leader => return TaskPlacement::Local,
            GroupPlacement::Local | GroupPlacement::LocalLeader => led_here = true,
            GroupPlacement::NoLeader => return TaskPlacement::NoLeader,
            GroupPlacement::RemoteLeader { leader } => {
                remote_unhosted |= !hosts_replica;
                match remote_leader {
                    None => remote_leader = Some(leader),
                    Some(prev) if prev != leader => split_leaders = true,
                    Some(_) => {}
                }
            }
        }
    }

    match (led_here, remote_leader) {
        (false, None) => TaskPlacement::Local,
        (false, Some(_)) if !split_leaders => TaskPlacement::Gateway,
        (true, None) => TaskPlacement::LocalConfirmed,
        // Several leaders, or leaders here and elsewhere: the gateway forwards
        // to one node, so the set runs here.
        _ if !needs_confirmed_leader => TaskPlacement::Local,
        _ if remote_unhosted => TaskPlacement::Unconfirmable,
        _ => TaskPlacement::LocalConfirmed,
    }
}

impl NodeDbPgHandler {
    /// Decide where `tasks` run.
    ///
    /// `needs_confirmed_leader` is false for a write: it reaches the leader by
    /// being proposed through Raft, which establishes leadership on its own, so
    /// a read-index round in front of it will only add a round trip.
    pub(super) fn placement_for_tasks(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
        needs_confirmed_leader: bool,
    ) -> TaskPlacement {
        let Some(routing) = self.state.cluster_routing.as_ref() else {
            return TaskPlacement::Local;
        };
        let my_node = self.state.node_id;
        // Routing facts are copied out under a short guard. The staleness
        // gate locks `MultiRaft`, so it runs only after the guard drops.
        let routed: Vec<Option<RoutedTask>> = {
            let routing = routing.read().unwrap_or_else(|p| p.into_inner());
            tasks
                .iter()
                .map(|task| {
                    let group_id = routing.group_for_vshard(task.vshard_id.as_u32()).ok()?;
                    let info = routing.group_info(group_id)?;
                    let is_member = info.members.contains(&my_node);
                    Some(RoutedTask {
                        group_id,
                        leader: info.leader,
                        is_member,
                        hosts_replica: is_member || info.learners.contains(&my_node),
                    })
                })
                .collect()
        };

        let groups = routed.into_iter().map(|routed| {
            let Some(RoutedTask {
                group_id,
                leader,
                is_member,
                hosts_replica,
            }) = routed
            else {
                return TaskGroup::Unmapped;
            };
            // Only cheap when consistency carries no bound: `max_staleness()`
            // returns `None` and the gate is never touched.
            let replica_fresh = self.replica_satisfies(group_id, consistency);
            // A node with no replica of the group has nothing to run locally:
            // a read will see none of its rows, and a proposal will wait for
            // an apply that never reaches this node. With no leader to forward
            // to, the task waits for one.
            let placement = if !hosts_replica && (leader == 0 || leader == my_node) {
                GroupPlacement::NoLeader
            } else {
                placement_for_group(
                    leader,
                    my_node,
                    is_member,
                    consistency,
                    needs_confirmed_leader,
                    replica_fresh,
                )
            };
            TaskGroup::Placed {
                placement,
                hosts_replica,
            }
        });
        fold_task_groups(groups, needs_confirmed_leader)
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

    use super::{GroupPlacement, TaskGroup, TaskPlacement, fold_task_groups, placement_for_group};
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

    const THIRD: u64 = 3;

    fn led_here() -> TaskGroup {
        TaskGroup::Placed {
            placement: GroupPlacement::LocalLeader,
            hosts_replica: true,
        }
    }

    fn led_by(leader: u64, hosts_replica: bool) -> TaskGroup {
        TaskGroup::Placed {
            placement: GroupPlacement::RemoteLeader { leader },
            hosts_replica,
        }
    }

    /// Every input a strong read can produce: no combination of leader,
    /// membership and freshness serves it here without proof.
    #[test]
    fn a_strong_read_never_places_a_group_locally() {
        for leader in [NO_LEADER, ME, OTHER] {
            for is_member in [false, true] {
                for fresh in [false, true] {
                    let placement = placement_for_group(
                        leader,
                        ME,
                        is_member,
                        ReadConsistency::Strong,
                        true,
                        fresh,
                    );
                    assert_ne!(placement, GroupPlacement::Local);
                }
            }
        }
    }

    #[test]
    fn a_strong_read_over_mixed_leaders_confirms_every_group() {
        let placement = fold_task_groups([led_here(), led_by(OTHER, true)], true);
        assert_eq!(placement, TaskPlacement::LocalConfirmed);
    }

    #[test]
    fn a_strong_read_over_several_remote_leaders_confirms_every_group() {
        let placement = fold_task_groups([led_by(OTHER, true), led_by(THIRD, true)], true);
        assert_eq!(placement, TaskPlacement::LocalConfirmed);
    }

    #[test]
    fn a_strong_read_over_a_group_with_no_replica_here_is_refused() {
        let mixed = fold_task_groups([led_here(), led_by(OTHER, false)], true);
        assert_eq!(mixed, TaskPlacement::Unconfirmable);
        let fan_out = fold_task_groups([led_by(OTHER, true), led_by(THIRD, false)], true);
        assert_eq!(fan_out, TaskPlacement::Unconfirmable);
    }

    #[test]
    fn a_strong_read_over_an_unmapped_vshard_is_refused() {
        let placement = fold_task_groups([led_here(), TaskGroup::Unmapped], true);
        assert_eq!(placement, TaskPlacement::NoLeader);
    }

    #[test]
    fn a_strong_read_led_by_one_remote_node_forwards() {
        let placement = fold_task_groups([led_by(OTHER, false), led_by(OTHER, false)], true);
        assert_eq!(placement, TaskPlacement::Gateway);
    }

    /// A write proves leadership through its proposal, so a mixed or
    /// unmapped set still runs here.
    #[test]
    fn a_write_over_mixed_leaders_runs_locally() {
        let write = |groups: Vec<TaskGroup>| fold_task_groups(groups, false);
        let local = TaskGroup::Placed {
            placement: GroupPlacement::Local,
            hosts_replica: true,
        };
        assert_eq!(
            write(vec![local, led_by(OTHER, false)]),
            TaskPlacement::Local
        );
        assert_eq!(
            write(vec![led_by(OTHER, false), led_by(THIRD, false)]),
            TaskPlacement::Local
        );
        assert_eq!(write(vec![TaskGroup::Unmapped]), TaskPlacement::Local);
    }
}
