// SPDX-License-Identifier: BUSL-1.1

//! Leader probe for data groups whose Raft does not keep this node's view.
//!
//! Two kinds of data group are probed:
//! - a group this node hosts no replica of;
//! - a group this node hosts but has left: its authored placement names
//!   other nodes only, and this node does not lead it.
//!
//! This node's Raft sees no leader of a group it hosts no replica of. Its
//! hint for such a group moves by redirects, and a redirect needs a node to
//! ask. A hint that names no leader, or names a node outside the group's
//! placement, leaves nothing to ask: every read, write and surrogate
//! exchange for the group would fail until an election somewhere happened
//! to reach this node.
//!
//! A hint that names a placement node goes stale too: a transfer or a
//! failover in the group moves its leadership, and nothing tells this node.
//! Nor does anything tell it the group's voters and learners: this node
//! applies none of the group's conf changes. A node that left a group learns
//! of its removal from one `AppendEntries` the leader sends as it applies
//! the removal. A lost send leaves the node listing itself as a replica.
//!
//! Each pass asks one node of every probed group for its leader status: the
//! leader it knows and the term that leader leads. The receiver answers from
//! its own Raft state with no quorum round, so a probe costs the leader
//! nothing but the answer. A routing hint needs no linearizable
//! confirmation: a request sent to a node that no longer leads is redirected.
//! The answer confirms the hint (see
//! [`crate::routing::RoutingTable::confirm_leader`]), so a newer term always
//! replaces it, and a stale answer never moves it back.
//! - A hint that names a placement node is checked at that node. A transfer
//!   or a failover reaches the hint within one pass.
//! - A hint that names no leader, or a node outside the placement, is
//!   probed at a placement node that rotates each pass, so a dead placement
//!   node does not stall the probe.
//!
//! An answer from the leader itself carries the group's voters and learners.
//! This node's routing view adopts them (see
//! [`crate::routing::RoutingTable::adopt_leader_membership`]). A node that
//! left a group then stops listing itself, and the unmount step drops its
//! replica.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tracing::debug;

use crate::forward::PlanExecutor;
use crate::routing::RoutingTable;
use crate::rpc_codec::{LeaderStatusRequest, LeaderStatusResponse, RaftRpc};

use super::super::loop_core::{CommitApplier, RaftLoop};

/// Ticks between leader-probe passes (about 0.5 s at the 10 ms tick).
pub(in crate::raft_loop) const LEADER_PROBE_TICK_INTERVAL: u64 = 50;

/// The `(leader, term)` a leader-status answer names, when it names one.
fn named_leader(status: &LeaderStatusResponse) -> Option<(u64, u64)> {
    (status.leader != 0 && status.term != 0).then_some((status.leader, status.term))
}

/// Apply `target`'s leader-status answer for `group_id` to `routing`: the
/// named leader confirms the hint, and a membership `target` answers as the
/// leader replaces the view of the group's voters and learners.
fn adopt_answer(
    routing: &RwLock<RoutingTable>,
    group_id: u64,
    target: u64,
    status: &LeaderStatusResponse,
) {
    let Some((leader, term)) = named_leader(status) else {
        return;
    };
    let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
    if table.confirm_leader(group_id, leader, term) {
        debug!(
            group_id,
            leader, term, "leader probe: routing hint names the group's leader"
        );
    }
    let Some(membership) = status.membership.as_ref().filter(|_| leader == target) else {
        return;
    };
    if table.adopt_leader_membership(
        group_id,
        leader,
        term,
        &membership.voters,
        &membership.learners,
    ) {
        debug!(
            group_id,
            leader,
            term,
            voters = ?membership.voters,
            learners = ?membership.learners,
            "leader probe: routing view adopts the leader's membership"
        );
    }
}

/// The hosted groups whose Raft keeps this node's view: those this node
/// leads, and those whose placement is not authored or names this node.
/// Every other hosted group is one this node has left.
fn kept_by_raft(
    self_id: u64,
    hosted: &HashSet<u64>,
    leading: &HashSet<u64>,
    routing: &RoutingTable,
) -> HashSet<u64> {
    hosted
        .iter()
        .copied()
        .filter(|gid| {
            leading.contains(gid)
                || routing
                    .group_info(*gid)
                    .and_then(|info| info.placement.as_ref())
                    .is_none_or(|placement| placement.contains(&self_id))
        })
        .collect()
}

/// `(group_id, target)` for each data group not in `kept` that the probe
/// asks on pass `pass`, per the rules in the module docs. `kept` holds the
/// hosted groups whose Raft keeps this node's view (see [`kept_by_raft`]).
/// Pure and deterministic.
pub(super) fn plan_probes(
    self_id: u64,
    kept: &HashSet<u64>,
    routing: &RoutingTable,
    pass: u64,
) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = routing
        .group_members()
        .iter()
        .filter(|(gid, _)| {
            **gid != crate::metadata_group::METADATA_GROUP_ID
                && **gid != crate::calvin::sequencer::SEQUENCER_GROUP_ID
                && !kept.contains(*gid)
        })
        .filter_map(|(&gid, info)| {
            let mut candidates: Vec<u64> = routing
                .effective_placement(gid)
                .into_iter()
                .filter(|n| *n != self_id)
                .collect();
            candidates.sort_unstable();
            candidates.dedup();
            if candidates.contains(&info.leader) {
                return Some((gid, info.leader));
            }
            if candidates.is_empty() {
                return None;
            }
            let idx = usize::try_from(pass % candidates.len() as u64).unwrap_or(0);
            candidates.get(idx).map(|&target| (gid, target))
        })
        .collect();
    out.sort_unstable();
    out
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Probe the data groups [`plan_probes`] names for `pass`. Each probe
    /// runs on its own task, one per group at a time.
    pub(in crate::raft_loop) fn probe_unhosted_leaders(&self, pass: u64) {
        let (probes, routing) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let group_ids = mr.group_ids();
            let hosted: HashSet<u64> = group_ids.iter().copied().collect();
            let leading: HashSet<u64> = group_ids
                .iter()
                .copied()
                .filter(|gid| mr.group_role_is_leader(*gid))
                .collect();
            let routing = mr.routing();
            let probes = {
                let table = routing.read().unwrap_or_else(|p| p.into_inner());
                let kept = kept_by_raft(self.node_id, &hosted, &leading, &table);
                plan_probes(self.node_id, &kept, &table, pass)
            };
            (probes, routing)
        };
        for (group_id, target) in probes {
            let addr = self
                .topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get_node(target)
                .and_then(|node| node.socket_addr());
            let Some(addr) = addr else {
                continue;
            };
            if !self.tick_state.begin_probe(group_id) {
                continue;
            }
            self.transport.register_peer(target, addr);
            let transport = Arc::clone(&self.transport);
            let routing = Arc::clone(&routing);
            let tick_state = Arc::clone(&self.tick_state);
            tokio::spawn(async move {
                let request = RaftRpc::LeaderStatusRequest(LeaderStatusRequest { group_id });
                if let Ok(RaftRpc::LeaderStatusResponse(status)) =
                    transport.send_rpc(target, request).await
                {
                    adopt_answer(&routing, group_id, target, &status);
                }
                tick_state.end_probe(group_id);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[u64]) -> HashSet<u64> {
        ids.iter().copied().collect()
    }

    /// An answer that names `leader` at `term`, with no membership.
    fn answer(leader: u64, term: u64) -> LeaderStatusResponse {
        LeaderStatusResponse {
            leader,
            term,
            membership: None,
        }
    }

    #[test]
    fn an_answer_names_a_leader_only_with_its_term() {
        assert_eq!(named_leader(&answer(3, 5)), Some((3, 5)));
        assert_eq!(named_leader(&answer(0, 0)), None);
        assert_eq!(named_leader(&answer(3, 0)), None);
    }

    #[test]
    fn every_probed_group_is_probed_every_pass() {
        let mut rt = RoutingTable::uniform(3, &[1, 2, 3], 1);
        rt.set_placement(1, vec![2]);
        rt.set_placement(2, vec![3]);
        rt.set_placement(3, vec![2]);
        // Group 1: the hint names node 1, outside its placement.
        assert!(rt.observe_leader(1, 1, 3));
        // Group 2: the hint names node 3, its placement node.
        assert!(rt.observe_leader(2, 3, 3));
        // Group 3: no leader known.
        rt.clear_leader(3);
        // A hint on a placement node is checked at that node; the others go
        // to the rotating placement node.
        assert_eq!(
            plan_probes(1, &set(&[0]), &rt, 0),
            vec![(1, 2), (2, 3), (3, 2)]
        );
        assert_eq!(
            plan_probes(1, &set(&[0]), &rt, 1),
            vec![(1, 2), (2, 3), (3, 2)]
        );
        // A group this node's Raft keeps is never probed.
        assert_eq!(plan_probes(1, &set(&[0, 1, 3]), &rt, 0), vec![(2, 3)]);
        assert_eq!(plan_probes(1, &set(&[0, 1, 2, 3]), &rt, 0), Vec::new());
    }

    /// A transfer in a group this node does not host leaves its hint on the
    /// old leader, a placement node. The probe asks that node, whose answer
    /// moves the hint to the new leader at the new term. A stale answer from
    /// an older term never moves it back.
    #[test]
    fn a_hint_left_on_the_old_leader_after_a_transfer_follows_the_answer() {
        let mut rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        rt.set_placement(1, vec![2, 3]);
        assert!(rt.observe_leader(1, 2, 4));
        assert_eq!(plan_probes(1, &set(&[]), &rt, 7), vec![(1, 2)]);
        // Node 2 answers that node 3 leads term 5.
        let (leader, term) = named_leader(&answer(3, 5)).expect("a named leader");
        assert!(rt.confirm_leader(1, leader, term));
        assert_eq!(rt.leader_at_term_for_vshard(0).unwrap(), (3, 5));
        assert!(!rt.confirm_leader(1, 2, 4));
        assert_eq!(plan_probes(1, &set(&[]), &rt, 8), vec![(1, 3)]);
    }

    /// A hosted group whose placement names other nodes only is probed
    /// unless this node leads it. A hosted group whose placement names this
    /// node, or has none authored, is kept by this node's Raft.
    #[test]
    fn a_hosted_group_this_node_left_is_probed() {
        let mut rt = RoutingTable::uniform(3, &[1, 2, 3], 2);
        rt.set_placement(1, vec![2, 3]);
        rt.set_placement(2, vec![1, 2]);
        rt.set_placement(3, vec![2, 3]);
        let hosted = set(&[0, 1, 2, 3]);
        // Node 1 leads group 3, which it left too.
        let kept = kept_by_raft(1, &hosted, &set(&[3]), &rt);
        assert_eq!(kept, set(&[0, 2, 3]));
        assert_eq!(
            plan_probes(1, &kept, &rt, 0)
                .into_iter()
                .map(|(gid, _)| gid)
                .collect::<Vec<_>>(),
            vec![1]
        );
        // A group with no authored placement is kept.
        let unplaced = RoutingTable::uniform(1, &[1, 2, 3], 2);
        assert_eq!(kept_by_raft(1, &set(&[1]), &set(&[]), &unplaced), set(&[1]));
    }

    /// The leader's answer confirms the hint and replaces the membership
    /// view. The same membership from a node that does not lead is ignored.
    #[test]
    fn a_leaders_answer_carries_the_membership_into_the_view() {
        let mut rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        rt.set_placement(1, vec![2]);
        let routing = RwLock::new(rt);
        let membership = Some(crate::rpc_codec::LeaderMembership {
            voters: vec![2],
            learners: vec![],
        });

        // Node 3 names node 2 and answers a membership it has no right to.
        adopt_answer(
            &routing,
            1,
            3,
            &LeaderStatusResponse {
                leader: 2,
                term: 4,
                membership: membership.clone(),
            },
        );
        {
            let table = routing.read().unwrap_or_else(|p| p.into_inner());
            let info = table.group_info(1).expect("group 1");
            assert_eq!((info.leader, info.leader_term), (2, 4));
            assert_eq!(info.members.len(), 3);
        }

        // Node 2 answers as the leader.
        adopt_answer(
            &routing,
            1,
            2,
            &LeaderStatusResponse {
                leader: 2,
                term: 4,
                membership,
            },
        );
        let table = routing.read().unwrap_or_else(|p| p.into_inner());
        let info = table.group_info(1).expect("group 1");
        assert_eq!(info.members, vec![2]);
        assert!(info.learners.is_empty());
    }

    #[test]
    fn the_target_rotates_over_the_placement() {
        let mut rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        rt.set_placement(1, vec![2, 3]);
        rt.clear_leader(1);
        assert_eq!(plan_probes(1, &set(&[]), &rt, 0), vec![(1, 2)]);
        assert_eq!(plan_probes(1, &set(&[]), &rt, 1), vec![(1, 3)]);
    }
}
