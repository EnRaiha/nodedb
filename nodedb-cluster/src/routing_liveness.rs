// SPDX-License-Identifier: BUSL-1.1

//! Liveness-driven routing invalidation.
//!
//! [`RoutingLivenessHook`] is a [`MembershipSubscriber`] that clears
//! the leader hint for every Raft group this node replicates whose
//! leaseholder has just been marked `Suspect`, `Dead`, or `Left` by the
//! SWIM failure detector. After the hook fires, the next query that consults the
//! routing table observes `leader == 0` (the "no leader known"
//! sentinel) and falls through to a fresh leader discovery via the
//! existing `NotLeader`-triggered election path.
//!
//! A clear keeps the hint's term, so only a new election or a
//! confirmation fills it (see [`RoutingTable::confirm_leader`]). A
//! suspicion SWIM refutes is such a confirmation: when a node it marked
//! `Suspect` or `Dead` answers as `Alive` again, the hook fills each
//! hint it cleared for that node back, where the hint is still cleared
//! at the same term. Without that, a leader wrongly suspected under
//! load stays unknown on this node until the group's next election.
//!
//! The hook is storage-agnostic: it holds `Arc<RwLock<RoutingTable>>`
//! and a resolver closure that maps the string-keyed SWIM `NodeId`
//! to the numeric `u64` id used throughout the rest of the cluster
//! crate. Wiring layers (start_cluster, tests) supply the resolver
//! appropriate to their topology source.
//!
//! The hook is sync and cheap: one `RwLock::write` and a linear scan
//! over group_members. No I/O, no spawning. That keeps it safe to call
//! directly from the detector run loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use nodedb_types::NodeId;
use tracing::debug;

use crate::routing::RoutingTable;
use crate::swim::MemberState;
use crate::swim::subscriber::MembershipSubscriber;

/// Resolver mapping SWIM `NodeId` → numeric `u64` routing-table id.
///
/// Returns `None` for members SWIM knows about but the routing table
/// does not (placeholder `seed:<addr>` entries before the first real
/// probe, transient learners, etc.). Those are silently ignored.
pub type NodeIdResolver = Arc<dyn Fn(&NodeId) -> Option<u64> + Send + Sync>;

/// Clears the leader hint for every group this node replicates that is
/// led by a node SWIM has marked Suspect/Dead/Left, and fills it back when
/// SWIM sees the node alive again.
///
/// The hint of a group this node does not replicate is left as it is. This
/// node's Raft never observes such a group, so a cleared hint there names
/// nobody to ask. A hint that names a dead node fails its request instead,
/// and the gateway retry and the leader probe move it on.
pub struct RoutingLivenessHook {
    routing: Arc<RwLock<RoutingTable>>,
    resolver: NodeIdResolver,
    /// This node's routing-table id.
    local_node_id: u64,
    /// Per suspected node: the `(group_id, term)` hints the hook cleared
    /// for it. Bounded by the groups the node led when it was suspected.
    cleared: Mutex<HashMap<u64, Vec<(u64, u64)>>>,
}

impl RoutingLivenessHook {
    pub fn new(
        routing: Arc<RwLock<RoutingTable>>,
        resolver: NodeIdResolver,
        local_node_id: u64,
    ) -> Self {
        Self {
            routing,
            resolver,
            local_node_id,
            cleared: Mutex::new(HashMap::new()),
        }
    }

    /// Clear every hint of a replicated group that names `numeric_id`, and
    /// remember each one.
    fn clear_led_by(&self, node_id: &NodeId, numeric_id: u64, new: MemberState) {
        let local = self.local_node_id;
        let mut rt = self.routing.write().unwrap_or_else(|p| p.into_inner());
        let affected: Vec<(u64, u64)> = rt
            .group_members()
            .iter()
            .filter(|(_, info)| {
                info.leader == numeric_id
                    && (info.members.contains(&local) || info.learners.contains(&local))
            })
            .map(|(gid, info)| (*gid, info.leader_term))
            .collect();
        for (gid, _) in &affected {
            rt.clear_leader(*gid);
        }
        drop(rt);
        if affected.is_empty() {
            return;
        }
        debug!(
            ?node_id,
            ?new,
            numeric_id,
            groups_invalidated = affected.len(),
            "routing liveness hook cleared leader hints"
        );
        let mut cleared = self.cleared.lock().unwrap_or_else(|p| p.into_inner());
        let entries = cleared.entry(numeric_id).or_default();
        for (gid, term) in affected {
            entries.retain(|(g, _)| *g != gid);
            entries.push((gid, term));
        }
    }

    /// Fill back every hint cleared for `numeric_id` that is still cleared
    /// at the term it was cleared at.
    fn restore_led_by(&self, node_id: &NodeId, numeric_id: u64) {
        let Some(entries) = self
            .cleared
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&numeric_id)
        else {
            return;
        };
        let mut rt = self.routing.write().unwrap_or_else(|p| p.into_inner());
        let restored = entries
            .into_iter()
            .filter(|&(gid, term)| rt.confirm_leader(gid, numeric_id, term))
            .count();
        if restored > 0 {
            debug!(
                ?node_id,
                numeric_id,
                groups_restored = restored,
                "routing liveness hook restored leader hints of a node alive again"
            );
        }
    }
}

impl MembershipSubscriber for RoutingLivenessHook {
    fn on_state_change(&self, node_id: &NodeId, old: Option<MemberState>, new: MemberState) {
        let Some(numeric_id) = (self.resolver)(node_id) else {
            // SWIM knows about a node the routing table doesn't — a
            // seed placeholder, a learner mid-join, or a node that
            // was never registered. Nothing to invalidate.
            return;
        };
        match new {
            MemberState::Suspect | MemberState::Dead | MemberState::Left => {
                self.clear_led_by(node_id, numeric_id, new);
            }
            // A node SWIM suspected answers again: the suspicion was wrong.
            MemberState::Alive if matches!(old, Some(MemberState::Suspect | MemberState::Dead)) => {
                self.restore_led_by(node_id, numeric_id);
            }
            MemberState::Alive => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The node the hook runs on. It replicates every group of
    /// [`rt_with_leaders`].
    const LOCAL: u64 = 100;

    fn rt_with_leaders(pairs: &[(u64, u64)]) -> Arc<RwLock<RoutingTable>> {
        // Build a routing table with `pairs.len()` groups where group
        // `gid` has leader `leader`. Every node, `LOCAL` included, is a
        // voter of every group. The leader is then overridden.
        let mut nodes: Vec<u64> = pairs.iter().map(|(_, l)| *l).collect();
        nodes.push(LOCAL);
        nodes.sort_unstable();
        nodes.dedup();
        let mut rt = RoutingTable::uniform(pairs.len() as u64, &nodes, nodes.len());
        for (gid, leader) in pairs {
            rt.set_leader(*gid, *leader);
        }
        Arc::new(RwLock::new(rt))
    }

    fn hook(
        rt: &Arc<RwLock<RoutingTable>>,
        map: &'static [(&'static str, u64)],
    ) -> RoutingLivenessHook {
        RoutingLivenessHook::new(rt.clone(), resolver_for(map), LOCAL)
    }

    fn resolver_for(map: &'static [(&'static str, u64)]) -> NodeIdResolver {
        Arc::new(move |nid: &NodeId| {
            map.iter()
                .find(|(s, _)| *s == nid.as_str())
                .map(|(_, n)| *n)
        })
    }

    #[test]
    fn dead_transition_clears_leader_for_owned_groups() {
        let rt = rt_with_leaders(&[(0, 1), (1, 2), (2, 1), (3, 3)]);
        let hook = hook(&rt, &[("a", 1), ("b", 2), ("c", 3)]);

        hook.on_state_change(
            &NodeId::try_new("a").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Dead,
        );

        let guard = rt.read().unwrap();
        assert_eq!(guard.group_info(0).unwrap().leader, 0);
        assert_eq!(guard.group_info(1).unwrap().leader, 2);
        assert_eq!(guard.group_info(2).unwrap().leader, 0);
        assert_eq!(guard.group_info(3).unwrap().leader, 3);
    }

    #[test]
    fn suspect_transition_also_invalidates() {
        let rt = rt_with_leaders(&[(0, 7)]);
        let hook = hook(&rt, &[("x", 7)]);
        hook.on_state_change(
            &NodeId::try_new("x").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Suspect,
        );
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 0);
    }

    #[test]
    fn alive_transition_is_noop() {
        let rt = rt_with_leaders(&[(0, 5)]);
        let hook = hook(&rt, &[("q", 5)]);
        hook.on_state_change(
            &NodeId::try_new("q").expect("test fixture"),
            None,
            MemberState::Alive,
        );
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 5);
    }

    #[test]
    fn unresolved_node_id_is_ignored() {
        let rt = rt_with_leaders(&[(0, 1)]);
        let hook = hook(&rt, &[("a", 1)]);
        // NodeId "seed:127.0.0.1:9000" is not in the resolver map.
        hook.on_state_change(
            &NodeId::try_new("seed:127.0.0.1:9000").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Dead,
        );
        // Leader untouched because the resolver returned None.
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 1);
    }

    #[test]
    fn left_is_also_invalidating() {
        let rt = rt_with_leaders(&[(0, 2)]);
        let hook = hook(&rt, &[("b", 2)]);
        hook.on_state_change(
            &NodeId::try_new("b").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Left,
        );
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 0);
    }

    /// A refuted suspicion fills the cleared hint back, unless a newer
    /// election already moved it.
    #[test]
    fn a_node_alive_again_gets_back_the_hints_its_suspicion_cleared() {
        let rt = rt_with_leaders(&[(0, 1), (1, 1)]);
        {
            let mut table = rt.write().unwrap();
            assert!(table.observe_leader(0, 1, 4));
            assert!(table.observe_leader(1, 1, 4));
        }
        let hook = hook(&rt, &[("a", 1)]);
        let a = NodeId::try_new("a").expect("test fixture");
        hook.on_state_change(&a, Some(MemberState::Alive), MemberState::Suspect);
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 0);

        // Group 1 elects node 2 at term 5 while node 1 is suspected.
        assert!(rt.write().unwrap().observe_leader(1, 2, 5));

        hook.on_state_change(&a, Some(MemberState::Suspect), MemberState::Alive);
        let table = rt.read().unwrap();
        let info0 = table.group_info(0).unwrap();
        assert_eq!((info0.leader, info0.leader_term), (1, 4));
        let info1 = table.group_info(1).unwrap();
        assert_eq!((info1.leader, info1.leader_term), (2, 5));
    }

    /// A group this node does not replicate keeps its hint: nothing else
    /// here could name a node to ask.
    #[test]
    fn a_group_this_node_does_not_replicate_keeps_its_hint() {
        let rt = rt_with_leaders(&[(0, 1), (1, 1)]);
        {
            let mut table = rt.write().unwrap();
            table.set_group_members(1, vec![1, 2]);
        }
        let hook = hook(&rt, &[("a", 1)]);
        hook.on_state_change(
            &NodeId::try_new("a").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Dead,
        );
        let table = rt.read().unwrap();
        assert_eq!(table.group_info(0).unwrap().leader, 0);
        assert_eq!(table.group_info(1).unwrap().leader, 1);
    }
}
