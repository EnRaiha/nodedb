// SPDX-License-Identifier: BUSL-1.1

//! Deterministic preferred leader per data group.
//!
//! Every node that bootstraps a cluster starts as the sole voter of every
//! group, so it leads them all. Nothing in Raft moves a leader that stays
//! healthy, and one node would keep serving every write. This module names
//! one preferred leader per data group, spread over the group placements.
//! The group's leader transfers leadership to it (see the leader-balance
//! tick phase).
//!
//! The assignment walks the data groups in ascending id. Each group takes
//! the node of its effective placement that has the fewest groups so far.
//! A tie goes to the higher rendezvous score for `(group_id, node_id)`, then
//! the lower node id. Every node that holds the same placements computes
//! the same map.

use std::collections::BTreeMap;

use crate::routing::RoutingTable;

use super::placement::hrw_score;

/// The preferred leader of every data group in `routing`: `group_id ->
/// node_id`. The metadata group and the sequencer group have none. A group
/// with an empty effective placement has none.
pub fn preferred_leaders(routing: &RoutingTable) -> BTreeMap<u64, u64> {
    let mut group_ids: Vec<u64> = routing
        .group_ids()
        .into_iter()
        .filter(|gid| {
            *gid != crate::metadata_group::METADATA_GROUP_ID
                && *gid != crate::calvin::sequencer::SEQUENCER_GROUP_ID
        })
        .collect();
    group_ids.sort_unstable();

    let mut led: BTreeMap<u64, usize> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for gid in group_ids {
        let mut candidates = routing.effective_placement(gid);
        candidates.sort_unstable();
        candidates.dedup();
        let chosen = candidates.into_iter().min_by(|&a, &b| {
            let count_a = led.get(&a).copied().unwrap_or(0);
            let count_b = led.get(&b).copied().unwrap_or(0);
            count_a
                .cmp(&count_b)
                .then(hrw_score(gid, b).cmp(&hrw_score(gid, a)))
                .then(a.cmp(&b))
        });
        if let Some(node) = chosen {
            *led.entry(node).or_default() += 1;
            out.insert(gid, node);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaders_spread_over_the_voters() {
        let rt = RoutingTable::uniform(6, &[1, 2, 3], 3);
        let preferred = preferred_leaders(&rt);
        assert_eq!(preferred.len(), 6, "every data group has one");
        assert!(!preferred.contains_key(&crate::metadata_group::METADATA_GROUP_ID));
        let mut per_node: BTreeMap<u64, usize> = BTreeMap::new();
        for node in preferred.values() {
            *per_node.entry(*node).or_default() += 1;
        }
        assert_eq!(per_node.len(), 3, "every voter leads a group");
        assert!(per_node.values().all(|&n| n == 2), "{per_node:?}");
    }

    #[test]
    fn two_groups_never_share_a_preferred_leader_on_three_voters() {
        let rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        let preferred = preferred_leaders(&rt);
        assert_ne!(preferred.get(&1), preferred.get(&2));
    }

    #[test]
    fn the_preferred_leader_is_in_the_placement() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        rt.set_placement(1, vec![3]);
        rt.set_placement(2, vec![2]);
        let preferred = preferred_leaders(&rt);
        assert_eq!(preferred.get(&1), Some(&3));
        assert_eq!(preferred.get(&2), Some(&2));
    }

    #[test]
    fn the_map_is_deterministic() {
        let rt = RoutingTable::uniform(8, &[1, 2, 3, 4], 3);
        assert_eq!(preferred_leaders(&rt), preferred_leaders(&rt.clone()));
    }
}
