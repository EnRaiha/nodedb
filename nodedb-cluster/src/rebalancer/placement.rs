// SPDX-License-Identifier: BUSL-1.1

//! Deterministic Raft-group placement via rendezvous (highest-random-weight) hashing.
//!
//! When the active node set changes, only ~1/N of group assignments are disturbed —
//! critical because moving a Raft voter requires streaming a full snapshot.

use std::collections::BTreeMap;

use crate::routing::RoutingTable;

/// Deterministic per-group target voter set via rendezvous hashing.
///
/// For each group, every active node is scored by a stable hash of
/// `(group_id, node_id)`; the top `min(rf, N)` nodes by score form
/// the placement. When one node is added or removed, only the groups
/// that would have selected that node are affected.
///
/// Returns a map from `group_id` to its placement, each placement
/// sorted ascending (canonical form for stable equality and diffing).
///
/// `active_nodes` is the candidate voter set; the caller filters by
/// node state. `data_group_ids` must already exclude the metadata
/// group and the sequencer group; the caller owns that filtering.
/// `rf == 0` yields empty placements for every group.
pub fn compute_placement(
    active_nodes: &[u64],
    data_group_ids: &[u64],
    rf: u32,
) -> BTreeMap<u64, Vec<u64>> {
    let mut out = BTreeMap::new();
    for &gid in data_group_ids {
        out.insert(gid, select_nodes_for_group(gid, active_nodes, rf as usize));
    }
    out
}

/// Select the top `min(rf, active_nodes.len())` nodes for a group by HRW score.
///
/// Score is the stable hash of `(group_id, node_id)`. Ties are broken by
/// ascending `node_id` so the result is fully deterministic even on collision.
/// The returned `Vec` is sorted ascending.
fn select_nodes_for_group(group_id: u64, active_nodes: &[u64], rf: usize) -> Vec<u64> {
    let take = rf.min(active_nodes.len());
    if take == 0 {
        return Vec::new();
    }
    let mut scored: Vec<(u64, u64)> = active_nodes
        .iter()
        .map(|&n| (hrw_score(group_id, n), n))
        .collect();
    // Highest score first; smallest node_id wins ties for stability.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut chosen: Vec<u64> = scored.into_iter().take(take).map(|(_, n)| n).collect();
    chosen.sort_unstable();
    chosen
}

/// Stable, portable HRW score for a `(group_id, node_id)` pair.
///
/// Uses a fixed 16-byte little-endian layout so the hash value is
/// identical on every node and platform.
fn hrw_score(group_id: u64, node_id: u64) -> u64 {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&group_id.to_le_bytes());
    key[8..].copy_from_slice(&node_id.to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&key)
}

/// Diff a computed target placement against the routing table's current
/// effective placement, returning the `(group_id, target)` pairs that differ
/// and therefore need a `SetPlacement` proposal. Both sides are compared as
/// sorted vecs (the target is already sorted; `effective_placement` may return
/// unsorted members on fallback, so the current side is sorted before compare).
pub fn compute_placement_changes(
    routing: &RoutingTable,
    target: &BTreeMap<u64, Vec<u64>>,
) -> Vec<(u64, Vec<u64>)> {
    target
        .iter()
        .filter_map(|(&gid, want)| {
            let mut current = routing.effective_placement(gid);
            current.sort_unstable();
            if current != *want {
                Some((gid, want.clone()))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn nodes(ids: &[u64]) -> Vec<u64> {
        ids.to_vec()
    }

    fn groups(ids: &[u64]) -> Vec<u64> {
        ids.to_vec()
    }

    // ── 1. Determinism ───────────────────────────────────────────────────────

    #[test]
    fn deterministic_identical_inputs() {
        let active = nodes(&[1, 2, 3, 4]);
        let gids = groups(&[10, 11, 12]);
        let a = compute_placement(&active, &gids, 3);
        let b = compute_placement(&active, &gids, 3);
        assert_eq!(a, b);
    }

    // ── 2. Cardinality ───────────────────────────────────────────────────────

    #[test]
    fn cardinality_rf3_n4() {
        let active = nodes(&[1, 2, 3, 4]);
        let gids = groups(&[10, 11, 12, 13]);
        let placement = compute_placement(&active, &gids, 3);
        for voters in placement.values() {
            assert_eq!(voters.len(), 3);
        }
    }

    #[test]
    fn cardinality_rf2_n4() {
        let active = nodes(&[1, 2, 3, 4]);
        let gids = groups(&[10, 11, 12]);
        let placement = compute_placement(&active, &gids, 2);
        for voters in placement.values() {
            assert_eq!(voters.len(), 2);
        }
    }

    // ── 3. N <= RF → all nodes ───────────────────────────────────────────────

    #[test]
    fn n_equals_rf_returns_all_nodes() {
        let active = nodes(&[1, 2, 3]);
        let gids = groups(&[10, 11, 12]);
        let placement = compute_placement(&active, &gids, 3);
        for voters in placement.values() {
            assert_eq!(voters, &[1u64, 2, 3]);
        }
    }

    #[test]
    fn n_less_than_rf_returns_all_nodes() {
        let active = nodes(&[1, 2]);
        let gids = groups(&[10, 11]);
        let placement = compute_placement(&active, &gids, 5);
        for voters in placement.values() {
            assert_eq!(voters, &[1u64, 2]);
        }
    }

    // ── 4. Subset + sorted ───────────────────────────────────────────────────

    #[test]
    fn placement_is_subset_sorted_no_duplicates() {
        let active = nodes(&[10, 3, 7, 1, 42]);
        let gids: Vec<u64> = (100..120).collect();
        let placement = compute_placement(&active, &gids, 3);
        let active_set: std::collections::BTreeSet<u64> = active.iter().copied().collect();
        for voters in placement.values() {
            // subset
            for &v in voters {
                assert!(active_set.contains(&v), "voter {v} not in active set");
            }
            // sorted ascending
            assert!(
                voters.windows(2).all(|w| w[0] < w[1]),
                "not sorted: {voters:?}"
            );
        }
    }

    // ── 5. Edge cases ────────────────────────────────────────────────────────

    #[test]
    fn rf_zero_yields_empty_placements() {
        let active = nodes(&[1, 2, 3]);
        let gids = groups(&[10, 11]);
        let placement = compute_placement(&active, &gids, 0);
        for voters in placement.values() {
            assert!(voters.is_empty());
        }
    }

    #[test]
    fn empty_active_nodes_yields_empty_placements() {
        let gids = groups(&[10, 11, 12]);
        let placement = compute_placement(&[], &gids, 3);
        for voters in placement.values() {
            assert!(voters.is_empty());
        }
    }

    #[test]
    fn empty_group_ids_yields_empty_map() {
        let active = nodes(&[1, 2, 3]);
        let placement = compute_placement(&active, &[], 3);
        assert!(placement.is_empty());
    }

    // ── 6. Minimal churn (HRW property) ─────────────────────────────────────

    #[test]
    fn removing_a_node_only_disturbs_groups_that_contained_it() {
        let active_full: Vec<u64> = vec![1, 2, 3, 4, 5];
        let active_trimmed: Vec<u64> = vec![1, 2, 3, 4]; // node 5 removed
        let gids: Vec<u64> = (1u64..=20).collect();
        let rf = 3;

        let full = compute_placement(&active_full, &gids, rf);
        let trimmed = compute_placement(&active_trimmed, &gids, rf);

        for &gid in &gids {
            let full_voters = full.get(&gid).unwrap();
            let trimmed_voters = trimmed.get(&gid).unwrap();
            if !full_voters.contains(&5) {
                // Node 5 was not in this group's voter set — placement must be unchanged.
                assert_eq!(
                    full_voters, trimmed_voters,
                    "group {gid} did not contain node 5 but its placement changed"
                );
            }
            // Groups that did contain node 5 may (and will) change — that is expected.
        }
    }

    // ── 7. Group 0 is not special ────────────────────────────────────────────

    #[test]
    fn group_zero_is_not_special_cased() {
        // The function does not filter or modify group 0 — that is the caller's job.
        let active = nodes(&[1, 2, 3]);
        let placement = compute_placement(&active, &[0], 3);
        assert_eq!(placement.get(&0).unwrap(), &[1u64, 2, 3]);
    }

    // ── 8. compute_placement_changes ─────────────────────────────────────────

    #[test]
    fn no_changes_when_target_equals_current() {
        // uniform(2, [1,2,3], 3) creates data groups 1,2 with explicit placement
        // = all three nodes. Target computed for those same groups must match.
        let mut routing = RoutingTable::uniform(2, &[1, 2, 3], 3);
        routing.set_placement(1, vec![1, 2, 3]);
        routing.set_placement(2, vec![1, 2, 3]);

        let mut target = BTreeMap::new();
        target.insert(1u64, vec![1u64, 2, 3]);
        target.insert(2u64, vec![1u64, 2, 3]);

        assert!(compute_placement_changes(&routing, &target).is_empty());
    }

    #[test]
    fn emits_change_when_target_differs() {
        let mut routing = RoutingTable::uniform(2, &[1, 2, 3], 3);
        routing.set_placement(1, vec![1, 2, 3]);
        routing.set_placement(2, vec![1, 2, 3]);

        let mut target = BTreeMap::new();
        target.insert(1u64, vec![1u64, 2, 3]); // unchanged
        target.insert(2u64, vec![1u64, 2, 4]); // node 3 -> 4

        let changes = compute_placement_changes(&routing, &target);
        assert_eq!(changes, vec![(2u64, vec![1u64, 2, 4])]);
    }

    #[test]
    fn members_fallback_is_sorted_before_compare() {
        // No explicit placement: effective_placement falls back to `members`,
        // which may be unsorted. An unsorted-but-equal-as-a-set members list
        // must NOT yield a spurious change once sorted.
        let mut routing = RoutingTable::uniform(1, &[1, 2, 3], 3);
        routing.set_group_members(1, vec![3, 1, 2]); // unsorted, no placement set

        let mut target = BTreeMap::new();
        target.insert(1u64, vec![1u64, 2, 3]); // sorted (canonical)

        assert!(compute_placement_changes(&routing, &target).is_empty());
    }
}
