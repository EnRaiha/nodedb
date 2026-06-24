// SPDX-License-Identifier: BUSL-1.1

//! Variable-length path expansion and neighbor collection.

use std::collections::HashSet;

use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::Direction;

/// Hard cap on results returned from a single variable-length expansion.
/// Defends the Control Plane against pathological queries even when the
/// DSL layer's depth cap is set high.
pub(super) const MAX_VARLEN_RESULTS: usize = 100_000;

/// Hard cap on live frontier size at any hop. Prevents a single wide hop
/// from blowing up intermediate allocation even when global node dedup
/// is in place (dense multigraphs, bidirectional traversal on large |V|).
pub(super) const MAX_VARLEN_FRONTIER: usize = 100_000;

/// Tunable caps for a single variable-length expansion.
///
/// Production constructs this via [`VarLenCaps::default`], which preserves the
/// historical `100_000` hard caps verbatim. The caps are a struct field rather
/// than a module const so tests (and the future cross-shard integration test)
/// can drive truncation deterministically on small graphs without mutating the
/// production ceiling.
#[derive(Debug, Clone, Copy)]
pub(super) struct VarLenCaps {
    /// Max emitted results before truncation fires.
    pub max_results: usize,
    /// Max live frontier (per-hop) before truncation fires.
    pub max_frontier: usize,
}

impl Default for VarLenCaps {
    fn default() -> Self {
        Self {
            max_results: MAX_VARLEN_RESULTS,
            max_frontier: MAX_VARLEN_FRONTIER,
        }
    }
}

/// Where a capped expansion should resume from on the next round.
///
/// Carries the **surviving un-expanded frontier** at a single hop boundary
/// (`frontier`, all reached at `depth - 1` and awaiting expansion AT `depth`)
/// so a follow-up call can continue the BFS from exactly that point. There is
/// deliberately **no `visited` set**: termination relies on the `min..max`
/// depth bound plus downstream coordinator row-dedup, so re-running a node
/// already emitted on the first pass yields a duplicate that is collapsed
/// later — never a skipped or mis-depthed row.
#[derive(Debug, Clone)]
pub(super) struct VarLenCursor {
    /// Un-expanded source node ids to resume the BFS from.
    pub frontier: Vec<u32>,
    /// Hop depth at which `frontier` is to be expanded (`resume_depth`).
    pub depth: usize,
}

/// Result of a variable-length expansion.
///
/// `cursor` is `Some` iff one of the hard caps (`max_results`,
/// `max_frontier`) fired: the result set for this round is incomplete and the
/// cursor records the live frontier/depth needed to resume next round. `None`
/// means the expansion ran to its natural completion.
pub(super) struct VarLenExpansion {
    pub results: Vec<(u32, String)>,
    pub cursor: Option<VarLenCursor>,
}

/// Pattern-shape parameters for a variable-length BFS expansion.
///
/// Bundles the immutable per-query shape (label filter, direction, depth
/// bounds, path-string flag) into a single borrowed struct so `run_bfs`,
/// `expand_variable_length`, and `resume_variable_length` each stay within
/// the 7-argument clippy limit without needing `#[allow]`.
pub(super) struct VarLenPattern<'a> {
    pub label_filter: Option<&'a str>,
    pub direction: Direction,
    pub min_hops: usize,
    pub max_hops: usize,
    /// `true` iff the edge variable is bound in the query (e.g. `[e*1..k]`).
    /// When `false` all `format!`/`String` path work in the hot loop is skipped.
    pub want_path: bool,
}

/// Return `csr.node_name_raw(node).to_string()` when `want_path`, else `""`.
///
/// Centralises the three identical `if want_path { … } else { String::new() }`
/// sites in the BFS hot paths.
#[inline]
fn node_name_or_empty(csr: &CsrIndex, node: u32, want_path: bool) -> String {
    if want_path {
        csr.node_name_raw(node).to_string()
    } else {
        String::new()
    }
}

/// Variable-length path expansion via iterative BFS with **global** per-node
/// dedup — the from-scratch entry point.
///
/// Returns `(dst_node_id, path_description)` for every node reachable in
/// `min_hops..=max_hops` hops from `source`. Each destination is emitted
/// at most once — along the first (shortest) path BFS finds. This is the
/// openCypher semantics for `(a)-[*min..max]->(b)` and the only safe
/// contract on dense graphs: without global dedup, result size grows as
/// `b^max_hops` and the query allocates itself out of the process.
///
/// Path-string construction is gated on `pattern.want_path`. Callers that
/// don't bind the edge variable (i.e. `MATCH (a)-[*1..k]->(b)` with no
/// `-[e*1..k]-`) pass `false` and skip all `format!`/`String` work in
/// the hot loop.
///
/// On a cap hit the returned [`VarLenExpansion::cursor`] is `Some`; callers
/// MUST surface that (as `partial = true` on the response envelope) and may
/// resume via [`resume_variable_length`] so silent partial results are
/// impossible.
pub(super) fn expand_variable_length(
    csr: &CsrIndex,
    source: u32,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
) -> VarLenExpansion {
    let mut results: Vec<(u32, String)> = Vec::new();
    if pattern.max_hops == 0 {
        if pattern.min_hops == 0 {
            results.push((source, node_name_or_empty(csr, source, pattern.want_path)));
        }
        return VarLenExpansion {
            results,
            cursor: None,
        };
    }

    let src_name = node_name_or_empty(csr, source, pattern.want_path);

    // Global visited set — each dst id is emitted and expanded at most once.
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(source);

    // `*0..k` includes the source at depth 0.
    if pattern.min_hops == 0 {
        results.push((source, src_name.clone()));
    }

    let frontier: Vec<(u32, String)> = vec![(source, src_name)];
    run_bfs(csr, results, visited, frontier, 1, pattern, caps)
}

/// Resume a previously-capped variable-length expansion from a [`VarLenCursor`].
///
/// `cursor.frontier` are nodes reached at `cursor.depth - 1` on the prior round
/// and awaiting expansion AT `cursor.depth`. The BFS continues with a **fresh**
/// `visited` set (seeded only with the resume frontier itself so a node is not
/// expanded twice within this round): per the cross-shard contract, dedup of
/// rows already emitted on the prior round is the coordinator's job, never the
/// executor's. The `min_hops..=max_hops` bound is honored across the resume
/// boundary because the loop continues at `cursor.depth`, so a node reached at
/// depth `d` here behaves exactly as a node reached at depth `d` in one pass.
//
// Exercised by the unit tests below; the cross-plane resume path that calls
// this on the owning shard is wired up in the following sub-unit (2b).
#[allow(dead_code)]
pub(super) fn resume_variable_length(
    csr: &CsrIndex,
    cursor: &VarLenCursor,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
) -> VarLenExpansion {
    // Seed visited with the resume frontier so an intra-round revisit of a
    // resume node is suppressed; rows already emitted in the PRIOR round are
    // intentionally NOT tracked (coordinator dedups across rounds).
    let mut visited: HashSet<u32> = HashSet::new();
    let mut frontier: Vec<(u32, String)> = Vec::with_capacity(cursor.frontier.len());
    for &node in &cursor.frontier {
        if !visited.insert(node) {
            continue;
        }
        frontier.push((node, node_name_or_empty(csr, node, pattern.want_path)));
    }

    run_bfs(
        csr,
        Vec::new(),
        visited,
        frontier,
        cursor.depth,
        pattern,
        caps,
    )
}

/// Shared BFS driver for both the from-scratch and resume paths.
///
/// Expands `frontier` hop-by-hop from `start_depth` through `pattern.max_hops`,
/// emitting destinations at `depth >= pattern.min_hops`. Caps are honored at
/// **hop boundaries**: a depth level is processed to completion, then the cap is
/// checked. This keeps the resume cursor depth-exact — the surviving
/// `next_frontier` is a single set all reached at the same depth, awaiting
/// expansion at `depth + 1`.
fn run_bfs(
    csr: &CsrIndex,
    mut results: Vec<(u32, String)>,
    mut visited: HashSet<u32>,
    mut frontier: Vec<(u32, String)>,
    start_depth: usize,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
) -> VarLenExpansion {
    let mut cursor: Option<VarLenCursor> = None;

    for depth in start_depth..=pattern.max_hops {
        if frontier.is_empty() {
            break;
        }

        let mut next_frontier: Vec<(u32, String)> = Vec::new();

        for (node, path) in &frontier {
            let neighbors = collect_neighbors(csr, *node, pattern.label_filter, pattern.direction);
            for (_, dst) in neighbors {
                if !visited.insert(dst) {
                    continue;
                }

                let new_path = if pattern.want_path {
                    let dst_name = csr.node_name_raw(dst).to_string();
                    format!("{path}->{dst_name}")
                } else {
                    String::new()
                };

                if depth >= pattern.min_hops {
                    results.push((dst, new_path.clone()));
                }

                if depth < pattern.max_hops {
                    next_frontier.push((dst, new_path));
                }
            }
        }

        // Honor caps at the hop boundary so the resume cursor is depth-exact:
        // `next_frontier` is a single set all reached at `depth`, awaiting
        // expansion at `depth + 1`. A node here behaves on resume exactly as
        // if reached at `depth` in one uninterrupted pass.
        let cap_hit = results.len() >= caps.max_results || next_frontier.len() >= caps.max_frontier;
        if cap_hit {
            if depth < pattern.max_hops && !next_frontier.is_empty() {
                cursor = Some(VarLenCursor {
                    frontier: next_frontier.into_iter().map(|(id, _)| id).collect(),
                    depth: depth + 1,
                });
            }
            break;
        }

        frontier = next_frontier;
    }

    VarLenExpansion { results, cursor }
}

/// Collect neighbor (label_id, node_id) pairs from CSR.
pub(super) fn collect_neighbors(
    csr: &CsrIndex,
    node: u32,
    label_filter: Option<&str>,
    direction: Direction,
) -> Vec<(u32, u32)> {
    let mut neighbors = Vec::new();
    match direction {
        Direction::Out => {
            for (lid, dst) in csr.iter_out_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, dst));
                }
            }
        }
        Direction::In => {
            for (lid, src) in csr.iter_in_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, src));
                }
            }
        }
        Direction::Both => {
            for (lid, dst) in csr.iter_out_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, dst));
                }
            }
            for (lid, src) in csr.iter_in_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, src));
                }
            }
        }
    }
    neighbors
}

fn csr_label_matches(csr: &CsrIndex, label_id: u32, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(f) => csr.label_name(label_id) == f,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::csr::CsrIndex;
    use crate::engine::graph::edge_store::Direction;

    /// Spec: variable-length expansion MUST apply global per-node dedup.
    ///
    /// On a densely connected graph the number of paths of length ≤ d grows
    /// as b^d, but the number of distinct (dst, min-path) pairs is bounded
    /// by |V| × (d - min + 1). The fix must enforce that bound; without it,
    /// a graph with branching factor b = 6 and max_hops = 8 allocates 6^8 =
    /// 1.6M paths, which is a DoS vector.
    ///
    /// Regression guard: result count must stay sublinear in b^max_hops,
    /// with a hard cap proportional to |V| × (max_hops - min_hops + 1).
    #[test]
    fn variable_length_expansion_dedups_nodes_across_paths() {
        // Build a near-complete directed graph on 6 nodes (branching 5 per
        // node, 30 edges). With max_hops = 8 and no dedup the BFS explores
        // 5^8 = 390,625 distinct paths. With dedup it explores ≤ 6 nodes
        // per depth level, i.e. ≤ 48 results over 8 hops.
        let mut csr = CsrIndex::new();
        let nodes = ["a", "b", "c", "d", "e", "f"];
        for &src in &nodes {
            for &dst in &nodes {
                if src != dst {
                    csr.add_edge(src, "l", dst).unwrap();
                }
            }
        }

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 1,
                max_hops: 8,
                want_path: false,
            },
            VarLenCaps::default(),
        );
        let results = expansion.results;

        // Spec: distinct destinations are bounded by (|V| - 1) = 5.
        let distinct_dsts: std::collections::HashSet<u32> =
            results.iter().map(|(d, _)| *d).collect();
        assert!(
            distinct_dsts.len() <= nodes.len(),
            "distinct dst count must be <= |V| ({}); got {}",
            nodes.len(),
            distinct_dsts.len()
        );

        // Regression guard against exponential fan-out: the total result
        // count must not approach b^max_hops. Cap at |V| × max_hops = 48.
        // Current buggy code returns hundreds of thousands of rows.
        assert!(
            results.len() <= nodes.len() * 8,
            "variable-length expansion must not allocate b^d paths; \
             got {} results on a 6-node graph with max_hops=8 \
             (expected ≤ {})",
            results.len(),
            nodes.len() * 8
        );
    }

    /// Spec: `*0..k` is openCypher-style "match the source itself plus
    /// paths up to length k". At depth 0 the source node must be in the
    /// result set. The current BFS starts `depth` at 1 and never emits
    /// the source even when `min_hops == 0`.
    #[test]
    fn variable_length_expansion_includes_source_at_zero_hops() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "l", "b").unwrap();
        csr.add_edge("b", "l", "c").unwrap();

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 0,
                max_hops: 2,
                want_path: false,
            },
            VarLenCaps::default(),
        );
        let results = expansion.results;

        let dsts: std::collections::HashSet<u32> = results.iter().map(|(d, _)| *d).collect();
        assert!(
            dsts.contains(&csr.node_id_raw("a").unwrap()),
            "*0..k must include the source node at depth 0; got dsts {dsts:?}"
        );
    }

    /// Spec: `*k..k` (exact length) returns only destinations reachable
    /// in exactly k hops — not the union of 1..=k. The current BFS does
    /// gate emission with `if depth >= min_hops`, but the expansion must
    /// remain correct once global dedup prunes shorter paths.
    #[test]
    fn variable_length_expansion_exact_length_returns_only_that_depth() {
        let mut csr = CsrIndex::new();
        // Chain a → b → c → d. At exactly 2 hops from `a` only `c` is
        // reachable, not `b` (1 hop) or `d` (3 hops).
        csr.add_edge("a", "l", "b").unwrap();
        csr.add_edge("b", "l", "c").unwrap();
        csr.add_edge("c", "l", "d").unwrap();

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 2,
                max_hops: 2,
                want_path: false,
            },
            VarLenCaps::default(),
        );
        let results = expansion.results;

        let dsts: std::collections::HashSet<u32> = results.iter().map(|(d, _)| *d).collect();
        let c = csr.node_id_raw("c").unwrap();
        let expected: std::collections::HashSet<u32> = [c].into_iter().collect();
        assert_eq!(
            dsts, expected,
            "*2..2 must return exactly the depth-2 reachable set {{c}}; got {dsts:?}"
        );
    }

    /// Spec: even with global node dedup in place, a single hop must
    /// not allow the live frontier to grow unboundedly. A pathological
    /// graph with many distinct nodes all reachable from the source in
    /// one hop should respect a per-hop frontier cap so subsequent hops
    /// cannot snowball.
    ///
    /// Regression guard: on a star with `N` leaves and `max_hops` large,
    /// the result set is bounded by `N`; a buggy no-cap implementation
    /// that forgets to cap the per-hop frontier under dedup can still
    /// allocate O(N × max_hops) in intermediate state. We assert result
    /// size is bounded.
    #[test]
    fn variable_length_expansion_caps_frontier_per_hop() {
        let mut csr = CsrIndex::new();
        const LEAVES: usize = 5_000;
        for i in 0..LEAVES {
            csr.add_edge("root", "l", &format!("leaf_{i}")).unwrap();
        }

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("root").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 1,
                max_hops: 5,
                want_path: false,
            },
            VarLenCaps::default(),
        );
        let results = expansion.results;

        // With global dedup every leaf appears exactly once across the
        // whole traversal — subsequent hops have no outgoing edges.
        assert!(
            results.len() <= LEAVES,
            "star with {LEAVES} leaves must return at most {LEAVES} results; \
             got {}",
            results.len()
        );
    }

    /// Build a simple directed chain `n0 -l-> n1 -l-> ... -l-> n{len}`.
    fn make_chain(len: usize) -> CsrIndex {
        let mut csr = CsrIndex::new();
        for i in 0..len {
            csr.add_edge(&format!("n{i}"), "l", &format!("n{}", i + 1))
                .unwrap();
        }
        csr
    }

    fn dst_set(results: &[(u32, String)]) -> std::collections::HashSet<u32> {
        results.iter().map(|(d, _)| *d).collect()
    }

    /// Spec: a capped expansion resumed from its `VarLenCursor` produces the
    /// SAME destination set as a single uncapped pass. Exact set equality of
    /// (first-pass ∪ resumed) vs (uncapped). The cap is injected via
    /// `VarLenCaps`, NOT by lowering the 100k production const.
    #[test]
    fn varlen_resume_union_equals_uncapped_pass() {
        // Chain n0 -> n1 -> ... -> n6 (6 edges). `*1..6` from n0 reaches
        // {n1..n6} in one pass. A low results cap forces truncation mid-way.
        let csr = make_chain(6);
        let src = csr.node_id_raw("n0").unwrap();

        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 6,
            want_path: false,
        };
        let uncapped = expand_variable_length(&csr, src, &pat, VarLenCaps::default());
        assert!(uncapped.cursor.is_none(), "uncapped pass must not truncate");
        let full = dst_set(&uncapped.results);

        // Inject a low results cap so truncation fires at a hop boundary.
        let caps = VarLenCaps {
            max_results: 2,
            max_frontier: usize::MAX,
        };
        let first = expand_variable_length(&csr, src, &pat, caps);
        let cursor = first
            .cursor
            .clone()
            .expect("low cap must produce a resume cursor");
        assert!(cursor.depth >= 2, "resume depth advances past the cap");

        // Resume — possibly more than once — until the BFS completes.
        let mut union: std::collections::HashSet<u32> = dst_set(&first.results);
        let mut next = Some(cursor);
        while let Some(c) = next {
            let resumed = resume_variable_length(&csr, &c, &pat, caps);
            union.extend(dst_set(&resumed.results));
            next = resumed.cursor;
        }

        assert_eq!(
            union, full,
            "first-pass ∪ resumed must equal the uncapped destination set"
        );
    }

    /// Spec: under the cap, an expansion completes in one pass with `cursor ==
    /// None` and the same results as before the resume machinery existed.
    #[test]
    fn varlen_no_truncation_path_unchanged() {
        let csr = make_chain(3); // n0 -> n1 -> n2 -> n3
        let src = csr.node_id_raw("n0").unwrap();
        let expansion = expand_variable_length(
            &csr,
            src,
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 1,
                max_hops: 3,
                want_path: false,
            },
            VarLenCaps::default(),
        );
        assert!(
            expansion.cursor.is_none(),
            "well under the cap → no truncation cursor"
        );
        let dsts = dst_set(&expansion.results);
        let expected: std::collections::HashSet<u32> = ["n1", "n2", "n3"]
            .iter()
            .map(|n| csr.node_id_raw(n).unwrap())
            .collect();
        assert_eq!(dsts, expected, "results identical to a normal pass");
    }

    /// Spec: the `min..max` depth bound is honored ACROSS the resume boundary.
    /// A `*1..2` expansion truncated at depth 1 and resumed at depth 2 must
    /// NOT emit depth-3 nodes.
    #[test]
    fn varlen_resume_honors_depth_bound() {
        let csr = make_chain(3); // n0 -> n1 -> n2 -> n3
        let src = csr.node_id_raw("n0").unwrap();
        let n3 = csr.node_id_raw("n3").unwrap();

        // cap=1 truncates after emitting n1 at depth 1; max_hops=2.
        let caps = VarLenCaps {
            max_results: 1,
            max_frontier: usize::MAX,
        };
        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 2,
            want_path: false,
        };
        let first = expand_variable_length(&csr, src, &pat, caps);
        let cursor = first.cursor.clone().expect("cap=1 must truncate");

        let resumed = resume_variable_length(&csr, &cursor, &pat, caps);

        let mut union = dst_set(&first.results);
        union.extend(dst_set(&resumed.results));

        assert!(
            !union.contains(&n3),
            "*1..2 must never emit the depth-3 node n3 across the resume boundary; \
             got {union:?}"
        );
        let expected: std::collections::HashSet<u32> = ["n1", "n2"]
            .iter()
            .map(|n| csr.node_id_raw(n).unwrap())
            .collect();
        assert_eq!(
            union, expected,
            "*1..2 resume union must be exactly the depth-1..2 set {{n1,n2}}"
        );
    }
}
