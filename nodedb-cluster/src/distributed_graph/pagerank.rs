// SPDX-License-Identifier: BUSL-1.1

//! Per-shard PageRank execution state for distributed BSP.

use std::collections::HashMap;

/// Per-superstep outbound cross-shard contributions: `target_shard_id ->
/// [(destination_vertex_name, contribution)]`. The coordinator routes each entry
/// to the shard that owns `target_shard_id` for the next superstep.
pub type OutboundContributions = HashMap<u16, Vec<(String, f64)>>;

/// Per-shard PageRank state maintained across supersteps.
#[derive(Debug)]
pub struct ShardPageRankState {
    pub vertex_count: usize,
    pub rank: Vec<f64>,
    pub next_rank: Vec<f64>,
    pub out_degrees: Vec<usize>,
    pub is_dangling: Vec<bool>,
    pub boundary_edges: HashMap<u32, Vec<(String, u16)>>,
    pub incoming_contributions: HashMap<String, f64>,
}

impl ShardPageRankState {
    /// Initialize from local CSR partition.
    pub fn init<F>(
        vertex_count: usize,
        out_degrees: Vec<usize>,
        _ghost_lookup: F,
        csr_out_edges: &dyn Fn(u32) -> Vec<(String, bool, u16)>,
    ) -> Self
    where
        F: Fn(&str) -> Option<u16>,
    {
        let init_rank = if vertex_count > 0 {
            1.0 / vertex_count as f64
        } else {
            0.0
        };

        let rank = vec![init_rank; vertex_count];
        let next_rank = vec![0.0; vertex_count];
        let is_dangling: Vec<bool> = out_degrees.iter().map(|&d| d == 0).collect();

        let mut boundary_edges: HashMap<u32, Vec<(String, u16)>> = HashMap::new();
        for node in 0..vertex_count {
            for (dst_name, is_ghost, target_shard) in csr_out_edges(node as u32) {
                if is_ghost {
                    boundary_edges
                        .entry(node as u32)
                        .or_default()
                        .push((dst_name, target_shard));
                }
            }
        }

        Self {
            vertex_count,
            rank,
            next_rank,
            out_degrees,
            is_dangling,
            boundary_edges,
            incoming_contributions: HashMap::new(),
        }
    }

    /// Execute one superstep. Returns `(local_delta, local_dangling_sum,
    /// outbound_contributions)`.
    ///
    /// # Global dangling mass
    ///
    /// `global_dangling_sum` is the dangling-node rank mass aggregated by the
    /// coordinator from ALL shards' previous-superstep local sums (supplied via
    /// the `local_dangling_sum` field of each shard's return value). The base
    /// teleport term therefore uses the GLOBAL dangling mass rather than this
    /// shard's local dangling mass, so dangling-node rank redistributes across
    /// the WHOLE graph, not just the shard that owns the dangling node.
    ///
    /// On superstep 0 the coordinator passes `0.0` (no previous local sums exist
    /// yet) and the base collapses to the plain teleport `(1−d)/n` — identical
    /// to before, correct for a single-shard graph with no dangling nodes. The
    /// 1-superstep lag converges to the correct fixed point, exactly like the
    /// existing cross-shard contribution round-trip.
    ///
    /// The returned `local_dangling_sum` is this shard's dangling-node mass
    /// computed from `self.rank` BEFORE the rank swap; the coordinator sums these
    /// across shards into the next superstep's `global_dangling_sum`.
    ///
    /// # Incoming contributions
    ///
    /// Cross-shard contributions accumulated via [`Self::add_remote_contribution`]
    /// are folded into `next_rank` **after** the local scatter and **before** the
    /// rank swap. This ordering is mandatory for a stateless per-superstep round
    /// trip: applying them after the swap silently lands them in the *previous*
    /// iteration's rank vector.
    ///
    /// `node_id_to_local` maps an incoming contribution's destination vertex name
    /// to its local dense index; contributions whose target is not owned by this
    /// shard are dropped.
    ///
    /// # Personalization (Personalized PageRank)
    ///
    /// `personalization` is `None` for standard (uniform) PageRank and
    /// `Some(p)` for Personalized PageRank, where `p` is this shard's per-owned-node
    /// GLOBALLY-normalized seed share, positionally aligned with `self.rank` /
    /// `self.next_rank` (so `Σ_global p_i == 1.0` across ALL shards). When present,
    /// both the teleport mass and the dangling mass redistribute by `p` instead of
    /// uniformly — identical to single-node `build_personalization` semantics. The
    /// scalar `redistributed` total mass is the same in both branches; only HOW it
    /// is spread differs (uniform `/ n` vs. per-node `* p_i`).
    pub fn superstep(
        &mut self,
        damping: f64,
        global_n: usize,
        global_dangling_sum: f64,
        personalization: Option<&[f64]>,
        local_edge_iter: &dyn Fn(u32) -> Vec<u32>,
        node_id_to_local: &dyn Fn(&str) -> Option<u32>,
    ) -> (f64, f64, OutboundContributions) {
        let n = global_n as f64;

        // Compute THIS shard's local dangling mass from the PRE-SWAP rank vector
        // and return it so the coordinator can aggregate it for the NEXT superstep.
        let local_dangling_sum: f64 = self
            .rank
            .iter()
            .enumerate()
            .filter(|(i, _)| self.is_dangling[*i])
            .map(|(_, r)| r)
            .sum();

        // Total mass to redistribute this superstep (SCALAR, matches single-node):
        // the (1 - damping) teleport budget plus the damped GLOBAL dangling mass
        // (aggregated by the coordinator from all shards' previous-superstep local
        // sums). When there are no dangling nodes anywhere, `global_dangling_sum`
        // is 0.0 and `redistributed == 1 - damping`.
        let redistributed = (1.0 - damping) + damping * global_dangling_sum;

        match personalization {
            // Uniform: every node gets an equal share of the redistributed mass.
            None => {
                let base = redistributed / n;
                for r in self.next_rank.iter_mut() {
                    *r = base;
                }
            }
            // PPR: each owned node's base is its GLOBAL seed share of the
            // redistributed mass. Summed over all shards this injects exactly
            // `redistributed * Σ_global p_i == redistributed` (mass conserved).
            Some(p) => {
                for (slot, &pi) in self.next_rank.iter_mut().zip(p) {
                    *slot = redistributed * pi;
                }
            }
        }

        let mut outbound: HashMap<u16, Vec<(String, f64)>> = HashMap::new();
        for u in 0..self.vertex_count {
            let deg = self.out_degrees[u];
            if deg == 0 {
                continue;
            }
            let contrib = damping * self.rank[u] / deg as f64;

            // Scatter to local edges.
            for dst in local_edge_iter(u as u32) {
                self.next_rank[dst as usize] += contrib;
            }

            // Scatter to boundary edges (cross-shard).
            if let Some(boundary) = self.boundary_edges.get(&(u as u32)) {
                for (dst_name, target_shard) in boundary {
                    outbound
                        .entry(*target_shard)
                        .or_default()
                        .push((dst_name.clone(), contrib));
                }
            }
        }

        // Fold incoming cross-shard contributions into next_rank BEFORE the
        // swap so they land in the iteration being computed, not the prior one.
        for (vertex_name, contrib) in &self.incoming_contributions {
            if let Some(local_id) = node_id_to_local(vertex_name) {
                self.next_rank[local_id as usize] += contrib;
            }
        }

        let delta: f64 = self
            .rank
            .iter()
            .zip(self.next_rank.iter())
            .map(|(old, new)| (old - new).abs())
            .sum();

        std::mem::swap(&mut self.rank, &mut self.next_rank);
        self.incoming_contributions.clear();

        (delta, local_dangling_sum, outbound)
    }

    pub fn add_remote_contribution(&mut self, vertex_name: String, value: f64) {
        *self
            .incoming_contributions
            .entry(vertex_name)
            .or_insert(0.0) += value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_state_init() {
        let state = ShardPageRankState::init(3, vec![2, 1, 0], |_| None, &|_node| Vec::new());
        assert_eq!(state.vertex_count, 3);
        assert!(!state.is_dangling[0]);
        assert!(state.is_dangling[2]);
    }

    #[test]
    fn shard_state_with_ghost_edges() {
        let state = ShardPageRankState::init(
            2,
            vec![2, 1],
            |node| if node == "remote" { Some(5) } else { None },
            &|node| {
                if node == 0 {
                    vec![("remote".into(), true, 5)]
                } else {
                    Vec::new()
                }
            },
        );
        assert_eq!(state.boundary_edges.len(), 1);
        assert_eq!(state.boundary_edges[&0][0].1, 5);
    }

    #[test]
    fn shard_superstep_local_only() {
        let mut state = ShardPageRankState::init(3, vec![1, 1, 1], |_| None, &|_| Vec::new());
        // No dangling nodes in this ring, so global_dangling_sum = 0.0 — behaviour
        // is unchanged: base == teleport only, mass is conserved.
        let (delta, local_dangling_sum, outbound) = state.superstep(
            0.85,
            3,
            0.0,  // no dangling nodes anywhere
            None, // uniform PageRank
            &|node| match node {
                0 => vec![1],
                1 => vec![2],
                2 => vec![0],
                _ => Vec::new(),
            },
            &|_name| None,
        );
        assert!(outbound.is_empty());
        assert!(delta >= 0.0);
        // No dangling nodes → local_dangling_sum must be 0.0.
        assert_eq!(local_dangling_sum, 0.0);
        let sum: f64 = state.rank.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6);
    }

    #[test]
    fn global_dangling_sum_used_in_base() {
        // 2 vertices: vertex 0 has out-degree 1 (non-dangling), vertex 1 has
        // out-degree 0 (dangling). Simulate a cross-shard scenario by passing a
        // global_dangling_sum LARGER than this shard's own dangling mass — as if
        // additional dangling mass exists on other shards. Assert each node's
        // base reflects the GLOBAL value, not just the local shard's dangling sum.
        let n: usize = 10; // fictional global node count — larger than local 2
        let damping = 0.85_f64;
        // Local dangling mass = rank of vertex 1 = 1/2 (uniform init).
        // External dangling mass = 0.3 (simulated from other shards).
        let local_dangling_init = 1.0 / 2.0_f64;
        let extra_dangling = 0.3_f64;
        let global_dangling = local_dangling_init + extra_dangling;

        let mut state = ShardPageRankState::init(
            2,
            vec![1, 0], // vertex 1 is dangling
            |_| None,
            &|_| Vec::new(), // no boundary edges in this focused test
        );
        // Edge 0 → 1 (local, no boundary edges needed for this assertion).
        let (_delta, _local_dangling_sum, _outbound) = state.superstep(
            damping,
            n,
            global_dangling,
            None, // uniform PageRank
            &|node| if node == 0 { vec![1] } else { Vec::new() },
            &|_name| None,
        );

        // The base each node receives must be ((1-d) + d*global_dangling) / n.
        let expected_base = ((1.0 - damping) + damping * global_dangling) / n as f64;
        // Vertex 0: base + contribution from no in-edge in this pass
        //           (vertex 1 is dangling; no out-edges scatter to 0 locally).
        // Vertex 1: base + damping * rank[0] / 1 (edge 0→1).
        //
        // We assert the minimum: both nodes start with at least `expected_base`
        // in next_rank (the base is applied before any scatter). Since next_rank
        // after the swap is state.rank, and vertex 1 also got vertex 0's scatter,
        // we check vertex 0's rank == expected_base (it received no in-edge
        // scatter this superstep).
        let rank_v0 = state.rank[0];
        assert!(
            (rank_v0 - expected_base).abs() < 1e-12,
            "vertex 0 rank should equal base={expected_base:.15} (no in-edge scatter), got {rank_v0:.15}"
        );
    }

    #[test]
    fn personalized_superstep_biases_base_toward_seed() {
        // 3-node ring, no dangling nodes. A seed concentrated on vertex 0
        // (p = [1, 0, 0]) must give vertex 0 a strictly-larger teleport base than
        // its peers — the distributed analogue of single-node PPR biasing.
        let damping = 0.85_f64;
        let n: usize = 3;
        // Globally-normalized seed share: all mass on vertex 0.
        let p = [1.0_f64, 0.0, 0.0];

        let mut state = ShardPageRankState::init(3, vec![1, 1, 1], |_| None, &|_| Vec::new());
        let (_delta, local_dangling_sum, outbound) = state.superstep(
            damping,
            n,
            0.0, // no dangling nodes
            Some(&p),
            &|node| match node {
                0 => vec![1],
                1 => vec![2],
                2 => vec![0],
                _ => Vec::new(),
            },
            &|_name| None,
        );
        assert!(outbound.is_empty());
        assert_eq!(local_dangling_sum, 0.0);

        // redistributed = (1 - d) since dangling sum is 0. The seeded node's base
        // is `redistributed * 1.0`, peers' base is `redistributed * 0.0 == 0.0`,
        // before any scatter. After scatter, vertex 0 (seeded base + incoming from
        // vertex 2 which has base 0 → 0 contribution) must strictly outrank the
        // others.
        let redistributed = 1.0 - damping;
        // Vertex 0 receives base = redistributed*1.0 plus a scatter from vertex 2,
        // whose pre-swap rank was the uniform init 1/3 (init unchanged here).
        // Regardless, vertex 0's rank must exceed both peers'.
        assert!(
            state.rank[0] > state.rank[1] && state.rank[0] > state.rank[2],
            "seeded vertex 0 base must dominate: {:?}",
            state.rank
        );
        // The seeded node's base alone (before scatter) is `redistributed`.
        assert!(
            state.rank[0] >= redistributed - 1e-12,
            "seeded base should be at least redistributed={redistributed}, got {}",
            state.rank[0]
        );
    }

    #[test]
    fn remote_contribution_accumulation() {
        let mut state = ShardPageRankState::init(2, vec![1, 0], |_| None, &|_| Vec::new());
        state.add_remote_contribution("n0".into(), 0.1);
        state.add_remote_contribution("n0".into(), 0.2);
        state.add_remote_contribution("n1".into(), 0.3);
        assert!((state.incoming_contributions["n0"] - 0.3).abs() < 1e-10);
        assert!((state.incoming_contributions["n1"] - 0.3).abs() < 1e-10);
    }
}
