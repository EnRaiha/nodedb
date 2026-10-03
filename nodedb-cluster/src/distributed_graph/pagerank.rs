// SPDX-License-Identifier: BUSL-1.1

//! Per-shard PageRank execution state for distributed BSP.
//!
//! One BSP iteration is one power iteration of single-node PageRank: every
//! new rank is computed from one rank vector. A shard's cross-shard
//! contributions therefore cross the barrier before they are applied:
//!
//! 1. [`ShardPageRankState::scatter`] computes this shard's dangling mass and
//!    its contributions to other shards' vertices from the current rank.
//! 2. The coordinator routes them to the owning shards.
//! 3. [`ShardPageRankState::update`] computes the next rank from the same
//!    current rank: the redistributed base, the local contributions, and the
//!    routed contributions.
//!
//! No rank mass is in flight when a run halts, so the ranks always sum to
//! 1.0.

use std::collections::HashMap;

use super::barrier::BspBarrierError;

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

/// The inputs of one [`ShardPageRankState::update`].
pub struct PageRankUpdate<'a> {
    pub damping: f64,
    /// The vertex count of the whole graph.
    pub global_n: usize,
    /// The dangling mass of the current rank over the whole graph: the sum
    /// of every shard's `scatter` dangling mass.
    pub global_dangling_sum: f64,
    /// `None` for uniform PageRank. `Some(p)` for Personalized PageRank:
    /// this shard's globally normalized seed share per owned vertex,
    /// aligned with `rank`. Both the teleport mass and the dangling mass
    /// spread by `p`, as single-node `build_personalization` spreads them.
    pub personalization: Option<&'a [f64]>,
    /// Owned vertex index to its owned destination indices.
    pub local_edge_iter: &'a dyn Fn(u32) -> Vec<u32>,
    /// Vertex name to its owned vertex index.
    pub node_id_to_local: &'a dyn Fn(&str) -> Option<u32>,
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

    /// This shard's dangling mass and its cross-shard contributions, both
    /// from the current rank. Returns `(local_dangling_sum, outbound)`.
    ///
    /// The coordinator sums every shard's dangling mass into the next
    /// update's `global_dangling_sum`, and routes each contribution to the
    /// shard that owns its destination.
    pub fn scatter(&self, damping: f64) -> (f64, OutboundContributions) {
        let local_dangling_sum: f64 = self
            .rank
            .iter()
            .zip(&self.is_dangling)
            .filter(|(_, dangling)| **dangling)
            .map(|(rank, _)| rank)
            .sum();

        let mut outbound: OutboundContributions = HashMap::new();
        for (u, boundary) in &self.boundary_edges {
            let u = *u as usize;
            let deg = self.out_degrees[u];
            if deg == 0 {
                continue;
            }
            let contrib = damping * self.rank[u] / deg as f64;
            for (dst_name, target_shard) in boundary {
                outbound
                    .entry(*target_shard)
                    .or_default()
                    .push((dst_name.clone(), contrib));
            }
        }
        (local_dangling_sum, outbound)
    }

    /// Replace the current rank with the next one and return the L1 delta
    /// between them.
    ///
    /// The next rank is the redistributed base, plus every local
    /// contribution from the current rank, plus every contribution added by
    /// [`Self::add_remote_contribution`]. Those came from the other shards'
    /// `scatter` of the same iteration's rank, so the step is one power
    /// iteration of the whole graph.
    ///
    /// A contribution to a vertex this shard does not own is an error: its
    /// mass would leave the graph.
    pub fn update(&mut self, step: PageRankUpdate<'_>) -> Result<f64, BspBarrierError> {
        let redistributed = (1.0 - step.damping) + step.damping * step.global_dangling_sum;

        match step.personalization {
            None => {
                let base = redistributed / step.global_n as f64;
                for r in self.next_rank.iter_mut() {
                    *r = base;
                }
            }
            Some(p) => {
                for (slot, &pi) in self.next_rank.iter_mut().zip(p) {
                    *slot = redistributed * pi;
                }
            }
        }

        for u in 0..self.vertex_count {
            let deg = self.out_degrees[u];
            if deg == 0 {
                continue;
            }
            let contrib = step.damping * self.rank[u] / deg as f64;
            for dst in (step.local_edge_iter)(u as u32) {
                self.next_rank[dst as usize] += contrib;
            }
        }

        for (vertex, contrib) in self.incoming_contributions.drain() {
            let Some(local_id) = (step.node_id_to_local)(&vertex) else {
                return Err(BspBarrierError::UnownedContribution {
                    algorithm: "pagerank".to_owned(),
                    vertex,
                });
            };
            self.next_rank[local_id as usize] += contrib;
        }

        let delta: f64 = self
            .rank
            .iter()
            .zip(self.next_rank.iter())
            .map(|(old, new)| (old - new).abs())
            .sum();

        std::mem::swap(&mut self.rank, &mut self.next_rank);
        Ok(delta)
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

    fn ring3(node: u32) -> Vec<u32> {
        match node {
            0 => vec![1],
            1 => vec![2],
            2 => vec![0],
            _ => Vec::new(),
        }
    }

    fn no_remote(_name: &str) -> Option<u32> {
        None
    }

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
    fn shard_update_local_only() {
        let mut state = ShardPageRankState::init(3, vec![1, 1, 1], |_| None, &|_| Vec::new());
        let (local_dangling_sum, outbound) = state.scatter(0.85);
        assert!(outbound.is_empty());
        assert_eq!(local_dangling_sum, 0.0);
        let delta = state
            .update(PageRankUpdate {
                damping: 0.85,
                global_n: 3,
                global_dangling_sum: 0.0,
                personalization: None,
                local_edge_iter: &ring3,
                node_id_to_local: &no_remote,
            })
            .expect("update");
        assert!(delta >= 0.0);
        let sum: f64 = state.rank.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6);
    }

    #[test]
    fn global_dangling_sum_used_in_base() {
        // Vertex 0 has out-degree 1, vertex 1 is dangling. The global dangling
        // mass is larger than this shard's own, as if other shards hold more.
        let n: usize = 10;
        let damping = 0.85_f64;
        let global_dangling = 0.5 + 0.3;

        let mut state = ShardPageRankState::init(2, vec![1, 0], |_| None, &|_| Vec::new());
        state
            .update(PageRankUpdate {
                damping,
                global_n: n,
                global_dangling_sum: global_dangling,
                personalization: None,
                local_edge_iter: &|node| if node == 0 { vec![1] } else { Vec::new() },
                node_id_to_local: &no_remote,
            })
            .expect("update");

        // Vertex 0 has no in-edge, so its rank is the base alone.
        let expected_base = ((1.0 - damping) + damping * global_dangling) / n as f64;
        assert!((state.rank[0] - expected_base).abs() < 1e-12);
    }

    #[test]
    fn personalized_update_biases_base_toward_seed() {
        let damping = 0.85_f64;
        let p = [1.0_f64, 0.0, 0.0];
        let mut state = ShardPageRankState::init(3, vec![1, 1, 1], |_| None, &|_| Vec::new());
        state
            .update(PageRankUpdate {
                damping,
                global_n: 3,
                global_dangling_sum: 0.0,
                personalization: Some(&p),
                local_edge_iter: &ring3,
                node_id_to_local: &no_remote,
            })
            .expect("update");
        let redistributed = 1.0 - damping;
        assert!(state.rank[0] > state.rank[1] && state.rank[0] > state.rank[2]);
        assert!(state.rank[0] >= redistributed - 1e-12);
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

    /// A contribution to a vertex the shard does not own is refused.
    #[test]
    fn an_unowned_contribution_is_an_error() {
        let mut state = ShardPageRankState::init(1, vec![0], |_| None, &|_| Vec::new());
        state.add_remote_contribution("elsewhere".into(), 0.1);
        let result = state.update(PageRankUpdate {
            damping: 0.85,
            global_n: 2,
            global_dangling_sum: 0.0,
            personalization: None,
            local_edge_iter: &|_| Vec::new(),
            node_id_to_local: &no_remote,
        });
        assert!(matches!(
            result,
            Err(BspBarrierError::UnownedContribution { .. })
        ));
    }

    /// A 4-ring split over two shards, every edge crossing shards, keeps a
    /// total rank of 1.0 after every iteration and stays uniform: no mass is
    /// in flight between iterations.
    #[test]
    fn a_cross_shard_ring_conserves_mass_every_iteration() {
        // Shard A owns r0, r2. Shard B owns r1, r3. Ring r0→r1→r2→r3→r0.
        let names = [["r0", "r2"], ["r1", "r3"]];
        let succ = |name: &str| -> &'static str {
            match name {
                "r0" => "r1",
                "r1" => "r2",
                "r2" => "r3",
                _ => "r0",
            }
        };
        let mut shards: Vec<ShardPageRankState> = names
            .iter()
            .enumerate()
            .map(|(shard, owned)| {
                let edges = |i: u32| {
                    vec![(
                        succ(owned[i as usize]).to_string(),
                        true,
                        (1 - shard) as u16,
                    )]
                };
                let mut state = ShardPageRankState::init(2, vec![1, 1], |_| None, &edges);
                state.rank = vec![0.25, 0.25];
                state
            })
            .collect();
        for _ in 0..30 {
            let mut dangling = 0.0;
            let mut routed: Vec<(usize, String, f64)> = Vec::new();
            for state in &shards {
                let (d, outbound) = state.scatter(0.85);
                dangling += d;
                for (target, contribs) in outbound {
                    for (name, c) in contribs {
                        routed.push((target as usize, name, c));
                    }
                }
            }
            for (target, name, c) in routed {
                shards[target].add_remote_contribution(name, c);
            }
            for (shard, state) in shards.iter_mut().enumerate() {
                let owned = names[shard];
                state
                    .update(PageRankUpdate {
                        damping: 0.85,
                        global_n: 4,
                        global_dangling_sum: dangling,
                        personalization: None,
                        local_edge_iter: &|_| Vec::new(),
                        node_id_to_local: &|name| {
                            owned.iter().position(|n| *n == name).map(|i| i as u32)
                        },
                    })
                    .expect("every contribution is owned");
            }
            let total: f64 = shards.iter().flat_map(|s| s.rank.iter()).sum();
            assert!((total - 1.0).abs() < 1e-12, "mass conserved, got {total}");
            for rank in shards.iter().flat_map(|s| s.rank.iter()) {
                assert!((rank - 0.25).abs() < 1e-12, "ring stays uniform");
            }
        }
    }
}
