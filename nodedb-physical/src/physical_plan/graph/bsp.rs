// SPDX-License-Identifier: Apache-2.0

//! Boxed payload/result pair for [`super::op::GraphOp::BspSuperstep`] — the
//! distributed-PageRank BSP superstep primitive.

use nodedb_graph::{AlgoParams, GraphAlgorithm};

/// Boxed payload of [`super::op::GraphOp::BspSuperstep`] — all per-superstep inputs.
///
/// Kept out-of-line (the variant holds a `Box`) so the large param + vector
/// fields don't bloat `PhysicalPlan`, which is cloned/moved on every request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BspSuperstepPlan {
    /// Algorithm selector. Only `PageRank` is supported in Phase A; other
    /// variants surface a typed `Unsupported` error from the handler.
    pub algorithm: GraphAlgorithm,
    /// Algorithm parameters. Carries the target `collection` (mirroring `Algo`)
    /// plus `damping`.
    pub params: AlgoParams,
    /// Zero-based superstep index. `0` triggers `1/global_n` initialization.
    pub superstep: u32,
    /// Total OWNED nodes across all shards (Control-Plane computed). Used as the
    /// PageRank `n` in the teleport / dangling redistribution terms.
    ///
    /// `global_n == 0` is the COUNT-ONLY sentinel: the coordinator dispatches one
    /// superstep with `global_n = 0` (and empty `rank_seed` / `incoming_contributions`)
    /// to every shard BEFORE superstep 0 so it can sum each shard's owned
    /// `vertex_count` into the real `global_n`. On that sentinel the handler
    /// short-circuits after building the owned-node set and runs NO superstep —
    /// it returns only `vertex_count` + `node_names`. Every real superstep
    /// (`superstep >= 0` of the actual run) passes `global_n > 0`.
    pub global_n: usize,
    /// The vShards this shard owns (Control-Plane supplied). A destination node
    /// whose `VShardId::from_key(name)` is not in this set is a ghost
    /// (cross-shard) edge target and its contribution is emitted in `outbound`
    /// rather than scattered locally.
    pub owned_vshards: Vec<u32>,
    /// Cross-shard contributions routed to THIS shard's owned nodes for this
    /// superstep: `(dst_node_name, contribution)`.
    pub incoming_contributions: Vec<(String, f64)>,
    /// Round-tripped per-shard rank seed as `(node_name, rank)` pairs (name-keyed,
    /// NOT positional) so the same plan can be fanned across a node's cores and
    /// each core self-filters to its owned nodes by name. EMPTY on superstep 0 →
    /// the handler initializes every owned node to `1/global_n`. A node absent from
    /// the seed also falls back to `1/global_n`.
    pub rank_seed: Vec<(String, f64)>,
    /// Global dangling-node rank mass aggregated by the coordinator from the
    /// PREVIOUS superstep across all shards; used for the teleport base so dangling
    /// mass redistributes across the WHOLE graph, not just this shard.
    ///
    /// `0.0` on superstep 0 and the count phase: no previous local sums exist yet,
    /// so the base collapses to the plain teleport `(1−d)/n` — identical to a
    /// non-dangling graph and correct for initialization.
    pub global_dangling: f64,
    /// Coordinator-computed GLOBAL `Σ max(w, 0.0)` over the Personalized-PageRank
    /// seed map (`params.personalization_vector`), summed across the WHOLE cluster.
    ///
    /// `0.0` means standard (uniform) PageRank — no personalization is active,
    /// either because no seed map was supplied, the summed weight was ≤ 0, or no
    /// seed name exists anywhere in the cluster graph (matching single-node
    /// `build_personalization` returning `None`). A value `> 0.0` activates
    /// Personalized PageRank on every shard.
    ///
    /// Each shard divides its OWNED nodes' raw seed weights by this GLOBAL sum to
    /// get a globally-normalized seed share `p_i` (`Σ_global p_i == 1.0`). Both the
    /// teleport mass and the dangling mass then redistribute by `p` instead of
    /// uniformly. Normalizing by the cluster-wide sum (never a per-shard sum) is
    /// what preserves the mass-conservation invariant across shards.
    pub personalization_sum: f64,
}

/// Result of one [`super::op::GraphOp::BspSuperstep`] on a single shard.
///
/// `rank_vec` and `node_names` are positionally aligned: `rank_vec[i]` is the
/// post-superstep PageRank of the owned node `node_names[i]`. The Control-Plane
/// coordinator (Phase B) round-trips `rank_vec` back into the next superstep's
/// `GraphOp::BspSuperstep::rank_vec` and uses `node_names` to map indices back
/// to node identities for final assembly and for routing `outbound`
/// contributions to the owning shard. `node_names` is returned on every
/// superstep (it is cheap and keeps the op stateless).
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BspSuperstepResult {
    /// Sum of `|rank_old - rank_new|` over this shard's owned nodes — the
    /// shard's contribution to the global convergence delta.
    pub local_delta: f64,
    /// Cross-shard contributions to scatter to other shards next superstep:
    /// `(target_vshard, dst_node_name, contribution)`.
    pub outbound: Vec<(u32, String, f64)>,
    /// Post-superstep rank vector over this shard's owned nodes, aligned with
    /// `node_names`.
    pub rank_vec: Vec<f64>,
    /// Number of owned nodes on this shard (== `rank_vec.len()`).
    pub vertex_count: usize,
    /// Owned-node names, positionally aligned with `rank_vec`.
    pub node_names: Vec<String>,
    /// This shard's dangling-node rank mass this superstep (sum of `rank` for all
    /// owned nodes with out-degree 0, computed BEFORE the rank swap). The
    /// coordinator sums these across shards into the next superstep's
    /// `global_dangling` field so dangling mass redistributes globally.
    pub dangling_sum: f64,
    /// Number of this shard's OWNED nodes that appear as a positively-weighted key
    /// in the Personalized-PageRank seed map (`params.personalization_vector`),
    /// reported by the COUNT phase (alongside `vertex_count`). The coordinator sums
    /// these across shards: a cluster-wide total of `0` means no seed name exists
    /// anywhere in the graph, so personalization falls back to uniform PageRank
    /// (matching single-node `build_personalization` returning `None`). `0` on
    /// every real superstep (only the count phase populates it).
    pub seed_hits: usize,
}
