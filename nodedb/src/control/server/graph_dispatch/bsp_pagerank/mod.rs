// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane coordinator for distributed BSP PageRank.
//!
//! Drives the `GraphOp::BspSuperstep` primitive across all shards: a count
//! phase to compute `global_n`, an initial scatter, then a superstep loop
//! with cross-shard contribution routing and `BspCoordinator`-based
//! convergence. It assembles the final ranks into the same `AlgoResultBatch`
//! shape as single-node PageRank.

mod coord;
/// Shard enumeration is shared with the distributed-WCC coordinator
/// (`bsp_wcc`): both need one dispatch per distinct owner node carrying that
/// node's full owned-vShard set. Exposed within `graph_dispatch` only.
pub(in crate::control::server::graph_dispatch) mod enumerate;
mod scatter;

pub use coord::run_bsp_pagerank;
