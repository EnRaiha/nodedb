// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane coordinator for distributed BSP PageRank (F1d-4 Phase B).
//!
//! Drives the `GraphOp::BspSuperstep` Phase A primitive across all shards: a
//! count phase to compute `global_n`, then a superstep loop with cross-shard
//! contribution routing and `BspCoordinator`-based convergence, assembling the
//! final ranks into the same `AlgoResultBatch` shape as single-node PageRank.

mod coord;
mod enumerate;
mod scatter;

pub use coord::run_bsp_pagerank;
