// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane coordinator for distributed WCC (single-round contraction).
//!
//! Dispatches one `GraphOp::WccSuperstep` per owner node, then stitches every
//! shard's local components + boundary edges into one global union-find over
//! node names, assembling the result into the same `AlgoResultBatch` shape as
//! single-node WCC.

mod coord;
mod scatter;

pub use coord::run_bsp_wcc;
