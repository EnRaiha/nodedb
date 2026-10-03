// SPDX-License-Identifier: BUSL-1.1

//! GraphRAG fusion in a cluster, run in stages from a coordinator.
//!
//! See `coord` for the stages and `stages` for the dispatches they make.

pub mod coord;
pub mod stages;

pub use coord::serve_rag_plan;
