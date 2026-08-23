// SPDX-License-Identifier: BUSL-1.1

//! Superstep barrier completeness for distributed BSP runs.
//!
//! Holds the proof-carrying aggregate a coordinator hands out once every shard
//! has ACKed, plus the error it returns while the barrier is still open.

use thiserror::Error;

/// A superstep aggregate was read while shards were still missing.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BspBarrierError {
    #[error(
        "superstep barrier incomplete for '{algorithm}' at iteration {iteration}: \
         {acked} of {expected} shards ACKed"
    )]
    Incomplete {
        algorithm: String,
        iteration: u32,
        acked: usize,
        expected: usize,
    },
}

/// Cluster-wide totals for one superstep.
///
/// Only [`crate::distributed_graph::BspCoordinator::totals`] constructs one, and
/// it refuses while any shard is missing, so holding a value is proof that every
/// shard contributed. A `debug_assert!` could not carry that proof: it is
/// compiled out in release, where a partial read returned a plausible number
/// indistinguishable from a complete one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SuperstepTotals {
    global_delta: f64,
    total_vertices: usize,
}

impl SuperstepTotals {
    pub(crate) fn new(global_delta: f64, total_vertices: usize) -> Self {
        Self {
            global_delta,
            total_vertices,
        }
    }

    /// Sum of every shard's convergence delta.
    pub fn global_delta(&self) -> f64 {
        self.global_delta
    }

    /// Vertex count summed across every shard.
    pub fn total_vertices(&self) -> usize {
        self.total_vertices
    }
}
