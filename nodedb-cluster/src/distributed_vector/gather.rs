// SPDX-License-Identifier: BUSL-1.1

//! Gather completeness for cross-shard k-NN search.
//!
//! Holds the proof-carrying merged ranking a coordinator hands out once every
//! shard has answered, plus the errors it returns while responses are missing,
//! duplicated, or from a shard that was never scattered to.

use std::time::Duration;

use thiserror::Error;

use super::merge::VectorHit;

/// Wait before a silent shard is reported as timed out rather than pending.
pub const DEFAULT_GATHER_TIMEOUT: Duration = Duration::from_secs(30);

/// A vector gather was read or fed in a state that breaks shard completeness.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum VectorGatherError {
    #[error(
        "vector gather incomplete: {responded} of {expected} shards responded, \
         missing shards {missing:?}"
    )]
    Incomplete {
        responded: usize,
        expected: usize,
        missing: Vec<u32>,
    },

    /// A second response from one shard would double-count its hits and make
    /// the gather read as complete while another shard is still silent.
    #[error("shard {vshard_id} answered this vector gather twice")]
    DuplicateResponse { vshard_id: u32 },

    #[error("shard {vshard_id} was never scattered to by this vector gather")]
    UnexpectedShard { vshard_id: u32 },
}

/// Global top-K hits across every shard.
///
/// Only [`crate::distributed_vector::VectorScatterGather::merge_top_k`]
/// constructs one, and it refuses while any shard is missing, so holding a
/// value is proof that every shard contributed. A `debug_assert!` could not
/// carry that proof: it is compiled out in release, where a short merge
/// returns a plausible ranking indistinguishable from a complete one.
#[derive(Debug, Clone)]
pub struct MergedTopK {
    hits: Vec<VectorHit>,
}

impl MergedTopK {
    pub(super) fn new(hits: Vec<VectorHit>) -> Self {
        Self { hits }
    }

    /// Merged hits, nearest first.
    pub fn hits(&self) -> &[VectorHit] {
        &self.hits
    }

    pub fn len(&self) -> usize {
        self.hits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }

    pub fn into_hits(self) -> Vec<VectorHit> {
        self.hits
    }
}
