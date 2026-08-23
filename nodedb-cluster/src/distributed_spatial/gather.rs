// SPDX-License-Identifier: BUSL-1.1

//! Gather completeness for cross-shard spatial queries.
//!
//! Holds the proof-carrying merged hit set a coordinator hands out once every
//! shard has answered, plus the errors it returns while responses are missing,
//! duplicated, or from a shard that was never scattered to.

use std::time::Duration;

use thiserror::Error;

use super::merge::SpatialHit;

/// Wait before a silent shard is reported as timed out rather than pending.
pub const DEFAULT_GATHER_TIMEOUT: Duration = Duration::from_secs(30);

/// A spatial gather was read or fed in a state that breaks shard completeness.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SpatialGatherError {
    #[error(
        "spatial gather incomplete: {responded} of {expected} shards responded, \
         missing shards {missing:?}"
    )]
    Incomplete {
        responded: usize,
        expected: usize,
        missing: Vec<u32>,
    },

    /// A second response from one shard makes the gather read as complete
    /// while another shard is still silent.
    #[error("shard {vshard_id} answered this spatial gather twice")]
    DuplicateResponse { vshard_id: u32 },

    #[error("shard {vshard_id} was never scattered to by this spatial gather")]
    UnexpectedShard { vshard_id: u32 },
}

/// Merged spatial hits across every shard.
///
/// Only [`crate::distributed_spatial::SpatialScatterGather::merge_results`]
/// constructs one, and it refuses while any shard is missing, so holding a
/// value is proof that every shard contributed. A `debug_assert!` could not
/// carry that proof: it is compiled out in release, where a short merge
/// returns a plausible hit set indistinguishable from a complete one.
#[derive(Debug, Clone)]
pub struct MergedSpatialHits {
    hits: Vec<SpatialHit>,
}

impl MergedSpatialHits {
    pub(super) fn new(hits: Vec<SpatialHit>) -> Self {
        Self { hits }
    }

    /// Merged hits, nearest first when the query sorted by distance.
    pub fn hits(&self) -> &[SpatialHit] {
        &self.hits
    }

    pub fn len(&self) -> usize {
        self.hits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }

    pub fn into_hits(self) -> Vec<SpatialHit> {
        self.hits
    }
}
