// SPDX-License-Identifier: BUSL-1.1

//! Gather completeness for cross-shard timeseries aggregation.
//!
//! Holds the proof-carrying merged partials a coordinator hands out once
//! every shard has answered, plus the errors it returns while responses are
//! missing, duplicated, or from a shard that was never scattered to.

use std::time::Duration;

use thiserror::Error;

use super::merge::PartialAgg;

/// Wait before a silent shard is reported as timed out rather than pending.
pub const DEFAULT_GATHER_TIMEOUT: Duration = Duration::from_secs(30);

/// A timeseries gather was read or fed in a state that breaks shard completeness.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TsGatherError {
    #[error(
        "timeseries gather incomplete: {responded} of {expected} shards responded, \
         missing shards {missing:?}"
    )]
    Incomplete {
        responded: usize,
        expected: usize,
        missing: Vec<u32>,
    },

    /// A second response from one shard would double-count its partials and
    /// make the gather read as complete while another shard is still silent.
    #[error("shard {vshard_id} answered this timeseries gather twice")]
    DuplicateResponse { vshard_id: u32 },

    #[error("shard {vshard_id} was never scattered to by this timeseries gather")]
    UnexpectedShard { vshard_id: u32 },
}

/// Merged partial aggregates across every shard, bucketed by time.
///
/// Only [`crate::distributed_timeseries::TsCoordinator::merge_results`]
/// constructs one, and it refuses while any shard is missing, so holding a
/// value is proof that every shard contributed. A `debug_assert!` could not
/// carry that proof: it is compiled out in release, where a SUM or COUNT
/// short of one shard is type-identical to a correct one.
#[derive(Debug, Clone)]
pub struct MergedPartials {
    partials: Vec<PartialAgg>,
}

impl MergedPartials {
    pub(super) fn new(partials: Vec<PartialAgg>) -> Self {
        Self { partials }
    }

    /// Merged partial aggregates, ordered by bucket timestamp.
    pub fn partials(&self) -> &[PartialAgg] {
        &self.partials
    }

    pub fn len(&self) -> usize {
        self.partials.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partials.is_empty()
    }

    pub fn into_partials(self) -> Vec<PartialAgg> {
        self.partials
    }
}
