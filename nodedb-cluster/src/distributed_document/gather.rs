// SPDX-License-Identifier: BUSL-1.1

//! Gather completeness for two-phase distributed BM25.
//!
//! Holds the proof-carrying aggregates a coordinator hands out once every
//! shard has reported — global corpus statistics for Phase 1, the merged
//! ranking for Phase 2 — plus the errors it returns while reports are
//! missing, duplicated, or from a shard that was never scattered to.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::bm25_global::ScoredHit;

/// Wait before a silent shard is reported as timed out rather than pending.
pub const DEFAULT_GATHER_TIMEOUT: Duration = Duration::from_secs(30);

/// A BM25 gather was read or fed in a state that breaks shard completeness.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Bm25GatherError {
    #[error(
        "global IDF incomplete: {reported} of {expected} shards reported document \
         frequencies, missing shards {missing:?}"
    )]
    DfReportsIncomplete {
        reported: usize,
        expected: usize,
        missing: Vec<u32>,
    },

    #[error(
        "scored hit merge incomplete: {responded} of {expected} shards returned \
         scored hits, missing shards {missing:?}"
    )]
    ScoredHitsIncomplete {
        responded: usize,
        expected: usize,
        missing: Vec<u32>,
    },

    /// A second DF report from one shard double-counts its documents and makes
    /// Phase 1 read as complete while another shard is still silent.
    #[error("shard {vshard_id} reported document frequencies twice")]
    DuplicateDfReport { vshard_id: u32 },

    #[error("shard {vshard_id} returned scored hits twice")]
    DuplicateScoredHits { vshard_id: u32 },

    #[error("shard {vshard_id} was never scattered to by this BM25 query")]
    UnexpectedShard { vshard_id: u32 },
}

/// Global IDF and avg_doc_len aggregated from every shard's DF report.
///
/// Fields are private and the only in-crate constructor is called by
/// [`crate::distributed_document::GlobalIdfCoordinator::compute_global_idf`]
/// after it has checked that every shard reported, so a locally built value is
/// proof that the whole corpus contributed. A `debug_assert!` could not carry
/// that proof: it is compiled out in release, where an IDF short of one shard
/// scores plausibly and is indistinguishable from a correct one.
///
/// Deserializing one is the Phase-2 shard side of the same proof: the
/// coordinator that serialized it had already established completeness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIdf {
    total_docs: u64,
    avg_doc_len: f64,
    term_idfs: HashMap<String, f64>,
}

impl GlobalIdf {
    pub(super) fn new(total_docs: u64, avg_doc_len: f64, term_idfs: HashMap<String, f64>) -> Self {
        Self {
            total_docs,
            avg_doc_len,
            term_idfs,
        }
    }

    /// Total documents across every shard.
    pub fn total_docs(&self) -> u64 {
        self.total_docs
    }

    /// Corpus-wide average document length.
    ///
    /// Shards use this instead of their local avg_doc_len for BM25.
    pub fn avg_doc_len(&self) -> f64 {
        self.avg_doc_len
    }

    /// Per-term IDF for every term of the query.
    pub fn term_idfs(&self) -> &HashMap<String, f64> {
        &self.term_idfs
    }

    /// IDF for one term, `None` when the term was not part of the query.
    pub fn idf(&self, term: &str) -> Option<f64> {
        self.term_idfs.get(term).copied()
    }
}

/// Global top-K by BM25 score across every shard.
///
/// Only [`crate::distributed_document::GlobalIdfCoordinator::merge_scored_hits`]
/// constructs one, and it refuses while any shard is missing, so holding a
/// value is proof that every shard contributed.
#[derive(Debug, Clone)]
pub struct MergedScoredHits {
    hits: Vec<ScoredHit>,
}

impl MergedScoredHits {
    pub(super) fn new(hits: Vec<ScoredHit>) -> Self {
        Self { hits }
    }

    /// Merged hits, highest BM25 score first.
    pub fn hits(&self) -> &[ScoredHit] {
        &self.hits
    }

    pub fn len(&self) -> usize {
        self.hits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }

    pub fn into_hits(self) -> Vec<ScoredHit> {
        self.hits
    }
}
