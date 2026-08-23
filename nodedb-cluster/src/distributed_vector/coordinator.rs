// SPDX-License-Identifier: BUSL-1.1

//! Vector scatter-gather coordinator for cross-shard k-NN search.
//!
//! Same pattern as graph BSP and timeseries scatter-gather:
//! coordinator → VShardEnvelope per shard → collect responses → merge.
//!
//! The merged ranking is only reachable through [`VectorScatterGather::merge_top_k`],
//! which checks that every scattered-to shard answered before it builds a
//! [`MergedTopK`].

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use super::gather::{DEFAULT_GATHER_TIMEOUT, MergedTopK, VectorGatherError};
use super::merge::{ShardSearchResult, VectorMerger};
use crate::error::{ClusterError, Result};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Wire message for vector scatter request payload (zerompk).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct VectorScatterPayload {
    pub collection: String,
    pub query_vector: Vec<f32>,
    pub top_k: u32,
    pub ef_search: u32,
    pub has_filter: bool,
}

/// Scatter-gather coordinator for distributed k-NN vector search.
pub struct VectorScatterGather {
    /// Source node ID (this coordinator's node).
    pub source_node: u64,
    /// Target shard IDs to fan out to.
    pub shard_ids: Vec<u32>,
    /// Shards that have answered, deduplicated by ID.
    responded: BTreeSet<u32>,
    /// Merger collecting shard responses.
    merger: VectorMerger,
    /// When the scatter round started, for timeout reporting.
    started_at: Instant,
    /// How long a shard may stay silent before it is reported as timed out.
    gather_timeout: Duration,
}

impl VectorScatterGather {
    pub fn new(source_node: u64, shard_ids: Vec<u32>) -> Self {
        let count = shard_ids.len();
        Self {
            source_node,
            shard_ids,
            responded: BTreeSet::new(),
            merger: VectorMerger::new(count),
            started_at: Instant::now(),
            gather_timeout: DEFAULT_GATHER_TIMEOUT,
        }
    }

    /// Override how long a shard may stay silent before it is timed out.
    pub fn with_timeout(mut self, gather_timeout: Duration) -> Self {
        self.gather_timeout = gather_timeout;
        self
    }

    /// Build scatter envelopes for a k-NN search query.
    ///
    /// Returns one `VShardEnvelope` per shard, each carrying the query vector
    /// and parameters as a MessagePack payload.
    pub fn build_scatter_envelopes(
        &self,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<&[u8]>,
    ) -> Result<Vec<(u32, VShardEnvelope)>> {
        let msg = VectorScatterPayload {
            collection: collection.to_string(),
            query_vector: query_vector.to_vec(),
            top_k: top_k as u32,
            ef_search: ef_search as u32,
            has_filter: filter_bitmap.is_some(),
        };
        let mut payload_bytes = zerompk::to_msgpack_vec(&msg).map_err(|e| ClusterError::Codec {
            detail: format!("encoding VectorScatterPayload: {e}"),
        })?;

        // Filter bitmap rides after the MessagePack body, length-prefixed.
        if let Some(bitmap) = filter_bitmap {
            payload_bytes.extend_from_slice(&(bitmap.len() as u32).to_le_bytes());
            payload_bytes.extend_from_slice(bitmap);
        }

        Ok(self
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::VectorScatterRequest,
                    self.source_node,
                    0, // target_node resolved by routing table
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect())
    }

    /// Record a shard's response.
    ///
    /// Rejects a shard outside the scatter set and a second response from a
    /// shard that already answered: either would let the gather read as
    /// complete while another shard is still silent.
    pub fn record_response(&mut self, result: &ShardSearchResult) -> Result<()> {
        if !self.shard_ids.contains(&result.shard_id) {
            return Err(VectorGatherError::UnexpectedShard {
                vshard_id: result.shard_id,
            }
            .into());
        }
        if !self.responded.insert(result.shard_id) {
            return Err(VectorGatherError::DuplicateResponse {
                vshard_id: result.shard_id,
            }
            .into());
        }
        self.merger.add_shard_result(result);
        Ok(())
    }

    /// Whether all shards have responded.
    pub fn all_responded(&self) -> bool {
        self.missing_shards().is_empty()
    }

    /// Global top-K across every shard.
    ///
    /// Refuses while any shard is missing: a short merge is a wrong ranking
    /// that reads as a correct one, so it never leaves here as a plain hit
    /// list. A shard silent past the gather timeout is reported as
    /// [`ClusterError::ShardTimeout`] instead, naming the first missing shard.
    pub fn merge_top_k(&mut self, top_k: usize) -> Result<MergedTopK> {
        self.check_complete()?;
        Ok(MergedTopK::new(self.merger.top_k(top_k)))
    }

    /// Number of shards that have responded.
    pub fn response_count(&self) -> usize {
        self.responded.len()
    }

    /// Shards that were scattered to and have not answered.
    fn missing_shards(&self) -> Vec<u32> {
        self.shard_ids
            .iter()
            .copied()
            .filter(|id| !self.responded.contains(id))
            .collect()
    }

    fn check_complete(&self) -> Result<()> {
        let missing = self.missing_shards();
        let Some(&first_missing) = missing.first() else {
            return Ok(());
        };
        let elapsed = self.started_at.elapsed();
        if elapsed >= self.gather_timeout {
            return Err(ClusterError::ShardTimeout {
                vshard_id: first_missing,
                elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            });
        }
        Err(VectorGatherError::Incomplete {
            responded: self.responded.len(),
            expected: self.shard_ids.len(),
            missing,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_vector::merge::VectorHit;

    fn shard_result(shard_id: u32, hits: Vec<(u32, f32)>) -> ShardSearchResult {
        ShardSearchResult {
            shard_id,
            hits: hits
                .into_iter()
                .map(|(vector_id, distance)| VectorHit {
                    vector_id,
                    distance,
                    shard_id,
                    doc_id: None,
                })
                .collect(),
            success: true,
            error: None,
        }
    }

    #[test]
    fn scatter_envelopes_built() {
        let coord = VectorScatterGather::new(1, vec![0, 1, 2]);
        let query = vec![0.1f32, 0.2, 0.3];
        let envs = coord
            .build_scatter_envelopes("embeddings", &query, 10, 100, None)
            .expect("payload encodes");
        assert_eq!(envs.len(), 3);
        for (shard_id, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::VectorScatterRequest);
            assert_eq!(env.vshard_id, *shard_id);
            assert!(!env.payload.is_empty());
        }
    }

    #[test]
    fn scatter_with_filter() {
        let coord = VectorScatterGather::new(1, vec![0, 1]);
        let query = vec![1.0f32; 32];
        let filter = vec![0xFF_u8; 128];
        let envs = coord
            .build_scatter_envelopes("col", &query, 5, 50, Some(&filter))
            .expect("payload encodes");
        assert_eq!(envs.len(), 2);
        let no_filter = coord
            .build_scatter_envelopes("col", &query, 5, 50, None)
            .expect("payload encodes");
        assert!(envs[0].1.payload.len() > no_filter[0].1.payload.len());
    }

    #[test]
    fn merge_returns_global_top_k_once_every_shard_answered() {
        let mut coord = VectorScatterGather::new(1, vec![0, 1]);
        assert!(!coord.all_responded());

        coord
            .record_response(&shard_result(0, vec![(1, 0.1), (2, 0.5)]))
            .expect("shard 0 is in the scatter set");
        assert!(!coord.all_responded());

        coord
            .record_response(&shard_result(1, vec![(10, 0.05), (11, 0.3)]))
            .expect("shard 1 is in the scatter set");
        assert!(coord.all_responded());

        let merged = coord.merge_top_k(2).expect("every shard answered");
        assert_eq!(merged.len(), 2);
        assert_eq!(merged.hits()[0].vector_id, 10); // distance 0.05
        assert_eq!(merged.hits()[1].vector_id, 1); // distance 0.1
    }

    #[test]
    fn merge_refused_while_a_shard_is_silent() {
        let mut coord = VectorScatterGather::new(1, vec![0, 1]);
        coord
            .record_response(&shard_result(0, vec![(1, 0.1)]))
            .expect("shard 0 is in the scatter set");

        match coord.merge_top_k(2) {
            Err(ClusterError::VectorGather(VectorGatherError::Incomplete {
                responded,
                expected,
                missing,
            })) => {
                assert_eq!(responded, 1);
                assert_eq!(expected, 2);
                assert_eq!(missing, vec![1]);
            }
            other => panic!("expected an incomplete-gather error, got {other:?}"),
        }
    }

    #[test]
    fn silent_shard_past_deadline_reports_timeout() {
        let mut coord =
            VectorScatterGather::new(1, vec![0, 1, 2]).with_timeout(Duration::from_millis(0));
        coord
            .record_response(&shard_result(0, vec![(1, 0.1)]))
            .expect("shard 0 is in the scatter set");

        match coord.merge_top_k(2) {
            Err(ClusterError::ShardTimeout { vshard_id, .. }) => assert_eq!(vshard_id, 1),
            other => panic!("expected a shard timeout, got {other:?}"),
        }
    }

    #[test]
    fn second_response_from_one_shard_refused() {
        let mut coord = VectorScatterGather::new(1, vec![0, 1]);
        coord
            .record_response(&shard_result(0, vec![(1, 0.1)]))
            .expect("shard 0 is in the scatter set");

        match coord.record_response(&shard_result(0, vec![(2, 0.2)])) {
            Err(ClusterError::VectorGather(VectorGatherError::DuplicateResponse { vshard_id })) => {
                assert_eq!(vshard_id, 0)
            }
            other => panic!("expected a duplicate-response error, got {other:?}"),
        }
        assert!(!coord.all_responded());
    }

    #[test]
    fn response_from_unscattered_shard_refused() {
        let mut coord = VectorScatterGather::new(1, vec![0, 1]);
        match coord.record_response(&shard_result(7, vec![(1, 0.1)])) {
            Err(ClusterError::VectorGather(VectorGatherError::UnexpectedShard { vshard_id })) => {
                assert_eq!(vshard_id, 7)
            }
            other => panic!("expected an unexpected-shard error, got {other:?}"),
        }
        assert_eq!(coord.response_count(), 0);
    }
}
