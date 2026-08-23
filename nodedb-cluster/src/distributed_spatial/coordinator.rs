// SPDX-License-Identifier: BUSL-1.1

//! Spatial scatter-gather coordinator for cross-shard spatial queries.
//!
//! Same pattern as vector/timeseries distributed queries:
//! coordinator → VShardEnvelope per shard → collect responses → merge.
//!
//! The merged hit set is only reachable through
//! [`SpatialScatterGather::merge_results`], which checks that every
//! scattered-to shard answered before it builds a [`MergedSpatialHits`].

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use super::gather::{DEFAULT_GATHER_TIMEOUT, MergedSpatialHits, SpatialGatherError};
use super::merge::{ShardSpatialResult, SpatialResultMerger};
use crate::error::{ClusterError, Result};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Wire message for spatial scatter request payload (zerompk).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SpatialScatterPayload {
    pub collection: String,
    pub field: String,
    pub predicate: String,
    /// Typed query geometry, parsed and validated on the originating CP.
    pub query_geometry: nodedb_types::geometry::Geometry,
    pub distance_meters: f64,
    pub limit: u32,
}

/// Scatter-gather coordinator for distributed spatial queries.
pub struct SpatialScatterGather {
    pub source_node: u64,
    pub shard_ids: Vec<u32>,
    /// Shards that have answered, deduplicated by ID.
    responded: BTreeSet<u32>,
    merger: SpatialResultMerger,
    /// When the scatter round started, for timeout reporting.
    started_at: Instant,
    /// How long a shard may stay silent before it is reported as timed out.
    gather_timeout: Duration,
}

impl SpatialScatterGather {
    pub fn new(source_node: u64, shard_ids: Vec<u32>) -> Self {
        let count = shard_ids.len();
        Self {
            source_node,
            shard_ids,
            responded: BTreeSet::new(),
            merger: SpatialResultMerger::new(count),
            started_at: Instant::now(),
            gather_timeout: DEFAULT_GATHER_TIMEOUT,
        }
    }

    /// Override how long a shard may stay silent before it is timed out.
    pub fn with_timeout(mut self, gather_timeout: Duration) -> Self {
        self.gather_timeout = gather_timeout;
        self
    }

    /// Build scatter envelopes for a spatial query.
    ///
    /// Each envelope carries the query geometry, predicate, and distance as a
    /// MessagePack payload.
    pub fn build_scatter_envelopes(
        &self,
        collection: &str,
        field: &str,
        predicate: &str,
        query_geometry: nodedb_types::geometry::Geometry,
        distance_meters: f64,
        limit: usize,
    ) -> Result<Vec<(u32, VShardEnvelope)>> {
        let msg = SpatialScatterPayload {
            collection: collection.to_string(),
            field: field.to_string(),
            predicate: predicate.to_string(),
            query_geometry,
            distance_meters,
            limit: limit as u32,
        };
        let payload_bytes = zerompk::to_msgpack_vec(&msg).map_err(|e| ClusterError::Codec {
            detail: format!("encoding SpatialScatterPayload: {e}"),
        })?;

        Ok(self
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::SpatialScatterRequest,
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
    pub fn record_response(&mut self, result: &ShardSpatialResult) -> Result<()> {
        if !self.shard_ids.contains(&result.shard_id) {
            return Err(SpatialGatherError::UnexpectedShard {
                vshard_id: result.shard_id,
            }
            .into());
        }
        if !self.responded.insert(result.shard_id) {
            return Err(SpatialGatherError::DuplicateResponse {
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

    /// Merged hits across every shard.
    ///
    /// Refuses while any shard is missing: a short merge silently drops a
    /// region of the query extent while reading as a complete answer. A shard
    /// silent past the gather timeout is reported as
    /// [`ClusterError::ShardTimeout`] instead, naming the first missing shard.
    pub fn merge_results(
        &mut self,
        limit: usize,
        sort_by_distance: bool,
    ) -> Result<MergedSpatialHits> {
        self.check_complete()?;
        Ok(MergedSpatialHits::new(
            self.merger.merge(limit, sort_by_distance),
        ))
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
        Err(SpatialGatherError::Incomplete {
            responded: self.responded.len(),
            expected: self.shard_ids.len(),
            missing,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::super::merge::SpatialHit;
    use super::*;

    fn shard_result(shard_id: u32, hits: Vec<(&str, f64)>) -> ShardSpatialResult {
        ShardSpatialResult {
            shard_id,
            hits: hits
                .into_iter()
                .map(|(doc_id, distance_meters)| SpatialHit {
                    doc_id: doc_id.to_string(),
                    shard_id,
                    distance_meters,
                })
                .collect(),
            success: true,
            error: None,
        }
    }

    #[test]
    fn scatter_envelopes_built() {
        let coord = SpatialScatterGather::new(1, vec![0, 1, 2]);
        let envs = coord
            .build_scatter_envelopes(
                "buildings",
                "geom",
                "st_dwithin",
                nodedb_types::geometry::Geometry::point(0.0, 0.0),
                1000.0,
                100,
            )
            .expect("payload encodes");
        assert_eq!(envs.len(), 3);
        for (shard_id, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::SpatialScatterRequest);
            assert_eq!(env.vshard_id, *shard_id);
        }
    }

    #[test]
    fn merge_returns_every_shard_hit_once_all_answered() {
        let mut coord = SpatialScatterGather::new(1, vec![0, 1]);
        coord
            .record_response(&shard_result(0, vec![("a", 200.0)]))
            .expect("shard 0 is in the scatter set");
        coord
            .record_response(&shard_result(1, vec![("b", 50.0)]))
            .expect("shard 1 is in the scatter set");
        assert!(coord.all_responded());

        let merged = coord.merge_results(10, true).expect("every shard answered");
        assert_eq!(merged.len(), 2);
        assert_eq!(merged.hits()[0].doc_id, "b"); // nearer
    }

    #[test]
    fn merge_refused_while_a_shard_is_silent() {
        let mut coord = SpatialScatterGather::new(1, vec![0, 1]);
        coord
            .record_response(&shard_result(0, vec![("a", 200.0)]))
            .expect("shard 0 is in the scatter set");

        match coord.merge_results(10, true) {
            Err(ClusterError::SpatialGather(SpatialGatherError::Incomplete {
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
            SpatialScatterGather::new(1, vec![0, 1, 2]).with_timeout(Duration::from_millis(0));
        coord
            .record_response(&shard_result(0, vec![("a", 200.0)]))
            .expect("shard 0 is in the scatter set");

        match coord.merge_results(10, true) {
            Err(ClusterError::ShardTimeout { vshard_id, .. }) => assert_eq!(vshard_id, 1),
            other => panic!("expected a shard timeout, got {other:?}"),
        }
    }

    #[test]
    fn second_response_from_one_shard_refused() {
        let mut coord = SpatialScatterGather::new(1, vec![0, 1]);
        coord
            .record_response(&shard_result(0, vec![("a", 200.0)]))
            .expect("shard 0 is in the scatter set");

        match coord.record_response(&shard_result(0, vec![("c", 10.0)])) {
            Err(ClusterError::SpatialGather(SpatialGatherError::DuplicateResponse {
                vshard_id,
            })) => assert_eq!(vshard_id, 0),
            other => panic!("expected a duplicate-response error, got {other:?}"),
        }
        assert!(!coord.all_responded());
    }

    #[test]
    fn response_from_unscattered_shard_refused() {
        let mut coord = SpatialScatterGather::new(1, vec![0, 1]);
        match coord.record_response(&shard_result(9, vec![("z", 1.0)])) {
            Err(ClusterError::SpatialGather(SpatialGatherError::UnexpectedShard { vshard_id })) => {
                assert_eq!(vshard_id, 9)
            }
            other => panic!("expected an unexpected-shard error, got {other:?}"),
        }
        assert_eq!(coord.response_count(), 0);
    }
}
