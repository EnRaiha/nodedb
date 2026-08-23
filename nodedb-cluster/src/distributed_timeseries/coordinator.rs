// SPDX-License-Identifier: BUSL-1.1

//! Timeseries scatter-gather coordinator.
//!
//! Runs on the Control Plane. Dispatches aggregation queries to all shards
//! via `VShardEnvelope`, collects partial aggregates, and merges them.
//!
//! Follows the same pattern as `distributed_vector::VectorScatterGather` and
//! `distributed_spatial::SpatialScatterGather`: coordinator → VShardEnvelope
//! per shard → collect responses → merge. The merged result is only
//! reachable through [`TsCoordinator::merge_results`], which checks that
//! every scattered-to shard answered before it builds a [`MergedPartials`].

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use super::gather::{DEFAULT_GATHER_TIMEOUT, MergedPartials, TsGatherError};
use super::merge::{PartialAgg, PartialAggMerger};
use super::retention::RetentionCommand;
use crate::error::{ClusterError, Result};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Wire message for timeseries scatter request payload (zerompk).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TsScatterPayload {
    pub collection: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub value_column: String,
    pub bucket_interval_ms: i64,
}

/// Wire message for S3 archive command payload (zerompk).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TsArchivePayload {
    pub collection: String,
    pub archive_before_ts: i64,
    pub s3_prefix: String,
}

/// Scatter-gather coordinator for cross-shard timeseries aggregation.
pub struct TsCoordinator {
    /// Source node ID (this coordinator's node).
    pub source_node: u64,
    /// Target shard IDs to fan out to.
    pub shard_ids: Vec<u32>,
    /// Shards that have answered, deduplicated by ID.
    responded: BTreeSet<u32>,
    /// Merger collecting shard responses.
    merger: PartialAggMerger,
    /// When the scatter round started, for timeout reporting.
    started_at: Instant,
    /// How long a shard may stay silent before it is reported as timed out.
    gather_timeout: Duration,
}

impl TsCoordinator {
    pub fn new(source_node: u64, shard_ids: Vec<u32>) -> Self {
        Self {
            source_node,
            shard_ids,
            responded: BTreeSet::new(),
            merger: PartialAggMerger::new(),
            started_at: Instant::now(),
            gather_timeout: DEFAULT_GATHER_TIMEOUT,
        }
    }

    /// Override how long a shard may stay silent before it is timed out.
    pub fn with_timeout(mut self, gather_timeout: Duration) -> Self {
        self.gather_timeout = gather_timeout;
        self
    }

    /// Build scatter envelopes for a timeseries aggregation query.
    ///
    /// Returns one `VShardEnvelope` per shard, each containing the query
    /// parameters as a MessagePack payload. The caller sends these via the
    /// QUIC transport (same as graph algorithm barriers).
    pub fn build_scatter_envelopes(
        &self,
        collection: &str,
        start_ms: i64,
        end_ms: i64,
        value_column: &str,
        bucket_interval_ms: i64,
    ) -> Result<Vec<(u32, VShardEnvelope)>> {
        let msg = TsScatterPayload {
            collection: collection.to_string(),
            start_ms,
            end_ms,
            value_column: value_column.to_string(),
            bucket_interval_ms,
        };
        let payload_bytes = zerompk::to_msgpack_vec(&msg).map_err(|e| ClusterError::Codec {
            detail: format!("encoding TsScatterPayload: {e}"),
        })?;

        Ok(self
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::TsScatterRequest,
                    self.source_node,
                    0, // target_node resolved by routing table
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect())
    }

    /// Record a shard's response (partial aggregates).
    ///
    /// Rejects a shard outside the scatter set and a second response from a
    /// shard that already answered: either would let the gather read as
    /// complete while another shard is still silent.
    pub fn record_response(&mut self, shard_id: u32, partials: Vec<PartialAgg>) -> Result<()> {
        if !self.shard_ids.contains(&shard_id) {
            return Err(TsGatherError::UnexpectedShard {
                vshard_id: shard_id,
            }
            .into());
        }
        if !self.responded.insert(shard_id) {
            return Err(TsGatherError::DuplicateResponse {
                vshard_id: shard_id,
            }
            .into());
        }
        self.merger.add_shard_results(&partials);
        Ok(())
    }

    /// Whether all shards have responded.
    pub fn all_responded(&self) -> bool {
        self.missing_shards().is_empty()
    }

    /// Merge all shard responses into the final result.
    ///
    /// Refuses while any shard is missing: a SUM or COUNT short of one shard
    /// is type-identical to a correct one. A shard silent past the gather
    /// timeout is reported as [`ClusterError::ShardTimeout`] instead, naming
    /// the first missing shard.
    pub fn merge_results(&self) -> Result<MergedPartials> {
        self.check_complete()?;
        Ok(MergedPartials::new(self.merger.finalize()))
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
        Err(TsGatherError::Incomplete {
            responded: self.responded.len(),
            expected: self.shard_ids.len(),
            missing,
        }
        .into())
    }

    /// Build retention command envelopes for coordinated retention.
    pub fn build_retention_envelopes(
        &self,
        command: &RetentionCommand,
    ) -> Result<Vec<(u32, VShardEnvelope)>> {
        let payload_bytes = zerompk::to_msgpack_vec(command).map_err(|e| ClusterError::Codec {
            detail: format!("encoding RetentionCommand: {e}"),
        })?;

        Ok(self
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::TsRetentionCommand,
                    self.source_node,
                    0,
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect())
    }

    /// Build S3 archive command envelopes.
    pub fn build_archive_envelopes(
        &self,
        collection: &str,
        archive_before_ts: i64,
        s3_prefix: &str,
    ) -> Result<Vec<(u32, VShardEnvelope)>> {
        let msg = TsArchivePayload {
            collection: collection.to_string(),
            archive_before_ts,
            s3_prefix: s3_prefix.to_string(),
        };
        let payload_bytes = zerompk::to_msgpack_vec(&msg).map_err(|e| ClusterError::Codec {
            detail: format!("encoding TsArchivePayload: {e}"),
        })?;

        Ok(self
            .shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::TsArchiveCommand,
                    self.source_node,
                    0,
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_envelopes() {
        let coord = TsCoordinator::new(1, vec![0, 1, 2]);
        let envs = coord
            .build_scatter_envelopes("metrics", 1000, 2000, "cpu", 60_000)
            .expect("payload encodes");
        assert_eq!(envs.len(), 3);
        for (shard_id, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::TsScatterRequest);
            assert_eq!(env.vshard_id, *shard_id);
            assert!(!env.payload.is_empty());
        }
    }

    #[test]
    fn collect_and_merge() {
        let mut coord = TsCoordinator::new(1, vec![0, 1]);
        assert!(!coord.all_responded());

        coord
            .record_response(
                0,
                vec![PartialAgg {
                    count: 100,
                    sum: 5000.0,
                    ..PartialAgg::from_single(0, 1, 50.0)
                }],
            )
            .expect("shard 0 is in the scatter set");
        assert!(!coord.all_responded());

        coord
            .record_response(
                1,
                vec![PartialAgg {
                    count: 80,
                    sum: 4000.0,
                    ..PartialAgg::from_single(0, 2, 50.0)
                }],
            )
            .expect("shard 1 is in the scatter set");
        assert!(coord.all_responded());

        let merged = coord.merge_results().expect("every shard answered");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged.partials()[0].count, 180);
        assert_eq!(merged.partials()[0].sum, 9000.0);
    }

    #[test]
    fn merge_refused_while_a_shard_is_silent() {
        let mut coord = TsCoordinator::new(1, vec![0, 1]);
        coord
            .record_response(0, vec![PartialAgg::from_single(0, 1, 50.0)])
            .expect("shard 0 is in the scatter set");

        match coord.merge_results() {
            Err(ClusterError::TsGather(TsGatherError::Incomplete {
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
        let mut coord = TsCoordinator::new(1, vec![0, 1, 2]).with_timeout(Duration::from_millis(0));
        coord
            .record_response(0, vec![PartialAgg::from_single(0, 1, 50.0)])
            .expect("shard 0 is in the scatter set");

        match coord.merge_results() {
            Err(ClusterError::ShardTimeout { vshard_id, .. }) => assert_eq!(vshard_id, 1),
            other => panic!("expected a shard timeout, got {other:?}"),
        }
    }

    #[test]
    fn second_response_from_one_shard_refused() {
        let mut coord = TsCoordinator::new(1, vec![0, 1]);
        coord
            .record_response(0, vec![PartialAgg::from_single(0, 1, 50.0)])
            .expect("shard 0 is in the scatter set");

        match coord.record_response(0, vec![PartialAgg::from_single(0, 2, 60.0)]) {
            Err(ClusterError::TsGather(TsGatherError::DuplicateResponse { vshard_id })) => {
                assert_eq!(vshard_id, 0)
            }
            other => panic!("expected a duplicate-response error, got {other:?}"),
        }
        assert!(!coord.all_responded());
    }

    #[test]
    fn response_from_unscattered_shard_refused() {
        let mut coord = TsCoordinator::new(1, vec![0, 1]);
        match coord.record_response(7, vec![PartialAgg::from_single(0, 1, 50.0)]) {
            Err(ClusterError::TsGather(TsGatherError::UnexpectedShard { vshard_id })) => {
                assert_eq!(vshard_id, 7)
            }
            other => panic!("expected an unexpected-shard error, got {other:?}"),
        }
        assert_eq!(coord.response_count(), 0);
    }

    #[test]
    fn retention_envelopes() {
        let coord = TsCoordinator::new(1, vec![0, 1, 2, 3]);
        let cmd = RetentionCommand {
            collection: "metrics".into(),
            drop_before_ts: 1000,
            command_id: 42,
        };
        let envs = coord
            .build_retention_envelopes(&cmd)
            .expect("payload encodes");
        assert_eq!(envs.len(), 4);
        for (_, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::TsRetentionCommand);
        }
    }

    #[test]
    fn archive_envelopes() {
        let coord = TsCoordinator::new(1, vec![0, 1]);
        let envs = coord
            .build_archive_envelopes("metrics", 5000, "nodedb/v1/cluster-abc")
            .expect("payload encodes");
        assert_eq!(envs.len(), 2);
        for (_, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::TsArchiveCommand);
        }
    }
}
