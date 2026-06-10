// SPDX-License-Identifier: BUSL-1.1

pub mod read;
pub mod write;

pub use read::{ArrayCoordParams, ArrayCoordinator, CoordAggResult, CoordSliceResult};
pub use write::{ArrayWriteCoordParams, coord_delete, coord_put, coord_put_partitioned};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use crate::error::Result;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    use super::super::merge::ArrayAggPartial;
    use super::super::rpc::ShardRpcDispatch;
    use super::super::wire::{
        ArrayShardAggReq, ArrayShardAggResp, ArrayShardSliceReq, ArrayShardSliceResp,
    };
    use super::read::{ArrayCoordParams, ArrayCoordinator};

    /// Mock dispatch that returns a pre-serialised `ArrayShardSliceResp`.
    struct SliceEchoDispatch {
        /// Rows to return from each shard.
        rows: Vec<Vec<u8>>,
    }

    #[async_trait]
    impl ShardRpcDispatch for SliceEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let resp = ArrayShardSliceResp {
                shard_id: req.vshard_id,
                rows_msgpack: self.rows.clone(),
                truncated: false,
                truncated_before_horizon: false,
            };
            let payload = zerompk::to_msgpack_vec(&resp).unwrap();
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardSliceResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                payload,
            ))
        }
    }

    /// Mock dispatch that returns a pre-canned `ArrayShardAggResp`.
    struct AggEchoDispatch {
        partials: Vec<ArrayAggPartial>,
    }

    #[async_trait]
    impl ShardRpcDispatch for AggEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let resp = ArrayShardAggResp {
                shard_id: req.vshard_id,
                partials: self.partials.clone(),
                truncated_before_horizon: false,
            };
            let payload = zerompk::to_msgpack_vec(&resp).unwrap();
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardSliceResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                payload,
            ))
        }
    }

    fn make_coordinator(
        shard_ids: Vec<u32>,
        dispatch: Arc<dyn ShardRpcDispatch>,
    ) -> ArrayCoordinator {
        ArrayCoordinator::new(
            ArrayCoordParams {
                source_node: 1,
                shard_ids,
                timeout_ms: 1000,
                // Tests use prefix_bits=0 so shard-side routing validation
                // is skipped — mock executors don't need to match Hilbert
                // ownership.
                prefix_bits: 0,
                slice_hilbert_ranges: vec![],
            },
            dispatch,
            Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
        )
    }

    #[tokio::test]
    async fn coord_slice_merges_rows_from_all_shards() {
        let row_a = zerompk::to_msgpack_vec(&"row-a").unwrap();
        let row_b = zerompk::to_msgpack_vec(&"row-b").unwrap();
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(SliceEchoDispatch {
            rows: vec![row_a.clone(), row_b.clone()],
        });
        let coord = make_coordinator(vec![0, 1, 2], dispatch);
        let req = ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 100,
            cell_filter_msgpack: vec![],
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        };

        // 3 shards × 2 rows each = 6 merged rows.
        let result = coord
            .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice should succeed");
        assert_eq!(result.rows.len(), 6);
        assert!(!result.truncated_before_horizon);
    }

    #[tokio::test]
    async fn coord_slice_applies_coordinator_limit() {
        let row = zerompk::to_msgpack_vec(&"row").unwrap();
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(SliceEchoDispatch {
            rows: vec![row.clone(), row.clone(), row.clone()],
        });
        // 2 shards × 3 rows = 6 total, but limit = 4.
        let coord = make_coordinator(vec![0, 1], dispatch);
        let req = ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 3,
            cell_filter_msgpack: vec![],
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        };

        let result = coord
            .coord_slice(req, 4, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice with limit should succeed");
        assert_eq!(result.rows.len(), 4);
    }

    fn make_agg_req() -> ArrayShardAggReq {
        // Sum reducer c_enum = 0.
        ArrayShardAggReq {
            array_id_msgpack: vec![],
            attr_idx: 0,
            reducer_msgpack: vec![0x00],
            group_by_dim: -1,
            cell_filter_msgpack: vec![],
            shard_hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        }
    }

    #[tokio::test]
    async fn coord_agg_merges_scalar_partials_from_shards() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(AggEchoDispatch {
            partials: vec![ArrayAggPartial::from_single(0, 10.0)],
        });
        // 3 shards each returning a partial with sum=10 → merged sum=30.
        let coord = make_coordinator(vec![0, 1, 2], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg should succeed");

        assert_eq!(merged.partials.len(), 1);
        assert_eq!(merged.partials[0].count, 3);
        assert!((merged.partials[0].sum - 30.0).abs() < f64::EPSILON);
        assert!(!merged.truncated_before_horizon);
    }

    #[tokio::test]
    async fn coord_agg_with_empty_shards_returns_empty() {
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(AggEchoDispatch { partials: vec![] });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg with empty shards should succeed");
        assert!(merged.partials.is_empty());
    }

    #[tokio::test]
    async fn coord_agg_merges_grouped_partials_across_shards() {
        // Shard 0 returns group_key=0 partial, shard 1 also group_key=0 + group_key=1.
        struct GroupedDispatch {
            shard0_partials: Vec<ArrayAggPartial>,
            shard1_partials: Vec<ArrayAggPartial>,
        }

        #[async_trait]
        impl ShardRpcDispatch for GroupedDispatch {
            async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
                let partials = if req.vshard_id == 0 {
                    self.shard0_partials.clone()
                } else {
                    self.shard1_partials.clone()
                };
                let resp = ArrayShardAggResp {
                    shard_id: req.vshard_id,
                    partials,
                    truncated_before_horizon: false,
                };
                let payload = zerompk::to_msgpack_vec(&resp).unwrap();
                Ok(VShardEnvelope::new(
                    VShardMessageType::ArrayShardSliceResp,
                    req.target_node,
                    req.source_node,
                    req.vshard_id,
                    payload,
                ))
            }
        }

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(GroupedDispatch {
            shard0_partials: vec![ArrayAggPartial::from_single(0, 5.0)],
            shard1_partials: vec![
                ArrayAggPartial::from_single(0, 15.0),
                ArrayAggPartial::from_single(1, 20.0),
            ],
        });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("grouped coord_agg should succeed");

        // group_key=0: sum=5+15=20, count=2; group_key=1: sum=20, count=1.
        assert_eq!(merged.partials.len(), 2);
        let g0 = merged
            .partials
            .iter()
            .find(|p| p.group_key == 0)
            .expect("group 0");
        let g1 = merged
            .partials
            .iter()
            .find(|p| p.group_key == 1)
            .expect("group 1");
        assert!((g0.sum - 20.0).abs() < f64::EPSILON);
        assert_eq!(g0.count, 2);
        assert!((g1.sum - 20.0).abs() < f64::EPSILON);
        assert_eq!(g1.count, 1);
    }

    #[tokio::test]
    async fn coord_agg_or_reduces_truncated_before_horizon() {
        // One shard reports below-horizon; the coordinator must OR-reduce the
        // flag so the upstream caller can surface an incomplete-result signal.
        // Dropping it here was a silent-partial-success bug.
        struct HorizonDispatch;

        #[async_trait]
        impl ShardRpcDispatch for HorizonDispatch {
            async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
                // Shard 1 is below horizon (zero partials); shard 0 has data.
                let (partials, below) = if req.vshard_id == 0 {
                    (vec![ArrayAggPartial::from_single(0, 10.0)], false)
                } else {
                    (vec![], true)
                };
                let resp = ArrayShardAggResp {
                    shard_id: req.vshard_id,
                    partials,
                    truncated_before_horizon: below,
                };
                let payload = zerompk::to_msgpack_vec(&resp).unwrap();
                Ok(VShardEnvelope::new(
                    VShardMessageType::ArrayShardSliceResp,
                    req.target_node,
                    req.source_node,
                    req.vshard_id,
                    payload,
                ))
            }
        }

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(HorizonDispatch);
        let coord = make_coordinator(vec![0, 1], dispatch);
        let merged = coord
            .coord_agg(make_agg_req())
            .await
            .expect("coord_agg should succeed");
        assert!(
            merged.truncated_before_horizon,
            "coordinator must OR-reduce the below-horizon flag across shards"
        );
    }

    #[tokio::test]
    async fn coord_slice_zero_limit_returns_all() {
        let row = zerompk::to_msgpack_vec(&"r").unwrap();
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(SliceEchoDispatch {
            rows: vec![row.clone(); 10],
        });
        let coord = make_coordinator(vec![0, 1], dispatch);
        let req = ArrayShardSliceReq {
            array_id_msgpack: vec![],
            slice_msgpack: vec![],
            attr_projection: vec![],
            limit: 0,
            cell_filter_msgpack: vec![],
            prefix_bits: 0,
            slice_hilbert_ranges: vec![],
            shard_hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        };

        // coordinator_limit = 0 → no cutoff → 20 rows.
        let result = coord
            .coord_slice(req, 0, nodedb_types::SystemTimeScope::Current)
            .await
            .expect("coord_slice unlimited should succeed");
        assert_eq!(result.rows.len(), 20);
    }

    // ── coord_put / coord_delete tests ────────────────────────────────────

    use super::super::wire::{ArrayShardDeleteResp, ArrayShardPutReq, ArrayShardPutResp};
    use super::write::{ArrayWriteCoordParams, coord_delete, coord_put};
    use crate::error::ClusterError;

    /// Records which vShard IDs were called and echoes back an `ArrayShardPutResp`.
    struct PutEchoDispatch;

    #[async_trait]
    impl ShardRpcDispatch for PutEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            let shard_req: ArrayShardPutReq = zerompk::from_msgpack(&req.payload).unwrap();
            let resp = ArrayShardPutResp {
                shard_id: req.vshard_id,
                applied_lsn: shard_req.wal_lsn,
            };
            let payload = zerompk::to_msgpack_vec(&resp).unwrap();
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardSliceResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                payload,
            ))
        }
    }

    /// Dispatch that always returns a Codec error — used for failure-propagation tests.
    struct FailDispatch;

    #[async_trait]
    impl ShardRpcDispatch for FailDispatch {
        async fn call(&self, _req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            Err(ClusterError::Codec {
                detail: "injected failure".into(),
            })
        }
    }

    /// Echo dispatch for delete that returns an `ArrayShardDeleteResp`.
    struct DeleteEchoDispatch;

    #[async_trait]
    impl ShardRpcDispatch for DeleteEchoDispatch {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            use super::super::wire::ArrayShardDeleteReq;
            let shard_req: ArrayShardDeleteReq = zerompk::from_msgpack(&req.payload).unwrap();
            let resp = ArrayShardDeleteResp {
                shard_id: req.vshard_id,
                applied_lsn: shard_req.wal_lsn,
            };
            let payload = zerompk::to_msgpack_vec(&resp).unwrap();
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardSliceResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                payload,
            ))
        }
    }

    fn write_params() -> ArrayWriteCoordParams {
        ArrayWriteCoordParams {
            source_node: 1,
            timeout_ms: 1000,
        }
    }

    fn cb() -> Arc<CircuitBreaker> {
        Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default()))
    }

    #[tokio::test]
    async fn coord_put_partitions_cells_by_tile() {
        // prefix_bits=10, stride=1 → vshard == top-10-bit bucket.
        // p0 → bucket 0 → vshard 0
        // p1 → bucket 1 → vshard 1
        // p2 → bucket 2 → vshard 2
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;
        let p2 = 0x0080_0000_0000_0000u64;

        let cells = vec![
            (p0, vec![0x01u8]),
            (p1, vec![0x02u8]),
            (p0, vec![0x03u8]),
            (p2, vec![0x04u8]),
            (p1, vec![0x05u8]),
        ];

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(PutEchoDispatch);
        let mut resps = coord_put(&write_params(), vec![], 10, 42, &cells, &dispatch, &cb())
            .await
            .expect("coord_put should succeed");

        resps.sort_by_key(|r| r.shard_id);
        assert_eq!(resps.len(), 3, "should fan-out to 3 shards");
        assert_eq!(resps[0].shard_id, 0);
        assert_eq!(resps[1].shard_id, 1);
        assert_eq!(resps[2].shard_id, 2);
        // Each shard echoes back wal_lsn=42.
        for r in &resps {
            assert_eq!(r.applied_lsn, 42);
        }
    }

    #[tokio::test]
    async fn coord_put_aggregates_partial_failures() {
        // A failing dispatch must surface as an error, not silent partial success.
        let cells = vec![(0u64, vec![0xAAu8])];
        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(FailDispatch);
        let err = coord_put(&write_params(), vec![], 10, 1, &cells, &dispatch, &cb())
            .await
            .expect_err("coord_put with failing shard should return error");
        assert!(
            matches!(err, ClusterError::Codec { .. }),
            "expected Codec error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn coord_delete_partitions_by_tile() {
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;

        let coords = vec![(p0, vec![0xAAu8]), (p1, vec![0xBBu8]), (p0, vec![0xCCu8])];

        let dispatch: Arc<dyn ShardRpcDispatch> = Arc::new(DeleteEchoDispatch);
        let mut resps = coord_delete(&write_params(), vec![], 10, 55, &coords, &dispatch, &cb())
            .await
            .expect("coord_delete should succeed");

        resps.sort_by_key(|r| r.shard_id);
        assert_eq!(resps.len(), 2, "should fan-out to 2 shards");
        assert_eq!(resps[0].shard_id, 0);
        assert_eq!(resps[1].shard_id, 1);
        for r in &resps {
            assert_eq!(r.applied_lsn, 55);
        }
    }
}
