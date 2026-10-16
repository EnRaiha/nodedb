// SPDX-License-Identifier: BUSL-1.1

//! Distributed `ARRAY_FLUSH` / `ARRAY_COMPACT`: every primary vShard runs the
//! maintenance on the core that holds its tiles.

use crate::error::ClusterError;

use super::super::scatter::{ArrayCoordError, fan_out};
use super::super::wire::{ArrayShardMaintReq, ArrayShardMaintResp};
use super::read::{ArrayCoordinator, decode_resps};

impl ArrayCoordinator {
    /// Send `req` to every target shard and return each shard's
    /// acknowledgement. Any shard error fails the whole operation.
    pub async fn coord_maint(
        &self,
        req: ArrayShardMaintReq,
    ) -> std::result::Result<Vec<ArrayShardMaintResp>, ArrayCoordError> {
        let req_bytes = zerompk::to_msgpack_vec(&req).map_err(|e| ClusterError::Codec {
            detail: format!("ArrayShardMaintReq serialise: {e}"),
        })?;
        let raw = fan_out(
            &self.fan_out_params(),
            super::super::opcodes::ARRAY_SHARD_MAINT_REQ,
            &req_bytes,
            &self.dispatch,
            &self.circuit_breaker,
        )
        .await?;
        Ok(decode_resps::<ArrayShardMaintResp>(&raw)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use crate::distributed_array::coordinator::read::ArrayCoordParams;
    use crate::distributed_array::rpc::ShardRpcDispatch;
    use crate::distributed_array::wire::ArrayShardMaintOp;
    use crate::error::Result;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    /// Answers each shard with its own vShard id as the count.
    struct ShardEcho;

    #[async_trait]
    impl ShardRpcDispatch for ShardEcho {
        async fn call(&self, req: VShardEnvelope, _timeout_ms: u64) -> Result<VShardEnvelope> {
            assert_eq!(req.msg_type, VShardMessageType::ArrayShardMaintReq);
            let shard_req: ArrayShardMaintReq =
                zerompk::from_msgpack(&req.payload).expect("request decodes");
            assert_eq!(shard_req.op, ArrayShardMaintOp::Flush);
            let resp = ArrayShardMaintResp {
                shard_id: req.vshard_id,
                affected: u64::from(req.vshard_id),
            };
            Ok(VShardEnvelope::new(
                VShardMessageType::ArrayShardMaintResp,
                req.target_node,
                req.source_node,
                req.vshard_id,
                zerompk::to_msgpack_vec(&resp).expect("encode"),
            ))
        }
    }

    /// Every primary shard runs the maintenance and answers.
    #[tokio::test]
    async fn every_primary_shard_runs_the_maintenance() {
        let bits = 2u8;
        let shard_ids =
            crate::distributed_array::routing::array_vshards_for_slice(&[], bits, 1024).unwrap();
        let expected: Vec<u32> = shard_ids.clone();
        let coord = ArrayCoordinator::new(
            ArrayCoordParams {
                source_node: 1,
                shard_ids,
                timeout_ms: 1000,
                prefix_bits: bits,
                slice_tile_keys: Vec::new(),
            },
            Arc::new(ShardEcho),
            Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
        );
        let resps = coord
            .coord_maint(ArrayShardMaintReq {
                array_id_msgpack: Vec::new(),
                op: ArrayShardMaintOp::Flush,
            })
            .await
            .expect("fan-out succeeds");
        let mut seen: Vec<u32> = resps.iter().map(|r| r.shard_id).collect();
        seen.sort_unstable();
        assert_eq!(seen, expected);
        assert!(resps.iter().all(|r| r.affected == u64::from(r.shard_id)));
    }
}
