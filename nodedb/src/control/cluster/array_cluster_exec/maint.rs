// SPDX-License-Identifier: BUSL-1.1

//! `ClusterArrayOp::Flush` / `Compact`: fan the maintenance out to every
//! primary vShard, so every core that holds tiles of the array runs it.

use std::sync::Arc;

use nodedb_cluster::distributed_array::coordinator::ArrayCoordinator;
use nodedb_cluster::distributed_array::wire::{ArrayShardMaintOp, ArrayShardMaintReq};

use crate::control::cluster::array_cluster_helpers::{cluster_err, coord_err, encode_err};
use crate::data::executor::response_codec;

use super::executor::{ARRAY_RPC_TIMEOUT_MS, ClusterArrayExecutor};

impl ClusterArrayExecutor {
    /// Answer `{"flushed": 1}` for a flush, as a local flush does. Answer
    /// `{"compacted": n}` for a compaction, where `n` counts the shards
    /// that merged segments.
    pub(super) async fn execute_maint(
        &self,
        array_id: &nodedb_array::types::ArrayId,
        op: ArrayShardMaintOp,
        prefix_bits: u8,
    ) -> crate::Result<Vec<u8>> {
        // No tile keys: maintenance covers every tile of the array.
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            &[],
            prefix_bits,
            self.total_shards,
            Arc::clone(&self.dispatch),
            Arc::clone(&self.circuit_breaker),
        )
        .map_err(cluster_err)?;
        let is_flush = op == ArrayShardMaintOp::Flush;
        let req = ArrayShardMaintReq {
            array_id_msgpack: zerompk::to_msgpack_vec(array_id).map_err(encode_err)?,
            op,
        };
        let resps = coordinator.coord_maint(req).await.map_err(coord_err)?;
        if is_flush {
            response_codec::encode_count("flushed", 1)
        } else {
            let merged: u64 = resps.iter().map(|r| r.affected).sum();
            response_codec::encode_count("compacted", merged as usize)
        }
    }
}
