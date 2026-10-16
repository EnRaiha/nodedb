// SPDX-License-Identifier: BUSL-1.1

//! The surrogate bitmap of an array slice, read from every shard that owns
//! the array's tiles.
//!
//! A vector search with an `ARRAY_SLICE` prefilter runs on the core that
//! owns its collection. That core holds only its own buckets of the array,
//! so the prefilter is read here across every primary vShard and handed to
//! the search as its candidate bitmap.

use std::sync::Arc;

use nodedb_cluster::distributed_array::coordinator::ArrayCoordinator;
use nodedb_cluster::distributed_array::wire::ArrayShardSurrogateBitmapReq;
use nodedb_types::SurrogateBitmap;

use crate::control::cluster::array_cluster_helpers::{cluster_err, coord_err, encode_err};

use super::executor::{ARRAY_RPC_TIMEOUT_MS, ClusterArrayExecutor};

impl ClusterArrayExecutor {
    /// The union of every shard's surrogate bitmap for the cells of
    /// `array_id` inside `slice_msgpack`.
    ///
    /// Every primary vShard answers. vShards that share one Data Plane
    /// report the same surrogates, which the union keeps once.
    pub async fn surrogate_bitmap(
        &self,
        array_id: &nodedb_array::types::ArrayId,
        slice_msgpack: &[u8],
        prefix_bits: u8,
    ) -> crate::Result<SurrogateBitmap> {
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
        let req = ArrayShardSurrogateBitmapReq {
            array_id_msgpack: zerompk::to_msgpack_vec(array_id).map_err(encode_err)?,
            slice_msgpack: slice_msgpack.to_vec(),
        };
        let resps = coordinator
            .coord_surrogate_bitmap_scan(req)
            .await
            .map_err(coord_err)?;
        let mut union = SurrogateBitmap::new();
        for resp in resps {
            let bitmap: SurrogateBitmap =
                zerompk::from_msgpack(&resp.bitmap_msgpack).map_err(|e| crate::Error::Codec {
                    detail: format!(
                        "array surrogate bitmap from vShard {} decode: {e}",
                        resp.shard_id
                    ),
                })?;
            union.union_in_place(&bitmap);
        }
        Ok(union)
    }
}
