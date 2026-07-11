// SPDX-License-Identifier: BUSL-1.1

//! Write handlers for [`DataPlaneArrayExecutor`] — put and delete.
//!
//! These run on the shard OWNER after the coordinator has RPC-routed a cell
//! batch to it. When the owner hosts a data-Raft proposer (multi-node cluster)
//! the write is proposed to the owning shard's data group — `to_replicated_entry`
//! encodes it as `ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete` and every
//! replica re-executes it through the distributed apply loop (which opens the
//! array and dispatches to its local Data Plane). Only when no proposer exists
//! (single-node) does the owner dispatch straight to its own Data Plane.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::wire::{ArrayShardDeleteReq, ArrayShardPutReq};
use nodedb_cluster::error::{ClusterError, Result};

use super::executor::DataPlaneArrayExecutor;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};

impl DataPlaneArrayExecutor {
    pub(super) async fn put(&self, req: &ArrayShardPutReq) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_put: {e}"),
            })?;

        // The coordinator encodes cells as `Vec<Vec<u8>>` (a blob-vec where
        // each inner bytes is a separately-encoded `ArrayPutCell`). The Data
        // Plane handler expects `Vec<ArrayPutCell>` encoded as a flat msgpack
        // array. Decode the outer blob-vec, parse each blob, and re-encode.
        let cell_blobs: Vec<Vec<u8>> =
            zerompk::from_msgpack(&req.cells_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("cell blob-vec decode in exec_put: {e}"),
            })?;

        let cells: Vec<crate::engine::array::wal::ArrayPutCell> = cell_blobs
            .iter()
            .map(|blob| {
                zerompk::from_msgpack(blob).map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayPutCell decode in exec_put: {e}"),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let cells_msgpack = zerompk::to_msgpack_vec(&cells).map_err(|e| ClusterError::Codec {
            detail: format!("cells re-encode in exec_put: {e}"),
        })?;

        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: array_id.clone(),
            cells_msgpack,
            wal_lsn: req.wal_lsn,
            provenance: None,
        });

        self.propose_or_dispatch(
            &array_id,
            plan,
            req.representative_hilbert_prefix,
            req.prefix_bits,
            req.wal_lsn,
            "array put",
        )
        .await
    }

    pub(super) async fn delete(&self, req: &ArrayShardDeleteReq) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_delete: {e}"),
            })?;

        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: req.coords_msgpack.clone(),
            wal_lsn: req.wal_lsn,
            provenance: None,
        });

        self.propose_or_dispatch(
            &array_id,
            plan,
            req.representative_hilbert_prefix,
            req.prefix_bits,
            req.wal_lsn,
            "array delete",
        )
        .await
    }

    /// Replicate `plan` to the owning shard's data Raft group when a proposer
    /// exists; otherwise (single-node) dispatch it straight to the local Data
    /// Plane. Returns the `applied_lsn` ack (`wal_lsn`) the coordinator expects.
    ///
    /// The owning vShard is derived from the batch's Hilbert-tile placement —
    /// the SAME routing the coordinator used to reach this owner and the handler
    /// validated against `local_vshard_id`. It is NOT the local-DP-only
    /// `VShardId::new(0)`: proposing under the wrong vShard would land the entry
    /// in the wrong data group.
    async fn propose_or_dispatch(
        &self,
        array_id: &ArrayId,
        plan: PhysicalPlan,
        representative_hilbert_prefix: u64,
        prefix_bits: u8,
        wal_lsn: u64,
        op_label: &str,
    ) -> Result<u64> {
        if let Some(proposer) = self.state.async_raft_proposer.get() {
            let vshard = derive_vshard(array_id, representative_hilbert_prefix, prefix_bits)?;
            // Array writes are database-scoped by convention to the default
            // database on the apply path (the wire req carries no database and
            // arrays route by name); the follower rebinds each carried surrogate
            // under this same scope, so identity is preserved regardless.
            let entry = crate::control::wal_replication::to_replicated_entry(
                array_id.tenant_id,
                DatabaseId::DEFAULT,
                VShardId::new(vshard),
                &plan,
            )
            .ok_or_else(|| ClusterError::Storage {
                detail: format!("{op_label}: plan is not encodable as a replicated entry"),
            })?;

            crate::control::wal_replication::propose_replicated_entry(&self.state, proposer, entry)
                .await
                .map_err(|e| ClusterError::Storage {
                    detail: format!("{op_label} raft propose: {e}"),
                })?;
            return Ok(wal_lsn);
        }

        // Single-node: no data-Raft group — apply directly on the local Data Plane.
        let resp = self.dispatch_and_await(plan).await?;
        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("{op_label} Data Plane error: {detail}"),
            });
        }
        Ok(wal_lsn)
    }
}

/// Derive the owning vShard from the batch's Hilbert-tile placement. Uses the
/// Hilbert-prefix routing (`array_vshard_for_tile`) that the coordinator + shard
/// handler already agree on; falls back to name-based routing only when routing
/// metadata is absent (`prefix_bits == 0`, pre-routing clients).
fn derive_vshard(
    array_id: &ArrayId,
    representative_hilbert_prefix: u64,
    prefix_bits: u8,
) -> Result<u32> {
    if prefix_bits == 0 {
        return Ok(nodedb_cluster::array_routing::array_vshard_for_name(
            &array_id.name,
        ));
    }
    nodedb_cluster::distributed_array::routing::array_vshard_for_tile(
        representative_hilbert_prefix,
        prefix_bits,
    )
    .map_err(|e| ClusterError::Storage {
        detail: format!("array vshard derive: {e}"),
    })
}
