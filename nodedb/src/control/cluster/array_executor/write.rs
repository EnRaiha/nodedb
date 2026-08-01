// SPDX-License-Identifier: BUSL-1.1

//! Write handlers for [`DataPlaneArrayExecutor`] — put and delete.
//!
//! These run on the shard OWNER after the coordinator has RPC-routed a cell
//! batch to it. When the owner hosts a data-Raft proposer (multi-node cluster)
//! the write is proposed to the owning shard's data group — `to_replicated_entry`
//! encodes it as `ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete` and every
//! replica re-executes it through the distributed apply loop (which opens the
//! array and dispatches to its local Data Plane). When no proposer exists
//! (single-node) the owner applies the write itself — through the shared
//! Control-Plane write funnel, which mints the redo record this write's only
//! durability rests on: the array engine is a memtable, so an array cell whose
//! `ArrayPut` record was never appended is simply gone after a restart.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::wire::{ArrayShardDeleteReq, ArrayShardPutReq};
use nodedb_cluster::error::{ClusterError, Result};

use super::executor::DataPlaneArrayExecutor;
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::types::{TraceId, VShardId};
use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};

impl DataPlaneArrayExecutor {
    pub(super) async fn put(&self, local_vshard_id: u32, req: &ArrayShardPutReq) -> Result<u64> {
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

        self.propose_or_dispatch(&array_id, local_vshard_id, plan, req.wal_lsn, "array put")
            .await
    }

    pub(super) async fn delete(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardDeleteReq,
    ) -> Result<u64> {
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
            local_vshard_id,
            plan,
            req.wal_lsn,
            "array delete",
        )
        .await
    }

    /// Replicate `plan` to the owning shard's data Raft group when a proposer
    /// exists; otherwise (single-node) apply it locally through the shared
    /// Control-Plane write funnel. Returns the `applied_lsn` the coordinator
    /// acks with.
    ///
    /// Both branches use the vShard from the validated RPC envelope. The Raft
    /// entry, the single-node bridge request, and WAL replay must retain this
    /// identity so they all select the same Data Plane core.
    async fn propose_or_dispatch(
        &self,
        array_id: &ArrayId,
        local_vshard_id: u32,
        plan: PhysicalPlan,
        wal_lsn: u64,
        op_label: &str,
    ) -> Result<u64> {
        if let Some(proposer) = self.state.async_raft_proposer() {
            let entry = crate::control::wal_replication::to_replicated_entry(
                array_id.tenant_id,
                array_id.database_id,
                VShardId::new(local_vshard_id),
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
            // On this branch no LSN exists to report: each replica mints its own
            // redo at apply, none of them is authoritative for the others, and
            // the proposer never sees any of them. The request's `wal_lsn` is
            // echoed back verbatim — it is what the coordinator sent, not a
            // claim about what was recorded.
            return Ok(wal_lsn);
        }

        // Single-node: no data-Raft group, so this node's WAL is the write's
        // only durability. The funnel appends the redo under write admission,
        // stamps the minted LSN into the plan (the array engine versions its
        // tiles from it, and replay re-stamps from the record header — the two
        // must name the same record), and fsyncs it before this ack returns.
        // This is a fresh originating write, not a Raft-committed entry, so its
        // ordering is decided HERE by the gate.
        let outcome: SubmitOutcome = submit_write(
            &self.state,
            single_node_submit(array_id, VShardId::new(local_vshard_id), plan),
        )
        .await
        .map_err(|e| ClusterError::Storage {
            detail: format!("{op_label}: {e}"),
        })?;

        if outcome.response.status == crate::bridge::envelope::Status::Error {
            let detail = outcome
                .response
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("{op_label} Data Plane error: {detail}"),
            });
        }

        // Ack with the LSN the funnel actually minted. `Put` and `Delete` both
        // append a record, so `None` here would mean the funnel classified this
        // plan as appending nothing — a wiring bug, not a durable write, and it
        // must not be acked as one.
        outcome
            .wal_lsn
            .map(|lsn| lsn.as_u64())
            .ok_or_else(|| ClusterError::Storage {
                detail: format!(
                    "{op_label}: applied with no WAL redo record — write is not durable"
                ),
            })
    }
}

/// Build the single-node write-funnel request using the envelope's validated
/// vShard. The funnel carries this through to both the bridge request and the
/// WAL record, whose replay uses the same vShard-to-core mapping.
fn single_node_submit(
    array_id: &ArrayId,
    local_vshard_id: VShardId,
    plan: PhysicalPlan,
) -> SubmitWrite {
    SubmitWrite {
        tenant_id: array_id.tenant_id,
        database_id: array_id.database_id,
        vshard_id: local_vshard_id,
        plan,
        trace_id: TraceId::generate(),
        event_source: crate::event::EventSource::User,
        txn_id: None,
        user_id: None,
        durability: WalDurability::AppendHere { now_override: None },
        ordering: WriteOrdering::Gate,
        change_feed: ChangeFeedOwner::Funnel,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    #[test]
    fn single_node_write_preserves_nonzero_vshard() {
        let array_id = ArrayId::new(TenantId::new(41), "measurements");
        let vshard_id = VShardId::new(19);
        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });

        let request = single_node_submit(&array_id, vshard_id, plan);

        assert_eq!(request.vshard_id, vshard_id);
    }
}
