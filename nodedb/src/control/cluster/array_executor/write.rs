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

use super::executor::{DataPlaneArrayExecutor, LOCAL_DISPATCH_VSHARD};
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::types::{DatabaseId, TraceId, VShardId};
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
    /// exists; otherwise (single-node) apply it locally through the shared
    /// Control-Plane write funnel. Returns the `applied_lsn` the coordinator
    /// acks with.
    ///
    /// The two branches derive their vShard from different things, and both are
    /// right for what they address. The PROPOSE branch addresses a Raft data
    /// group, so it uses the batch's Hilbert-tile placement — the SAME routing
    /// the coordinator used to reach this owner and the handler validated
    /// against `local_vshard_id`; proposing under any other vShard would land
    /// the entry in the wrong group. The LOCAL branch addresses a Data Plane
    /// core on this node, where placement is already settled (the RPC arrived
    /// here), so it uses `LOCAL_DISPATCH_VSHARD` — the one this executor's reads
    /// dispatch under, and hence the one core holding this array's engine state.
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
            SubmitWrite {
                tenant_id: array_id.tenant_id,
                // Arrays are database-scoped by convention to the default
                // database on the apply path (the wire req carries no database
                // and arrays route by name); the redo record must be appended
                // under that same scope or replay lands it in the wrong
                // catalog namespace.
                database_id: DatabaseId::DEFAULT,
                vshard_id: LOCAL_DISPATCH_VSHARD,
                plan,
                trace_id: TraceId::generate(),
                // The coordinator RPC-routed an originating user write here; no
                // sync op-log or CRDT delta is involved, so this keeps the
                // `User` source its cluster counterpart applies under — the one
                // source that lets AFTER triggers and CDC fire for it.
                event_source: crate::event::EventSource::User,
                txn_id: None,
                // Auth ran on the coordinator before the fan-out; the shard
                // request carries no session user.
                user_id: None,
                durability: WalDurability::AppendHere { now_override: None },
                ordering: WriteOrdering::Gate,
                // This path hand-built its own `Request` before it moved onto
                // the funnel and never published a change event for the array
                // write it dispatched; see [`ChangeFeedOwner::Unowned`].
                change_feed: ChangeFeedOwner::Unowned,
            },
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
