// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for the Raft-native array cell writes
//! (`ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete`) — the cluster SQL DML
//! array path, distinct from the Lite-sync `ArrayOp` CRDT variant intercepted
//! upstream by the distributed applier.
//!
//! Delegated from `decode/entry.rs`'s grouped match arm. Each `ArrayPutCell`'s
//! leader-assigned surrogate is bound to its coord tuple on this replica via
//! the shared `DecodeCtx` assigner (exactly as `entry_document` binds a
//! document's surrogate to its `document_id`), so the same `(array, coord)`
//! resolves to the same global identity on every node. The reconstructed
//! `ArrayOp::Put` / `Delete` carries the cell/coord bytes verbatim with
//! `wal_lsn: 0` — the follower allocates its own WAL LSN at apply.

use super::super::types::ReplicatedWrite;
use super::ctx::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;
use crate::engine::array::wal::ArrayPutCell;
use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_types::sync::wire::SyncProvenance;

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::ArrayCellPut {
            array,
            cells_msgpack,
            provenance,
        } => cell_put(ctx, array, cells_msgpack, provenance),
        ReplicatedWrite::ArrayCellDelete {
            array,
            coords_msgpack,
            provenance,
        } => cell_delete(ctx, array, coords_msgpack, provenance),
        _ => Err(crate::Error::Internal {
            detail: "entry_array::decode_arm called with a non-array-cell ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped array match arm)"
                .into(),
        }),
    }
}

/// Reconstruct `ArrayOp::Put`, binding every cell's carried surrogate to its
/// coord tuple on this replica. `pk_bytes` is `zerompk(coord)` — the SAME key
/// the leader's plan-time `assign` used (`array_convert/dml.rs`) — so the bind
/// is byte-identical on every node. `cells_msgpack` passes through verbatim:
/// `bind` is first-wins and the carried surrogate is already stamped in each
/// cell, so nothing is rewritten.
fn cell_put(
    ctx: &DecodeCtx,
    array: &str,
    cells_msgpack: &[u8],
    provenance: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let cells: Vec<ArrayPutCell> =
        zerompk::from_msgpack(cells_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array cell put decode: {e}"),
        })?;
    if let Some(assigner) = ctx.assigner {
        for cell in &cells {
            let pk_bytes =
                zerompk::to_msgpack_vec(&cell.coord).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array coord pk encode: {e}"),
                })?;
            assigner.bind(
                ctx.database_id,
                ctx.tenant_id,
                array,
                &pk_bytes,
                cell.surrogate,
            )?;
        }
    }
    Ok(PhysicalPlan::Array(ArrayOp::Put {
        array_id: ArrayId::in_database(ctx.tenant_id, ctx.database_id, array),
        cells_msgpack: cells_msgpack.to_vec(),
        wal_lsn: 0,
        provenance: decode_provenance(provenance)?,
    }))
}

/// Reconstruct `ArrayOp::Delete`. Deletes are keyed by exact coordinate and
/// carry no surrogate, so there is nothing to bind — `coords_msgpack` passes
/// through verbatim.
fn cell_delete(
    ctx: &DecodeCtx,
    array: &str,
    coords_msgpack: &[u8],
    provenance: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Array(ArrayOp::Delete {
        array_id: ArrayId::in_database(ctx.tenant_id, ctx.database_id, array),
        coords_msgpack: coords_msgpack.to_vec(),
        wal_lsn: 0,
        provenance: decode_provenance(provenance)?,
    }))
}

/// Decode replicated sync provenance bytes back into `SyncProvenance`. Absent
/// (`None`) is normal for locally-originated cluster writes.
fn decode_provenance(bytes: &Option<Vec<u8>>) -> crate::Result<Option<SyncProvenance>> {
    match bytes {
        None => Ok(None),
        Some(b) => zerompk::from_msgpack::<SyncProvenance>(b)
            .map(Some)
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array provenance decode: {e}"),
            }),
    }
}
