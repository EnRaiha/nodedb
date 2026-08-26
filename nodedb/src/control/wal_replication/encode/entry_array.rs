// SPDX-License-Identifier: BUSL-1.1

//! Classify an `ArrayOp` into an optional `ReplicatedWrite`.
//!
//! `Put`/`Delete` replicate to the shard's data Raft group as `ArrayCellPut`/
//! `ArrayCellDelete`, carrying the cell/coord payload verbatim. Exhaustive
//! match (no catch-all) forces an explicit decision for new variants.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::entry::encode_provenance;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_types::sync::wire::SyncProvenance;

/// Encode an `ArrayOp` write variant into its `ReplicatedWrite` wire shape.
/// `Put`/`Delete` return `Some`; `Flush` and every read/DDL op return `None`.
pub(super) fn array_write(op: &ArrayOp) -> Option<ReplicatedWrite> {
    match op {
        // wal_lsn omitted; followers allocate their own LSN at apply time.
        ArrayOp::Put {
            array_id,
            cells_msgpack,
            wal_lsn: _,
            provenance,
        } => Some(cell_put(&array_id.name, cells_msgpack, provenance)),
        ArrayOp::Delete {
            array_id,
            coords_msgpack,
            wal_lsn: _,
            provenance,
        } => Some(cell_delete(&array_id.name, coords_msgpack, provenance)),

        // `Flush` forces a memtable flush of already-committed writes; a follower
        // rebuilds it from those, so proposing it would be a no-op.
        ArrayOp::Flush { .. } => None,

        // Not a write — array DDL and reads.
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => None,
    }
}

/// Build the `ArrayCellPut` wire shape. `cells_msgpack` carries each cell's
/// leader-assigned surrogate verbatim, no separate sidecar.
fn cell_put(
    array: &str,
    cells_msgpack: &[u8],
    provenance: &Option<SyncProvenance>,
) -> ReplicatedWrite {
    ReplicatedWrite::ArrayCellPut {
        array: array.to_owned(),
        cells_msgpack: cells_msgpack.to_vec(),
        provenance: encode_provenance(provenance),
    }
}

/// Build the `ArrayCellDelete` wire shape. `coords_msgpack` is the exact
/// encoding the owner's Data Plane delete handler consumes; deletes carry no
/// surrogate (keyed by coordinate).
fn cell_delete(
    array: &str,
    coords_msgpack: &[u8],
    provenance: &Option<SyncProvenance>,
) -> ReplicatedWrite {
    ReplicatedWrite::ArrayCellDelete {
        array: array.to_owned(),
        coords_msgpack: coords_msgpack.to_vec(),
        provenance: encode_provenance(provenance),
    }
}
