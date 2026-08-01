// SPDX-License-Identifier: BUSL-1.1

//! Classify an `ArrayOp` into an optional `ReplicatedWrite`.
//!
//! `ArrayOp::Put` / `Delete` are the Raft-native array-write path: on the shard
//! owner they replicate to the shard's data Raft group as
//! `ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete`, carrying the cell/coord
//! payload (with each cell's leader-assigned surrogate embedded) verbatim. The
//! match is exhaustive (not a catch-all) so a new variant forces an explicit
//! decision here.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::entry::encode_provenance;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_types::sync::wire::SyncProvenance;

/// Encode an `ArrayOp` write variant into its `ReplicatedWrite` wire shape.
///
/// `Put` / `Delete` return `Some(...)` — replicated to the shard's data group.
/// `Flush` and every read / DDL op return `None`.
pub(super) fn array_write(op: &ArrayOp) -> Option<ReplicatedWrite> {
    match op {
        ArrayOp::Put {
            array_id,
            cells_msgpack,
            provenance,
            // wal_lsn is omitted from the wire envelope; followers allocate
            // their own LSN at apply time (like `ColumnarIngest`).
            ..
        } => Some(cell_put(&array_id.name, cells_msgpack, provenance)),
        ArrayOp::Delete {
            array_id,
            coords_msgpack,
            provenance,
            ..
        } => Some(cell_delete(&array_id.name, coords_msgpack, provenance)),

        // Not replicated: `Flush` is durable-elsewhere — it forces a memtable
        // flush of already-committed Put/Delete writes and is rebuilt from
        // those replicated writes on a follower, so proposing it would be a
        // redundant no-op.
        ArrayOp::Flush { .. } => None,

        // Not a write — array DDL (open/drop/compact) and reads
        // (slice / project / aggregate / elementwise / bitmap scan).
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

/// Build the `ArrayCellPut` wire shape. `cells_msgpack` is carried verbatim —
/// each `ArrayPutCell` inside it already holds its leader-assigned global
/// surrogate, so the surrogates ride across the wire losslessly with no
/// separate sidecar. Provenance is `Some` only on the sync path.
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
