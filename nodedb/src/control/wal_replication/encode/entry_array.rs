// SPDX-License-Identifier: BUSL-1.1

//! Classify an `ArrayOp` into an optional `ReplicatedWrite`.
//!
//! No `ArrayOp` variant is replicated today, but the match is exhaustive (not
//! a catch-all) so a new variant forces an explicit decision here instead of
//! silently returning `None`.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use nodedb_physical::physical_plan::ArrayOp;

/// Encode an `ArrayOp` write variant into its `ReplicatedWrite` wire shape.
/// Always `None` for now — array writes are not yet cross-node replicated.
pub(super) fn array_write(op: &ArrayOp) -> Option<ReplicatedWrite> {
    match op {
        // Known replication gaps: genuine array writes not yet wired to a
        // `ReplicatedWrite`. The data still lands via the leader's own
        // redb/WAL; only cross-node Raft replication of these ops is missing.
        // (They are also `Unroutable` in `plan_vshard` — tile->vshard needs
        // catalog tile_extents absent from the plan.)
        ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. } => None,

        // Not a write — array DDL (open/drop/compact) and reads
        // (slice / project / aggregate / elementwise / bitmap scan).
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => None,
    }
}
