// SPDX-License-Identifier: BUSL-1.1

//! Write the funnel's minted LSN back into the plan it is about to enqueue.

#![deny(clippy::wildcard_enum_match_arm)]

use crate::bridge::envelope::PhysicalPlan;
use crate::types::Lsn;
use nodedb_physical::physical_plan::ArrayOp;

/// Overwrite the LSN carried *inside* `plan` with `lsn` — the LSN of the
/// durable record the funnel just minted for this write.
///
/// Almost every engine takes its committed version from `Request.wal_lsn`,
/// which the funnel stamps on the envelope; for those plans this is a no-op.
/// The array engine is the exception: `handle_array_put` / `handle_array_delete`
/// stamp each tile version from the LSN carried in the plan, while WAL replay
/// stamps the same tile version from the record header
/// (`data/executor/wal_replay/array.rs`). Both must be the SAME number, or a
/// replayed cell carries a different version than the live cell it is meant to
/// reproduce. Rewriting the plan here — once, next to the mint — is what makes
/// the two agree by construction; no caller can allocate an LSN of its own and
/// hope it matches.
///
/// The match over [`PhysicalPlan`] and [`ArrayOp`] is exhaustive
/// (`wildcard_enum_match_arm` is denied), so a future plan variant that carries
/// its own LSN must decide by name whether the funnel's LSN belongs in it.
pub fn stamp_minted_lsn(plan: &mut PhysicalPlan, lsn: Lsn) {
    match plan {
        PhysicalPlan::Array(op) => stamp_array_op(op, lsn),
        // Every other engine reads its committed version off the request
        // envelope; nothing inside these plans names a WAL record.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => {}
    }
}

fn stamp_array_op(op: &mut ArrayOp, lsn: Lsn) {
    match op {
        ArrayOp::Put { wal_lsn, .. } | ArrayOp::Delete { wal_lsn, .. } => *wal_lsn = lsn.as_u64(),
        // `Flush`'s `wal_lsn` is the segment's flush watermark — the WAL
        // frontier the flushed tiles are complete up to — not the LSN of a
        // record. It appends nothing, so the funnel mints no LSN for it and
        // this arm is never reached with one. Every remaining variant is a
        // read / DDL that carries no LSN at all.
        ArrayOp::Flush { .. }
        | ArrayOp::OpenArray { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::SurrogateBitmapScan { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Compact { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;
    use nodedb_array::types::ArrayId;

    fn array_id() -> ArrayId {
        ArrayId::new(TenantId::new(1), "g")
    }

    #[test]
    fn put_carries_the_minted_lsn() {
        let mut plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: array_id(),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        stamp_minted_lsn(&mut plan, Lsn::new(77));
        let PhysicalPlan::Array(ArrayOp::Put { wal_lsn, .. }) = plan else {
            panic!("expected an array put");
        };
        assert_eq!(wal_lsn, 77);
    }

    #[test]
    fn delete_carries_the_minted_lsn() {
        let mut plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id(),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        stamp_minted_lsn(&mut plan, Lsn::new(9));
        let PhysicalPlan::Array(ArrayOp::Delete { wal_lsn, .. }) = plan else {
            panic!("expected an array delete");
        };
        assert_eq!(wal_lsn, 9);
    }

    #[test]
    fn flush_watermark_is_left_alone() {
        let mut plan = PhysicalPlan::Array(ArrayOp::Flush {
            array_id: array_id(),
            wal_lsn: 5,
        });
        stamp_minted_lsn(&mut plan, Lsn::new(100));
        let PhysicalPlan::Array(ArrayOp::Flush { wal_lsn, .. }) = plan else {
            panic!("expected an array flush");
        };
        assert_eq!(wal_lsn, 5);
    }
}
