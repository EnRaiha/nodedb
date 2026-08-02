// SPDX-License-Identifier: BUSL-1.1

//! Per-engine "already durable through LSN X" floors recovered from on-disk
//! checkpoints at boot, consulted by WAL replay so a restored checkpoint is not
//! re-derived from records it already contains.
//!
//! ## Why replaying ABOVE the floor is safe
//!
//! Every KV WAL record replays either as an absolute overwrite (`kv_put`,
//! `kv_batch_put`, `kv_delete`, `kv_truncate`) or as a delta re-executed against
//! the engine's current state (`kv_incr`, `kv_cas`, `kv_field_set`,
//! `kv_transfer`, ...). Restoring a checkpoint durable through LSN F reproduces
//! exactly the engine state that existed after record F was applied, so feeding
//! the records above F back through the same replay paths — in LSN order, on top
//! of that state — reaches the state a full from-zero replay would.
//!
//! It is records at or below F that MUST be skipped. For the absolute-overwrite
//! records re-applying is merely redundant, but for the delta records it is
//! corruption: an increment already folded into the checkpoint would be counted
//! twice.
//!
//! ## Why a floor is engine-wide rather than per-collection
//!
//! A KV record can span two collections (`kv_transfer_item` moves a row between
//! them). With per-collection floors those two collections could disagree —
//! source covered, destination not — and the record is then unrepresentable:
//! skipping it drops the destination's insert, applying it double-debits the
//! source. Publishing every collection's file under one generation, named by a
//! single manifest, makes disagreement unreachable by construction: all live
//! collections advance to one LSN together or none do.
//!
//! ## Adding an engine
//!
//! Add a field to [`ReplayFloors`], populate it from that engine's
//! `load_*_checkpoints` boot path, and consult it from that engine's replay
//! arms. Nothing here needs reshaping — engines do not share a floor.
//!
//! ## Which engines need one
//!
//! Only those whose WAL records are DELTAS against current state. A floor is not
//! a general "I restored a checkpoint" marker, and adding one where replay is
//! already idempotent gates records for no reason.
//!
//! Four checkpointed engines deliberately have no field here:
//!
//! * Sparse vector — `SparseVectorPut` is an upsert keyed by `doc_id` and
//!   `SparseVectorDelete` is a no-op against an absent document, so a record
//!   re-applied over the restored index reproduces it.
//! * The sync idempotency gate — `SyncSeqAdvance` advances both its maps by
//!   max-wins, so re-folding a record already contained in the restored state
//!   cannot change it. What that restore needs instead is for replay to MERGE
//!   into it rather than replace it; see `install_sync_hwm_maps`.
//! * Graph node labels — `GraphNodeLabelSet` ORs a bit on and
//!   `GraphNodeLabelRemove` ANDs it off, both keyed by `(node, label)` NAME, so
//!   a record re-applied over the restored bitset lands on the same bit. The
//!   names are why: the restore keys by name rather than by local node id
//!   precisely because ids are not stable across restarts, and replay uses the
//!   same `add_node_label` / `remove_node_label` entry points as the live
//!   handler.
//! * The array engine — it carries its own per-array floor rather than one
//!   here: each array's manifest records the `durable_lsn` its flushed segments
//!   reach, and `replay_array_wal` gates on that. A shared engine-wide field
//!   would be wrong for it, since arrays flush independently of one another.

use crate::types::Lsn;

/// Checkpoint-restored replay floors for every engine on one core.
///
/// Lives on `CoreLoop` rather than being threaded through `replay_all_wal` as a
/// parameter because it is restored core state, exactly like the watermark: the
/// `load_*_checkpoints` boot methods produce it and every replay path reads it
/// through `&self`. Default (all-unset) means "no checkpoint restored", which
/// gates nothing and replays the full WAL — the safe direction.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct ReplayFloors {
    /// KV engine floor, populated by `CoreLoop::load_kv_checkpoints`.
    pub(in crate::data::executor) kv: ReplayFloor,

    /// Columnar engine floor, populated by
    /// `CoreLoop::load_columnar_checkpoints`.
    ///
    /// Columnar needs a floor for a blunter reason than KV's. KV's delta
    /// records are a minority of its record classes; for columnar,
    /// re-applying is corrupting on the ordinary path:
    ///
    /// * `ColumnarOp::Update` is implemented as delete-old-PK + insert-new-row
    ///   (`wal_replay_columnar_dml.rs` states this as its idempotence
    ///   constraint), so a record folded into the restored engine and
    ///   replayed again appends a duplicate row.
    /// * `ColumnarOp::Insert` upserts by tombstoning the prior row for its
    ///   PK, which masks the duplicate on a plain collection but NOT on a
    ///   `bitemporal=true` one: `MutationEngine::insert` deliberately skips
    ///   the upsert-tombstone there so every version is retained, and a
    ///   replayed insert becomes a second version visible to `AS OF` queries.
    ///
    /// `ColumnarOp::Delete` is idempotent (tombstone bit + PK-index removal),
    /// but it shares the floor with the two above: the floor is engine-wide,
    /// and a record class that tolerates gating does not need an exemption
    /// from it.
    pub(in crate::data::executor) columnar: ReplayFloor,
}

/// The LSN an engine's restored checkpoint is durable through.
///
/// `None` means no checkpoint was restored, so nothing is gated and the full WAL
/// replays. Shared by every engine in [`ReplayFloors`]: the gating rule (an
/// inclusive `record_lsn <= durable_through` check) is identical across
/// engines — what differs between them is WHY they need one at all, which is
/// documented on each field above rather than on this type.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct ReplayFloor {
    durable_through: Option<Lsn>,
}

impl ReplayFloor {
    /// Record that the restored checkpoint is durable through `lsn`.
    ///
    /// Set once per boot, from the manifest that named the restored generation.
    pub(in crate::data::executor) fn set(&mut self, lsn: Lsn) {
        self.durable_through = Some(lsn);
    }

    /// Whether a record at `record_lsn` is already folded into the restored
    /// checkpoint and must therefore NOT be replayed.
    ///
    /// Inclusive: the manifest's LSN is the one the generation is durable
    /// THROUGH, so that record's effect is already present.
    pub(in crate::data::executor) fn covers(&self, record_lsn: u64) -> bool {
        self.durable_through
            .is_some_and(|floor| record_lsn <= floor.as_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_floor_covers_nothing() {
        let floor = ReplayFloor::default();
        assert!(!floor.covers(1));
        assert!(
            !floor.covers(u64::MAX),
            "no checkpoint restored must never gate a record"
        );
    }

    #[test]
    fn covers_is_inclusive_of_the_stamped_lsn() {
        let mut floor = ReplayFloor::default();
        floor.set(Lsn::new(100));
        assert!(floor.covers(99), "below the floor is already durable");
        assert!(floor.covers(100), "the floor itself is already durable");
        assert!(!floor.covers(101), "above the floor must replay");
    }

    #[test]
    fn unset_columnar_floor_covers_nothing() {
        let floor = ReplayFloor::default();
        assert!(!floor.covers(1));
        assert!(
            !floor.covers(u64::MAX),
            "no checkpoint restored must never gate a record"
        );
    }

    #[test]
    fn columnar_covers_is_inclusive_of_the_stamped_lsn() {
        let mut floor = ReplayFloor::default();
        floor.set(Lsn::new(100));
        assert!(floor.covers(99), "below the floor is already durable");
        assert!(floor.covers(100), "the floor itself is already durable");
        assert!(
            !floor.covers(101),
            "above the floor must replay — gating it would drop the write"
        );
    }
}
