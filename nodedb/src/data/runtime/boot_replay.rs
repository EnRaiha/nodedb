// SPDX-License-Identifier: BUSL-1.1

//! Boot stage 3 of 3: replay the WAL, then run the crash-recovery HNSW rebuild
//! backstop.
//!
//! Runs last. See `replay_wal_and_rebuild_indexes` for why that order is the
//! only sound one.

use crate::data::executor::core_loop::CoreLoop;

/// Replay WAL records for crash recovery, then re-index the HNSW from the
/// durable store.
///
/// # Ordering (load-bearing)
///
/// Runs AFTER `load_boot_checkpoints` and `seed_catalog_state`, never before:
/// each checkpoint restores state as of the LSN it was stamped with and installs
/// the replay floor that makes this replay resume strictly ABOVE that LSN, so
/// replaying first and restoring after would overwrite the newer replayed state
/// with the older checkpoint's rows. The seeds must likewise already be in place,
/// or replay infers schemas it should have been handed.
///
/// The vector rebuild backstop runs after the replay within this function for
/// the same reason it is an idempotent overlay — see the comment at the call.
pub(super) fn replay_wal_and_rebuild_indexes(
    core: &mut CoreLoop,
    wal_records: &[nodedb_wal::WalRecord],
    num_cores: usize,
    tombstones: &nodedb_wal::TombstoneSet,
    vector_index_param_seed: &[nodedb_types::StoredVectorIndexParams],
) {
    // Tombstones are pre-built by the caller from
    // (persisted `_system.wal_tombstones` ∪
    // `extract_tombstones(&wal_records)`). The persisted half
    // is load-bearing once segment-truncation advances past a
    // tombstone record: the tombstone falls out of the live
    // WAL, but shadowed writes in un-truncated older segments
    // must still be skipped. Every per-engine replay method
    // consults the merged set.
    core.replay_all_wal(wal_records, num_cores, tombstones);

    // Crash-recovery backstop: rebuild the HNSW by re-indexing
    // every document from the durable redb `sparse` store. The WAL is
    // not crash-durable, so on a hard crash it may be empty on reopen
    // while the documents survived in redb. Idempotent (per-surrogate
    // remove-then-insert), so it safely overlays whatever the vector
    // checkpoint + WAL replay above already restored.
    core.rebuild_vector_indexes_from_store(vector_index_param_seed);

    // The in-memory R-tree spatial index needs no separate backstop here.
    // A document collection's geometry is indexed by the same
    // `apply_point_put_spatial` side-effect on both the live write and the
    // WAL redo path, so `replay_all_wal` above already repopulated the
    // R-tree for every document `Put` still in the WAL; state that predates
    // the last checkpoint is covered by the periodic spatial checkpoint
    // restored in `load_boot_checkpoints`. Columnar-family (`engine='spatial'`)
    // geometry is re-derived from the restored columnar rows by
    // `restore_columnar_geometry_indexes` during checkpoint restore.
}
