// SPDX-License-Identifier: BUSL-1.1

//! Boot stage 3 of 3: replay the WAL, then run the crash-recovery rebuild
//! backstops.
//!
//! Runs last. See `replay_wal_and_rebuild_indexes` for why that order is the
//! only sound one.

use crate::data::executor::core_loop::CoreLoop;

/// Replay WAL records for crash recovery, then re-index the HNSW and R-tree from
/// the durable store.
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
/// The two rebuild backstops run after the replay within this function for the
/// same reason they are idempotent overlays — see the comments at each call.
pub(super) fn replay_wal_and_rebuild_indexes(
    core: &mut CoreLoop,
    wal_records: &[nodedb_wal::WalRecord],
    num_cores: usize,
    tombstones: &nodedb_wal::TombstoneSet,
    vector_index_param_seed: &[nodedb_types::StoredVectorIndexParams],
    spatial_collection_seed: &[(u64, String)],
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

    // Same crash-recovery backstop for the in-memory R-tree spatial
    // index: spatial checkpoints run only on a manual snapshot and the
    // WAL is not crash-durable, so on a hard crash the R-tree may come
    // back empty while the geometry documents survived in redb. Re-index
    // every geometry document from the durable store. Idempotent (per-
    // document remove-then-insert), so it safely overlays whatever the
    // spatial checkpoint + WAL replay above already restored.
    core.rebuild_spatial_indexes_from_store(spatial_collection_seed);
}
