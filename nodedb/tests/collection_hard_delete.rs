// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for the collection hard-delete pipeline at
//! the SystemCatalog layer. Exercises the idempotency contracts the
//! pgwire `drop_collection` handler relies on, plus the per-engine
//! reclaim helpers at their public surface.
//!
//! Full end-to-end `DROP → UNDROP → INSERT` integration needs a live
//! pgwire session + running Data Plane; that surface is exercised in
//! `tests/cluster_post_apply_follower_dispatch.rs` on the raft path
//! and in `nodedb-wal/tests/wal_collection_tombstone.rs` on the
//! replay path.

mod catalog_integrity_helpers;

use nodedb::control::security::catalog::SystemCatalog;
use nodedb::data::executor::handlers::reclaim;

use catalog_integrity_helpers::{TENANT, make_catalog, make_collection};

/// `delete_collection` is idempotent per its doc comment. The
/// `drop_collection` handler short-circuits re-runs by checking
/// `get_collection` — this test locks in both sides of that
/// contract at the redb layer.
#[test]
fn delete_collection_is_idempotent_and_reflected_in_get() {
    let (_tmp, catalog) = make_catalog();
    let mut coll = make_collection("users");
    coll.is_active = true;
    catalog
        .put_collection(nodedb_types::DatabaseId::DEFAULT, &coll)
        .unwrap();

    assert!(
        catalog
            .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "users")
            .unwrap()
            .is_some()
    );

    catalog
        .delete_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "users")
        .unwrap();
    assert!(
        catalog
            .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "users")
            .unwrap()
            .is_none(),
        "post-delete get_collection must return None"
    );

    // Second call: must not error, still returns None.
    catalog
        .delete_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "users")
        .unwrap();
    catalog
        .delete_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "users")
        .unwrap();
    assert!(
        catalog
            .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "users")
            .unwrap()
            .is_none()
    );
}

/// Soft-delete flips `is_active` without removing the row — this is
/// the invariant `UNDROP` relies on and the sweeper reads to decide
/// whether the retention window has elapsed.
#[test]
fn soft_delete_preserves_row_and_clears_active_flag() {
    let (_tmp, catalog) = make_catalog();
    let mut coll = make_collection("logs");
    coll.is_active = true;
    catalog
        .put_collection(nodedb_types::DatabaseId::DEFAULT, &coll)
        .unwrap();

    // Simulate the applier's `DeactivateCollection` path: flip
    // `is_active` in place and re-put.
    let mut stored = catalog
        .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "logs")
        .unwrap()
        .unwrap();
    stored.is_active = false;
    catalog
        .put_collection(nodedb_types::DatabaseId::DEFAULT, &stored)
        .unwrap();

    let after = catalog
        .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "logs")
        .unwrap()
        .unwrap();
    assert!(
        !after.is_active,
        "is_active must be false after soft-delete"
    );
    assert_eq!(after.name, "logs", "row must still exist for UNDROP");

    // `load_dropped_collections` must surface the soft-deleted row
    // so the GC sweeper + `_system.dropped_collections` view see it.
    let dropped = catalog
        .load_dropped_collections(nodedb_types::DatabaseId::DEFAULT)
        .unwrap();
    assert!(
        dropped.iter().any(|c| c.name == "logs"),
        "soft-deleted row must appear in load_dropped_collections"
    );
}

/// L2 cleanup queue CRUD preserves the idempotency the purge pipeline
/// depends on: re-enqueue replaces in place, record-attempt updates
/// without creating a duplicate, remove is safe to call on a missing
/// key.
#[test]
fn l2_cleanup_queue_is_idempotent_end_to_end() {
    use nodedb::control::security::catalog::StoredL2CleanupEntry;

    let (_tmp, catalog) = make_catalog();
    let entry = |lsn: u64, bytes: u64, attempts: u32, err: &str| StoredL2CleanupEntry {
        database_id: 0,
        tenant_id: TENANT,
        name: "events".into(),
        purge_lsn: lsn,
        enqueued_at_ns: 100,
        bytes_pending: bytes,
        last_error: err.to_string(),
        attempts,
    };

    catalog
        .enqueue_l2_cleanup(&entry(500, 2_000, 0, ""))
        .unwrap();
    // Re-enqueue with updated fields — replaces, not appends.
    catalog
        .enqueue_l2_cleanup(&entry(700, 9_000, 0, ""))
        .unwrap();

    let rows = catalog.load_l2_cleanup_queue().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].purge_lsn, 700);
    assert_eq!(rows[0].bytes_pending, 9_000);

    // record_attempt bumps in place.
    catalog
        .record_l2_cleanup_attempt(0, TENANT, "events", "s3: 503")
        .unwrap();
    catalog
        .record_l2_cleanup_attempt(0, TENANT, "events", "s3: 503")
        .unwrap();
    let rows = catalog.load_l2_cleanup_queue().unwrap();
    assert_eq!(rows[0].attempts, 2);
    assert_eq!(rows[0].last_error, "s3: 503");

    // Remove is idempotent.
    catalog.remove_l2_cleanup(0, TENANT, "events").unwrap();
    catalog.remove_l2_cleanup(0, TENANT, "events").unwrap();
    assert!(catalog.load_l2_cleanup_queue().unwrap().is_empty());

    // record_attempt on a missing key is a no-op, not an error.
    catalog
        .record_l2_cleanup_attempt(0, TENANT, "events", "doesn't matter")
        .unwrap();
    assert!(catalog.load_l2_cleanup_queue().unwrap().is_empty());
}

/// Per-engine reclaim handlers are idempotent — missing directories
/// / files produce zero stats, not errors. This is the contract the
/// `execute_unregister_collection` retry loop relies on.
#[test]
fn reclaim_handlers_are_idempotent_on_missing_files() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // All four reclaim helpers must return default stats on a fresh
    // empty data dir.
    let vector = reclaim::vector::reclaim_vector_checkpoints(base, 0, TENANT, "x").unwrap();
    let spatial = reclaim::spatial::reclaim_spatial_checkpoints(base, 0, TENANT, "x").unwrap();
    let sparse =
        reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(base, 0, TENANT, "x").unwrap();
    let ts = reclaim::timeseries::reclaim_timeseries_partitions(base, 0, TENANT, "x").unwrap();

    assert_eq!(vector.files_unlinked, 0);
    assert_eq!(spatial.files_unlinked, 0);
    assert_eq!(sparse.files_unlinked, 0);
    assert_eq!(ts.files_unlinked, 0);

    // Re-running must still succeed (no "already deleted" error).
    reclaim::vector::reclaim_vector_checkpoints(base, 0, TENANT, "x").unwrap();
    reclaim::spatial::reclaim_spatial_checkpoints(base, 0, TENANT, "x").unwrap();
    reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(base, 0, TENANT, "x").unwrap();
    reclaim::timeseries::reclaim_timeseries_partitions(base, 0, TENANT, "x").unwrap();
}

/// Reclaim never unlinks a file it cannot prove is reachable. A checkpoint
/// directory with no published generation (no manifest) holds only debris from
/// a cycle that never committed — nothing a boot could restore — so the pass
/// must report success and touch nothing, leaving the debris to the write
/// path's own generation cleanup.
///
/// Per-collection and per-tenant scoping WITHIN a published generation is
/// covered by the unit tests next to each reclaim handler, which can build a
/// live generation through the engine's own manifest writer instead of
/// hand-rolling the on-disk layout here.
#[test]
fn reclaim_leaves_unpublished_checkpoint_debris_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    let vec_dir = base.join("vector-ckpt").join("core-0").join("gen-0");
    std::fs::create_dir_all(&vec_dir).unwrap();
    std::fs::write(vec_dir.join("0:1:users.ckpt"), b"a").unwrap();
    std::fs::write(vec_dir.join("0:1:orders.ckpt"), b"b").unwrap();

    let stats = reclaim::vector::reclaim_vector_checkpoints(base, 0, 1, "users").unwrap();
    assert_eq!(
        stats.files_unlinked, 0,
        "with no manifest there is no live generation, so nothing is reclaimable"
    );
    assert!(vec_dir.join("0:1:users.ckpt").exists());
    assert!(vec_dir.join("0:1:orders.ckpt").exists());
}

fn _cat_ref_witness(_cat: &SystemCatalog) {}
