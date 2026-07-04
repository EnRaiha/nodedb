// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for document undo — bitemporal version reversal, hash-chain
//! reversal, and backward-compatibility of the plain (non-versioned) path.

use super::UndoEntry;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
use crate::types::TenantId;

const DB: u64 = 0;
const TID: u64 = 1;

fn seed_version(core: &crate::data::executor::core_loop::CoreLoop, doc: &str, t: i64, body: &[u8]) {
    core.sparse
        .versioned_put(VersionedPut {
            database_id: DB,
            tenant: TID,
            coll: "c",
            doc_id: doc,
            sys_from_ms: t,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
            body,
        })
        .unwrap();
}

fn seed_index(core: &crate::data::executor::core_loop::CoreLoop, doc: &str, t: i64) {
    core.sparse
        .versioned_index_put(VersionedIndexEntry {
            database_id: DB,
            tenant: TID,
            coll: "c",
            field: "status",
            value: "active",
            doc_id: doc,
            sys_from_ms: t,
        })
        .unwrap();
}

fn index_lookup(core: &crate::data::executor::core_loop::CoreLoop) -> Vec<String> {
    core.sparse
        .versioned_index_lookup_as_of(DB, TID, "c", "status", "active", None)
        .unwrap()
}

#[test]
fn bitemporal_put_undo_removes_version_and_index() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    let t = 1_000;
    seed_version(&core, "d1", t, b"v1");
    seed_index(&core, "d1", t);

    assert!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap()
            .is_some()
    );
    assert_eq!(index_lookup(&core), vec!["d1".to_string()]);

    let entry = UndoEntry::PutDocument {
        collection: "c".into(),
        document_id: "d1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        old_value: None,
        bitemporal_sys_from_ms: Some(t),
        bitemporal_index_tuples: vec![("status".into(), "active".into())],
        secondary_index_added: Vec::new(),
        secondary_index_removed: Vec::new(),
        chain_hash_prior: None,
    };
    core.apply_undo_document(TID, 0, entry).unwrap();

    assert!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap()
            .is_none(),
        "version row must be physically gone"
    );
    assert!(index_lookup(&core).is_empty(), "index entry must be gone");
}

#[test]
fn bitemporal_delete_undo_restores_prior_live_version() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    // Live version at T1, then a tombstone at T2 (plus an index tombstone).
    seed_version(&core, "d1", 1_000, b"v1");
    seed_index(&core, "d1", 1_000);
    core.sparse
        .versioned_tombstone(DB, TID, "c", "d1", 2_000)
        .unwrap();
    core.sparse
        .versioned_index_tombstone(VersionedIndexEntry {
            database_id: DB,
            tenant: TID,
            coll: "c",
            field: "status",
            value: "active",
            doc_id: "d1",
            sys_from_ms: 2_000,
        })
        .unwrap();

    // Tombstone hides the row.
    assert!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap()
            .is_none()
    );

    let entry = UndoEntry::DeleteDocument {
        collection: "c".into(),
        document_id: "d1".into(),
        old_value: b"v1".to_vec(),
        bitemporal_sys_from_ms: Some(2_000),
        bitemporal_index_tuples: vec![("status".into(), "active".into())],
        secondary_index_tuples: Vec::new(),
        chain_hash_prior: None,
    };
    core.apply_undo_document(TID, 0, entry).unwrap();

    // Removing the tombstone restores the prior live version as current.
    assert_eq!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(index_lookup(&core), vec!["d1".to_string()]);
}

#[test]
fn chain_hash_undo_restores_prior_and_removes_genesis() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let key = || (TenantId::new(TID), "c".to_string());

    // Restore-to-prior case: map holds "h1", undo restores "h0".
    core.chain_hashes.insert(key(), "h1".into());
    let restore = UndoEntry::PutDocument {
        collection: "c".into(),
        document_id: "nonexistent".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        old_value: None,
        bitemporal_sys_from_ms: None,
        bitemporal_index_tuples: Vec::new(),
        secondary_index_added: Vec::new(),
        secondary_index_removed: Vec::new(),
        chain_hash_prior: Some(Some("h0".into())),
    };
    core.apply_undo_document(TID, 0, restore).unwrap();
    assert_eq!(
        core.chain_hashes.get(&key()).map(String::as_str),
        Some("h0")
    );

    // Genesis case: undo removes the key entirely.
    let genesis = UndoEntry::PutDocument {
        collection: "c".into(),
        document_id: "nonexistent".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        old_value: None,
        bitemporal_sys_from_ms: None,
        bitemporal_index_tuples: Vec::new(),
        secondary_index_added: Vec::new(),
        secondary_index_removed: Vec::new(),
        chain_hash_prior: Some(None),
    };
    core.apply_undo_document(TID, 0, genesis).unwrap();
    assert!(!core.chain_hashes.contains_key(&key()));
}

/// Scenario 4 (unit level): a rolled-back transaction that does a
/// bitemporal PUT followed by a bitemporal DELETE (tombstone) must, via
/// `rollback_undo_log` — the same reverse-order driver `execute_transaction_batch`
/// uses on abort — restore `core.sparse.versioned_get_current` to its
/// pre-transaction state (nothing) with the version rows and index entries
/// physically gone, not merely hidden.
#[test]
fn rollback_undo_log_restores_pre_txn_state_for_bitemporal_put_then_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    // Pre-txn state: nothing exists for "d1".
    assert!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap()
            .is_none()
    );

    // Forward tx: PUT at t=1000, then DELETE (tombstone) at t=2000.
    seed_version(&core, "d1", 1_000, b"v1");
    seed_index(&core, "d1", 1_000);
    core.sparse
        .versioned_tombstone(DB, TID, "c", "d1", 2_000)
        .unwrap();
    core.sparse
        .versioned_index_tombstone(VersionedIndexEntry {
            database_id: DB,
            tenant: TID,
            coll: "c",
            field: "status",
            value: "active",
            doc_id: "d1",
            sys_from_ms: 2_000,
        })
        .unwrap();

    // Sanity: the forward tx did delete the row (as observed mid-tx).
    assert!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap()
            .is_none()
    );

    let undo_log = vec![
        UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: "d1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            old_value: None,
            bitemporal_sys_from_ms: Some(1_000),
            bitemporal_index_tuples: vec![("status".into(), "active".into())],
            secondary_index_added: Vec::new(),
            secondary_index_removed: Vec::new(),
            chain_hash_prior: None,
        },
        UndoEntry::DeleteDocument {
            collection: "c".into(),
            document_id: "d1".into(),
            old_value: b"v1".to_vec(),
            bitemporal_sys_from_ms: Some(2_000),
            bitemporal_index_tuples: vec![("status".into(), "active".into())],
            secondary_index_tuples: Vec::new(),
            chain_hash_prior: None,
        },
    ];

    // Abort: roll back in reverse order, exactly as `execute_transaction_batch`
    // does when a sub-plan fails.
    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");

    // Pre-txn state restored: no current version, no index entry.
    assert!(
        core.sparse
            .versioned_get_current(DB, TID, "c", "d1")
            .unwrap()
            .is_none(),
        "aborted bitemporal put+delete must leave no current version behind"
    );
    assert!(
        index_lookup(&core).is_empty(),
        "aborted bitemporal put+delete must leave no index entry behind"
    );
}

#[test]
fn plain_put_undo_backward_compatible() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    // Overwrite case: current holds "new", undo restores "old".
    core.sparse.put(DB, TID, "c", "d1", b"new").unwrap();
    let overwrite = UndoEntry::PutDocument {
        collection: "c".into(),
        document_id: "d1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        old_value: Some(b"old".to_vec()),
        bitemporal_sys_from_ms: None,
        bitemporal_index_tuples: Vec::new(),
        secondary_index_added: Vec::new(),
        secondary_index_removed: Vec::new(),
        chain_hash_prior: None,
    };
    core.apply_undo_document(TID, 0, overwrite).unwrap();
    assert_eq!(
        core.sparse.get(DB, TID, "c", "d1").unwrap(),
        Some(b"old".to_vec())
    );

    // Insert case: undo deletes the row.
    core.sparse.put(DB, TID, "c", "d2", b"inserted").unwrap();
    let insert = UndoEntry::PutDocument {
        collection: "c".into(),
        document_id: "d2".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        old_value: None,
        bitemporal_sys_from_ms: None,
        bitemporal_index_tuples: Vec::new(),
        secondary_index_added: Vec::new(),
        secondary_index_removed: Vec::new(),
        chain_hash_prior: None,
    };
    core.apply_undo_document(TID, 0, insert).unwrap();
    assert!(core.sparse.get(DB, TID, "c", "d2").unwrap().is_none());
}

#[test]
fn plain_delete_undo_backward_compatible() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    // Row was deleted by the forward op; undo re-inserts its prior value.
    let entry = UndoEntry::DeleteDocument {
        collection: "c".into(),
        document_id: "d1".into(),
        old_value: b"prior".to_vec(),
        bitemporal_sys_from_ms: None,
        bitemporal_index_tuples: Vec::new(),
        secondary_index_tuples: Vec::new(),
        chain_hash_prior: None,
    };
    core.apply_undo_document(TID, 0, entry).unwrap();
    assert_eq!(
        core.sparse.get(DB, TID, "c", "d1").unwrap(),
        Some(b"prior".to_vec())
    );
}
