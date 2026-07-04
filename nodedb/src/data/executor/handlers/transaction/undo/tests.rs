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
        surrogate: nodedb_types::Surrogate::ZERO,
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
            surrogate: nodedb_types::Surrogate::ZERO,
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
        surrogate: nodedb_types::Surrogate::ZERO,
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

// ── Spatial undo ─────────────────────────────────────────────────────────────

fn spatial_key() -> (nodedb_types::DatabaseId, TenantId, String, String) {
    (
        nodedb_types::DatabaseId::new(DB),
        TenantId::new(TID),
        "c".to_string(),
        "geom".to_string(),
    )
}

fn rtree_has(core: &crate::data::executor::core_loop::CoreLoop, entry_id: u64) -> bool {
    core.spatial_indexes
        .get(&spatial_key())
        .map(|rt| rt.entries().into_iter().any(|e| e.id == entry_id))
        .unwrap_or(false)
}

#[test]
fn spatial_insert_undo_removes_entry_and_reverse_map() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    let key = spatial_key();
    let entry_id: u64 = 42;
    let bbox = nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0);

    // Seed as though a forward spatial insert had run.
    let rtree = core.spatial_indexes.entry(key.clone()).or_default();
    rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
    core.spatial_doc_map.insert(
        (key.0, key.1, key.2.clone(), key.3.clone(), entry_id),
        "d1".to_string(),
    );
    assert!(rtree_has(&core, entry_id));

    let undo = UndoEntry::SpatialInsert {
        key: key.clone(),
        entry_id,
    };
    core.apply_undo_spatial(0, undo).unwrap();

    assert!(!rtree_has(&core, entry_id), "R-tree entry must be removed");
    assert!(
        !core
            .spatial_doc_map
            .contains_key(&(key.0, key.1, key.2, key.3, entry_id)),
        "reverse map record must be removed"
    );
}

#[test]
fn spatial_delete_undo_reinserts_entry_with_bbox() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    let key = spatial_key();
    let entry_id: u64 = 7;
    let bbox = nodedb_types::BoundingBox::new(10.0, 20.0, 30.0, 40.0);

    // R-tree starts empty (the forward op removed the entry).
    assert!(!rtree_has(&core, entry_id));

    let undo = UndoEntry::SpatialDelete {
        key: key.clone(),
        entry_id,
        bbox,
        document_id: "d1".to_string(),
    };
    core.apply_undo_spatial(0, undo).unwrap();

    let restored = core
        .spatial_indexes
        .get(&key)
        .and_then(|rt| rt.entries().into_iter().find(|e| e.id == entry_id).cloned());
    let restored = restored.expect("R-tree entry must be re-inserted");
    assert_eq!(
        restored.bbox, bbox,
        "restored bbox must match captured bbox"
    );
    assert_eq!(
        core.spatial_doc_map
            .get(&(key.0, key.1, key.2, key.3, entry_id))
            .map(String::as_str),
        Some("d1"),
        "reverse map record must be restored"
    );
}

// ── Vector undo (vector_doc_map symmetry) ───────────────────────────────────

fn vector_index_key() -> (nodedb_types::DatabaseId, TenantId, String) {
    crate::data::executor::core_loop::CoreLoop::vector_index_key(DB, TID, "c", "emb")
}

fn vector_doc_key() -> (nodedb_types::DatabaseId, TenantId, String, String, String) {
    let key = vector_index_key();
    (
        key.0,
        key.1,
        "c".to_string(),
        "emb".to_string(),
        "d1".to_string(),
    )
}

/// A rolled-back transactional document INSERT must remove the stale
/// `vector_doc_map` entry the forward `apply_point_put_vector_indexes`
/// insert created — otherwise the reverse doc→vector_id mapping leaks
/// unboundedly (it never gets cleaned up since the document that would have
/// triggered a delete cascade doesn't actually exist post-rollback). Mirrors
/// `spatial_insert_undo_removes_entry_and_reverse_map`.
#[test]
fn vector_insert_undo_removes_stale_doc_map_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    let index_key = vector_index_key();
    let coll = core
        .vector_collections
        .entry(index_key.clone())
        .or_insert_with(|| nodedb_vector::VectorCollection::new(2, Default::default()));
    let vector_id = coll.insert_with_surrogate(vec![1.0, 2.0], nodedb_types::Surrogate::ZERO);

    // Seed as though the forward `apply_point_put_vector_indexes` insert had
    // run: it populates `vector_doc_map` alongside the HNSW insert.
    core.vector_doc_map.insert(vector_doc_key(), vector_id);
    assert!(core.vector_doc_map.contains_key(&vector_doc_key()));

    let undo = UndoEntry::InsertVector {
        index_key,
        vector_id,
        collection: "c".to_string(),
        field: "emb".to_string(),
        doc_id: "d1".to_string(),
    };
    core.apply_undo_vector(TID, 0, undo).unwrap();

    assert!(
        !core.vector_doc_map.contains_key(&vector_doc_key()),
        "stale vector_doc_map entry must be removed on rolled-back insert"
    );
}

/// A rolled-back transactional document DELETE must restore the
/// `vector_doc_map` entry the forward delete cascade removed — otherwise the
/// doc→vector reverse lookup is permanently missing and a later delete of the
/// same document can never find (and soft-delete) its vector: a permanent
/// orphan. Mirrors `spatial_delete_undo_reinserts_entry_with_bbox`. Also
/// verifies the restored mapping is immediately usable by a subsequent delete
/// cascade lookup (the exact key `apply_point_delete` probes).
#[test]
fn vector_delete_undo_restores_doc_map_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    let index_key = vector_index_key();
    let coll = core
        .vector_collections
        .entry(index_key.clone())
        .or_insert_with(|| nodedb_vector::VectorCollection::new(2, Default::default()));
    let vector_id = coll.insert_with_surrogate(vec![3.0, 4.0], nodedb_types::Surrogate::ZERO);
    coll.delete(vector_id);

    // The forward delete cascade already removed the reverse-map entry (as
    // `apply_point_delete` does) — it must be absent before undo runs.
    assert!(!core.vector_doc_map.contains_key(&vector_doc_key()));

    let undo = UndoEntry::DeleteVector {
        index_key,
        vector_id,
        collection: "c".to_string(),
        field: "emb".to_string(),
        doc_id: "d1".to_string(),
    };
    core.apply_undo_vector(TID, 0, undo).unwrap();

    assert_eq!(
        core.vector_doc_map.get(&vector_doc_key()).copied(),
        Some(vector_id),
        "vector_doc_map entry must be restored so a later delete can find the vector again"
    );
}

// ── Graph edge-cascade undo ─────────────────────────────────────────────────

/// A rolled-back transactional document DELETE must restore every edge the
/// unconditional graph-edge cascade removed — into BOTH the persistent edge
/// store (`get_edge`) AND the in-memory CSR partition (`neighbors`), with the
/// original edge properties intact. This exercises the full capture→restore
/// path: `delete_edges_for_node` returns the removed edges, and
/// `apply_undo_edge` re-inserts each via a `DeleteEdge` undo entry.
#[test]
fn edge_cascade_delete_rollback_restores_csr_and_edge_store() {
    use crate::engine::graph::csr::Direction;
    use crate::engine::graph::edge_store::EdgeRef;

    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let tenant = TenantId::new(TID);

    // Seed alice-[KNOWS]->bob in BOTH stores, as a forward EdgePut would.
    let seed_ord = core.hlc.next_ordinal();
    core.edge_store
        .put_edge_versioned(
            EdgeRef::new(
                nodedb_types::DatabaseId::new(DB),
                tenant,
                "c",
                "alice",
                "KNOWS",
                "bob",
            ),
            b"p1",
            seed_ord,
            nodedb_types::ordinal_to_ms(seed_ord),
            i64::MAX,
        )
        .unwrap();
    core.csr_partition_mut(DB, TID)
        .add_edge("alice", "KNOWS", "bob")
        .unwrap();

    // Sanity: edge present in both stores.
    assert_eq!(
        core.edge_store
            .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
            .unwrap(),
        Some(b"p1".to_vec())
    );
    assert_eq!(
        core.csr_partition_mut(DB, TID)
            .neighbors("alice", None, Direction::Out),
        vec![("KNOWS".to_string(), "bob".to_string())]
    );

    // Forward document-delete cascade (Cascade 3): remove from CSR + edge store,
    // capturing the removed edges for rollback.
    core.csr_partition_mut(DB, TID).remove_node_edges("alice");
    let cascade_ord = core.hlc.next_ordinal();
    let removed = core
        .edge_store
        .delete_edges_for_node(DB, tenant, "alice", cascade_ord)
        .unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(
        removed[0],
        (
            "c".to_string(),
            "alice".to_string(),
            "KNOWS".to_string(),
            "bob".to_string(),
            b"p1".to_vec()
        )
    );

    // Both stores now show the edge gone.
    assert!(
        core.edge_store
            .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
            .unwrap()
            .is_none()
    );
    assert!(
        core.csr_partition_mut(DB, TID)
            .neighbors("alice", None, Direction::Out)
            .is_empty()
    );

    // Rollback: push one DeleteEdge undo per captured edge and apply it.
    for (idx, (collection, src_id, label, dst_id, old_properties)) in
        removed.into_iter().enumerate()
    {
        let undo = UndoEntry::DeleteEdge {
            collection,
            src_id,
            label,
            dst_id,
            old_properties,
        };
        core.apply_undo_edge(DB, TID, idx, undo).unwrap();
    }

    // Both stores fully restored, properties intact.
    assert_eq!(
        core.edge_store
            .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
            .unwrap(),
        Some(b"p1".to_vec()),
        "edge store must be restored with original properties"
    );
    assert_eq!(
        core.csr_partition_mut(DB, TID)
            .neighbors("alice", None, Direction::Out),
        vec![("KNOWS".to_string(), "bob".to_string())],
        "CSR adjacency must be restored"
    );
}

// ── Column-stats undo ─────────────────────────────────────────────────────────

fn stats_key_str() -> String {
    format!("{DB}:{TID}:c:name")
}

/// Serialize a `ColumnStats` built from the given observed values, returning
/// both the value and its wire bytes (the pre-image shape `StatsRestore` holds).
fn make_stats(values: &[&str]) -> (crate::engine::sparse::stats::ColumnStats, Vec<u8>) {
    let mut stats = crate::engine::sparse::stats::ColumnStats::new();
    for v in values {
        stats.observe(Some(&serde_json::Value::String((*v).to_string())));
    }
    let bytes = zerompk::to_msgpack_vec(&stats).unwrap();
    (stats, bytes)
}

#[test]
fn stats_restore_undo_rewrites_prior_image() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    // Seed the original (pre-op) stats and capture its exact bytes.
    let (original, original_bytes) = make_stats(&["alice"]);
    core.stats_store
        .put(DB, TID, "c", "name", &original)
        .unwrap();

    // Simulate the read-modify-write op having merged another value and
    // committed the mutated stats.
    let (mutated, _) = make_stats(&["alice", "bob"]);
    core.stats_store
        .put(DB, TID, "c", "name", &mutated)
        .unwrap();
    assert_eq!(
        core.stats_store
            .get(DB, TID, "c", "name")
            .unwrap()
            .unwrap()
            .row_count,
        2,
        "mutated stats must be observed before undo"
    );

    // Rollback restores the exact pre-image.
    let undo = UndoEntry::StatsRestore {
        key: stats_key_str(),
        prior: Some(original_bytes),
    };
    core.apply_undo_stats(0, undo).unwrap();

    let restored = core.stats_store.get(DB, TID, "c", "name").unwrap().unwrap();
    assert_eq!(
        restored.row_count, original.row_count,
        "row_count must match pre-image"
    );
    assert_eq!(
        restored.non_null_count, 1,
        "non_null_count must match pre-image"
    );
    assert_eq!(restored.min_value.as_deref(), Some("alice"));
    assert_eq!(
        restored.max_value.as_deref(),
        Some("alice"),
        "'bob' merge must be reversed"
    );
}

#[test]
fn stats_restore_undo_removes_key_when_no_prior() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    // The op created stats for a (coll, field) that had none before.
    let (created, _) = make_stats(&["carol"]);
    core.stats_store
        .put(DB, TID, "c", "name", &created)
        .unwrap();
    assert!(
        core.stats_store
            .get(DB, TID, "c", "name")
            .unwrap()
            .is_some()
    );

    // `prior = None` => undo removes the key entirely.
    let undo = UndoEntry::StatsRestore {
        key: stats_key_str(),
        prior: None,
    };
    core.apply_undo_stats(0, undo).unwrap();

    assert!(
        core.stats_store
            .get(DB, TID, "c", "name")
            .unwrap()
            .is_none(),
        "key with no prior image must be removed on undo"
    );
}
