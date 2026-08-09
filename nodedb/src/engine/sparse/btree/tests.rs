// SPDX-License-Identifier: BUSL-1.1

use super::SparseEngine;

fn open_temp() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
    (engine, dir)
}

#[test]
fn put_and_get() {
    let (engine, _dir) = open_temp();
    engine.put(0, 1, "users", "u1", b"alice").unwrap();
    engine.put(0, 1, "users", "u2", b"bob").unwrap();
    assert_eq!(
        engine.get(0, 1, "users", "u1").unwrap(),
        Some(b"alice".to_vec())
    );
    assert_eq!(
        engine.get(0, 1, "users", "u2").unwrap(),
        Some(b"bob".to_vec())
    );
    assert_eq!(engine.get(0, 1, "users", "u3").unwrap(), None);
}

#[test]
fn databases_are_isolated() {
    let (engine, _dir) = open_temp();
    engine.put(0, 1, "users", "u1", b"alice").unwrap();
    engine.put(7, 1, "users", "u1", b"alice-db7").unwrap();
    assert_eq!(
        engine.get(0, 1, "users", "u1").unwrap(),
        Some(b"alice".to_vec())
    );
    assert_eq!(
        engine.get(7, 1, "users", "u1").unwrap(),
        Some(b"alice-db7".to_vec())
    );
}

#[test]
fn put_overwrites() {
    let (engine, _dir) = open_temp();
    engine.put(0, 1, "users", "u1", b"alice").unwrap();
    engine.put(0, 1, "users", "u1", b"ALICE").unwrap();
    assert_eq!(
        engine.get(0, 1, "users", "u1").unwrap(),
        Some(b"ALICE".to_vec())
    );
}

#[test]
fn delete_removes() {
    let (engine, _dir) = open_temp();
    engine.put(0, 1, "users", "u1", b"alice").unwrap();
    assert_eq!(
        engine.delete(0, 1, "users", "u1").unwrap(),
        Some(b"alice".to_vec())
    );
    assert_eq!(engine.get(0, 1, "users", "u1").unwrap(), None);
    assert_eq!(engine.delete(0, 1, "users", "u1").unwrap(), None);
}

#[test]
fn range_scan_with_index() {
    let (engine, _dir) = open_temp();
    engine.index_put(0, 1, "users", "age", "025", "u1").unwrap();
    engine.index_put(0, 1, "users", "age", "030", "u2").unwrap();
    engine.index_put(0, 1, "users", "age", "035", "u3").unwrap();
    engine.index_put(0, 1, "users", "age", "040", "u4").unwrap();
    let results = engine
        .range_scan(crate::engine::sparse::btree_index::RangeScanParams {
            database_id: 0,
            tenant_id: 1,
            collection: "users",
            field: "age",
            lower: Some(b"025"),
            upper: Some(b"036"),
            limit: 10,
        })
        .unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn collections_are_isolated() {
    let (engine, _dir) = open_temp();
    engine.put(0, 1, "users", "u1", b"alice").unwrap();
    engine.put(0, 1, "orders", "u1", b"order-1").unwrap();
    assert_eq!(
        engine.get(0, 1, "users", "u1").unwrap(),
        Some(b"alice".to_vec())
    );
    assert_eq!(
        engine.get(0, 1, "orders", "u1").unwrap(),
        Some(b"order-1".to_vec())
    );
}

#[test]
fn delete_index_entries_for_field() {
    let (engine, _dir) = open_temp();
    engine
        .index_put(0, 1, "users", "email", "alice@example.com", "u1")
        .unwrap();
    engine
        .index_put(0, 1, "users", "email", "bob@example.com", "u2")
        .unwrap();
    engine.index_put(0, 1, "users", "age", "30", "u1").unwrap();
    engine.index_put(0, 1, "users", "age", "25", "u2").unwrap();
    let removed = engine
        .delete_index_entries_for_field(0, 1, "users", "email")
        .unwrap();
    assert_eq!(removed, 2);
    let age_entries = engine.scan_index_groups(0, 1, "users", "age").unwrap();
    assert_eq!(age_entries.len(), 2);
    let email_entries = engine.scan_index_groups(0, 1, "users", "email").unwrap();
    assert!(email_entries.is_empty());
}

/// A chain head written inside a caller-owned transaction must be readable
/// after that transaction commits, and must survive reopening the database —
/// the whole point of persisting it.
#[test]
fn chain_heads_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sparse.redb");
    {
        let engine = SparseEngine::open(&path).unwrap();
        let txn = engine.begin_write().unwrap();
        engine
            .put_chain_head_in_txn(&txn, 0, 1, "ledger", "abc123")
            .unwrap();
        txn.commit().unwrap();
    }
    let engine = SparseEngine::open(&path).unwrap();
    assert_eq!(
        engine.get_chain_head(0, 1, "ledger").unwrap(),
        Some("abc123".to_string())
    );
    let heads = engine.load_chain_heads().unwrap();
    assert_eq!(
        heads.get(&(
            nodedb_types::DatabaseId::new(0),
            nodedb_types::TenantId::new(1),
            "ledger".to_string()
        )),
        Some(&"abc123".to_string())
    );
}

/// A dropped head must not be resurrected by the tenant sweep or by a reopen.
#[test]
fn chain_heads_are_removable_per_collection_and_per_tenant() {
    let (engine, _dir) = open_temp();
    engine.put_chain_head(0, 1, "ledger", "h1").unwrap();
    engine.put_chain_head(0, 1, "audit", "h2").unwrap();
    engine.put_chain_head(0, 2, "ledger", "h3").unwrap();

    engine.delete_chain_head(0, 1, "ledger").unwrap();
    assert_eq!(engine.get_chain_head(0, 1, "ledger").unwrap(), None);

    assert_eq!(engine.delete_chain_heads_for_tenant(1).unwrap(), 1);
    assert_eq!(engine.get_chain_head(0, 1, "audit").unwrap(), None);
    assert_eq!(
        engine.get_chain_head(0, 2, "ledger").unwrap(),
        Some("h3".to_string())
    );
}

/// Renaming a collection moves its chain head with its rows. Leaving the head
/// under the old name would restart the renamed collection at genesis while
/// its already-chained rows travelled to the new name.
#[test]
fn rename_moves_the_chain_head() {
    let (engine, _dir) = open_temp();
    engine.put(0, 1, "ledger", "0000002a", b"row").unwrap();
    engine.put_chain_head(0, 1, "ledger", "h1").unwrap();

    engine
        .rename_collection(0, 0, 1, "ledger", "journal")
        .unwrap();

    assert_eq!(engine.get_chain_head(0, 1, "ledger").unwrap(), None);
    assert_eq!(
        engine.get_chain_head(0, 1, "journal").unwrap(),
        Some("h1".to_string())
    );
}
