// SPDX-License-Identifier: BUSL-1.1

//! Integration of keys + payload + write + read paths.

#![cfg(test)]

use nodedb_types::{DatabaseId, TenantId};
use redb::ReadableDatabase;

use super::{
    EdgeRef, EdgeValuePayload, GDPR_ERASURE_SENTINEL, TOMBSTONE_SENTINEL, versioned_edge_key,
};
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::graph::edge_store::store::{EDGES, REVERSE_EDGES};

const T: TenantId = TenantId::new(1);
const DB: DatabaseId = DatabaseId::DEFAULT;
const COLL: &str = "people";

fn make_store() -> (EdgeStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
    (store, dir)
}

fn e<'a>(src: &'a str, label: &'a str, dst: &'a str) -> EdgeRef<'a> {
    EdgeRef::new(DB, T, COLL, src, label, dst)
}

#[test]
fn put_and_ceiling_resolves_latest_at_cutoff() {
    let (store, _dir) = make_store();
    store
        .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
        .unwrap();
    store
        .put_edge_versioned(e("a", "L", "b"), b"v2", 200, 200, i64::MAX)
        .unwrap();
    store
        .put_edge_versioned(e("a", "L", "b"), b"v3", 300, 300, i64::MAX)
        .unwrap();

    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 99, None)
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 100, None)
            .unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 250, None)
            .unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, None)
            .unwrap(),
        Some(b"v3".to_vec())
    );
}

#[test]
fn soft_delete_shadows_prior_version() {
    let (store, _dir) = make_store();
    store
        .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
        .unwrap();
    store.soft_delete_edge(e("a", "L", "b"), 200).unwrap();

    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 150, None)
            .unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 200, None)
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, None)
            .unwrap(),
        None
    );
}

#[test]
fn gdpr_erase_distinct_from_tombstone_but_both_hide() {
    let (store, _dir) = make_store();
    store
        .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
        .unwrap();
    store.gdpr_erase_edge(e("a", "L", "b"), 200).unwrap();

    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 150, None)
            .unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 300, None)
            .unwrap(),
        None
    );

    // Raw read confirms the sentinel byte is 0xFE, distinct from 0xFF.
    let key = versioned_edge_key(COLL, "a", "L", "b", 200).unwrap();
    let txn = store.db.begin_read().unwrap();
    let table = txn.open_table(EDGES).unwrap();
    let val = table
        .get((DB.as_u64(), T.as_u64(), key.as_str()))
        .unwrap()
        .unwrap();
    assert_eq!(val.value(), GDPR_ERASURE_SENTINEL);
    assert_ne!(val.value(), TOMBSTONE_SENTINEL);
}

#[test]
fn valid_time_filter_skips_nonmatching_versions() {
    let (store, _dir) = make_store();
    // v1: valid_time [0, 100)
    store
        .put_edge_versioned(e("a", "L", "b"), b"v1", 10, 0, 100)
        .unwrap();
    // v2: valid_time [200, 300)  — disjoint hole between 100 and 200
    store
        .put_edge_versioned(e("a", "L", "b"), b"v2", 20, 200, 300)
        .unwrap();

    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(150))
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(50))
            .unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(250))
            .unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn reverse_index_is_versioned_symmetrically() {
    let (store, _dir) = make_store();
    store
        .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
        .unwrap();

    let rev_key = versioned_edge_key(COLL, "b", "L", "a", 100).unwrap();
    let txn = store.db.begin_read().unwrap();
    let table = txn.open_table(REVERSE_EDGES).unwrap();
    let val = table
        .get((DB.as_u64(), T.as_u64(), rev_key.as_str()))
        .unwrap()
        .unwrap();
    assert!(val.value().is_empty());

    // Payload encoding check.
    let p = EdgeValuePayload::new(0, i64::MAX, b"x".to_vec());
    assert_eq!(p.encode().unwrap()[0], 0x93);
}

#[test]
fn close_referrer_edge_appends_closed_version() {
    let (store, _dir) = make_store();
    store
        .put_edge_versioned(e("a", "L", "b"), b"props", 100, 100, i64::MAX)
        .unwrap();

    // Close at system=200, valid_until=500.
    let closed = store
        .close_referrer_edge(e("a", "L", "b"), 200, 500)
        .unwrap();
    assert!(closed);

    // Read inside the closed valid interval still returns the props.
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(300))
            .unwrap(),
        Some(b"props".to_vec())
    );
}

#[test]
fn close_referrer_edge_noop_when_already_closed_or_absent() {
    let (store, _dir) = make_store();
    // No prior version → false.
    let r = store
        .close_referrer_edge(e("missing", "L", "x"), 100, 200)
        .unwrap();
    assert!(!r);

    // Tombstoned referent → false.
    store
        .put_edge_versioned(e("a", "L", "b"), b"p", 50, 50, i64::MAX)
        .unwrap();
    store.soft_delete_edge(e("a", "L", "b"), 60).unwrap();
    let r = store
        .close_referrer_edge(e("a", "L", "b"), 200, 100)
        .unwrap();
    assert!(!r);
}
