// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for the versioned document + index tables.

use super::value::{VersionedIndexEntry, VersionedPut, VersionedScanParams};
use crate::engine::sparse::btree::SparseEngine;

fn open_temp() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = SparseEngine::open(&dir.path().join("v.redb")).unwrap();
    (engine, dir)
}

fn put(e: &SparseEngine, coll: &str, id: &str, sys_from: i64, body: &[u8]) {
    e.versioned_put(VersionedPut {
        database_id: 1,
        tenant: 1,
        coll,
        doc_id: id,
        sys_from_ms: sys_from,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
        body,
    })
    .unwrap();
}

fn idx_entry<'a>(
    coll: &'a str,
    field: &'a str,
    value: &'a str,
    doc_id: &'a str,
    sys_from_ms: i64,
) -> VersionedIndexEntry<'a> {
    VersionedIndexEntry {
        database_id: 1,
        tenant: 1,
        coll,
        field,
        value,
        doc_id,
        sys_from_ms,
    }
}

fn put_valid(
    e: &SparseEngine,
    coll: &str,
    id: &str,
    sys_from: i64,
    valid_from: i64,
    valid_until: i64,
    body: &[u8],
) {
    e.versioned_put(VersionedPut {
        database_id: 1,
        tenant: 1,
        coll,
        doc_id: id,
        sys_from_ms: sys_from,
        valid_from_ms: valid_from,
        valid_until_ms: valid_until,
        body,
    })
    .unwrap();
}

#[test]
fn put_and_read_current() {
    let (e, _d) = open_temp();
    put(&e, "users", "u1", 100, b"v1");
    let got = e.versioned_get_current(1, 1, "users", "u1").unwrap();
    assert_eq!(got.as_deref(), Some(b"v1" as &[u8]));
}

#[test]
fn ceiling_picks_newest_le_cutoff() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"a");
    put(&e, "c", "k", 200, b"b");
    put(&e, "c", "k", 300, b"c");
    assert_eq!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(150), None)
            .unwrap()
            .as_deref(),
        Some(b"a" as &[u8])
    );
    assert_eq!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(250), None)
            .unwrap()
            .as_deref(),
        Some(b"b" as &[u8])
    );
    assert_eq!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(400), None)
            .unwrap()
            .as_deref(),
        Some(b"c" as &[u8])
    );
}

#[test]
fn ceiling_before_first_version_is_none() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 200, b"x");
    assert!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(100), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn tombstone_hides_row_at_and_after_cutoff() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"x");
    e.versioned_tombstone(1, 1, "c", "k", 200).unwrap();
    assert_eq!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(150), None)
            .unwrap()
            .as_deref(),
        Some(b"x" as &[u8])
    );
    assert!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(250), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn valid_time_predicate_skips_out_of_window_versions() {
    let (e, _d) = open_temp();
    put_valid(&e, "c", "k", 10, 0, 100, b"v1");
    put_valid(&e, "c", "k", 20, 200, 300, b"v2");
    // valid-time hole at 150: neither version applies.
    assert!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(10_000), Some(150))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(10_000), Some(50))
            .unwrap()
            .as_deref(),
        Some(b"v1" as &[u8])
    );
    assert_eq!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(10_000), Some(250))
            .unwrap()
            .as_deref(),
        Some(b"v2" as &[u8])
    );
}

#[test]
fn gdpr_erase_preserves_history_structure_but_hides_body() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"pii");
    put(&e, "c", "k", 200, b"more-pii");
    let n = e.versioned_gdpr_erase(1, 1, "c", "k").unwrap();
    assert_eq!(n, 2);
    assert!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(150), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn scan_returns_latest_per_doc_id() {
    let (e, _d) = open_temp();
    put(&e, "c", "a", 100, b"a1");
    put(&e, "c", "a", 200, b"a2");
    put(&e, "c", "b", 150, b"b1");
    let all = e
        .versioned_scan_as_of(
            VersionedScanParams {
                database_id: 1,
                tenant: 1,
                coll: "c",
                sys_cutoff_ms: None,
                valid_at_ms: None,
                limit: 100,
            },
            &|_: &[u8]| true,
        )
        .unwrap();
    let map: std::collections::HashMap<_, _> = all.into_iter().collect();
    assert_eq!(map.get("a").map(|v| v.as_slice()), Some(b"a2" as &[u8]));
    assert_eq!(map.get("b").map(|v| v.as_slice()), Some(b"b1" as &[u8]));
}

#[test]
fn scan_all_returns_every_version_in_system_time_order() {
    let (e, _d) = open_temp();
    // One document updated three times under different system times.
    put(&e, "c", "a", 100, b"a1");
    put(&e, "c", "a", 200, b"a2");
    put(&e, "c", "a", 300, b"a3");
    // A second document interleaved by system time.
    put(&e, "c", "b", 150, b"b1");

    let all = e
        .versioned_scan_all(1, 1, "c", None, 100, &|_: &[u8]| true)
        .unwrap();
    // Every version is present (no newest-per-id collapse).
    assert_eq!(all.len(), 4);
    // Ascending by system time globally.
    let times: Vec<i64> = all.iter().map(|r| r.system_from_ms).collect();
    assert_eq!(times, vec![100, 150, 200, 300]);
    // System-time and body line up per version.
    let row_of =
        |r: &super::doc::VersionedRow| (r.doc_id.clone(), r.system_from_ms, r.body.clone());
    assert_eq!(row_of(&all[0]), ("a".to_string(), 100, b"a1".to_vec()));
    assert_eq!(row_of(&all[1]), ("b".to_string(), 150, b"b1".to_vec()));
    assert_eq!(row_of(&all[2]), ("a".to_string(), 200, b"a2".to_vec()));
    assert_eq!(row_of(&all[3]), ("a".to_string(), 300, b"a3".to_vec()));
}

#[test]
fn scan_all_skips_tombstoned_versions() {
    let (e, _d) = open_temp();
    put(&e, "c", "a", 100, b"a1");
    e.versioned_tombstone(1, 1, "c", "a", 200).unwrap();
    put(&e, "c", "a", 300, b"a3");
    let all = e
        .versioned_scan_all(1, 1, "c", None, 100, &|_: &[u8]| true)
        .unwrap();
    // The tombstone version is excluded; the two live versions remain.
    let times: Vec<i64> = all.iter().map(|r| r.system_from_ms).collect();
    assert_eq!(times, vec![100, 300]);
}

#[test]
fn scan_all_pushes_predicate_down_so_limit_counts_matches() {
    // Regression: the audit-log handler used to fetch a capped window then
    // filter, so a selective predicate silently under-returned. The predicate
    // is now applied inside the scan, before `limit` truncation, so `limit`
    // counts MATCHING versions — not raw scanned rows.
    let (e, _d) = open_temp();
    for i in 0..10i64 {
        put(&e, "c", "a", 100 + i, format!("v{i}").as_bytes());
    }
    // Match only odd-suffixed bodies: v1, v3, v5, v7, v9.
    let odd = |body: &[u8]| body.last().map(|b| (b - b'0') % 2 == 1).unwrap_or(false);

    let rows = e.versioned_scan_all(1, 1, "c", None, 3, &odd).unwrap();
    assert_eq!(
        rows.len(),
        3,
        "limit must count matching versions, not scanned rows"
    );
    let bodies: Vec<Vec<u8>> = rows.into_iter().map(|r| r.body).collect();
    // Oldest three matches in ascending system-time order.
    assert_eq!(bodies, vec![b"v1".to_vec(), b"v3".to_vec(), b"v5".to_vec()]);
}

#[test]
fn scan_as_of_pushes_predicate_down_so_limit_counts_matches() {
    // Same regression for the point-in-time (newest-per-doc) scan: the `limit`
    // early-stop must count matching documents, so a selective filter cannot
    // make the scan return fewer rows than exist.
    let (e, _d) = open_temp();
    for (i, id) in ["a", "b", "c", "d", "e", "f"].iter().enumerate() {
        put(&e, "c", id, 100 + i as i64, format!("x{i}").as_bytes());
    }
    // Match only even-suffixed bodies: x0 (a), x2 (c), x4 (e).
    let even = |body: &[u8]| {
        body.last()
            .map(|b| (b - b'0').is_multiple_of(2))
            .unwrap_or(false)
    };

    let rows = e
        .versioned_scan_as_of(
            VersionedScanParams {
                database_id: 1,
                tenant: 1,
                coll: "c",
                sys_cutoff_ms: None,
                valid_at_ms: None,
                limit: 2,
            },
            &even,
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "limit must count matching docs, not scanned rows"
    );
    for (_, body) in &rows {
        assert_eq!(
            (body.last().unwrap() - b'0') % 2,
            0,
            "only even-suffixed docs match"
        );
    }
}

#[test]
fn scan_as_of_hides_tombstoned_rows() {
    let (e, _d) = open_temp();
    put(&e, "c", "a", 100, b"a1");
    e.versioned_tombstone(1, 1, "c", "a", 200).unwrap();
    let at_150 = e
        .versioned_scan_as_of(
            VersionedScanParams {
                database_id: 1,
                tenant: 1,
                coll: "c",
                sys_cutoff_ms: Some(150),
                valid_at_ms: None,
                limit: 100,
            },
            &|_: &[u8]| true,
        )
        .unwrap();
    assert_eq!(at_150.len(), 1);
    let at_250 = e
        .versioned_scan_as_of(
            VersionedScanParams {
                database_id: 1,
                tenant: 1,
                coll: "c",
                sys_cutoff_ms: Some(250),
                valid_at_ms: None,
                limit: 100,
            },
            &|_: &[u8]| true,
        )
        .unwrap();
    assert!(at_250.is_empty());
}

#[test]
fn index_lookup_honors_cutoff_and_tombstone() {
    let (e, _d) = open_temp();
    e.versioned_index_put(idx_entry("c", "email", "a@x", "u1", 100))
        .unwrap();
    e.versioned_index_put(idx_entry("c", "email", "a@x", "u2", 150))
        .unwrap();
    e.versioned_index_tombstone(idx_entry("c", "email", "a@x", "u1", 200))
        .unwrap();

    let at_120 = e
        .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(120))
        .unwrap();
    assert_eq!(at_120, vec!["u1"]);

    let at_175 = e
        .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(175))
        .unwrap();
    assert_eq!(at_175.len(), 2);

    let at_250 = e
        .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(250))
        .unwrap();
    assert_eq!(at_250, vec!["u2"]);
}

#[test]
fn versioned_remove_in_txn_deletes_the_version() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"v1");
    assert_eq!(
        e.versioned_get_current(1, 1, "c", "k").unwrap().as_deref(),
        Some(b"v1" as &[u8])
    );

    let txn = e.db.begin_write().unwrap();
    e.versioned_remove_in_txn(&txn, 1, 1, "c", "k", 100)
        .unwrap();
    txn.commit().unwrap();

    assert!(e.versioned_get_current(1, 1, "c", "k").unwrap().is_none());
    assert!(
        e.versioned_get_as_of(1, 1, "c", "k", Some(100), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn versioned_remove_in_txn_on_missing_key_is_ok() {
    let (e, _d) = open_temp();
    let txn = e.db.begin_write().unwrap();
    let r = e.versioned_remove_in_txn(&txn, 1, 1, "c", "does-not-exist", 999);
    assert!(r.is_ok());
    txn.commit().unwrap();
}

#[test]
fn versioned_index_remove_in_txn_removes_entry() {
    let (e, _d) = open_temp();
    e.versioned_index_put(idx_entry("c", "email", "a@x", "u1", 100))
        .unwrap();
    let before = e
        .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(150))
        .unwrap();
    assert_eq!(before, vec!["u1"]);

    let txn = e.db.begin_write().unwrap();
    e.versioned_index_remove_in_txn(&txn, idx_entry("c", "email", "a@x", "u1", 100))
        .unwrap();
    txn.commit().unwrap();

    let after = e
        .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(150))
        .unwrap();
    assert!(after.is_empty());
}

#[test]
fn versioned_index_remove_in_txn_on_missing_key_is_ok() {
    let (e, _d) = open_temp();
    let txn = e.db.begin_write().unwrap();
    let r = e.versioned_index_remove_in_txn(&txn, idx_entry("c", "email", "nobody@x", "u9", 999));
    assert!(r.is_ok());
    txn.commit().unwrap();
}

#[test]
fn nul_in_doc_id_is_rejected() {
    let (e, _d) = open_temp();
    let r = e.versioned_put(VersionedPut {
        database_id: 1,
        tenant: 1,
        coll: "c",
        doc_id: "a\x00b",
        sys_from_ms: 100,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
        body: b"x",
    });
    assert!(r.is_err());
}
