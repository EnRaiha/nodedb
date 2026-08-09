// SPDX-License-Identifier: BUSL-1.1

use super::super::engine_index::RegisterIndexParams;
use super::super::scan::KvScanParams;
use super::super::sorted_index::key::{SortColumn, SortDirection, SortKeyEncoder};
use super::super::sorted_index::manager::SortedIndexDef;
use super::super::sorted_index::window::WindowConfig;
use super::*;

fn now() -> u64 {
    1_000_000
}

fn make_engine() -> KvEngine {
    KvEngine::new(now(), 16, 0.75, 4, 64, 1000, 1024)
}

#[test]
fn basic_get_put_delete() {
    let mut e = make_engine();
    let n = now();

    assert!(e.get(0, 1, "cache", b"k1", n).is_none());

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "cache",
        key: b"k1",
        value: b"v1",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.get(0, 1, "cache", b"k1", n).unwrap(), b"v1");

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "cache",
        key: b"k1",
        value: b"v2",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.get(0, 1, "cache", b"k1", n).unwrap(), b"v2");

    assert_eq!(e.delete(0, 1, "cache", &[b"k1".to_vec()], n), 1);
    assert!(e.get(0, 1, "cache", b"k1", n).is_none());
}

#[test]
fn ttl_expiry_via_tick() {
    let mut e = make_engine();
    let n = now();

    // Put with 5-second TTL.
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        key: b"s1",
        value: b"data",
        ttl_ms: 5000,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert!(e.get(0, 1, "sess", b"s1", n).is_some());

    // Still alive at t+4999.
    assert!(e.get(0, 1, "sess", b"s1", n + 4999).is_some());

    // Expired at t+5000 (lazy fallback).
    assert!(e.get(0, 1, "sess", b"s1", n + 5000).is_none());

    // Tick reaps it.
    let reaped = e.tick_expiry(n + 5000);
    assert_eq!(reaped.len(), 1);
    assert_eq!(reaped[0].collection, "sess");
    assert_eq!(reaped[0].key, b"s1");
    assert_eq!(e.total_entries(), 0);
}

#[test]
fn persist_removes_ttl() {
    let mut e = make_engine();
    let n = now();

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "cache",
        key: b"k",
        value: b"v",
        ttl_ms: 3000,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert!(e.persist(0, 1, "cache", b"k"));

    // Should never expire now.
    assert!(e.get(0, 1, "cache", b"k", n + 100_000).is_some());
}

#[test]
fn expire_sets_ttl() {
    let mut e = make_engine();
    let n = now();

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "cache",
        key: b"k",
        value: b"v",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert!(e.get(0, 1, "cache", b"k", n + 100_000).is_some()); // No TTL.

    assert!(e.expire(0, 1, "cache", b"k", 2000, n));
    assert!(e.get(0, 1, "cache", b"k", n + 1999).is_some());
    assert!(e.get(0, 1, "cache", b"k", n + 2000).is_none()); // Expired.
}

#[test]
fn batch_get_and_put() {
    let mut e = make_engine();
    let n = now();

    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5u8).map(|i| (vec![i], vec![i * 10])).collect();
    let surrogates = vec![Surrogate::ZERO; entries.len()];
    let new_count = e.batch_put(KvBatchPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        entries: &entries,
        ttl_ms: 0,
        now_ms: n,
        surrogates: &surrogates,
    });
    assert_eq!(new_count, 5);

    let keys: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i]).collect();
    let results = e.batch_get(0, 1, "c", &keys, n);
    assert_eq!(results.len(), 7);
    assert_eq!(results[0], Some(vec![0]));
    assert_eq!(results[4], Some(vec![40]));
    assert!(results[5].is_none()); // Key 5 doesn't exist.
    assert!(results[6].is_none());
}

/// Regression: a native `KvBatchPut` used to call
/// `KvEngine::batch_put` with no per-entry surrogate, so every batch-put
/// row landed with `Surrogate::ZERO` -- invisible to any surrogate-keyed
/// cross-engine read/join, unlike a single-key `put` which always
/// carries a real, CP-assigned surrogate. This asserts `batch_put`
/// stores the REAL surrogate passed for each entry (observable via
/// `get_with_surrogate`, the same accessor the clone-delegated read path
/// uses), exactly mirroring what a loop of single-key `put` calls would
/// do. Fails pre-fix because pre-fix `batch_put` took no `surrogates`
/// parameter at all and hardcoded `Surrogate::ZERO` for every entry --
/// this test would not have compiled against that signature, and the
/// equivalent assertion against the old code (stubbing `Surrogate::ZERO`
/// in) observes `get_with_surrogate` returning `Surrogate::ZERO` instead
/// of the distinct real identity asserted here.
#[test]
fn batch_put_stores_real_per_entry_surrogates() {
    let mut e = make_engine();
    let n = now();

    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..3u8).map(|i| (vec![i], vec![i * 10])).collect();
    let surrogates: Vec<Surrogate> = (1..=3u32).map(Surrogate::new).collect();
    let new_count = e.batch_put(KvBatchPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        entries: &entries,
        ttl_ms: 0,
        now_ms: n,
        surrogates: &surrogates,
    });
    assert_eq!(new_count, 3);

    for (i, expected) in surrogates.iter().enumerate() {
        let key = &entries[i].0;
        let (value, stored_surrogate) = e
            .get_with_surrogate(0, 1, "c", key, n)
            .unwrap_or_else(|| panic!("entry {i} must be present"));
        assert_eq!(value, entries[i].1, "entry {i} value must round-trip");
        assert_eq!(
            stored_surrogate, *expected,
            "entry {i} must carry its assigned surrogate, not Surrogate::ZERO"
        );
        assert_ne!(
            stored_surrogate,
            Surrogate::ZERO,
            "entry {i} must not fall back to the unbound sentinel"
        );
    }
}

#[test]
fn tenant_isolation() {
    let mut e = make_engine();
    let n = now();

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k",
        value: b"t1",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 2,
        collection: "c",
        key: b"k",
        value: b"t2",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    assert_eq!(e.get(0, 1, "c", b"k", n).unwrap(), b"t1");
    assert_eq!(e.get(0, 2, "c", b"k", n).unwrap(), b"t2");
}

#[test]
fn stats() {
    let mut e = make_engine();
    let n = now();

    assert_eq!(e.total_entries(), 0);

    for i in 0..10u32 {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: &i.to_be_bytes(),
            value: &[0; 32],
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }
    assert_eq!(e.total_entries(), 10);
    assert_eq!(e.collection_len(0, 1, "c"), 10);
    assert!(e.total_mem_usage() > 0);
}

/// Helper: create a MessagePack-encoded JSON object value.
fn mp_obj(fields: &[(&str, &str)]) -> Vec<u8> {
    let obj: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
        .collect();
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(obj)).unwrap()
}

#[test]
fn register_index_and_lookup() {
    let mut e = make_engine();
    let n = now();

    // Insert some entries before creating the index.
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        key: b"s1",
        value: &mp_obj(&[("region", "us-east"), ("status", "active")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        key: b"s2",
        value: &mp_obj(&[("region", "us-east"), ("status", "inactive")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        key: b"s3",
        value: &mp_obj(&[("region", "eu-west"), ("status", "active")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    // Create index with backfill.
    let backfilled = e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        field: "region",
        field_position: 0,
        backfill: true,
        now_ms: n,
    });
    assert_eq!(backfilled, 3);

    // Lookup by indexed field.
    let us_east = e.index_lookup_eq(0, 1, "sessions", "region", b"us-east");
    assert_eq!(us_east.len(), 2);
    assert!(us_east.contains(&b"s1".to_vec()));
    assert!(us_east.contains(&b"s2".to_vec()));

    let eu_west = e.index_lookup_eq(0, 1, "sessions", "region", b"eu-west");
    assert_eq!(eu_west.len(), 1);
}

#[test]
fn index_maintained_on_put() {
    let mut e = make_engine();
    let n = now();

    // Create index first (no backfill needed — empty collection).
    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        field: "status",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });

    // Insert.
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k1",
        value: &mp_obj(&[("status", "active")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.index_lookup_eq(0, 1, "c", "status", b"active").len(), 1);

    // Update: status changes.
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k1",
        value: &mp_obj(&[("status", "inactive")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert!(e.index_lookup_eq(0, 1, "c", "status", b"active").is_empty());
    assert_eq!(e.index_lookup_eq(0, 1, "c", "status", b"inactive").len(), 1);
}

#[test]
fn index_cleaned_on_delete() {
    let mut e = make_engine();
    let n = now();

    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        field: "region",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k1",
        value: &mp_obj(&[("region", "us")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k2",
        value: &mp_obj(&[("region", "us")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    assert_eq!(e.index_lookup_eq(0, 1, "c", "region", b"us").len(), 2);

    e.delete(0, 1, "c", &[b"k1".to_vec()], n);
    assert_eq!(e.index_lookup_eq(0, 1, "c", "region", b"us").len(), 1);
}

// ── TTL × index interaction ──────────────────────────────────────────────
//
// The expiry reaper is a delete path, so every index a DELETE maintains it
// must maintain too. These cases put a TTL and an index on the SAME
// collection: `ttl_expiry_via_tick` covers TTL on an index-less collection
// and `index_cleaned_on_delete` covers an index on a TTL-less collection, so
// neither observes the reaper touching an index.

/// An unwindowed leaderboard on `score` DESC, keyed on `player_id`.
///
/// Built inline rather than through the Data Plane's
/// `build_sorted_index_def`: this is an engine unit test, and the engine does
/// not depend on the executor that owns that builder.
fn leaderboard_def(collection: &str, name: &str) -> SortedIndexDef {
    SortedIndexDef {
        name: name.into(),
        collection: collection.into(),
        key_column: "player_id".into(),
        encoder: SortKeyEncoder::new(vec![SortColumn {
            name: "score".into(),
            direction: SortDirection::Desc,
        }]),
        window: WindowConfig::none(),
    }
}

/// The reaper must remove a single-field index entry along with the row.
/// Pre-fix `tick_expiry` reaped the hash slot only, so the index kept
/// pointing at a key that no longer existed — an unbounded leak that the
/// checkpoint then persisted verbatim.
#[test]
fn index_cleaned_on_ttl_reap() {
    let mut e = make_engine();
    let n = now();

    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        field: "region",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        key: b"s1",
        value: &mp_obj(&[("region", "us")]),
        ttl_ms: 5000,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.index_lookup_eq(0, 1, "sess", "region", b"us").len(), 1);

    let reaped = e.tick_expiry(n + 5000);
    assert_eq!(reaped.len(), 1);

    assert_eq!(e.total_entries(), 0);
    assert!(
        e.index_lookup_eq(0, 1, "sess", "region", b"us").is_empty(),
        "reaping the row must remove its index entry"
    );
    assert_eq!(
        e.stats().total_index_entries,
        0,
        "no index entry may outlive the row it points at"
    );
}

/// The hard-wrong-answer case. `sorted_index_rank` / `top_k` return tree
/// entries verbatim with no re-check against the hash table, so a sorted
/// index entry stranded by the reaper is not merely a leak: the expired
/// player keeps rank 1 and pushes every live player one rank down.
#[test]
fn sorted_index_cleaned_on_ttl_reap() {
    let mut e = make_engine();
    let n = now();

    // Register before the PUTs: `register_sorted_index` backfills against
    // wall-clock now, which is far past this test's synthetic `now()`.
    e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "players",
        key: b"p1",
        value: &mp_obj(&[("player_id", "p1"), ("score", "200")]),
        ttl_ms: 5000,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "players",
        key: b"p2",
        value: &mp_obj(&[("player_id", "p2"), ("score", "100")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(1));
    assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p2", n), Some(2));

    let reaped = e.tick_expiry(n + 5000);
    assert_eq!(reaped.len(), 1);

    assert_eq!(
        e.sorted_index_rank(0, 1, "lb", b"p1", n + 5000),
        None,
        "the expired leader must not still hold a rank"
    );
    assert_eq!(
        e.sorted_index_rank(0, 1, "lb", b"p2", n + 5000),
        Some(1),
        "the live player must move up, not stay shifted down by a ghost"
    );
    assert_eq!(
        e.sorted_index_top_k(0, 1, "lb", 10, n + 5000),
        Some(vec![(1, b"p2".to_vec())]),
        "top_k must not return the expired key"
    );
}

/// TRUNCATE must take the sorted indexes with the rows.
///
/// They live in their own manager rather than in the `KvIndexSet` that
/// `truncate` drops, so forgetting them strands the tree. That is the same
/// hard-wrong-answer as an unreaped expiry, by another route: `rank` / `top_k`
/// never re-check the table, so a truncated collection would keep serving
/// ranked keys for rows that no longer exist.
#[test]
fn sorted_index_cleaned_on_truncate() {
    let mut e = make_engine();
    let n = now();

    e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "players",
        key: b"p1",
        value: &mp_obj(&[("player_id", "p1"), ("score", "200")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(1));

    assert_eq!(e.truncate(0, 1, "players"), 1);

    assert_eq!(e.total_entries(), 0);
    assert_eq!(
        e.sorted_index_rank(0, 1, "lb", b"p1", n),
        None,
        "a truncated collection must not leave its leaderboard ranking ghosts"
    );
    assert_eq!(
        e.sorted_index_top_k(0, 1, "lb", 10, n),
        None,
        "the sorted index itself must be gone, as the secondary indexes are"
    );
}

/// Composite indexes are cleaned by a separate loop in `KvIndexSet::on_delete`
/// than single-field ones, so the reaper needs its own case for them.
///
/// Seeded through `KvIndexSet::add_composite_index` directly: that is the only
/// registration path a composite index has — the engine exposes no
/// `register_composite_index` counterpart to `register_index`.
#[test]
fn composite_index_cleaned_on_ttl_reap() {
    let mut e = make_engine();
    let n = now();
    let tkey = table_key(0, 1, "sess");

    e.indexes
        .entry(tkey)
        .or_default()
        .add_composite_index(vec!["region".into(), "status".into()], vec![0, 1]);

    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        key: b"s1",
        value: &mp_obj(&[("region", "us"), ("status", "active")]),
        ttl_ms: 5000,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    let ci_fields = vec!["region".to_string(), "status".to_string()];
    let hits = |e: &KvEngine| -> usize {
        e.indexes
            .get(&tkey)
            .and_then(|s| s.get_composite_index(&ci_fields))
            .map(|ci| ci.lookup_eq(&[b"us", b"active"]).len())
            .unwrap_or(0)
    };
    assert_eq!(hits(&e), 1);

    let reaped = e.tick_expiry(n + 5000);
    assert_eq!(reaped.len(), 1);
    assert_eq!(
        hits(&e),
        0,
        "reaping the row must remove its composite entry"
    );
}

/// DELETE of a key whose TTL has elapsed but which the wheel has not reaped
/// yet. `KvHashTable::delete` succeeds regardless of expiry, so reading the
/// old field values through the expiry-checking `get` used to return `None`
/// and strand the index entries behind a DELETE that reported success.
#[test]
fn index_cleaned_on_delete_of_expired_key() {
    let mut e = make_engine();
    let n = now();

    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        field: "region",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        key: b"s1",
        value: &mp_obj(&[("region", "us")]),
        ttl_ms: 5000,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.index_lookup_eq(0, 1, "sess", "region", b"us").len(), 1);

    // No tick — the row is expired but still present.
    assert_eq!(e.delete(0, 1, "sess", &[b"s1".to_vec()], n + 5000), 1);
    assert!(
        e.index_lookup_eq(0, 1, "sess", "region", b"us").is_empty(),
        "DELETE of an expired-pending-reap key must still clean the index"
    );
}

/// A rehash moves every existing entry into `rehash_source`; a row reaped
/// while it sits there is exactly the row whose index cleanup a probe of the
/// primary slots alone would skip. Guards `get_ignoring_expiry`'s
/// `rehash_source` fallback.
#[test]
fn index_cleaned_on_ttl_reap_during_rehash() {
    let mut e = make_engine();
    let n = now();

    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sess",
        field: "region",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });

    // make_engine's table starts at capacity 16 with a 0.75 rehash threshold,
    // so the 13th insert starts a rehash and parks all 13 rows in the source.
    // No PUT follows, so none of them get migrated back out.
    let rows = 13u32;
    for i in 0..rows {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sess",
            key: &i.to_be_bytes(),
            value: &mp_obj(&[("region", "us")]),
            ttl_ms: 5000,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }
    assert!(
        e.stats().is_rehashing,
        "test premise: the reaped rows must sit in the rehash source"
    );
    assert_eq!(
        e.index_lookup_eq(0, 1, "sess", "region", b"us").len(),
        rows as usize
    );

    let reaped = e.tick_expiry(n + 5000);
    assert_eq!(reaped.len(), rows as usize);
    assert_eq!(e.total_entries(), 0);
    assert!(
        e.index_lookup_eq(0, 1, "sess", "region", b"us").is_empty(),
        "rows reaped out of the rehash source must clean their index entries too"
    );
}

#[test]
fn zero_index_fast_path() {
    let mut e = make_engine();
    let n = now();

    // No indexes — PUT should work without index overhead.
    assert!(!e.has_indexes(0, 1, "c"));
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k",
        value: b"raw_value",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert!(e.get(0, 1, "c", b"k", n).is_some());
    assert_eq!(e.write_amp_ratio(0, 1, "c"), 0.0);
}

#[test]
fn drop_index_clears_entries() {
    let mut e = make_engine();
    let n = now();

    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        field: "status",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k1",
        value: &mp_obj(&[("status", "active")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    assert_eq!(e.index_count(0, 1, "c"), 1);

    let dropped = e.drop_index(0, 1, "c", "status");
    assert_eq!(dropped, 1);
    assert_eq!(e.index_count(0, 1, "c"), 0);
    assert!(e.index_lookup_eq(0, 1, "c", "status", b"active").is_empty());
}

#[test]
fn write_amp_tracking() {
    let mut e = make_engine();
    let n = now();

    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        field: "a",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });
    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        field: "b",
        field_position: 1,
        backfill: false,
        now_ms: n,
    });

    for i in 0..10u32 {
        let k = format!("k{i}");
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: k.as_bytes(),
            value: &mp_obj(&[("a", "x"), ("b", "y")]),
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }

    // 10 PUTs, 2 indexes each = write amp ratio of 2.0.
    let ratio = e.write_amp_ratio(0, 1, "c");
    assert!((ratio - 2.0).abs() < f64::EPSILON);
}

#[test]
fn raw_put_timing() {
    let mut e = make_engine();
    let n = now();
    let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
    let value = [0u8; 64];

    // Warmup: insert all keys once.
    for key in &keys {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "b",
            key,
            value: &value,
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }

    // Timed: 100K updates (keys already exist).
    let iters = 100_000u64;
    let start = std::time::Instant::now();
    for i in 0..iters {
        let key = &keys[(i as usize) % 10_000];
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "b",
            key,
            value: &value,
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() / iters as u128;
    // 691 ns/op measured — well under document's 12μs.
    assert!(ns_per_op < 5_000, "PUT too slow: {ns_per_op} ns/op");
}

/// Build the full-visibility, no-filter scan params used by the normalizer.
fn scan_params<'a>(collection: &'a str, count: usize, now_ms: u64) -> KvScanParams<'a> {
    KvScanParams {
        database_id: 0,
        tenant_id: 1,
        collection,
        cursor: &[],
        count,
        now_ms,
        match_pattern: None,
        filter_field: None,
        filter_value: None,
        surrogate_ceiling: None,
    }
}

#[test]
fn scan_for_each_matches_scan() {
    let mut e = make_engine();
    let n = now();
    for i in 0..5u8 {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: &[i],
            value: &[i * 10],
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }

    let (materialized, _next) = e.scan(scan_params("c", usize::MAX, n));

    let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    e.scan_for_each(scan_params("c", usize::MAX, n), |k, v| {
        streamed.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    // Same order, same keys, same bytes.
    assert_eq!(materialized, streamed);
}

#[test]
fn scan_for_each_respects_count() {
    let mut e = make_engine();
    let n = now();
    for i in 0..10u8 {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: &[i],
            value: &[i * 10],
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
    }

    let (materialized, _next) = e.scan(scan_params("c", 3, n));

    let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    e.scan_for_each(scan_params("c", 3, n), |k, v| {
        streamed.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(materialized.len(), 3);
    assert_eq!(materialized, streamed);
}

#[test]
fn scan_for_each_matches_scan_index_path() {
    let mut e = make_engine();
    let n = now();
    e.register_index(RegisterIndexParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        field: "region",
        field_position: 0,
        backfill: false,
        now_ms: n,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        key: b"s1",
        value: &mp_obj(&[("region", "us-east")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        key: b"s2",
        value: &mp_obj(&[("region", "us-east")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "sessions",
        key: b"s3",
        value: &mp_obj(&[("region", "eu-west")]),
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    let indexed_params = || KvScanParams {
        filter_field: Some("region"),
        filter_value: Some(b"us-east"),
        ..scan_params("sessions", usize::MAX, n)
    };
    let (materialized, _next) = e.scan(indexed_params());

    let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    e.scan_for_each(indexed_params(), |k, v| {
        streamed.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(materialized, streamed);
}

#[test]
fn scan_for_each_propagates_callback_error() {
    let mut e = make_engine();
    let n = now();
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k1",
        value: b"v1",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "c",
        key: b"k2",
        value: b"v2",
        ttl_ms: 0,
        now_ms: n,
        surrogate: Surrogate::ZERO,
    });

    let mut seen = 0usize;
    let result = e.scan_for_each(scan_params("c", usize::MAX, n), |_k, _v| {
        seen += 1;
        Err(crate::Error::Internal {
            detail: "stop".to_string(),
        })
    });

    assert!(result.is_err());
    // Stops at the first row — does not visit every row.
    assert_eq!(seen, 1);
}

// ── Sorted index: population, maintenance, and range bounds ─────────────

/// A leaderboard row whose `score` is a NUMBER, which is what a SQL
/// `INSERT ... (score INT)` stores and what the sort-key encoders assume.
/// `mp_obj` above builds string-valued fields, which sort as UTF-8 and would
/// hide an ordering bug behind lexicographic luck.
fn mp_scored(player_id: &str, score: i64) -> Vec<u8> {
    nodedb_types::json_to_msgpack(&serde_json::json!({
        "player_id": player_id,
        "score": score,
    }))
    .expect("encode leaderboard row")
}

fn put_scored(e: &mut KvEngine, collection: &str, player_id: &str, score: i64) {
    e.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection,
        key: player_id.as_bytes(),
        value: &mp_scored(player_id, score),
        ttl_ms: 0,
        now_ms: now(),
        surrogate: Surrogate::ZERO,
    });
}

fn ranked_keys(entries: Option<Vec<(u32, Vec<u8>)>>) -> Vec<String> {
    entries
        .unwrap_or_default()
        .into_iter()
        .map(|(_, key)| String::from_utf8_lossy(&key).into_owned())
        .collect()
}

/// Registration must adopt the rows that are already there.
///
/// An index that starts empty and only tracks later writes disagrees with its
/// own collection from the moment it is created, and nothing in the read path
/// re-checks the table — `top_k` returns the tree verbatim.
#[test]
fn sorted_index_backfills_rows_written_before_registration() {
    let mut e = make_engine();
    let n = now();

    put_scored(&mut e, "players", "p1", 10);
    put_scored(&mut e, "players", "p2", 30);
    put_scored(&mut e, "players", "p3", 20);

    let backfilled = e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

    assert_eq!(backfilled, 3, "every pre-existing row must be indexed");
    assert_eq!(e.sorted_index_count(0, 1, "lb", n), Some(3));
    assert_eq!(
        ranked_keys(e.sorted_index_top_k(0, 1, "lb", 10, n)),
        vec!["p2", "p3", "p1"],
        "backfill must order by the indexed column, highest first"
    );
}

/// Rows written after registration must be tracked, and the index must hold
/// exactly the collection's rows — no more, no fewer — however they arrived.
#[test]
fn sorted_index_holds_the_same_rows_as_the_collection() {
    let mut e = make_engine();
    let n = now();

    put_scored(&mut e, "players", "p1", 10);
    e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));
    put_scored(&mut e, "players", "p2", 30);
    put_scored(&mut e, "players", "p3", 20);

    let mut stored: Vec<String> = Vec::new();
    e.scan_for_each(scan_params("players", usize::MAX, n), |key, _value| {
        stored.push(String::from_utf8_lossy(key).into_owned());
        Ok(())
    })
    .expect("scan the collection");
    stored.sort();

    let mut indexed = ranked_keys(e.sorted_index_top_k(0, 1, "lb", u32::MAX, n));
    indexed.sort();

    assert_eq!(
        indexed, stored,
        "the index must answer with the collection's row set, not a subset"
    );
}

/// `INCR` / `CAS` / `GETSET` / `TRANSFER` reach the store through the atomic
/// write body, not through `put`. Maintaining the index on only one of the two
/// routes leaves `RANK` / `TOPK` answering from the pre-update score with
/// nothing to signal it.
///
/// `incr` is the route exercised here because it is the one that genuinely
/// rewrites the indexed column: on a typed row it re-writes the first numeric
/// field in place, which is what `KV_INCR` and RESP `ZINCRBY` do to a
/// leaderboard score. (`getset` and `cas` replace the first STRING field, so
/// neither can move `score` — they are the wrong shape to test an ordering
/// change with, not a second version of this case.)
#[test]
fn sorted_index_tracks_an_atomic_update() {
    let mut e = make_engine();
    let n = now();

    put_scored(&mut e, "players", "p1", 10);
    put_scored(&mut e, "players", "p2", 30);
    e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));
    assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(2));

    let updated = e.incr(
        crate::engine::kv::AtomicKeyCtx {
            database_id: 0,
            tenant_id: 1,
            collection: "players",
            key: b"p1",
            now_ms: n,
            surrogate: Surrogate::ZERO,
        },
        89,
        // `ttl_ms == 0` preserves whatever TTL the key already has, so the
        // increment under test is the only thing this write changes.
        0,
        &crate::engine::kv::admit_any,
    );
    assert_eq!(updated.ok(), Some(99), "p1's score must become 10 + 89");

    assert_eq!(
        ranked_keys(e.sorted_index_top_k(0, 1, "lb", 10, n)),
        vec!["p1", "p2"],
        "the updated score must re-order the leaderboard"
    );
    assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(1));
    assert_eq!(
        e.sorted_index_count(0, 1, "lb", n),
        Some(2),
        "an update re-keys a row, it does not add one"
    );
}

/// A DELETE must take the row out of the index too, or the deleted key keeps
/// its rank and displaces every live key below it.
#[test]
fn sorted_index_tracks_a_delete() {
    let mut e = make_engine();
    let n = now();

    put_scored(&mut e, "players", "p1", 10);
    put_scored(&mut e, "players", "p2", 30);
    e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

    assert_eq!(e.delete(0, 1, "players", &[b"p2".to_vec()], n), 1);

    assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p2", n), None);
    assert_eq!(e.sorted_index_count(0, 1, "lb", n), Some(1));
    assert_eq!(
        ranked_keys(e.sorted_index_top_k(0, 1, "lb", 10, n)),
        vec!["p1"]
    );
}

/// `RANGE(index, lo, hi)` bounds arrive as the leading column's raw value
/// bytes; the tree is keyed by length-prefixed, direction-complemented
/// composite keys. Comparing the two spaces directly matches nothing, so the
/// bounds must be lifted into the key space — including the swap a descending
/// column forces.
#[test]
fn sorted_index_range_selects_by_score() {
    let mut e = make_engine();
    let n = now();

    put_scored(&mut e, "players", "p1", 10);
    put_scored(&mut e, "players", "p2", 30);
    put_scored(&mut e, "players", "p3", 20);
    e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

    let bound = |v: i64| SortKeyEncoder::encode_i64(v).to_vec();

    let mid = e.sorted_index_range(crate::engine::kv::SortedIndexRangeParams {
        database_id: 0,
        tenant_id: 1,
        index_name: "lb",
        score_min: Some(&bound(15)),
        score_max: Some(&bound(30)),
        now_ms: n,
    });
    let mut keys = ranked_keys(mid);
    keys.sort();
    assert_eq!(
        keys,
        vec!["p2", "p3"],
        "an inclusive [15, 30] window must hold exactly the rows scoring 20 and 30"
    );

    let all = e.sorted_index_range(crate::engine::kv::SortedIndexRangeParams {
        database_id: 0,
        tenant_id: 1,
        index_name: "lb",
        score_min: None,
        score_max: None,
        now_ms: n,
    });
    assert_eq!(
        ranked_keys(all).len(),
        3,
        "an unbounded range must return every indexed row"
    );

    let below = e.sorted_index_range(crate::engine::kv::SortedIndexRangeParams {
        database_id: 0,
        tenant_id: 1,
        index_name: "lb",
        score_min: None,
        score_max: Some(&bound(10)),
        now_ms: n,
    });
    assert_eq!(
        ranked_keys(below),
        vec!["p1"],
        "an upper bound must include the row sitting exactly on it"
    );
}
