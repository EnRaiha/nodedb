// SPDX-License-Identifier: BUSL-1.1

use super::super::engine_index::RegisterIndexParams;
use super::super::scan::KvScanParams;
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
