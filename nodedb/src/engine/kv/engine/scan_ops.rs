// SPDX-License-Identifier: BUSL-1.1

//! `KvEngine::scan` and `KvEngine::scan_for_each`: cursor-based iteration
//! with optional key pattern matching and index-accelerated predicate
//! pushdown. Split out from the core engine impl as its own concern.

use super::{KvEngine, ScanResult};
use crate::engine::kv::engine_helpers::table_key;
use crate::engine::kv::scan::{KvScanParams, matches_pattern_pub};

/// The candidate position an index-scan cursor names. An index-scan cursor
/// is the 8-byte big-endian position of the next candidate; any other
/// cursor starts at the first candidate.
fn index_cursor_position(cursor: &[u8]) -> usize {
    <[u8; 8]>::try_from(cursor)
        .map(u64::from_be_bytes)
        .map_or(0, |pos| usize::try_from(pos).unwrap_or(usize::MAX))
}

impl KvEngine {
    /// SCAN: cursor-based iteration with optional key pattern matching and
    /// index-accelerated predicate pushdown.
    ///
    /// If `filter_field` and `filter_value` are provided AND a complete
    /// secondary index covers that field, the scan returns only the index's
    /// candidates (O(log n) + O(k) where k = matching keys), none on a miss.
    /// Without a complete index it reads every row.
    ///
    /// Returns `(entries, next_cursor_bytes)`. `next_cursor_bytes` is empty
    /// when the scan is complete. Each entry is `(key, value)`.
    /// `params.surrogate_ceiling` enforces clone snapshot isolation when set.
    pub fn scan(&self, params: KvScanParams<'_>) -> ScanResult {
        let KvScanParams {
            database_id,
            tenant_id,
            collection,
            cursor,
            count,
            now_ms,
            match_pattern,
            filter_field,
            filter_value,
            surrogate_ceiling,
        } = params;
        let tkey = table_key(database_id, tenant_id, collection);
        let table = match self.tables.get(&tkey) {
            Some(t) => t,
            None => return (Vec::new(), Vec::new()),
        };

        // Every stored entry holds a bound surrogate, so the ceiling alone
        // decides visibility.
        let surrogate_visible = |s: u32| -> bool {
            match surrogate_ceiling {
                Some(c) => s <= c,
                None => true,
            }
        };

        // Index-accelerated path: a complete index answers the equality, an
        // empty answer included. Without one, the scan reads every row.
        if let Some(field) = filter_field
            && let Some(value) = filter_value
            && let Some(candidate_keys) = self
                .indexes
                .get(&tkey)
                .and_then(|idx_set| idx_set.complete_eq_candidates(field, value))
        {
            // The cursor of an index scan is the position of the next
            // candidate, so a caller paging with `count` reads every
            // candidate exactly once.
            let start = index_cursor_position(cursor).min(candidate_keys.len());
            let mut results = Vec::with_capacity(count.min(candidate_keys.len() - start));
            let mut next = candidate_keys.len();
            for (pos, pk) in candidate_keys.iter().enumerate().skip(start) {
                if results.len() >= count {
                    next = pos;
                    break;
                }
                if let Some((val, surrogate)) = table.get_with_surrogate(pk, now_ms)
                    && (match_pattern.is_none() || matches_pattern_pub(pk, match_pattern))
                    && surrogate_visible(surrogate.as_u32())
                {
                    results.push((pk.to_vec(), val.to_vec()));
                }
            }
            let next_cursor = if next < candidate_keys.len() {
                (next as u64).to_be_bytes().to_vec()
            } else {
                Vec::new()
            };
            return (results, next_cursor);
        }

        // Full scan fallback: iterate hash table slots.
        let cursor_idx = if cursor.len() >= 4 {
            u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize
        } else {
            0
        };

        let (entries, next_cursor_idx) =
            table.scan_with_surrogate(cursor_idx, count, now_ms, match_pattern);

        let owned: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .filter_map(|(k, v, s)| {
                if surrogate_visible(s.as_u32()) {
                    Some((k.to_vec(), v.to_vec()))
                } else {
                    None
                }
            })
            .collect();

        let next_cursor = if next_cursor_idx == 0 {
            Vec::new()
        } else {
            (next_cursor_idx as u32).to_be_bytes().to_vec()
        };

        (owned, next_cursor)
    }

    /// Streaming variant of [`scan`]: invokes `f(key, value)` for each visible
    /// row instead of materializing a result `Vec`.
    ///
    /// Mirrors [`scan`] exactly: same index-accelerated path (when
    /// `filter_field` + `filter_value` + a matching index are present), same
    /// full-scan fallback over the hash-table slots, same `match_pattern`
    /// filtering, same `count` cutoff, and the same `surrogate_ceiling`
    /// visibility rule. The `(key, value)` rows passed to `f` — and their
    /// order — are byte-identical to the rows [`scan`] would return for the
    /// same `params`. Peak memory is a single borrowed row, not the whole scan.
    ///
    /// The callback receives borrowed `&[u8]` slices; it must copy any bytes it
    /// wants to retain beyond the call. If `f` returns `Err`, iteration stops
    /// immediately and the error is propagated — no row is silently dropped and
    /// the callback error is never swallowed into `Ok`.
    ///
    /// [`scan`]: KvEngine::scan
    // consumed by `scan_collection_for_each` streaming routing
    pub fn scan_for_each<F>(&self, params: KvScanParams<'_>, mut f: F) -> crate::Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> crate::Result<()>,
    {
        let KvScanParams {
            database_id,
            tenant_id,
            collection,
            cursor,
            count,
            now_ms,
            match_pattern,
            filter_field,
            filter_value,
            surrogate_ceiling,
        } = params;
        let tkey = table_key(database_id, tenant_id, collection);
        let table = match self.tables.get(&tkey) {
            Some(t) => t,
            None => return Ok(()),
        };

        // Every stored entry holds a bound surrogate, so the ceiling alone
        // decides visibility.
        let surrogate_visible = |s: u32| -> bool {
            match surrogate_ceiling {
                Some(c) => s <= c,
                None => true,
            }
        };

        // Index-accelerated path: a complete index answers the equality, an
        // empty answer included. Without one, the scan reads every row.
        if let Some(field) = filter_field
            && let Some(value) = filter_value
            && let Some(candidate_keys) = self
                .indexes
                .get(&tkey)
                .and_then(|idx_set| idx_set.complete_eq_candidates(field, value))
        {
            let start = index_cursor_position(cursor);
            let mut emitted = 0usize;
            for pk in candidate_keys.into_iter().skip(start) {
                if emitted >= count {
                    break;
                }
                if let Some((val, surrogate)) = table.get_with_surrogate(pk, now_ms)
                    && (match_pattern.is_none() || matches_pattern_pub(pk, match_pattern))
                    && surrogate_visible(surrogate.as_u32())
                {
                    f(pk, val)?;
                    emitted += 1;
                }
            }
            return Ok(());
        }

        // Full scan fallback: iterate hash table slots.
        let cursor_idx = if cursor.len() >= 4 {
            u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize
        } else {
            0
        };

        table.scan_with_surrogate_for_each(
            cursor_idx,
            count,
            now_ms,
            match_pattern,
            |k, v, s| {
                if surrogate_visible(s.as_u32()) {
                    f(k, v)?;
                }
                Ok(())
            },
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::kv::test_support::row_surrogate;
    use crate::engine::kv::{KvPutParams, RegisterIndexParams};

    use super::*;

    fn now() -> u64 {
        1_000_000
    }

    fn make_engine() -> KvEngine {
        KvEngine::new(now(), 16, 0.75, 4, 64, 1000, 1024)
    }

    /// Helper: create a MessagePack-encoded JSON object value.
    fn mp_obj(fields: &[(&str, &str)]) -> Vec<u8> {
        let obj: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(obj)).unwrap()
    }

    /// The index key an equality on `'us-east'` probes.
    fn us_east_probe() -> Vec<u8> {
        crate::engine::kv::index::kv_probe_key(&nodedb_types::Value::String("us-east".into()))
            .expect("text probes")
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
                surrogate: row_surrogate(&[i]),
            })
            .expect("a bound row writes");
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
                surrogate: row_surrogate(&[i]),
            })
            .expect("a bound row writes");
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
            surrogate: row_surrogate(b"s1"),
        })
        .expect("a bound row writes");
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sessions",
            key: b"s2",
            value: &mp_obj(&[("region", "us-east")]),
            ttl_ms: 0,
            now_ms: n,
            surrogate: row_surrogate(b"s2"),
        })
        .expect("a bound row writes");
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sessions",
            key: b"s3",
            value: &mp_obj(&[("region", "eu-west")]),
            ttl_ms: 0,
            now_ms: n,
            surrogate: row_surrogate(b"s3"),
        })
        .expect("a bound row writes");

        let probe = us_east_probe();
        let indexed_params = || KvScanParams {
            filter_field: Some("region"),
            filter_value: Some(&probe),
            ..scan_params("sessions", usize::MAX, n)
        };
        let (materialized, _next) = e.scan(indexed_params());
        assert_eq!(
            materialized.len(),
            2,
            "the index path returns both us-east rows"
        );

        let mut streamed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        e.scan_for_each(indexed_params(), |k, v| {
            streamed.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(materialized, streamed);
    }

    #[test]
    fn index_scan_pages_through_every_candidate() {
        // An index scan cut at `count` returned an empty cursor, so a
        // caller paging to the end saw only the first page.
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
        for i in 0..5u8 {
            let key = [b's', i];
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "sessions",
                key: &key,
                value: &mp_obj(&[("region", "us-east")]),
                ttl_ms: 0,
                now_ms: n,
                surrogate: row_surrogate(&key),
            })
            .expect("a bound row writes");
        }

        let probe = us_east_probe();
        let mut cursor: Vec<u8> = Vec::new();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        loop {
            let (page, next) = e.scan(KvScanParams {
                cursor: &cursor,
                filter_field: Some("region"),
                filter_value: Some(&probe),
                ..scan_params("sessions", 2, n)
            });
            assert!(page.len() <= 2);
            seen.extend(page.into_iter().map(|(k, _)| k));
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen.len(), 5);
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 5, "no candidate is read twice");
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
            surrogate: row_surrogate(b"k1"),
        })
        .expect("a bound row writes");
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: b"k2",
            value: b"v2",
            ttl_ms: 0,
            now_ms: n,
            surrogate: row_surrogate(b"k2"),
        })
        .expect("a bound row writes");

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
}
