// SPDX-License-Identifier: BUSL-1.1

//! `KvEngine::scan` and `KvEngine::scan_for_each`: cursor-based iteration
//! with optional key pattern matching and index-accelerated predicate
//! pushdown. Split out from the core engine impl as its own concern.

use super::{KvEngine, ScanResult};
use crate::engine::kv::engine_helpers::table_key;
use crate::engine::kv::scan::{KvScanParams, matches_pattern_pub};

impl KvEngine {
    /// SCAN: cursor-based iteration with optional key pattern matching and
    /// index-accelerated predicate pushdown.
    ///
    /// If `filter_field` and `filter_value` are provided AND a secondary index
    /// exists for that field, the scan uses the index to narrow candidates
    /// (O(log n) + O(k) where k = matching keys) instead of full table scan.
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

        let surrogate_visible = |s: u32| -> bool {
            match surrogate_ceiling {
                Some(c) => s == 0 || s <= c,
                None => true,
            }
        };

        // Index-accelerated path: if we have an equality filter and an index, use it.
        // Also checks composite indexes for prefix matches.
        if let Some(field) = filter_field
            && let Some(value) = filter_value
            && let Some(idx_set) = self.indexes.get(&tkey)
        {
            // Try single-field index first.
            let candidate_keys = if idx_set.get_index(field).is_some() {
                idx_set.lookup_eq(field, value)
            } else if let Some(ci) = idx_set.find_composite_with_prefix(field) {
                // Composite index prefix match: use leading field.
                ci.lookup_prefix(&[value])
            } else {
                Vec::new() // No index available — will fall through to full scan.
            };

            if !candidate_keys.is_empty() {
                let mut results = Vec::with_capacity(count.min(candidate_keys.len()));

                for pk in candidate_keys {
                    if results.len() >= count {
                        break;
                    }
                    if let Some((val, surrogate)) = table.get_with_surrogate(pk, now_ms)
                        && (match_pattern.is_none() || matches_pattern_pub(pk, match_pattern))
                        && surrogate_visible(surrogate.as_u32())
                    {
                        results.push((pk.to_vec(), val.to_vec()));
                    }
                }

                return (results, Vec::new());
            }
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

        let surrogate_visible = |s: u32| -> bool {
            match surrogate_ceiling {
                Some(c) => s == 0 || s <= c,
                None => true,
            }
        };

        // Index-accelerated path: if we have an equality filter and an index, use it.
        // Also checks composite indexes for prefix matches.
        if let Some(field) = filter_field
            && let Some(value) = filter_value
            && let Some(idx_set) = self.indexes.get(&tkey)
        {
            // Try single-field index first.
            let candidate_keys = if idx_set.get_index(field).is_some() {
                idx_set.lookup_eq(field, value)
            } else if let Some(ci) = idx_set.find_composite_with_prefix(field) {
                // Composite index prefix match: use leading field.
                ci.lookup_prefix(&[value])
            } else {
                Vec::new() // No index available — will fall through to full scan.
            };

            if !candidate_keys.is_empty() {
                let mut emitted = 0usize;
                for pk in candidate_keys {
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
    use nodedb_types::Surrogate;

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
}
