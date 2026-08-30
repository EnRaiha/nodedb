// SPDX-License-Identifier: BUSL-1.1

//! Secondary index management for the KV engine.
//!
//! Implements register, drop, lookup, and stats methods on [`super::engine::KvEngine`].

use super::engine::KvEngine;
use super::engine_helpers::{extract_field_values_from_msgpack, table_key};

/// Parameters for [`KvEngine::register_index`].
#[derive(Debug, Clone, Copy)]
pub struct RegisterIndexParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub field_position: usize,
    pub backfill: bool,
    pub now_ms: u64,
}

impl KvEngine {
    /// Register a secondary index on a field for a collection.
    ///
    /// If `backfill` is true, scans all existing entries and populates the index.
    /// Returns the number of entries backfilled (0 if index already existed).
    ///
    /// **Note**: backfill scans all entries synchronously. For large collections
    /// (> 10k entries), consider `backfill=false` and rebuilding offline.
    pub fn register_index(&mut self, params: RegisterIndexParams<'_>) -> usize {
        let RegisterIndexParams {
            database_id,
            tenant_id,
            collection,
            field,
            field_position,
            backfill,
            now_ms,
        } = params;
        let tkey = table_key(database_id, tenant_id, collection);

        // Name the collection even though no row has been written to it yet.
        // `CREATE INDEX` before the first `INSERT` is ordinary usage, and the
        // reverse maps are how the checkpoint writer recovers a collection's
        // identity from its hashed table key — an unnamed collection cannot be
        // given a checkpoint file, so its registration would be dropped from the
        // checkpoint while the WAL record carrying it was truncated away.
        self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
        self.hash_to_collection
            .entry(tkey)
            .or_insert_with(|| collection.to_string());

        let idx_set = self.indexes.entry(tkey).or_default();

        if !idx_set.add_index(field, field_position) {
            return 0; // Already indexed.
        }

        if !backfill {
            return 0;
        }

        // Backfill: collect entries first, then update indexes.
        // Two-phase approach avoids borrow conflicts on self.indexes vs self.tables.
        let entries_to_backfill: Vec<(Vec<u8>, Vec<u8>)> = match self.tables.get(&tkey) {
            Some(table) => {
                let mut all = Vec::new();
                let mut cursor = 0;
                loop {
                    let (entries, next) = table.scan(cursor, 1000, now_ms, None);
                    if entries.is_empty() {
                        break;
                    }
                    all.extend(entries.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
                    if next == 0 {
                        break;
                    }
                    cursor = next;
                }
                all
            }
            None => return 0,
        };

        // Now update indexes — idx_set is guaranteed to exist (inserted above).
        let idx_set = self
            .indexes
            .get_mut(&tkey)
            .expect("index set was inserted at entry point of register_index");
        let mut backfilled = 0;
        for (key, value) in &entries_to_backfill {
            let field_values = extract_field_values_from_msgpack(value, field);
            for fv in &field_values {
                let fv_pairs: Vec<(&str, &[u8])> = vec![(field, fv.as_slice())];
                idx_set.on_put(key, &fv_pairs, None);
                backfilled += 1;
            }
        }

        backfilled
    }

    /// Remove a secondary index on a field.
    ///
    /// Returns the number of index entries that were dropped.
    pub fn drop_index(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
    ) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        let idx_set = match self.indexes.get_mut(&tkey) {
            Some(s) => s,
            None => return 0,
        };

        match idx_set.remove_index(field) {
            Some(removed) => removed.entry_count(),
            None => 0,
        }
    }

    /// Lookup primary keys by exact field value match using a secondary index.
    ///
    /// Returns empty if the field is not indexed.
    pub fn index_lookup_eq(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        value: &[u8],
    ) -> Vec<Vec<u8>> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.indexes
            .get(&tkey)
            .map(|idx| {
                idx.lookup_eq(field, value)
                    .into_iter()
                    .map(|k| k.to_vec())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if a collection has any secondary indexes.
    pub fn has_indexes(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        self.indexes.get(&tkey).is_some_and(|s| !s.is_empty())
    }

    /// Get the write amplification ratio for a collection.
    pub fn write_amp_ratio(&self, database_id: u64, tenant_id: u64, collection: &str) -> f64 {
        let tkey = table_key(database_id, tenant_id, collection);
        self.indexes
            .get(&tkey)
            .map(|s| s.write_amp_ratio())
            .unwrap_or(0.0)
    }

    /// Get the number of secondary indexes for a collection.
    pub fn index_count(&self, database_id: u64, tenant_id: u64, collection: &str) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        self.indexes
            .get(&tkey)
            .map(|s| s.index_count())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::engine::kv::KvPutParams;

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
}
