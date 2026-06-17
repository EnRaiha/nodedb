// SPDX-License-Identifier: BUSL-1.1

//! KvEngine: per-core KV engine owning hash tables and expiry wheel.
//!
//! `!Send` — owned by a single TPC core. Each collection gets its own
//! hash table; the expiry wheel is shared across all collections on
//! this core (one wheel tick processes all collections).

use std::collections::HashMap;

use nodedb_types::Surrogate;

use super::engine_helpers::table_key;
use super::expiry_wheel::ExpiryWheel;
use super::hash_table::KvHashTable;
use super::index::KvIndexSet;
use super::scan::KvScanParams;

/// Result of a KV SCAN operation: `(entries, next_cursor_bytes)`.
///
/// Each entry is `(key_bytes, value_bytes)`. `next_cursor` is empty
/// when the scan is complete, otherwise an opaque cursor for continuation.
pub type ScanResult = (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

/// Per-core KV engine.
///
/// Owns a hash table per collection and a shared expiry wheel.
/// Dispatched from the Data Plane executor via `PhysicalPlan::Kv(KvOp)`.
pub struct KvEngine {
    /// Per-collection hash tables. Key: "{tenant_id}:{collection}".
    pub(crate) tables: HashMap<u64, KvHashTable>,
    /// Per-collection secondary index sets. Key: "{tenant_id}:{collection}".
    pub(crate) indexes: HashMap<u64, KvIndexSet>,
    /// Reverse mapping: hash → tenant_id. Enables tenant purge without
    /// reversing the FxHash. Maintained in sync with `tables`.
    pub(crate) hash_to_tenant: HashMap<u64, u64>,
    /// Reverse mapping: hash → collection name. Enables snapshot export
    /// to include human-readable collection names (FxHash is not reversible).
    pub(crate) hash_to_collection: HashMap<u64, String>,
    /// Shared expiry wheel across all collections on this core.
    pub(super) expiry: ExpiryWheel,
    /// Default tuning parameters for new collections.
    pub(super) default_capacity: usize,
    pub(super) load_factor_threshold: f32,
    pub(super) rehash_batch_size: usize,
    pub(super) inline_threshold: usize,
    /// Memory budget in bytes (0 = unlimited). When total_mem_usage() exceeds
    /// this, new PUTs are rejected with a retriable error.
    memory_budget_bytes: usize,
    /// Sorted index manager: order-statistic trees for leaderboard-style queries.
    pub(super) sorted_indexes: super::sorted_index::SortedIndexManager,
}

impl KvEngine {
    /// Create a new KV engine with the given tuning parameters.
    pub fn new(
        now_ms: u64,
        default_capacity: usize,
        load_factor_threshold: f32,
        rehash_batch_size: usize,
        inline_threshold: usize,
        expiry_tick_ms: u64,
        expiry_reap_budget: usize,
    ) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            hash_to_tenant: HashMap::new(),
            hash_to_collection: HashMap::new(),
            expiry: ExpiryWheel::new(now_ms, expiry_tick_ms, expiry_reap_budget),
            default_capacity,
            load_factor_threshold,
            rehash_batch_size,
            inline_threshold,
            memory_budget_bytes: 0, // 0 = unlimited (set via set_memory_budget).
            sorted_indexes: super::sorted_index::SortedIndexManager::new(),
        }
    }

    /// Create a KV engine from `KvTuning` config.
    pub fn from_tuning(now_ms: u64, tuning: &nodedb_types::config::tuning::KvTuning) -> Self {
        Self::new(
            now_ms,
            tuning.default_capacity,
            tuning.rehash_load_factor,
            tuning.rehash_batch_size,
            tuning.default_inline_threshold,
            tuning.expiry_tick_ms,
            tuning.expiry_reap_budget,
        )
    }

    /// Set the memory budget in bytes. 0 = unlimited.
    pub fn set_memory_budget(&mut self, budget_bytes: usize) {
        self.memory_budget_bytes = budget_bytes;
    }

    /// Check if the memory budget is exceeded.
    ///
    /// Returns `true` if the budget is set and current usage exceeds it.
    /// Used by PUT handlers to reject new writes with a retriable error.
    pub fn is_over_budget(&self) -> bool {
        self.memory_budget_bytes > 0 && self.total_mem_usage() > self.memory_budget_bytes
    }

    /// Remove the hash table and indexes for a single `(tenant_id, collection)`.
    ///
    /// Returns `1` if the table existed and was removed, `0` otherwise.
    /// Idempotent — safe to re-run after partial completion.
    pub fn purge_collection(&mut self, tenant_id: u64, collection: &str) -> usize {
        let tkey = super::engine_helpers::table_key(tenant_id, collection);
        let mut removed = 0;
        if self.tables.remove(&tkey).is_some() {
            removed += 1;
        }
        self.indexes.remove(&tkey);
        self.hash_to_tenant.remove(&tkey);
        self.hash_to_collection.remove(&tkey);
        self.sorted_indexes.purge_collection(tenant_id, collection);

        // Eagerly drop pending TTL-wheel entries for this collection.
        // Stale entries would otherwise no-op at fire time (the table
        // they reference is gone), but they still consume reap budget
        // per tick — for a large collection with many TTLs, that's
        // wasted work until every scheduled time has passed.
        let prefix = format!("{tenant_id}:{collection}\0").into_bytes();
        let wheel_removed = self.expiry.purge_prefix(&prefix);
        if wheel_removed > 0 {
            tracing::debug!(
                tenant_id,
                collection,
                wheel_removed,
                "kv: dropped expiry-wheel entries for purged collection"
            );
        }

        removed
    }

    /// Remove all hash tables and indexes belonging to a specific tenant.
    ///
    /// Uses the `hash_to_tenant` reverse map to identify which tables belong
    /// to the tenant. Returns the number of tables removed.
    pub fn purge_tenant(&mut self, tenant_id: u64) -> usize {
        let keys_to_remove: Vec<u64> = self
            .hash_to_tenant
            .iter()
            .filter(|(_, tid)| **tid == tenant_id)
            .map(|(hash, _)| *hash)
            .collect();

        let removed = keys_to_remove.len();
        for key in &keys_to_remove {
            self.tables.remove(key);
            self.indexes.remove(key);
            self.hash_to_tenant.remove(key);
            self.hash_to_collection.remove(key);
        }
        removed
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Look up the user primary key bytes for a given surrogate within
    /// `(tenant_id, collection)`. Returns `None` when the surrogate is
    /// unbound or the collection is empty.
    pub fn key_for_surrogate(
        &self,
        tenant_id: u64,
        collection: &str,
        surrogate: Surrogate,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(tenant_id, collection);
        self.tables
            .get(&tkey)?
            .key_for_surrogate(surrogate)
            .map(|k| k.to_vec())
    }

    /// GET: O(1) hash table lookup. Returns None if not found or expired.
    pub fn get(
        &self,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(tenant_id, collection);
        self.tables.get(&tkey)?.get(key, now_ms).map(|v| v.to_vec())
    }

    /// GET with surrogate: returns the value bytes AND the row's stable
    /// surrogate when the binding was made.  Used by the clone-delegated
    /// read path to enforce a per-row surrogate ceiling — bindings the
    /// source allocated AFTER the clone's AS-OF point are filtered out.
    pub fn get_with_surrogate(
        &self,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<(Vec<u8>, nodedb_types::Surrogate)> {
        let tkey = table_key(tenant_id, collection);
        self.tables
            .get(&tkey)?
            .get_with_surrogate(key, now_ms)
            .map(|(v, s)| (v.to_vec(), s))
    }

    /// GET TTL: Returns the remaining TTL in milliseconds for a key.
    ///
    /// - `None` — key does not exist (or is expired)
    /// - `Some(-1)` — key exists but has no TTL (persistent)
    /// - `Some(remaining_ms)` — key exists and expires in `remaining_ms` milliseconds
    pub fn get_ttl_ms(
        &self,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<i64> {
        let tkey = table_key(tenant_id, collection);
        let table = self.tables.get(&tkey)?;

        // First check the key exists and isn't expired.
        table.get(key, now_ms)?;

        // Now get the metadata for TTL info.
        let meta = table.get_entry_meta(key)?;
        if !meta.has_ttl {
            Some(-1)
        } else {
            let remaining = meta.expire_at_ms.saturating_sub(now_ms);
            Some(remaining as i64)
        }
    }

    /// BATCH GET: fetch multiple keys. Returns values in order (None for missing).
    pub fn batch_get(
        &self,
        tenant_id: u64,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> Vec<Option<Vec<u8>>> {
        keys.iter()
            .map(|k| self.get(tenant_id, collection, k, now_ms))
            .collect()
    }

    /// BATCH PUT: insert/update multiple pairs. Returns count of new keys.
    pub fn batch_put(
        &mut self,
        tenant_id: u64,
        collection: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
        ttl_ms: u64,
        now_ms: u64,
    ) -> usize {
        let mut new_count = 0;
        for (key, value) in entries {
            if self
                .put(
                    tenant_id,
                    collection,
                    key,
                    value,
                    ttl_ms,
                    now_ms,
                    Surrogate::ZERO,
                )
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }

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
        let tkey = table_key(tenant_id, collection);
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
                        && (match_pattern.is_none()
                            || super::scan::matches_pattern_pub(pk, match_pattern))
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
        let tkey = table_key(tenant_id, collection);
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
                        && (match_pattern.is_none()
                            || super::scan::matches_pattern_pub(pk, match_pattern))
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

        assert!(e.get(1, "cache", b"k1", n).is_none());

        e.put(1, "cache", b"k1", b"v1", 0, n, Surrogate::ZERO);
        assert_eq!(e.get(1, "cache", b"k1", n).unwrap(), b"v1");

        e.put(1, "cache", b"k1", b"v2", 0, n, Surrogate::ZERO);
        assert_eq!(e.get(1, "cache", b"k1", n).unwrap(), b"v2");

        assert_eq!(e.delete(1, "cache", &[b"k1".to_vec()], n), 1);
        assert!(e.get(1, "cache", b"k1", n).is_none());
    }

    #[test]
    fn ttl_expiry_via_tick() {
        let mut e = make_engine();
        let n = now();

        // Put with 5-second TTL.
        e.put(1, "sess", b"s1", b"data", 5000, n, Surrogate::ZERO);
        assert!(e.get(1, "sess", b"s1", n).is_some());

        // Still alive at t+4999.
        assert!(e.get(1, "sess", b"s1", n + 4999).is_some());

        // Expired at t+5000 (lazy fallback).
        assert!(e.get(1, "sess", b"s1", n + 5000).is_none());

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

        e.put(1, "cache", b"k", b"v", 3000, n, Surrogate::ZERO);
        assert!(e.persist(1, "cache", b"k"));

        // Should never expire now.
        assert!(e.get(1, "cache", b"k", n + 100_000).is_some());
    }

    #[test]
    fn expire_sets_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "cache", b"k", b"v", 0, n, Surrogate::ZERO);
        assert!(e.get(1, "cache", b"k", n + 100_000).is_some()); // No TTL.

        assert!(e.expire(1, "cache", b"k", 2000, n));
        assert!(e.get(1, "cache", b"k", n + 1999).is_some());
        assert!(e.get(1, "cache", b"k", n + 2000).is_none()); // Expired.
    }

    #[test]
    fn batch_get_and_put() {
        let mut e = make_engine();
        let n = now();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5u8).map(|i| (vec![i], vec![i * 10])).collect();
        let new_count = e.batch_put(1, "c", &entries, 0, n);
        assert_eq!(new_count, 5);

        let keys: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i]).collect();
        let results = e.batch_get(1, "c", &keys, n);
        assert_eq!(results.len(), 7);
        assert_eq!(results[0], Some(vec![0]));
        assert_eq!(results[4], Some(vec![40]));
        assert!(results[5].is_none()); // Key 5 doesn't exist.
        assert!(results[6].is_none());
    }

    #[test]
    fn tenant_isolation() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "c", b"k", b"t1", 0, n, Surrogate::ZERO);
        e.put(2, "c", b"k", b"t2", 0, n, Surrogate::ZERO);

        assert_eq!(e.get(1, "c", b"k", n).unwrap(), b"t1");
        assert_eq!(e.get(2, "c", b"k", n).unwrap(), b"t2");
    }

    #[test]
    fn stats() {
        let mut e = make_engine();
        let n = now();

        assert_eq!(e.total_entries(), 0);

        for i in 0..10u32 {
            e.put(1, "c", &i.to_be_bytes(), &[0; 32], 0, n, Surrogate::ZERO);
        }
        assert_eq!(e.total_entries(), 10);
        assert_eq!(e.collection_len(1, "c"), 10);
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
        e.put(
            1,
            "sessions",
            b"s1",
            &mp_obj(&[("region", "us-east"), ("status", "active")]),
            0,
            n,
            Surrogate::ZERO,
        );
        e.put(
            1,
            "sessions",
            b"s2",
            &mp_obj(&[("region", "us-east"), ("status", "inactive")]),
            0,
            n,
            Surrogate::ZERO,
        );
        e.put(
            1,
            "sessions",
            b"s3",
            &mp_obj(&[("region", "eu-west"), ("status", "active")]),
            0,
            n,
            Surrogate::ZERO,
        );

        // Create index with backfill.
        let backfilled = e.register_index(1, "sessions", "region", 0, true, n);
        assert_eq!(backfilled, 3);

        // Lookup by indexed field.
        let us_east = e.index_lookup_eq(1, "sessions", "region", b"us-east");
        assert_eq!(us_east.len(), 2);
        assert!(us_east.contains(&b"s1".to_vec()));
        assert!(us_east.contains(&b"s2".to_vec()));

        let eu_west = e.index_lookup_eq(1, "sessions", "region", b"eu-west");
        assert_eq!(eu_west.len(), 1);
    }

    #[test]
    fn index_maintained_on_put() {
        let mut e = make_engine();
        let n = now();

        // Create index first (no backfill needed — empty collection).
        e.register_index(1, "c", "status", 0, false, n);

        // Insert.
        e.put(
            1,
            "c",
            b"k1",
            &mp_obj(&[("status", "active")]),
            0,
            n,
            Surrogate::ZERO,
        );
        assert_eq!(e.index_lookup_eq(1, "c", "status", b"active").len(), 1);

        // Update: status changes.
        e.put(
            1,
            "c",
            b"k1",
            &mp_obj(&[("status", "inactive")]),
            0,
            n,
            Surrogate::ZERO,
        );
        assert!(e.index_lookup_eq(1, "c", "status", b"active").is_empty());
        assert_eq!(e.index_lookup_eq(1, "c", "status", b"inactive").len(), 1);
    }

    #[test]
    fn index_cleaned_on_delete() {
        let mut e = make_engine();
        let n = now();

        e.register_index(1, "c", "region", 0, false, n);
        e.put(
            1,
            "c",
            b"k1",
            &mp_obj(&[("region", "us")]),
            0,
            n,
            Surrogate::ZERO,
        );
        e.put(
            1,
            "c",
            b"k2",
            &mp_obj(&[("region", "us")]),
            0,
            n,
            Surrogate::ZERO,
        );

        assert_eq!(e.index_lookup_eq(1, "c", "region", b"us").len(), 2);

        e.delete(1, "c", &[b"k1".to_vec()], n);
        assert_eq!(e.index_lookup_eq(1, "c", "region", b"us").len(), 1);
    }

    #[test]
    fn zero_index_fast_path() {
        let mut e = make_engine();
        let n = now();

        // No indexes — PUT should work without index overhead.
        assert!(!e.has_indexes(1, "c"));
        e.put(1, "c", b"k", b"raw_value", 0, n, Surrogate::ZERO);
        assert!(e.get(1, "c", b"k", n).is_some());
        assert_eq!(e.write_amp_ratio(1, "c"), 0.0);
    }

    #[test]
    fn drop_index_clears_entries() {
        let mut e = make_engine();
        let n = now();

        e.register_index(1, "c", "status", 0, false, n);
        e.put(
            1,
            "c",
            b"k1",
            &mp_obj(&[("status", "active")]),
            0,
            n,
            Surrogate::ZERO,
        );
        assert_eq!(e.index_count(1, "c"), 1);

        let dropped = e.drop_index(1, "c", "status");
        assert_eq!(dropped, 1);
        assert_eq!(e.index_count(1, "c"), 0);
        assert!(e.index_lookup_eq(1, "c", "status", b"active").is_empty());
    }

    #[test]
    fn write_amp_tracking() {
        let mut e = make_engine();
        let n = now();

        e.register_index(1, "c", "a", 0, false, n);
        e.register_index(1, "c", "b", 1, false, n);

        for i in 0..10u32 {
            let k = format!("k{i}");
            e.put(
                1,
                "c",
                k.as_bytes(),
                &mp_obj(&[("a", "x"), ("b", "y")]),
                0,
                n,
                Surrogate::ZERO,
            );
        }

        // 10 PUTs, 2 indexes each = write amp ratio of 2.0.
        let ratio = e.write_amp_ratio(1, "c");
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
            e.put(1, "b", key, &value, 0, n, Surrogate::ZERO);
        }

        // Timed: 100K updates (keys already exist).
        let iters = 100_000u64;
        let start = std::time::Instant::now();
        for i in 0..iters {
            let key = &keys[(i as usize) % 10_000];
            e.put(1, "b", key, &value, 0, n, Surrogate::ZERO);
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() / iters as u128;
        // 691 ns/op measured — well under document's 12μs.
        assert!(ns_per_op < 5_000, "PUT too slow: {ns_per_op} ns/op");
    }

    /// Build the full-visibility, no-filter scan params used by the normalizer.
    fn scan_params<'a>(collection: &'a str, count: usize, now_ms: u64) -> KvScanParams<'a> {
        KvScanParams {
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
            e.put(1, "c", &[i], &[i * 10], 0, n, Surrogate::ZERO);
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
            e.put(1, "c", &[i], &[i * 10], 0, n, Surrogate::ZERO);
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
        e.register_index(1, "sessions", "region", 0, false, n);
        e.put(
            1,
            "sessions",
            b"s1",
            &mp_obj(&[("region", "us-east")]),
            0,
            n,
            Surrogate::ZERO,
        );
        e.put(
            1,
            "sessions",
            b"s2",
            &mp_obj(&[("region", "us-east")]),
            0,
            n,
            Surrogate::ZERO,
        );
        e.put(
            1,
            "sessions",
            b"s3",
            &mp_obj(&[("region", "eu-west")]),
            0,
            n,
            Surrogate::ZERO,
        );

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
        e.put(1, "c", b"k1", b"v1", 0, n, Surrogate::ZERO);
        e.put(1, "c", b"k2", b"v2", 0, n, Surrogate::ZERO);

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
