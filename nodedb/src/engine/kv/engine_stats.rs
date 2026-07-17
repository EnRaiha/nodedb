// SPDX-License-Identifier: BUSL-1.1

//! KV engine expiry, truncate, and observability stats.
//!
//! Methods on [`super::engine::KvEngine`] for expiry wheel management,
//! collection truncation, and comprehensive stats snapshots.

use super::engine::KvEngine;
use super::engine_helpers::{extract_all_field_values_from_msgpack, parse_expiry_key, table_key};

/// A key that was reaped by the expiry wheel.
///
/// Returned by [`KvEngine::tick_expiry`] so the caller can produce WAL
/// tombstones and CDC/keyspace notification events.
#[derive(Debug, Clone)]
pub struct ExpiredKey {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    pub key: Vec<u8>,
}

/// Observability snapshot for the KV engine on a single TPC core.
///
/// Produced by [`KvEngine::stats`]. Written to the telemetry ring
/// for the Control Plane to expose via HTTP metrics endpoint.
#[derive(Debug, Clone, Default)]
pub struct KvStats {
    /// Total key count across all collections.
    pub total_entries: usize,
    /// Approximate total memory usage in bytes.
    pub total_mem_bytes: usize,
    /// Number of active KV collections on this core.
    pub collection_count: usize,
    /// Highest load factor across all hash tables (triggers rehash at threshold).
    pub max_load_factor: f32,
    /// Whether any hash table is currently in incremental rehash.
    pub is_rehashing: bool,
    /// Total secondary index entries across all collections.
    pub total_index_entries: usize,
    /// Number of entries in the expiry wheel.
    pub expiry_queue_depth: usize,
    /// Number of deferred expirations (reap budget exceeded).
    pub expiry_backlog: usize,
}

impl KvEngine {
    // -----------------------------------------------------------------------
    // Expiry wheel tick — called from the TPC event loop
    // -----------------------------------------------------------------------

    /// Advance the expiry wheel and reap expired keys.
    ///
    /// Call this from the TPC core's event loop at the configured tick interval.
    /// Returns a list of `(tenant_id, collection, key)` for each reaped key,
    /// enabling the caller to produce WAL tombstones and CDC/keyspace events.
    ///
    /// A reap is a delete, so it maintains the row's secondary, composite, and
    /// sorted index entries exactly as [`KvEngine::delete`] does. Reaping the
    /// hash slot alone would strand them: the sorted index answers `rank` /
    /// `top_k` straight out of its tree with no re-check against the table, so
    /// a stranded entry is a wrong answer (an expired key holding a rank and
    /// displacing every live key below it), and the checkpoint exports index
    /// content verbatim, so it would survive a restart.
    ///
    /// [`KvEngine::delete`]: KvEngine::delete
    pub fn tick_expiry(&mut self, now_ms: u64) -> Vec<ExpiredKey> {
        let batch = self.expiry.tick(now_ms);
        let mut reaped = Vec::new();

        for (composite_key, expire_at_ms) in &batch.expired {
            let Some((did, tid, collection, key)) = parse_expiry_key(composite_key) else {
                continue;
            };
            let tkey = table_key(did, tid, &collection);

            // Zero-index fast path: the common index-less TTL collection pays
            // nothing beyond the reap itself.
            let has_indexes = self.indexes.get(&tkey).is_some_and(|s| !s.is_empty());
            let has_sorted = self.sorted_indexes.has_indexes(tkey);

            // Read the indexed field values BEFORE the reap frees the value they
            // live in, and own them so the table borrow ends here — the reap and
            // the index update below both need `&mut self`. Expiry-blind by
            // necessity: the row is expired by definition at this point.
            let old_fields: Option<Vec<(String, Vec<u8>)>> = if has_indexes {
                self.tables
                    .get(&tkey)
                    .and_then(|t| t.get_ignoring_expiry(&key))
                    .map(extract_all_field_values_from_msgpack)
            } else {
                None
            };

            let Some(table) = self.tables.get_mut(&tkey) else {
                continue;
            };
            // A mismatched `expire_at_ms` means the TTL was replaced after this
            // wheel entry was scheduled — the row is still live, so nothing to
            // clean up.
            if !table.reap_expired(&key, *expire_at_ms) {
                continue;
            }

            if let Some(fields) = &old_fields
                && let Some(idx_set) = self.indexes.get_mut(&tkey)
            {
                let refs: Vec<(&str, &[u8])> = fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                idx_set.on_delete(&key, &refs);
            }

            if has_sorted {
                self.sorted_indexes.on_delete(tkey, &key);
            }

            reaped.push(ExpiredKey {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
            });
        }

        reaped
    }

    /// Number of entries tracked in the expiry wheel.
    pub fn expiry_queue_depth(&self) -> usize {
        self.expiry.len()
    }

    /// Number of deferred expirations (backlog gauge).
    pub fn expiry_backlog(&self) -> usize {
        self.expiry.backlog()
    }

    // -----------------------------------------------------------------------
    // Truncate
    // -----------------------------------------------------------------------

    /// Truncate: delete all entries in a KV collection. Returns count deleted.
    pub fn truncate(&mut self, database_id: u64, tenant_id: u64, collection: &str) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        let count = self.tables.get(&tkey).map(|t| t.len()).unwrap_or(0);

        // Remove the hash table entirely.
        self.tables.remove(&tkey);
        // Remove all indexes.
        self.indexes.remove(&tkey);
        // Sorted indexes live in their own manager rather than in the
        // `KvIndexSet` above, so dropping that set leaves them behind. A
        // stranded sorted index is not merely a leak: `rank` / `top_k` return
        // their tree entries verbatim without re-checking the table, so a
        // truncated collection would keep serving ranked keys for rows that no
        // longer exist. Purging matches what removing the `KvIndexSet` does for
        // the secondary indexes — the registrations go with the rows.
        self.sorted_indexes
            .purge_collection(database_id, tenant_id, collection);
        // Note: expiry wheel entries for this collection will be no-ops
        // when they fire (key won't be found in the hash table).

        count
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    /// Total number of entries across all collections.
    pub fn total_entries(&self) -> usize {
        self.tables.values().map(|t| t.len()).sum()
    }

    /// Total approximate memory usage across all collections.
    pub fn total_mem_usage(&self) -> usize {
        self.tables.values().map(|t| t.mem_usage()).sum()
    }

    /// Entry count for a specific collection.
    pub fn collection_len(&self, database_id: u64, tenant_id: u64, collection: &str) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey).map(|t| t.len()).unwrap_or(0)
    }

    /// Approximate memory usage for a specific collection. Sums the
    /// hash table's own `mem_usage()` estimate; returns 0 if no table
    /// exists for `(tenant_id, collection)`.
    pub fn collection_mem_usage(&self, database_id: u64, tenant_id: u64, collection: &str) -> u64 {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables
            .get(&tkey)
            .map(|t| t.mem_usage() as u64)
            .unwrap_or(0)
    }

    /// Comprehensive observability snapshot for this KV engine.
    pub fn stats(&self) -> KvStats {
        let mut total_entries = 0usize;
        let mut total_mem = 0usize;
        let mut total_index_entries = 0usize;
        let mut is_rehashing = false;
        let mut max_load_factor: f32 = 0.0;

        for table in self.tables.values() {
            total_entries += table.len();
            total_mem += table.mem_usage();
            if table.load_factor() > max_load_factor {
                max_load_factor = table.load_factor();
            }
            if table.is_rehashing() {
                is_rehashing = true;
            }
        }

        for idx_set in self.indexes.values() {
            for field in idx_set.indexed_fields() {
                if let Some(idx) = idx_set.get_index(field) {
                    total_index_entries += idx.entry_count();
                }
            }
        }

        KvStats {
            total_entries,
            total_mem_bytes: total_mem,
            collection_count: self.tables.len(),
            max_load_factor,
            is_rehashing,
            total_index_entries,
            expiry_queue_depth: self.expiry.len(),
            expiry_backlog: self.expiry.backlog(),
        }
    }
}
