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
