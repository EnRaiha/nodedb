// SPDX-License-Identifier: BUSL-1.1

//! KvEngine methods for sorted index lifecycle and query.
//!
//! Extends `KvEngine` with:
//! - `register_sorted_index()` / `drop_sorted_index()` — DDL
//! - `sorted_index_rank()` / `sorted_index_top_k()` / etc. — query
//!
//! Write-time maintenance is NOT here. `KvEngine::put` / `delete` /
//! `atomic_put` / `tick_expiry` reach `SortedIndexManager::on_put` /
//! `on_delete` directly, alongside the secondary-index update they already do
//! from the same field extraction — one entry point per write path, so there is
//! no second place to edit that turns out to be called by nothing.

use super::engine::KvEngine;
use super::engine_helpers::table_key;
use super::sorted_index::manager::SortedIndexDef;

/// Parameters for [`KvEngine::sorted_index_range`].
#[derive(Debug, Clone, Copy)]
pub struct SortedIndexRangeParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub index_name: &'a str,
    pub score_min: Option<&'a [u8]>,
    pub score_max: Option<&'a [u8]>,
    pub now_ms: u64,
}

impl KvEngine {
    /// Register a new sorted index with backfill from existing KV data.
    ///
    /// Scans the hash table for all entries, extracts sort key columns,
    /// and populates the order-statistic tree. Returns backfill count.
    pub fn register_sorted_index(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        def: SortedIndexDef,
    ) -> u32 {
        let tkey = table_key(database_id, tenant_id, collection);
        let now_ms = super::current_ms();

        // Name the collection even when it holds no rows yet: the checkpoint
        // writer recovers a collection's identity from these reverse maps, and
        // an unnamed collection gets no checkpoint file — which would drop this
        // registration from the checkpoint while WAL truncation deleted the
        // record that carries it.
        self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
        self.hash_to_collection
            .entry(tkey)
            .or_insert_with(|| collection.to_string());

        // Collect existing entries from the hash table for backfill.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .tables
            .get(&tkey)
            .map(|t| {
                let (entries, _) = t.scan(0, usize::MAX, now_ms, None);
                entries
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect()
            })
            .unwrap_or_default();

        self.sorted_indexes
            .register(database_id, tenant_id, def, entries.into_iter())
    }

    /// Drop a sorted index. Returns `true` if it existed.
    pub fn drop_sorted_index(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
    ) -> bool {
        self.sorted_indexes.drop(database_id, tenant_id, index_name)
    }

    /// Check if any sorted indexes exist for this tenant/collection.
    pub fn has_sorted_indexes(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        self.sorted_indexes.has_indexes(tkey)
    }

    // ── Query methods ──────────────────────────────────────────────────

    pub fn sorted_index_rank(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        primary_key: &[u8],
        now_ms: u64,
    ) -> Option<u32> {
        self.sorted_indexes
            .rank(database_id, tenant_id, index_name, primary_key, now_ms)
    }

    pub fn sorted_index_top_k(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        k: u32,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        self.sorted_indexes
            .top_k(database_id, tenant_id, index_name, k, now_ms)
    }

    pub fn sorted_index_range(
        &self,
        params: SortedIndexRangeParams<'_>,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        let SortedIndexRangeParams {
            database_id,
            tenant_id,
            index_name,
            score_min,
            score_max,
            now_ms,
        } = params;
        self.sorted_indexes.range(
            database_id,
            tenant_id,
            index_name,
            score_min,
            score_max,
            now_ms,
        )
    }

    pub fn sorted_index_count(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        now_ms: u64,
    ) -> Option<u32> {
        self.sorted_indexes
            .count(database_id, tenant_id, index_name, now_ms)
    }

    pub fn sorted_index_score(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        primary_key: &[u8],
    ) -> Option<Vec<u8>> {
        self.sorted_indexes
            .score(database_id, tenant_id, index_name, primary_key)
    }

    pub fn sorted_index_def(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
    ) -> Option<&SortedIndexDef> {
        self.sorted_indexes
            .get_def(database_id, tenant_id, index_name)
    }
}
