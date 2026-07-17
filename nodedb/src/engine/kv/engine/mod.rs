// SPDX-License-Identifier: BUSL-1.1

//! KvEngine: per-core KV engine owning hash tables and expiry wheel.
//!
//! `!Send` — owned by a single TPC core. Each collection gets its own
//! hash table; the expiry wheel is shared across all collections on
//! this core (one wheel tick processes all collections).

mod checkpoint_export;
mod checkpoint_restore;
mod scan_ops;
#[cfg(test)]
mod tests;

pub use checkpoint_export::KvCollectionRef;
pub use checkpoint_restore::{RestoreCompositeIndexParams, RestoreFieldIndexParams};

use std::collections::HashMap;

use nodedb_types::Surrogate;

use super::batch_put::KvBatchPutParams;
use super::engine_helpers::{expiry_prefix, table_key};
use super::engine_write::KvPutParams;
use super::expiry_wheel::ExpiryWheel;
use super::hash_table::{EntryMeta, KvHashTable};
use super::index::KvIndexSet;

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
    /// Per-collection hash tables. Key: hash of "{database_id}:{tenant_id}:{collection}".
    pub(crate) tables: HashMap<u64, KvHashTable>,
    /// Per-collection secondary index sets. Key: hash of "{database_id}:{tenant_id}:{collection}".
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
    pub fn purge_collection(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> usize {
        let tkey = super::engine_helpers::table_key(database_id, tenant_id, collection);
        let mut removed = 0;
        if self.tables.remove(&tkey).is_some() {
            removed += 1;
        }
        self.indexes.remove(&tkey);
        self.hash_to_tenant.remove(&tkey);
        self.hash_to_collection.remove(&tkey);
        self.sorted_indexes
            .purge_collection(database_id, tenant_id, collection);

        // Eagerly drop pending TTL-wheel entries for this collection.
        // Stale entries would otherwise no-op at fire time (the table
        // they reference is gone), but they still consume reap budget
        // per tick — for a large collection with many TTLs, that's
        // wasted work until every scheduled time has passed.
        let prefix = expiry_prefix(database_id, tenant_id, collection).into_bytes();
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
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        surrogate: Surrogate,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables
            .get(&tkey)?
            .key_for_surrogate(surrogate)
            .map(|k| k.to_vec())
    }

    /// GET: O(1) hash table lookup. Returns None if not found or expired.
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey)?.get(key, now_ms).map(|v| v.to_vec())
    }

    /// GET with surrogate: returns the value bytes AND the row's stable
    /// surrogate when the binding was made.  Used by the clone-delegated
    /// read path to enforce a per-row surrogate ceiling — bindings the
    /// source allocated AFTER the clone's AS-OF point are filtered out.
    pub fn get_with_surrogate(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<(Vec<u8>, nodedb_types::Surrogate)> {
        let tkey = table_key(database_id, tenant_id, collection);
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
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<i64> {
        let tkey = table_key(database_id, tenant_id, collection);
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

    /// Return the current TTL metadata for a key, or `None` if the key does
    /// not exist in this collection.
    ///
    /// Unlike [`KvEngine::get_ttl_ms`], this does not resolve against
    /// `now_ms` or check expiry -- it returns the raw `(has_ttl,
    /// expire_at_ms)` pair verbatim. Used to capture a key's exact prior TTL
    /// state before `Expire`/`Persist` mutate it, so a transaction rollback
    /// can restore the precise absolute instant rather than an
    /// approximation derived from elapsed wall-clock time.
    pub fn get_ttl_meta(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> Option<EntryMeta> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey)?.get_entry_meta(key)
    }

    /// BATCH GET: fetch multiple keys. Returns values in order (None for missing).
    pub fn batch_get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> Vec<Option<Vec<u8>>> {
        keys.iter()
            .map(|k| self.get(database_id, tenant_id, collection, k, now_ms))
            .collect()
    }

    /// BATCH PUT: insert/update multiple pairs. Returns count of new keys.
    ///
    /// `surrogates` carries each entry's stable cross-engine identity,
    /// same order and length as `entries` -- assigned by the CP-side
    /// `SurrogateAssigner` from `(collection, key)`, same mechanism as a
    /// single-key `put`. Pass `Surrogate::ZERO` per-entry only from internal
    /// RMW callers that do not allocate one (existing entries preserve
    /// their bound surrogate either way, per `put`'s semantics).
    pub fn batch_put(&mut self, params: KvBatchPutParams<'_>) -> usize {
        let KvBatchPutParams {
            database_id,
            tenant_id,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        } = params;
        let mut new_count = 0;
        for (i, (key, value)) in entries.iter().enumerate() {
            let surrogate = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
            if self
                .put(KvPutParams {
                    database_id,
                    tenant_id,
                    collection,
                    key: key.as_slice(),
                    value: value.as_slice(),
                    ttl_ms,
                    now_ms,
                    surrogate,
                })
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }

    /// BATCH PUT installing an already-resolved absolute expiry instant on
    /// every entry. Mirrors [`KvEngine::put_with_absolute_expiry`]: WAL redo
    /// replay uses this so a TTL'd batch recovers with the exact expiry the
    /// original write computed, rather than recomputing `now_ms + ttl_ms` at
    /// recovery time (which would push expiry forward by the crash-to-restart
    /// delay). `params.ttl_ms` is carried through `put_with_absolute_expiry`
    /// only for `KvPutParams`'s shape; the installed expiry is `expire_at_ms`
    /// verbatim, same for every entry in the batch.
    pub fn batch_put_with_absolute_expiry(
        &mut self,
        params: KvBatchPutParams<'_>,
        expire_at_ms: u64,
    ) -> usize {
        let KvBatchPutParams {
            database_id,
            tenant_id,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        } = params;
        let mut new_count = 0;
        for (i, (key, value)) in entries.iter().enumerate() {
            let surrogate = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
            if self
                .put_with_absolute_expiry(
                    KvPutParams {
                        database_id,
                        tenant_id,
                        collection,
                        key: key.as_slice(),
                        value: value.as_slice(),
                        ttl_ms,
                        now_ms,
                        surrogate,
                    },
                    expire_at_ms,
                )
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }
}
