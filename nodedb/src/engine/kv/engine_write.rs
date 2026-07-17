// SPDX-License-Identifier: BUSL-1.1

//! KvEngine write operations: PUT, DELETE, EXPIRE, PERSIST.

use nodedb_types::Surrogate;

use super::engine::KvEngine;
use super::engine_helpers::{expiry_key, extract_all_field_values_from_msgpack, table_key};
use super::entry::NO_EXPIRY;
use super::hash_table::KvHashTable;

/// Parameters for [`KvEngine::put`].
#[derive(Debug, Clone, Copy)]
pub struct KvPutParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub now_ms: u64,
    pub surrogate: Surrogate,
}

impl KvEngine {
    /// PUT: insert or update. Returns old value if overwritten.
    ///
    /// If `ttl_ms > 0`, schedules expiry. If the key already had a TTL,
    /// the old expiry is cancelled and replaced.
    ///
    /// `surrogate` is the row's stable global identity. Pass
    /// `Surrogate::ZERO` from internal RMW callers that do not allocate
    /// one — existing entries preserve their bound surrogate either way.
    pub fn put(&mut self, params: KvPutParams<'_>) -> Option<Vec<u8>> {
        let expire_at = if params.ttl_ms > 0 {
            params.now_ms + params.ttl_ms
        } else {
            NO_EXPIRY
        };
        self.put_resolved(params, expire_at)
    }

    /// PUT installing an already-resolved absolute expiry instant.
    ///
    /// `expire_at_ms` is the absolute wall-clock instant (ms since epoch) the
    /// key expires at, or [`NO_EXPIRY`] (`0`) for no TTL. Unlike [`put`], which
    /// derives expiry as `now_ms + ttl_ms`, this installs the supplied instant
    /// verbatim. WAL redo replay uses it so a TTL'd key recovers with the exact
    /// expiry the original write computed — recomputing `now_ms + ttl_ms` at
    /// recovery time would push expiry forward by the crash-to-restart delay.
    ///
    /// [`put`]: KvEngine::put
    pub fn put_with_absolute_expiry(
        &mut self,
        params: KvPutParams<'_>,
        expire_at_ms: u64,
    ) -> Option<Vec<u8>> {
        self.put_resolved(params, expire_at_ms)
    }

    /// Shared PUT body: insert/update the key with an already-resolved absolute
    /// `expire_at` (or [`NO_EXPIRY`]), maintaining the expiry wheel and both
    /// secondary and sorted indexes. `params.ttl_ms` is intentionally unused
    /// here — expiry is fully determined by `expire_at`.
    fn put_resolved(&mut self, params: KvPutParams<'_>, expire_at: u64) -> Option<Vec<u8>> {
        let KvPutParams {
            database_id,
            tenant_id,
            collection,
            key,
            value,
            ttl_ms: _,
            now_ms,
            surrogate,
        } = params;

        let tkey = table_key(database_id, tenant_id, collection);

        // Single-pass: check indexes + get old entry meta in one HashMap lookup.
        let has_indexes = self.indexes.get(&tkey).is_some_and(|idx| !idx.is_empty());
        let old_expire = self
            .tables
            .get(&tkey)
            .and_then(|t| t.get_entry_meta(key))
            .and_then(|m| {
                if m.has_ttl {
                    Some(m.expire_at_ms)
                } else {
                    None
                }
            });

        // Cancel old expiry (before mutating the table).
        if let Some(old_ms) = old_expire {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, old_ms);
        }

        // Insert/update. Use get_mut (no clone) for existing tables,
        // entry (clones tkey) only for first-time table creation.
        let table = if let Some(t) = self.tables.get_mut(&tkey) {
            t
        } else {
            self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
            self.hash_to_collection
                .entry(tkey)
                .or_insert_with(|| collection.to_string());
            self.tables.entry(tkey).or_insert_with(|| {
                KvHashTable::new(
                    self.default_capacity,
                    self.load_factor_threshold,
                    self.rehash_batch_size,
                    self.inline_threshold,
                )
            })
        };
        let old = table.put(key, value, expire_at, surrogate);

        // Schedule new expiry.
        if expire_at != NO_EXPIRY {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
        }

        // Secondary index maintenance (zero-index fast path: skip entirely).
        let has_sorted = self.sorted_indexes.has_indexes(tkey);
        if has_indexes || has_sorted {
            let new_value_bytes: Vec<u8> = self
                .tables
                .get(&tkey)
                .and_then(|t| t.get(key, now_ms))
                .map(|v| v.to_vec())
                .unwrap_or_default();
            let new_fields = extract_all_field_values_from_msgpack(&new_value_bytes);
            let old_fields = old
                .as_ref()
                .map(|v| extract_all_field_values_from_msgpack(v));

            if has_indexes {
                let new_refs: Vec<(&str, &[u8])> = new_fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                let old_refs: Option<Vec<(&str, &[u8])>> = old_fields
                    .as_ref()
                    .map(|f| f.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect());

                if let Some(idx_set) = self.indexes.get_mut(&tkey) {
                    idx_set.on_put(key, &new_refs, old_refs.as_deref());
                }
            }

            if has_sorted {
                self.sorted_indexes.on_put(tkey, key, &new_fields);
            }
        }

        old
    }

    /// DELETE: remove key(s). Returns count of keys actually deleted.
    pub fn delete(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return 0,
        };

        let mut count = 0;
        let has_indexes = self.indexes.get(&tkey).is_some_and(|s| !s.is_empty());
        let has_sorted = self.sorted_indexes.has_indexes(tkey);

        for key in keys {
            // Cancel expiry if the key had one.
            if let Some(meta) = table.get_entry_meta(key)
                && meta.has_ttl
            {
                let composite = expiry_key(database_id, tenant_id, collection, key);
                self.expiry.cancel(&composite, meta.expire_at_ms);
            }

            // Extract field values before deletion (for index cleanup).
            // Expiry-blind: `table.delete` below removes the row regardless of
            // expiry, so for a key whose TTL has elapsed but which the wheel has
            // not reaped yet, a `get(key, now_ms)` would return `None` and leave
            // its index entries stranded behind a successful DELETE.
            let old_fields = if has_indexes {
                table
                    .get_ignoring_expiry(key)
                    .map(extract_all_field_values_from_msgpack)
            } else {
                None
            };

            if table.delete(key, now_ms) {
                count += 1;

                // Clean up secondary indexes.
                if let Some(fields) = &old_fields
                    && let Some(idx_set) = self.indexes.get_mut(&tkey)
                {
                    let refs: Vec<(&str, &[u8])> = fields
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_slice()))
                        .collect();
                    idx_set.on_delete(key, &refs);
                }

                // Clean up sorted indexes.
                if has_sorted {
                    self.sorted_indexes.on_delete(tkey, key);
                }
            }
        }
        count
    }

    /// EXPIRE: set or update TTL on an existing key.
    /// Returns true if the key was found and TTL was set.
    pub fn expire(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        ttl_ms: u64,
        now_ms: u64,
    ) -> bool {
        self.expire_resolved(database_id, tenant_id, collection, key, now_ms + ttl_ms)
    }

    /// EXPIRE installing an already-resolved absolute expiry instant.
    ///
    /// `expire_at_ms` is the absolute wall-clock instant (ms since epoch) the
    /// key expires at. Unlike [`expire`], which derives it as
    /// `now_ms + ttl_ms`, this installs the supplied instant verbatim. WAL
    /// redo replay uses it so a key's expiry recovers with the exact instant
    /// the original write computed — recomputing `now_ms + ttl_ms` at
    /// recovery time would push expiry forward by the crash-to-restart delay.
    ///
    /// [`expire`]: KvEngine::expire
    pub fn expire_with_absolute_expiry(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        expire_at_ms: u64,
    ) -> bool {
        self.expire_resolved(database_id, tenant_id, collection, key, expire_at_ms)
    }

    /// Shared EXPIRE body: install an already-resolved absolute `expire_at`
    /// on an existing key. Returns true if the key was found and TTL was set.
    fn expire_resolved(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        expire_at: u64,
    ) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return false,
        };

        // Cancel old expiry.
        if let Some(meta) = table.get_entry_meta(key)
            && meta.has_ttl
        {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        if table.set_expire(key, expire_at) {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
            true
        } else {
            false
        }
    }

    /// PERSIST: remove TTL from a key. Returns true if the key was found.
    pub fn persist(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return false,
        };

        if let Some(meta) = table.get_entry_meta(key)
            && meta.has_ttl
        {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        table.persist(key)
    }
}
