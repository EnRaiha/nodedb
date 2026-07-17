// SPDX-License-Identifier: BUSL-1.1

//! Public KvHashTable API: get, put, delete, reap, set_expire, persist.

use nodedb_types::Surrogate;

use super::super::entry::{KvEntry, NO_EXPIRY};
use super::super::hash_helpers::{
    extract_value_from, free_value_from, hash_key, read_value_from, store_value_in,
};
use super::types::{EntryMeta, KvHashTable};

impl KvHashTable {
    /// Get a value by key. Returns None if not found or expired.
    ///
    /// Checks the primary table first, then the rehash source (if active).
    /// Expired keys return None (lazy expiry fallback).
    pub fn get(&self, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        self.get_with_surrogate(key, now_ms).map(|(v, _)| v)
    }

    /// Get the value AND the row's stable surrogate for a key.  Used by
    /// the clone-delegated read path to filter rows whose binding was
    /// allocated AFTER the clone's AS-OF point (snapshot isolation).
    /// Returns `None` if the key is missing or expired.
    pub fn get_with_surrogate(&self, key: &[u8], now_ms: u64) -> Option<(&[u8], Surrogate)> {
        let h = hash_key(key);

        // Check primary table.
        if let Some(entry) = self.probe_find(&self.slots, h, key) {
            if entry.is_expired(now_ms) {
                return None;
            }
            return Some((read_value_from(entry, &self.overflow), entry.surrogate));
        }

        // Check rehash source if active.
        if let Some(old) = &self.rehash_source
            && let Some(entry) = self.probe_find(old, h, key)
        {
            if entry.is_expired(now_ms) {
                return None;
            }
            return Some((read_value_from(entry, &self.overflow), entry.surrogate));
        }

        None
    }

    /// Get entry metadata without returning the value.
    ///
    /// Returns the TTL state and expiry timestamp for a key, used by
    /// the KV engine to cancel old expiry entries before updates.
    /// Does NOT check expiry — returns metadata even for expired keys,
    /// since the caller needs the original `expire_at_ms` for cancellation.
    pub fn get_entry_meta(&self, key: &[u8]) -> Option<EntryMeta> {
        let h = hash_key(key);

        if let Some(entry) = self.probe_find(&self.slots, h, key) {
            return Some(EntryMeta {
                has_ttl: entry.has_ttl(),
                expire_at_ms: entry.expire_at_ms,
            });
        }

        if let Some(old) = &self.rehash_source
            && let Some(entry) = self.probe_find(old, h, key)
        {
            return Some(EntryMeta {
                has_ttl: entry.has_ttl(),
                expire_at_ms: entry.expire_at_ms,
            });
        }

        None
    }

    /// Read a key's value ignoring expiry.
    ///
    /// Unlike [`get`], returns the bytes of an entry whose TTL has elapsed but
    /// which the expiry wheel has not reaped yet. The reaper needs them to
    /// remove the row's secondary/sorted index entries, which are keyed on the
    /// field values held in the value itself — by the time the reaper runs the
    /// TTL has elapsed by definition, so a `get` would report the row as
    /// already gone and the index entries would be stranded.
    ///
    /// [`get`]: KvHashTable::get
    pub fn get_ignoring_expiry(&self, key: &[u8]) -> Option<&[u8]> {
        let h = hash_key(key);

        if let Some(entry) = self.probe_find(&self.slots, h, key) {
            return Some(read_value_from(entry, &self.overflow));
        }

        // A live entry that a rehash has not migrated yet exists ONLY here —
        // probing the primary alone would silently skip index cleanup for
        // exactly the rows a rehash is in the middle of moving.
        if let Some(old) = &self.rehash_source
            && let Some(entry) = self.probe_find(old, h, key)
        {
            return Some(read_value_from(entry, &self.overflow));
        }

        None
    }

    /// Insert or update a key-value pair. Returns the old value bytes if overwritten.
    ///
    /// `surrogate` is the row's stable global identity:
    /// - On insert of a new row, it is recorded in the reverse map.
    /// - On update of an existing row, the entry's existing surrogate is
    ///   preserved unless `surrogate` is non-zero AND the existing entry
    ///   is unbound (`Surrogate::ZERO`), in which case the entry is bound.
    /// - `Surrogate::ZERO` is the unbound sentinel — used by internal
    ///   read-modify-write callers that do not allocate an identity.
    ///
    /// Triggers incremental rehash migration if a rehash is in progress.
    /// Triggers a new rehash if the load factor exceeds the threshold.
    pub fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        expire_at_ms: u64,
        surrogate: Surrogate,
    ) -> Option<Vec<u8>> {
        // Progress incremental rehash.
        self.rehash_step();

        let h = hash_key(key);

        // Check if key exists in primary — update in place (no key copy needed).
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            // probe_find_index_static guarantees slots[idx] is Some.
            let old_value = {
                let slot = self.slots[idx].as_ref()?;
                let v = extract_value_from(&slot.value, &self.overflow);
                free_value_from(&slot.value, &mut self.overflow);
                v
            };
            let new_kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
            if let Some(entry) = self.slots[idx].as_mut() {
                entry.value = new_kv_value;
                entry.expire_at_ms = expire_at_ms;
                // Late-bind a surrogate onto a previously-unbound entry.
                if entry.surrogate == Surrogate::ZERO && surrogate != Surrogate::ZERO {
                    entry.surrogate = surrogate;
                    self.surrogate_to_key.insert(surrogate.0, key.to_vec());
                }
            }
            return Some(old_value);
        }

        // Check rehash source — if found, remove from old and insert into primary.
        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            let old_entry = old_slots[idx].take()?;
            let old_value = extract_value_from(&old_entry.value, &self.overflow);
            free_value_from(&old_entry.value, &mut self.overflow);
            let new_kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
            let preserved = if old_entry.surrogate != Surrogate::ZERO {
                old_entry.surrogate
            } else {
                surrogate
            };
            if preserved != Surrogate::ZERO {
                self.surrogate_to_key.insert(preserved.0, key.to_vec());
            }
            let new_entry = KvEntry {
                hash: h,
                key: key.to_vec(), // Only copy key when migrating from rehash source.
                value: new_kv_value,
                expire_at_ms,
                surrogate: preserved,
            };
            Self::robin_hood_insert(&mut self.slots, new_entry);
            return Some(old_value);
        }

        // New key — insert into primary. Single key copy here (unavoidable — entry owns key).
        let kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
        if surrogate != Surrogate::ZERO {
            self.surrogate_to_key.insert(surrogate.0, key.to_vec());
        }
        let entry = KvEntry {
            hash: h,
            key: key.to_vec(),
            value: kv_value,
            expire_at_ms,
            surrogate,
        };
        Self::robin_hood_insert(&mut self.slots, entry);
        self.len += 1;

        // Check if we need to start a rehash.
        self.maybe_start_rehash();

        None
    }

    /// Delete a key. Returns true if the key existed and was removed.
    pub fn delete(&mut self, key: &[u8], now_ms: u64) -> bool {
        let h = hash_key(key);

        // Try primary table.
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            let Some(entry) = self.slots[idx].take() else {
                return false;
            };
            free_value_from(&entry.value, &mut self.overflow);
            if entry.surrogate != Surrogate::ZERO {
                self.surrogate_to_key.remove(&entry.surrogate.0);
            }
            Self::repair_after_delete_static(&mut self.slots, idx);
            self.len -= 1;
            return true;
        }

        // Try rehash source.
        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            let Some(entry) = old_slots[idx].take() else {
                return false;
            };
            free_value_from(&entry.value, &mut self.overflow);
            if entry.surrogate != Surrogate::ZERO {
                self.surrogate_to_key.remove(&entry.surrogate.0);
            }
            Self::repair_after_delete_static(old_slots, idx);
            self.len -= 1;
            return true;
        }

        // If key doesn't exist at all, nothing to do.
        let _ = now_ms;
        false
    }

    /// Remove an expired entry by key (called by the expiry wheel reaper).
    /// Only removes if the entry still exists and its expire_at_ms matches.
    pub fn reap_expired(&mut self, key: &[u8], expected_expire_ms: u64) -> bool {
        let h = hash_key(key);

        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key)
            && self.slots[idx]
                .as_ref()
                .is_some_and(|e| e.expire_at_ms == expected_expire_ms)
        {
            let Some(entry) = self.slots[idx].take() else {
                return false;
            };
            free_value_from(&entry.value, &mut self.overflow);
            if entry.surrogate != Surrogate::ZERO {
                self.surrogate_to_key.remove(&entry.surrogate.0);
            }
            Self::repair_after_delete_static(&mut self.slots, idx);
            self.len -= 1;
            return true;
        }

        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
            && old_slots[idx]
                .as_ref()
                .is_some_and(|e| e.expire_at_ms == expected_expire_ms)
        {
            let Some(entry) = old_slots[idx].take() else {
                return false;
            };
            free_value_from(&entry.value, &mut self.overflow);
            if entry.surrogate != Surrogate::ZERO {
                self.surrogate_to_key.remove(&entry.surrogate.0);
            }
            Self::repair_after_delete_static(old_slots, idx);
            self.len -= 1;
            return true;
        }

        false
    }

    /// Update the TTL of an existing key. Returns true if the key was found.
    pub fn set_expire(&mut self, key: &[u8], expire_at_ms: u64) -> bool {
        let h = hash_key(key);

        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            if let Some(entry) = self.slots[idx].as_mut() {
                entry.expire_at_ms = expire_at_ms;
            }
            return true;
        }

        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            if let Some(entry) = old_slots[idx].as_mut() {
                entry.expire_at_ms = expire_at_ms;
            }
            return true;
        }

        false
    }

    /// Remove TTL from a key (make it persistent). Returns true if found.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        self.set_expire(key, NO_EXPIRY)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::super::super::entry::NO_EXPIRY;
    use super::super::types::KvHashTable;

    fn make_table() -> KvHashTable {
        KvHashTable::new(16, 0.75, 4, 64)
    }

    #[test]
    fn basic_put_get_delete() {
        let mut t = make_table();
        assert!(t.is_empty());

        t.put(b"key1", b"value1", NO_EXPIRY, Surrogate::ZERO);
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(b"key1", 0), Some(b"value1".as_slice()));

        t.put(b"key2", b"value2", NO_EXPIRY, Surrogate::ZERO);
        assert_eq!(t.len(), 2);

        assert!(t.delete(b"key1", 0));
        assert_eq!(t.len(), 1);
        assert!(t.get(b"key1", 0).is_none());
        assert_eq!(t.get(b"key2", 0), Some(b"value2".as_slice()));
    }

    #[test]
    fn overwrite_returns_old_value() {
        let mut t = make_table();
        assert!(t.put(b"k", b"v1", NO_EXPIRY, Surrogate::ZERO).is_none());
        let old = t.put(b"k", b"v2", NO_EXPIRY, Surrogate::ZERO);
        assert_eq!(old, Some(b"v1".to_vec()));
        assert_eq!(t.get(b"k", 0), Some(b"v2".as_slice()));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let mut t = make_table();
        assert!(!t.delete(b"nope", 0));
    }

    #[test]
    fn lazy_expiry_on_get() {
        let mut t = make_table();
        t.put(b"k", b"v", 1000, Surrogate::ZERO);

        assert_eq!(t.get(b"k", 999), Some(b"v".as_slice()));
        assert!(t.get(b"k", 1000).is_none()); // Expired.
        assert!(t.get(b"k", 2000).is_none());
    }

    #[test]
    fn set_expire_and_persist() {
        let mut t = make_table();
        t.put(b"k", b"v", NO_EXPIRY, Surrogate::ZERO);

        assert!(t.set_expire(b"k", 5000));
        assert!(t.get(b"k", 4999).is_some());
        assert!(t.get(b"k", 5000).is_none());

        // Reset expiry to force it to be visible again — need to re-put.
        t.put(b"k", b"v", 10000, Surrogate::ZERO);
        assert!(t.persist(b"k"));
        assert!(t.get(b"k", u64::MAX).is_some()); // Never expires.
    }

    #[test]
    fn reap_expired_removes_matching() {
        let mut t = make_table();
        t.put(b"k", b"v", 5000, Surrogate::ZERO);

        // Wrong expire_at_ms — should not reap.
        assert!(!t.reap_expired(b"k", 9999));
        assert_eq!(t.len(), 1);

        // Correct expire_at_ms — should reap.
        assert!(t.reap_expired(b"k", 5000));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn incremental_rehash() {
        let mut t = KvHashTable::new(16, 0.5, 2, 64);

        // Fill to trigger rehash (>50% of 16 = >8 entries).
        for i in 0..10 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            t.put(key.as_bytes(), val.as_bytes(), NO_EXPIRY, Surrogate::ZERO);
        }

        // Rehash should have been triggered.
        // Continue inserting to drive incremental migration.
        for i in 10..20 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            t.put(key.as_bytes(), val.as_bytes(), NO_EXPIRY, Surrogate::ZERO);
        }

        // All entries should be findable.
        for i in 0..20 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            assert_eq!(
                t.get(key.as_bytes(), 0),
                Some(val.as_bytes()),
                "missing key{i:03}"
            );
        }
        assert_eq!(t.len(), 20);
    }

    #[test]
    fn overflow_values() {
        let mut t = KvHashTable::new(16, 0.75, 4, 8); // 8-byte inline threshold.
        let small = b"tiny".to_vec(); // 4 bytes — inline.
        let large = vec![0xAB; 100]; // 100 bytes — overflow.

        t.put(b"s", &small, NO_EXPIRY, Surrogate::ZERO);
        t.put(b"l", &large, NO_EXPIRY, Surrogate::ZERO);

        assert_eq!(t.get(b"s", 0), Some(small.as_slice()));
        assert_eq!(t.get(b"l", 0), Some(large.as_slice()));
    }

    #[test]
    fn many_inserts_and_deletes_no_corruption() {
        let mut t = KvHashTable::new(32, 0.75, 8, 64);

        // Insert 500 keys.
        for i in 0u32..500 {
            t.put(
                &i.to_be_bytes(),
                &(i * 7).to_be_bytes(),
                NO_EXPIRY,
                Surrogate::ZERO,
            );
        }
        assert_eq!(t.len(), 500);

        // Delete even keys.
        for i in (0u32..500).step_by(2) {
            let key = i.to_be_bytes().to_vec();
            assert!(t.delete(&key, 0), "failed to delete key {i}");
        }
        assert_eq!(t.len(), 250);

        // Verify odd keys are still present.
        for i in (1u32..500).step_by(2) {
            let key = i.to_be_bytes();
            let expected = (i * 7).to_be_bytes();
            assert_eq!(
                t.get(&key, 0),
                Some(expected.as_slice()),
                "missing odd key {i}"
            );
        }

        // Verify even keys are gone.
        for i in (0u32..500).step_by(2) {
            let key = i.to_be_bytes();
            assert!(t.get(&key, 0).is_none(), "even key {i} should be deleted");
        }
    }

    #[test]
    fn get_entry_meta_returns_ttl_info() {
        let mut t = make_table();
        // Key without TTL.
        t.put(b"persistent", b"v", NO_EXPIRY, Surrogate::ZERO);
        let meta = t.get_entry_meta(b"persistent").unwrap();
        assert!(!meta.has_ttl);
        assert_eq!(meta.expire_at_ms, NO_EXPIRY);

        // Key with TTL.
        t.put(b"ephemeral", b"v", 5000, Surrogate::ZERO);
        let meta = t.get_entry_meta(b"ephemeral").unwrap();
        assert!(meta.has_ttl);
        assert_eq!(meta.expire_at_ms, 5000);

        // Non-existent key.
        assert!(t.get_entry_meta(b"nope").is_none());
    }

    #[test]
    fn surrogate_round_trip_via_reverse_map() {
        let mut t = make_table();
        let s1 = Surrogate::new(101);
        let s2 = Surrogate::new(202);

        t.put(b"alpha", b"v1", NO_EXPIRY, s1);
        t.put(b"beta", b"v2", NO_EXPIRY, s2);

        assert_eq!(t.surrogate_count(), 2);
        assert_eq!(t.key_for_surrogate(s1), Some(b"alpha".as_slice()));
        assert_eq!(t.key_for_surrogate(s2), Some(b"beta".as_slice()));
        assert!(t.key_for_surrogate(Surrogate::new(999)).is_none());
        assert!(t.key_for_surrogate(Surrogate::ZERO).is_none());

        // Updating an existing key with a non-zero surrogate must
        // preserve the original surrogate (assigner is idempotent).
        t.put(b"alpha", b"v1b", NO_EXPIRY, Surrogate::new(303));
        assert_eq!(t.key_for_surrogate(s1), Some(b"alpha".as_slice()));
        assert!(t.key_for_surrogate(Surrogate::new(303)).is_none());

        // Delete drops the reverse mapping.
        assert!(t.delete(b"alpha", 0));
        assert!(t.key_for_surrogate(s1).is_none());
        assert_eq!(t.surrogate_count(), 1);
    }

    #[test]
    fn unbound_entries_dont_pollute_reverse_map() {
        let mut t = make_table();
        t.put(b"k", b"v", NO_EXPIRY, Surrogate::ZERO);
        assert_eq!(t.surrogate_count(), 0);

        // Late-bind a surrogate via update.
        let s = Surrogate::new(7);
        t.put(b"k", b"v2", NO_EXPIRY, s);
        assert_eq!(t.surrogate_count(), 1);
        assert_eq!(t.key_for_surrogate(s), Some(b"k".as_slice()));
    }

    #[test]
    fn mem_usage_grows_with_entries() {
        let mut t = make_table();
        let base = t.mem_usage();

        for i in 0..100u32 {
            t.put(&i.to_be_bytes(), &[0u8; 32], NO_EXPIRY, Surrogate::ZERO);
        }

        assert!(t.mem_usage() > base);
    }
}
