// SPDX-License-Identifier: BUSL-1.1

//! Robin Hood probing primitives and incremental rehash implementation.

use super::super::entry::KvEntry;
use super::types::KvHashTable;

impl KvHashTable {
    // -----------------------------------------------------------------------
    // Internal: Robin Hood probing
    // -----------------------------------------------------------------------

    /// Probe distance (PSL): how far an entry is from its ideal slot.
    pub(super) fn probe_distance(capacity: usize, hash: u64, current_idx: usize) -> usize {
        let ideal = (hash as usize) & (capacity - 1);
        current_idx.wrapping_sub(ideal) & (capacity - 1)
    }

    /// Find an entry by key in a slot array. Returns a reference.
    pub(super) fn probe_find<'a>(
        &self,
        slots: &'a [Option<KvEntry>],
        hash: u64,
        key: &[u8],
    ) -> Option<&'a KvEntry> {
        let cap = slots.len();
        let mut idx = (hash as usize) & (cap - 1);
        let mut dist = 0;

        loop {
            let entry = slots[idx].as_ref()?;
            if entry.hash == hash && entry.key == key {
                return Some(entry);
            }
            let entry_dist = Self::probe_distance(cap, entry.hash, idx);
            if dist > entry_dist {
                return None; // Robin Hood invariant: key can't be further.
            }
            idx = (idx + 1) & (cap - 1);
            dist += 1;
        }
    }

    pub(super) fn probe_find_index_static(
        slots: &[Option<KvEntry>],
        hash: u64,
        key: &[u8],
    ) -> Option<usize> {
        let cap = slots.len();
        let mut idx = (hash as usize) & (cap - 1);
        let mut dist = 0;

        loop {
            let entry = slots[idx].as_ref()?;
            if entry.hash == hash && entry.key == key {
                return Some(idx);
            }
            let entry_dist = Self::probe_distance(cap, entry.hash, idx);
            if dist > entry_dist {
                return None;
            }
            idx = (idx + 1) & (cap - 1);
            dist += 1;
        }
    }

    /// Robin Hood insertion: insert an entry, swapping with entries that have
    /// shorter probe distances to maintain the Robin Hood invariant.
    pub(super) fn robin_hood_insert(slots: &mut [Option<KvEntry>], mut entry: KvEntry) {
        let cap = slots.len();
        let mut idx = (entry.hash as usize) & (cap - 1);
        let mut dist = 0;

        loop {
            match &slots[idx] {
                None => {
                    slots[idx] = Some(entry);
                    return;
                }
                Some(existing) => {
                    let existing_dist = Self::probe_distance(cap, existing.hash, idx);
                    if dist > existing_dist {
                        // Steal this slot (Robin Hood: take from the rich).
                        // slots[idx] is Some — we matched Some(existing) in this arm.
                        let Some(displaced) = slots[idx].take() else {
                            return;
                        };

                        slots[idx] = Some(entry);
                        entry = displaced;
                        dist = existing_dist;
                    }
                }
            }
            idx = (idx + 1) & (cap - 1);
            dist += 1;
        }
    }

    /// Backward-shift deletion: after removing a slot, shift subsequent entries
    /// back to fill the gap, maintaining Robin Hood probe distance invariant.
    pub(super) fn repair_after_delete_static(slots: &mut [Option<KvEntry>], deleted_idx: usize) {
        let cap = slots.len();
        let mut idx = deleted_idx;
        loop {
            let next = (idx + 1) & (cap - 1);
            match &slots[next] {
                None => break,
                Some(entry) => {
                    let d = Self::probe_distance(cap, entry.hash, next);
                    if d == 0 {
                        break; // Entry is at its ideal slot, no shift needed.
                    }
                }
            }
            slots.swap(idx, next);
            idx = next;
        }
    }

    // -----------------------------------------------------------------------
    // Internal: incremental rehash
    // -----------------------------------------------------------------------

    /// Start a rehash if load factor exceeds the threshold.
    pub(super) fn maybe_start_rehash(&mut self) {
        if self.rehash_source.is_some() {
            return; // Already rehashing.
        }
        if self.load_factor() <= self.load_factor_threshold {
            return;
        }
        let new_capacity = self.capacity * 2;
        let old_slots = std::mem::replace(&mut self.slots, vec![None; new_capacity]);
        self.rehash_source = Some(old_slots);
        self.rehash_cursor = 0;
        self.capacity = new_capacity;
    }

    /// Migrate `rehash_batch_size` entries from the old table to the new one.
    pub(super) fn rehash_step(&mut self) {
        let batch = self.rehash_batch_size;
        let Some(old) = &mut self.rehash_source else {
            return;
        };

        let old_len = old.len();
        let mut migrated = 0;

        while migrated < batch && self.rehash_cursor < old_len {
            if let Some(entry) = old[self.rehash_cursor].take() {
                Self::robin_hood_insert(&mut self.slots, entry);
                migrated += 1;
            }
            self.rehash_cursor += 1;
        }

        // If we've scanned the entire old table, rehash is complete.
        if self.rehash_cursor >= old_len {
            self.rehash_source = None;
            self.rehash_cursor = 0;
        }
    }
}
