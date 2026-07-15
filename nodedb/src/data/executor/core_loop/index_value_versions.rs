// SPDX-License-Identifier: BUSL-1.1

//! Per-core, per-index write-VALUE version substrate.
//!
//! Sibling of [`super::write_index::WriteVersionIndex`]. That index records,
//! per written key/collection, the WAL LSN of the last committed write; this
//! one records — per `(database, tenant, collection, field)` secondary-index
//! dimension — the last committed-write LSN against each distinct indexed
//! VALUE. A later change validates an index-range read against the max LSN over
//! the read's value range instead of the coarse collection floor.
//!
//! Presence of the outer `(db, tenant, collection, field)` key is the
//! monotonic "tracked" flag: once any autocommit write records a value for a
//! field, that field stays tracked (horizon GC keeps the — now possibly empty
//! — inner map). An untracked field returns `None` (the validator falls back
//! to the collection floor); a tracked field with no in-range value returns
//! `Some(ZERO)`. A tracked field MUST have had every autocommit write's values
//! recorded — completeness is the recording chokepoints' contract.
//!
//! Lives on the `!Send` core: plain `HashMap`/`BTreeMap`, no atomics/locks.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use nodedb_types::{DatabaseId, TenantId};

use crate::types::Lsn;

use super::CoreLoop;

/// Horizon retain window for per-value entries, in LSNs. Mirrors the per-key
/// index so both substrates age out together.
const RETAIN_WINDOW: u64 = 16_384;

/// Hard upper bound on total per-value entries across every tracked dimension.
const MAX_INDEX_VALUE_ENTRIES: usize = 65_536;

/// Per-index dimension key: one secondary-index `field` of a
/// `(database, tenant, collection)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexKey {
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
    pub field: Box<str>,
}

/// The identity of an index dimension expressed as borrowable parts, so a
/// `HashMap` keyed by owned [`IndexKey`] can be probed with borrowed `&str`s
/// without allocating an owned key per lookup.
trait IndexDim {
    fn parts(&self) -> (DatabaseId, TenantId, &str, &str);
}

impl IndexDim for IndexKey {
    fn parts(&self) -> (DatabaseId, TenantId, &str, &str) {
        (self.db, self.tenant, &self.collection, &self.field)
    }
}

/// Borrowed probe into the per-index map — holds `&str`s, allocates nothing.
struct IndexDimRef<'a> {
    db: DatabaseId,
    tenant: TenantId,
    collection: &'a str,
    field: &'a str,
}

impl IndexDim for IndexDimRef<'_> {
    fn parts(&self) -> (DatabaseId, TenantId, &str, &str) {
        (self.db, self.tenant, self.collection, self.field)
    }
}

impl Hash for dyn IndexDim + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (db, tenant, collection, field) = self.parts();
        db.hash(state);
        tenant.hash(state);
        collection.hash(state);
        field.hash(state);
    }
}

impl PartialEq for dyn IndexDim + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn IndexDim + '_ {}

impl<'a> Borrow<dyn IndexDim + 'a> for IndexKey {
    fn borrow(&self) -> &(dyn IndexDim + 'a) {
        self
    }
}

impl Hash for IndexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as &dyn IndexDim).hash(state);
    }
}

/// Per-core, per-index write-VALUE version index.
#[derive(Default)]
pub struct IndexValueVersionIndex {
    per_index: HashMap<IndexKey, BTreeMap<Box<str>, Lsn>>,
}

impl IndexValueVersionIndex {
    /// Record a committed write of `value` on `(db, tenant, collection, field)`
    /// at `lsn`. Marks the dimension tracked if new; advances the value slot to
    /// `max(current, lsn)`.
    pub fn record(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        field: &str,
        value: &str,
        lsn: Lsn,
    ) {
        let key = IndexKey {
            db,
            tenant,
            collection: Box::from(collection),
            field: Box::from(field),
        };
        let values = self.per_index.entry(key).or_default();
        let slot = values.entry(Box::from(value)).or_insert(Lsn::ZERO);
        if lsn > *slot {
            *slot = lsn;
        }
    }

    /// Max committed-write LSN for an exact indexed value. `None` == the
    /// `(collection, field)` dimension is UNtracked (validator falls back to the
    /// collection floor). `Some(ZERO)` == tracked but this value has no recorded
    /// write on this core (never invalidated → always current).
    pub(in crate::data::executor) fn eq_max_lsn(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        field: &str,
        value: &str,
    ) -> Option<Lsn> {
        let values = self.per_index.get(&IndexDimRef {
            db,
            tenant,
            collection,
            field,
        } as &dyn IndexDim)?;
        Some(values.get(value).copied().unwrap_or(Lsn::ZERO))
    }

    /// Max committed-write LSN over the inclusive value range `[lo, hi]` (a
    /// `None` bound is open). `None` == untracked dimension; `Some(ZERO)` ==
    /// tracked with no in-range recorded write.
    pub(in crate::data::executor) fn range_max_lsn(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        field: &str,
        lo: Option<&str>,
        hi: Option<&str>,
    ) -> Option<Lsn> {
        use std::ops::Bound;
        let values = self.per_index.get(&IndexDimRef {
            db,
            tenant,
            collection,
            field,
        } as &dyn IndexDim)?;
        let lo_b = match lo {
            Some(s) => Bound::Included(s),
            None => Bound::Unbounded,
        };
        let hi_b = match hi {
            Some(s) => Bound::Included(s),
            None => Bound::Unbounded,
        };
        let mut max = Lsn::ZERO;
        for (_v, lsn) in values.range::<str, _>((lo_b, hi_b)) {
            if *lsn > max {
                max = *lsn;
            }
        }
        Some(max)
    }

    /// Test accessor: the recorded LSN for a `(db, tenant, collection, field)`
    /// dimension's `value`, or `None` if the dimension or value is untracked.
    #[cfg(test)]
    pub(in crate::data::executor) fn value_lsn(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        field: &str,
        value: &str,
    ) -> Option<Lsn> {
        self.per_index
            .get(&IndexDimRef {
                db,
                tenant,
                collection,
                field,
            } as &dyn IndexDim)
            .and_then(|values| values.get(value).copied())
    }

    /// Horizon GC against `watermark`, mirroring the per-key index. Evicts value
    /// entries below `watermark - RETAIN_WINDOW`, keeping the (possibly empty)
    /// inner map so the dimension stays tracked. A count backstop drops the
    /// lowest-LSN entries under a TOTAL order so tied-LSN eviction is
    /// replica-identical.
    pub fn gc(&mut self, watermark: Lsn) {
        let floor = watermark.as_u64().saturating_sub(RETAIN_WINDOW);
        let mut total = 0usize;
        for values in self.per_index.values_mut() {
            values.retain(|_, lsn| lsn.as_u64() >= floor);
            total += values.len();
        }
        if total > MAX_INDEX_VALUE_ENTRIES {
            let overflow = total - MAX_INDEX_VALUE_ENTRIES;
            let mut all: Vec<(Lsn, IndexKey, Box<str>)> = self
                .per_index
                .iter()
                .flat_map(|(k, values)| {
                    values
                        .iter()
                        .map(move |(v, lsn)| (*lsn, k.clone(), v.clone()))
                })
                .collect();
            all.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.db.as_u64().cmp(&b.1.db.as_u64()))
                    .then_with(|| a.1.tenant.as_u64().cmp(&b.1.tenant.as_u64()))
                    .then_with(|| a.1.collection.cmp(&b.1.collection))
                    .then_with(|| a.1.field.cmp(&b.1.field))
                    .then_with(|| a.2.cmp(&b.2))
            });
            for (_lsn, key, value) in all.into_iter().take(overflow) {
                if let Some(values) = self.per_index.get_mut(&key) {
                    values.remove(&value);
                }
            }
        }
    }
}

impl CoreLoop {
    /// Record a committed document write's touched secondary-index values into
    /// the per-index substrate. `tuples` is the write's `(field_path, value)`
    /// pairs (added ∪ removed), already materialized by the apply path. No-op on
    /// empty `tuples`.
    pub(in crate::data::executor) fn note_index_write_values(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        tuples: &[(String, String)],
        lsn: Lsn,
    ) {
        for (field, value) in tuples {
            self.write_index
                .index_values
                .record(db, tenant, collection, field, value, lsn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> DatabaseId {
        DatabaseId::DEFAULT
    }

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    fn key(field: &str) -> IndexKey {
        IndexKey {
            db: db(),
            tenant: tenant(),
            collection: Box::from("orders"),
            field: Box::from(field),
        }
    }

    #[test]
    fn record_tracks_dimension_and_stores_value_lsn() {
        let mut index = IndexValueVersionIndex::default();
        index.record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(10));
        let values = index
            .per_index
            .get(&key("email"))
            .expect("dimension tracked after record");
        assert_eq!(values.get("a@b.c"), Some(&Lsn::new(10)));
    }

    #[test]
    fn record_is_monotonic_max_per_value() {
        let mut index = IndexValueVersionIndex::default();
        index.record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(10));
        index.record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(30));
        // A later record with a LOWER LSN must not regress the slot.
        index.record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(20));
        let values = index.per_index.get(&key("email")).expect("tracked");
        assert_eq!(values.get("a@b.c"), Some(&Lsn::new(30)));
    }

    #[test]
    fn gc_horizon_evicts_but_keeps_dimension_tracked() {
        let mut index = IndexValueVersionIndex::default();
        index.record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(1));
        // Watermark far above the entry + retain window → entry evicted.
        index.gc(Lsn::new(RETAIN_WINDOW + 100));
        let values = index
            .per_index
            .get(&key("email"))
            .expect("dimension stays tracked after gc");
        assert!(values.is_empty(), "evicted value must not remain");
    }

    #[test]
    fn gc_horizon_retains_entries_above_floor() {
        let mut index = IndexValueVersionIndex::default();
        let watermark = Lsn::new(RETAIN_WINDOW + 100);
        // Below floor (evicted) and above floor (retained).
        index.record(db(), tenant(), "orders", "email", "old", Lsn::new(1));
        index.record(
            db(),
            tenant(),
            "orders",
            "email",
            "new",
            Lsn::new(watermark.as_u64()),
        );
        index.gc(watermark);
        let values = index.per_index.get(&key("email")).expect("tracked");
        assert_eq!(values.get("old"), None);
        assert_eq!(values.get("new"), Some(&watermark));
    }

    #[test]
    fn gc_count_backstop_is_insert_order_independent() {
        // Overflow the count backstop by one, with all entries tied at the same
        // LSN so eviction order falls entirely to the total order over
        // (db, tenant, collection, field, value). The surviving set must be the
        // same regardless of insertion order.
        let n = MAX_INDEX_VALUE_ENTRIES + 1;
        let lsn = Lsn::new(1_000_000);

        let build = |ascending: bool| {
            let mut index = IndexValueVersionIndex::default();
            let mut order: Vec<usize> = (0..n).collect();
            if !ascending {
                order.reverse();
            }
            for i in order {
                let value = format!("{i:08}");
                index.record(db(), tenant(), "orders", "email", &value, lsn);
            }
            // Watermark keeps every entry above the horizon floor, so only the
            // count backstop fires.
            index.gc(lsn);
            index
        };

        let asc = build(true);
        let desc = build(false);

        assert_eq!(asc.per_index, desc.per_index);
        // The lowest value under the total order is dropped, the next survives.
        let dropped = format!("{:08}", 0);
        let survivor = format!("{:08}", 1);
        let values = asc.per_index.get(&key("email")).expect("tracked");
        assert_eq!(values.get(dropped.as_str()), None);
        assert_eq!(values.get(survivor.as_str()), Some(&lsn));
    }
}
