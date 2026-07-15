// SPDX-License-Identifier: BUSL-1.1

//! Per-core last-write-LSN version index.
//!
//! Records, for every committed write applied on this Data-Plane core, the WAL
//! LSN of the write against the written key (`last_write_lsn`) and against the
//! written collection (`coll_write_lsn`). The WAL LSN is allocated in the
//! Control Plane at wal-dispatch time and threaded onto the write task; the
//! apply chokepoints on this core feed it here.
//!
//! This is the shard-local write-version substrate the optimistic-concurrency
//! commit path validates a transaction's read-set against (see
//! [`CoreLoop::read_set_still_current`]). Because the index lives on the
//! `!Send` core it is a plain `HashMap`: no atomics, no locks, no cross-core
//! sharing.
//!
//! The per-key map is bounded: horizon GC (run from the periodic maintenance
//! hook) evicts entries far below the core watermark and enforces a hard
//! entry-count backstop. The per-collection map is bounded by the number of
//! live collections and is never LSN-GC'd.

use std::collections::HashMap;

use nodedb_types::calvin::{ReadKeyIdent, VersionedReadEntry};
use nodedb_types::{DatabaseId, TenantId};

use crate::types::{Lsn, VShardId};

use super::CoreLoop;

/// Row identity type, re-exported from its plane-neutral home
/// ([`crate::types::KeyRepr`]) so Data-Plane call sites can keep referring to
/// it through this module. Read keys and write keys share this one namespace.
pub use crate::types::KeyRepr;

/// Horizon retain window for per-key entries, in LSNs. Horizon GC evicts any
/// `last_write_lsn` entry whose LSN is more than this far below the core
/// watermark. Sized in the same order of magnitude as the idempotency-cache
/// cap (16,384 entries): a bounded recent-write history — enough to validate
/// in-flight transactions — not an unbounded write log.
const RETAIN_WINDOW: u64 = 16_384;

/// Hard upper bound on the `last_write_lsn` entry count. When horizon GC leaves
/// more entries than this (a burst of distinct keys all inside the retain
/// window), the lowest-LSN (oldest) entries are dropped until the map is back
/// within bound.
const MAX_KEY_ENTRIES: usize = 65_536;

/// Fully-qualified per-key version-index key. Scoped by `(database, tenant)`
/// exactly like the write path, so two tenants (or databases) never alias.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteKey {
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
    pub key: KeyRepr,
}

/// Fully-qualified per-collection version-index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollKey {
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
}

/// Per-core last-write-LSN version index.
#[derive(Default)]
pub struct WriteVersionIndex {
    /// Last committed-write LSN per written key.
    last_write_lsn: HashMap<WriteKey, Lsn>,
    /// Last committed-write LSN per written collection (the phantom-safe floor:
    /// a predicate reader validates against this when it owns no per-key entry).
    coll_write_lsn: HashMap<CollKey, Lsn>,
    /// Per-secondary-index-dimension write-VALUE versions — the finer-grained
    /// sibling of `coll_write_lsn` an index-range read validates against.
    pub(in crate::data::executor) index_values: super::index_value_versions::IndexValueVersionIndex,
}

impl WriteVersionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a committed write at `lsn`.
    ///
    /// Always advances the collection floor `coll_write_lsn[collection]` to the
    /// max of its current value and `lsn`. When `key` is `Some`, also advances
    /// the per-key version `last_write_lsn[key]` monotonically. Advancing the
    /// core watermark is the caller's responsibility (see
    /// [`CoreLoop::note_write_lsn`]).
    pub fn note_write_lsn(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        lsn: Lsn,
    ) {
        let coll_key = CollKey {
            db,
            tenant,
            collection: Box::from(collection),
        };
        let slot = self.coll_write_lsn.entry(coll_key).or_insert(Lsn::ZERO);
        if lsn > *slot {
            *slot = lsn;
        }

        if let Some(key) = key {
            let write_key = WriteKey {
                db,
                tenant,
                collection: Box::from(collection),
                key,
            };
            let slot = self.last_write_lsn.entry(write_key).or_insert(Lsn::ZERO);
            if lsn > *slot {
                *slot = lsn;
            }
        }
    }

    /// Current per-key version, if recorded.
    pub(crate) fn key_write_lsn(&self, key: &WriteKey) -> Option<Lsn> {
        self.last_write_lsn.get(key).copied()
    }

    /// Current per-collection floor version, if recorded.
    pub(crate) fn collection_write_lsn(&self, key: &CollKey) -> Option<Lsn> {
        self.coll_write_lsn.get(key).copied()
    }

    /// Whether a previously observed read is still current against this
    /// core's recorded write versions.
    ///
    /// A `Point` read is current iff no write to that exact key has been
    /// recorded since the read (`last_write_lsn <= read_lsn`); a key with no
    /// recorded write has never been written on this core since the read, so
    /// it is treated as version zero and is always current. A `Predicate`
    /// read is current iff no write to the collection has been recorded
    /// since the read, checked the same way against the collection floor.
    pub(crate) fn read_is_valid(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: &ReadKeyIdent,
        read_lsn: Lsn,
    ) -> bool {
        // Collection-floor check shared by `Predicate` and the fallback for
        // untracked secondary-index dimensions: a read is current iff no write
        // to the collection has been recorded since it. `IndexEq` / `IndexRange`
        // consult the per-value substrate first and only fall back here when the
        // `(collection, field)` dimension is untracked.
        let collection_floor_current = || {
            let coll_key = CollKey {
                db,
                tenant,
                collection: Box::from(collection),
            };
            self.collection_write_lsn(&coll_key).unwrap_or(Lsn::ZERO) <= read_lsn
        };

        match key {
            ReadKeyIdent::Point(repr) => {
                let write_key = WriteKey {
                    db,
                    tenant,
                    collection: Box::from(collection),
                    key: repr.clone(),
                };
                self.key_write_lsn(&write_key).unwrap_or(Lsn::ZERO) <= read_lsn
            }
            ReadKeyIdent::Predicate => collection_floor_current(),
            ReadKeyIdent::IndexEq { field, value } => {
                match self
                    .index_values
                    .eq_max_lsn(db, tenant, collection, field, value)
                {
                    Some(max) => max <= read_lsn,
                    None => collection_floor_current(),
                }
            }
            ReadKeyIdent::IndexRange { field, lo, hi } => {
                match self.index_values.range_max_lsn(
                    db,
                    tenant,
                    collection,
                    field,
                    lo.as_deref(),
                    hi.as_deref(),
                ) {
                    Some(max) => max <= read_lsn,
                    None => collection_floor_current(),
                }
            }
        }
    }

    /// Horizon garbage-collect the per-key map against `watermark`.
    ///
    /// Evicts every entry whose LSN falls below `watermark - RETAIN_WINDOW`,
    /// then, if more than [`MAX_KEY_ENTRIES`] remain, drops the lowest-LSN
    /// entries until back within bound. The per-collection map is bounded by
    /// the live-collection count and is intentionally left untouched.
    pub fn gc(&mut self, watermark: Lsn) {
        let floor = watermark.as_u64().saturating_sub(RETAIN_WINDOW);
        self.last_write_lsn.retain(|_, lsn| lsn.as_u64() >= floor);

        if self.last_write_lsn.len() > MAX_KEY_ENTRIES {
            let overflow = self.last_write_lsn.len() - MAX_KEY_ENTRIES;
            // Drop the `overflow` oldest (lowest-LSN) entries.
            let mut by_lsn: Vec<(Lsn, WriteKey)> = self
                .last_write_lsn
                .iter()
                .map(|(k, lsn)| (*lsn, k.clone()))
                .collect();
            // TOTAL order so tied-LSN eviction is replica-identical: a plain
            // `sort_by_key(lsn)` over a `HashMap`-collected Vec would let the
            // dropped set depend on hash-iteration layout, diverging across
            // replicas. `DatabaseId`/`TenantId` lack `Ord` (compare via
            // `as_u64()`); `KeyRepr` is `Ord`.
            by_lsn.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.db.as_u64().cmp(&b.1.db.as_u64()))
                    .then_with(|| a.1.tenant.as_u64().cmp(&b.1.tenant.as_u64()))
                    .then_with(|| a.1.collection.cmp(&b.1.collection))
                    .then_with(|| a.1.key.cmp(&b.1.key))
            });
            for (_, key) in by_lsn.into_iter().take(overflow) {
                self.last_write_lsn.remove(&key);
            }
        }

        self.index_values.gc(watermark);
    }
}

impl CoreLoop {
    /// Record a committed write into the per-core version index and advance the
    /// core watermark monotonically.
    ///
    /// Called once per written key at every Data-Plane apply chokepoint, using
    /// the WAL LSN the Control Plane allocated at wal-dispatch and threaded onto
    /// the write task. `key` is `None` for engines whose per-key identity is
    /// internal (columnar / timeseries / array / spatial / FTS) — those record
    /// only the collection floor.
    pub(in crate::data::executor) fn note_write_lsn(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        lsn: Lsn,
    ) {
        self.write_index
            .note_write_lsn(db, tenant, collection, key, lsn);
        if lsn > self.watermark {
            self.watermark = lsn;
        }
    }

    /// Record a committed write's collection floor only (no per-key entry),
    /// if a WAL LSN was threaded onto `task`. Shared by the columnar-family
    /// write handlers (columnar / timeseries / array / spatial / FTS) whose
    /// per-key identity is internal — a predicate reader validates against the
    /// collection floor when it owns no per-key version.
    pub(in crate::data::executor) fn note_collection_write_lsn(
        &mut self,
        task: &super::super::task::ExecutionTask,
        collection: &str,
    ) {
        if let Some(lsn) = task.wal_lsn() {
            self.note_write_lsn(
                task.request.database_id,
                task.request.tenant_id,
                collection,
                None,
                lsn,
            );
        }
    }

    /// Run horizon GC on the per-core version index. Invoked from the periodic
    /// maintenance hook — no dedicated timer.
    pub(in crate::data::executor) fn gc_write_index(&mut self) {
        self.write_index.gc(self.watermark);
    }

    /// Whether this shard's slice of a transaction's LSN-versioned read-set was
    /// still current against the local write versions.
    ///
    /// Filters the read-set to the entries whose collection homes to this
    /// request's vShard — the only reads this core holds versions for — then
    /// checks each against the per-core write-version index via
    /// [`WriteVersionIndex::read_is_valid`]. Short-circuits on the first entry
    /// that is no longer current. An empty or fully-remote slice is vacuously
    /// current (`true`). The `(database, tenant)` scope mirrors the write-version
    /// recorder so a read validates against the same key space it was recorded
    /// in; homing uses the same collection-in-database function the scheduler
    /// routes plans with.
    pub(in crate::data::executor) fn read_set_still_current(
        &self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        versioned_reads: &[VersionedReadEntry],
    ) -> bool {
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        let local_vshard = task.request.vshard_id.as_u32();
        versioned_reads
            .iter()
            .filter(|entry| {
                VShardId::from_collection_in_database(db, &entry.collection).as_u32()
                    == local_vshard
            })
            .all(|entry| {
                self.write_index.read_is_valid(
                    db,
                    tenant,
                    &entry.collection,
                    &entry.key,
                    entry.read_lsn,
                )
            })
    }

    /// Record a committed document/vector write's version, keyed by the
    /// written row's cross-engine surrogate, if a WAL LSN was threaded onto
    /// `task`. Shared by every per-surrogate write chokepoint (point put,
    /// point insert, point delete, bulk update, bulk delete).
    pub(in crate::data::executor) fn note_surrogate_write_lsn(
        &mut self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) {
        if let Some(lsn) = task.wal_lsn() {
            self.note_write_lsn(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                Some(KeyRepr::Surrogate(surrogate)),
                lsn,
            );
        }
    }

    /// Record a committed WAL-replay write's version. A no-op when
    /// `record_lsn == 0` (no durable LSN was recorded for this write); `key`
    /// is `None` for collection-only entries (e.g. truncate) and `Some` for
    /// per-key/per-surrogate entries, exactly like [`Self::note_write_lsn`].
    ///
    /// Shared by every WAL replay chokepoint (KV, document, document-vector):
    /// unlike the live write path, replay only has the raw `(database_id,
    /// tenant_id, record_lsn)` off the WAL record header, not an
    /// `ExecutionTask` — hence the separate `u64`-typed entry point rather
    /// than reusing [`Self::note_surrogate_write_lsn`] /
    /// [`Self::note_collection_write_lsn`].
    pub(in crate::data::executor) fn note_replay_write_lsn(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: Option<KeyRepr>,
        record_lsn: u64,
    ) {
        if record_lsn != 0 {
            self.note_write_lsn(
                DatabaseId::new(database_id),
                TenantId::new(tenant_id),
                collection,
                key,
                Lsn::new(record_lsn),
            );
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

    #[test]
    fn point_read_is_valid_when_key_never_written() {
        let index = WriteVersionIndex::new();
        let key = ReadKeyIdent::Point(KeyRepr::Surrogate(7));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn point_read_is_valid_when_write_at_or_before_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(
            db(),
            tenant(),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(10),
        );

        let key = ReadKeyIdent::Point(KeyRepr::Surrogate(7));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(20)));
    }

    #[test]
    fn point_read_is_invalid_when_write_after_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(
            db(),
            tenant(),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(20),
        );

        let key = ReadKeyIdent::Point(KeyRepr::Surrogate(7));
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn predicate_read_is_valid_when_collection_never_written() {
        let index = WriteVersionIndex::new();
        let key = ReadKeyIdent::Predicate;
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn predicate_read_is_valid_when_floor_at_or_before_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(10));

        let key = ReadKeyIdent::Predicate;
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(20)));
    }

    #[test]
    fn predicate_read_is_invalid_when_floor_after_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(20));

        let key = ReadKeyIdent::Predicate;
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn untracked_index_dimension_falls_back_to_collection_floor() {
        // A `(collection, field)` never recorded in the per-value substrate is
        // untracked → `eq_max_lsn`/`range_max_lsn` return `None` → the validator
        // falls back to the collection floor, producing the SAME verdict as a
        // `Predicate` read for every floor position.
        let index_eq = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        let index_range = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("18".to_string()),
            hi: None,
        };
        let predicate = ReadKeyIdent::Predicate;

        // Floor below the read LSN (current), at it (current), and above it
        // (stale) — the untracked index variants track `Predicate` in every case.
        for (floor, read_lsn) in [(5u64, 10u64), (10, 10), (20, 10)] {
            let mut index = WriteVersionIndex::new();
            index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(floor));
            let want =
                index.read_is_valid(db(), tenant(), "orders", &predicate, Lsn::new(read_lsn));
            assert_eq!(
                index.read_is_valid(db(), tenant(), "orders", &index_eq, Lsn::new(read_lsn)),
                want,
                "untracked IndexEq must match Predicate (floor {floor}, read {read_lsn})"
            );
            assert_eq!(
                index.read_is_valid(db(), tenant(), "orders", &index_range, Lsn::new(read_lsn)),
                want,
                "untracked IndexRange must match Predicate (floor {floor}, read {read_lsn})"
            );
        }
    }

    #[test]
    fn index_eq_disjoint_write_does_not_abort() {
        // Read of email = "a@b.c" at read_lsn 10; a later write to a DIFFERENT
        // value on the same dimension must NOT abort the read (the coarse
        // collection floor would have).
        let mut index = WriteVersionIndex::new();
        // A real write to a disjoint value advances BOTH the collection floor
        // (which the coarse `Predicate` path would abort on) AND records the
        // per-value entry — so this proves the per-value check reduces the abort,
        // not merely that nothing was written.
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(20));
        index
            .index_values
            .record(db(), tenant(), "orders", "email", "z@z.z", Lsn::new(20));
        let key = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_disjoint_write_does_not_abort() {
        // Range [10, 20] read at read_lsn 10; a write to out-of-range "50" must
        // not abort.
        let mut index = WriteVersionIndex::new();
        // Advance the collection floor too (the coarse path aborts on it) so this
        // proves range validation reduces the abort, not just an empty index.
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(20));
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "50", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_eq_same_value_conflict_aborts() {
        // A write to the SAME read value after the read LSN must abort.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(20));
        let key = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_in_range_added_value_conflict_aborts() {
        // An added value INSIDE the read range after the read LSN must abort.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "15", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_in_range_removed_value_conflict_aborts() {
        // A delete of an in-range value also records that value's LSN, so a
        // removal inside the read range must abort the read just like an insert.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "17", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_phantom_insert_aborts() {
        // Phantom protection: the range is current while it holds no in-range
        // value (tracked → `Some(ZERO)`), then a NEW in-range value recorded
        // after the read LSN invalidates it — proving the range captures the
        // predicate, not just values extant at read time.
        let mut index = WriteVersionIndex::new();
        // Track the dimension with an OUT-of-range value so the read starts
        // current (tracked, no in-range write).
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "99", Lsn::new(5));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(
            index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)),
            "range with no in-range write is current"
        );

        // Phantom insert inside the range after the read LSN.
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "15", Lsn::new(20));
        assert!(
            !index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)),
            "phantom in-range insert must abort"
        );
    }

    #[test]
    fn tracked_index_eq_missing_value_is_current() {
        // A tracked dimension (some other value recorded) queried for a value
        // with no entry returns `Some(ZERO)` → current.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "email", "other@x.y", Lsn::new(20));
        let key = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn tracked_index_range_empty_range_is_current() {
        // A tracked dimension queried for a range with no in-range entry returns
        // `Some(ZERO)` → current.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "99", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }
}
