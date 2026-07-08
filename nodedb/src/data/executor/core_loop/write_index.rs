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
//! commit path later validates a transaction's read-set against. Nothing reads
//! the index yet — it is populated here so those versions exist and are
//! comparable. Because the index lives on the `!Send` core it is a plain
//! `HashMap`: no atomics, no locks, no cross-core sharing.
//!
//! The per-key map is bounded: horizon GC (run from the periodic maintenance
//! hook) evicts entries far below the core watermark and enforces a hard
//! entry-count backstop. The per-collection map is bounded by the number of
//! live collections and is never LSN-GC'd.

use std::collections::HashMap;

use nodedb_types::{DatabaseId, TenantId};

use crate::types::Lsn;

use super::CoreLoop;

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

/// Identity of a written row within a collection.
///
/// The engine that owns the write chooses the representation:
/// - `Surrogate` for the cross-engine `u32` surrogate (schemaless + strict
///   document writes, and vector-by-document upserts keyed on the owning doc).
/// - `KvKey` for the raw Key-Value engine key bytes.
/// - `Edge` for a graph edge, whose identity is the `(src, label, dst)` tuple
///   rather than a surrogate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyRepr {
    /// Cross-engine `u32` surrogate identity.
    Surrogate(u32),
    /// Raw Key-Value engine key bytes.
    KvKey(Box<[u8]>),
    /// Graph edge identity: `(source node, edge label, destination node)`.
    Edge {
        src: Box<str>,
        label: Box<str>,
        dst: Box<str>,
    },
}

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

    /// Current per-key version, if recorded. Reading the index is not a U1
    /// concern (the OCC validator that consumes it lands later); exposed for
    /// the substrate's own tests only.
    #[cfg(test)]
    pub fn key_write_lsn(&self, key: &WriteKey) -> Option<Lsn> {
        self.last_write_lsn.get(key).copied()
    }

    /// Current per-collection floor version, if recorded. Test-only (see
    /// [`Self::key_write_lsn`]).
    #[cfg(test)]
    pub fn collection_write_lsn(&self, key: &CollKey) -> Option<Lsn> {
        self.coll_write_lsn.get(key).copied()
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
            by_lsn.sort_by_key(|(lsn, _)| *lsn);
            for (_, key) in by_lsn.into_iter().take(overflow) {
                self.last_write_lsn.remove(&key);
            }
        }
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
}
