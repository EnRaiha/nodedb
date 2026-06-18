// SPDX-License-Identifier: BUSL-1.1

//! `SurrogateRegistry` — thread-safe monotonic surrogate allocator.
//!
//! ## Counter semantics
//!
//! The internal `AtomicU64` counter stores the **next** surrogate to be
//! handed out. `alloc_one()` does `fetch_add(1, AcqRel)`; the returned
//! value is the previous counter (i.e. the surrogate the caller now owns).
//! After every successful allocation, `current_hwm()` returns the highest
//! surrogate ever issued — equivalently, `counter - 1`.
//!
//! ## Restart semantics
//!
//! `from_persisted_hwm(hwm)` initializes `counter = hwm + 1`. On a fresh
//! database, persisted hwm is `0` and the first allocation returns `1`
//! (`Surrogate::ZERO` is reserved). After a restart with persisted hwm
//! `5000`, the first allocation returns `5001`.
//!
//! ## Width / overflow
//!
//! Surrogates are `u32`. The internal counter is `u64` so we can detect
//! the boundary cleanly: any allocation that would push `counter` past
//! `u32::MAX + 1` returns `SurrogateAllocError::Exhausted`. Concurrent
//! callers who race past the boundary all observe the typed error rather
//! than silently wrapping into `0`.
//!
//! ## Cluster-mode global watermark `G` (HiLo reservations)
//!
//! In cluster mode the `counter` is the global watermark `G`, advanced ONLY
//! by `reserve_at_index` (the replay-idempotent wrapper over
//! `reserve_from_global`) from the `SurrogateReserve` apply path running
//! deterministically in identical Raft log order on every node. The metadata
//! Raft group has no snapshot, so on every (re)start the full committed log
//! replays from index 1. To avoid double-counting `G` on that replay, each
//! node persists `(G, last_reserve_index)` to the catalog ATOMICALLY on every
//! reservation; on restart the registry is seeded with both via
//! `from_persisted`, and `reserve_at_index` skips every `SurrogateReserve`
//! whose index `<= last_reserve_index` (already in the seeded `G`). Because
//! the carved range is computed identically on every node, the persisted hwm
//! is equal cluster-wide — see `metadata_applier.rs` `SurrogateReserve` arm.
//!
//! ## Declared out-of-scope follow-ups (surfaced, not hidden)
//!
//! These are latent gaps acknowledged by the HiLo unit and deferred to their
//! own work — they are NOT bugs in the current single-node + pure-cluster
//! paths, but must be handled when the named mechanisms are built:
//!
//! (a) **Metadata-group snapshot must capture/restore `G`.** When a snapshot
//!     mechanism is added to the metadata Raft group (truncating the log so it
//!     no longer fully replays from index 1), the snapshot MUST capture and
//!     restore the surrogate global watermark `G`. Otherwise post-snapshot
//!     replay would rebuild a `G` that omits the truncated reservations. This
//!     is the same latent gap the existing `SurrogateAlloc` path carries.
//!
//! (b) **single→cluster surrogate-hwm migration.** A node that previously ran
//!     single-node (non-zero catalog hwm, `last_reserve_index == 0`) and later
//!     joins a cluster would double-seed `G`: `from_persisted` gives a non-zero
//!     base from the single-node hwm AND full log replay re-advances `G` on top
//!     of it (every historical reservation has index `> 0`, so none are
//!     skipped). A proper single→cluster surrogate-hwm migration is out of
//!     scope for this unit.

use std::ops::RangeInclusive;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nodedb_types::Surrogate;

use super::persist::SurrogateHwmPersist;

/// Periodic flush trigger: every N allocations, regardless of elapsed time.
pub const FLUSH_OPS_THRESHOLD: u64 = 1024;

/// Cluster-mode reservation batch size. Each node carves a disjoint
/// `[start, end)` range of this many surrogates from the global
/// watermark per `MetadataEntry::SurrogateReserve` apply, then hands
/// them out locally lock-free via `try_alloc_reserved`. Larger batches
/// amortize the Raft round-trip; a partially-used tail is abandoned on
/// restart (gap-tolerant), so the size trades amortization for the
/// worst-case wasted tail.
pub const RESERVE_BATCH_SIZE: u32 = 4096;

/// Periodic flush trigger: every T elapsed since the last flush.
pub const FLUSH_ELAPSED_THRESHOLD: Duration = Duration::from_millis(200);

/// Allocation errors. Surfaced to the caller; `From` impl wires this into
/// the crate's central `Error` enum (see bottom of this file).
#[derive(Debug, thiserror::Error)]
pub enum SurrogateAllocError {
    #[error("surrogate space exhausted (u32::MAX reached)")]
    Exhausted,

    #[error("surrogate batch size 0 is not allowed")]
    EmptyBatch,

    #[error("surrogate flush failed: {detail}")]
    FlushFailed { detail: String },
}

/// Thread-safe surrogate allocator.
///
/// The `Mutex<Instant>` for `last_flush_at` is uncontended on the hot path
/// (`alloc_one`/`alloc` only touch atomics); only `should_flush` and
/// `flush` take the lock, which run at most once per ~200 ms or per 1024
/// allocations.
pub struct SurrogateRegistry {
    /// Next surrogate to hand out. Starts at `1` on a fresh registry, or
    /// `persisted_hwm + 1` on restart.
    ///
    /// In single-node mode this is the local allocator (`alloc_one` /
    /// `alloc`). In cluster mode the SAME counter doubles as the global
    /// watermark `G`: every node advances it deterministically in
    /// `reserve_from_global` (driven by `SurrogateReserve` apply in the
    /// same Raft log order), so all nodes agree on which disjoint range
    /// each reservation carves. `alloc_one` is simply never called in
    /// cluster mode.
    counter: AtomicU64,
    /// Cluster-mode reserved batch — next surrogate to hand out locally.
    /// `0` (with `reserved_end == 0`) means "no batch reserved". Set by
    /// `set_reserved_batch`, drained by `try_alloc_reserved`. Interior
    /// atomics so the Raft applier can populate it under a read guard.
    reserved_next: AtomicU64,
    /// Cluster-mode reserved batch — exclusive upper bound `[start, end)`.
    reserved_end: AtomicU64,
    /// Cluster-mode replay-idempotency cursor: the highest metadata Raft
    /// log index whose `SurrogateReserve` has already been folded into the
    /// global watermark `G` (and persisted to the catalog atomically with
    /// `G`). Seeded from the catalog on restart so full metadata-log replay
    /// re-applies ONLY reservations committed after the last persist; every
    /// reservation at-or-below this index is already in the seeded `G` and
    /// must be skipped to avoid double-counting `G` (which would diverge
    /// across nodes). Interior atomic so the Raft applier can advance it
    /// under a registry read guard.
    last_reserve_index: AtomicU64,
    /// Allocations since the last flush. Reset by `flush()`.
    allocs_since_flush: AtomicU64,
    /// Wall-clock anchor for the elapsed-time flush trigger.
    last_flush_at: Mutex<Instant>,
}

impl SurrogateRegistry {
    /// Create an empty registry — first allocation returns `Surrogate(1)`.
    pub fn new() -> Self {
        Self::from_persisted_hwm(0)
    }

    /// Restore from a persisted high-watermark. Next allocation returns
    /// `hwm + 1`. `hwm == 0` is equivalent to `new()` (no allocations yet).
    ///
    /// Seeds `last_reserve_index = 0` — correct for the single-node path
    /// (which never proposes `SurrogateReserve`, so the cursor is unused)
    /// and for a pure-cluster node whose catalog has no persisted reserve
    /// state yet. Cluster restart paths that have persisted a reserve
    /// cursor must use [`from_persisted`] instead.
    pub fn from_persisted_hwm(hwm: u32) -> Self {
        Self::from_persisted(hwm, 0)
    }

    /// Restore from a persisted high-watermark AND the persisted
    /// applied-reserve cursor. Used by the cluster restart path: seeding
    /// both fields together makes metadata-log replay idempotent — every
    /// `SurrogateReserve` whose index is `<= reserve_index` is already
    /// folded into `hwm` (the seeded `G`) and is skipped by
    /// [`reserve_at_index`], so replay does not double-count `G`.
    pub fn from_persisted(hwm: u32, reserve_index: u64) -> Self {
        Self {
            counter: AtomicU64::new(u64::from(hwm) + 1),
            reserved_next: AtomicU64::new(0),
            reserved_end: AtomicU64::new(0),
            last_reserve_index: AtomicU64::new(reserve_index),
            allocs_since_flush: AtomicU64::new(0),
            last_flush_at: Mutex::new(Instant::now()),
        }
    }

    /// Allocate a single surrogate. Returns `Exhausted` if the u32 space
    /// is full.
    pub fn alloc_one(&self) -> Result<Surrogate, SurrogateAllocError> {
        let prev = self.counter.fetch_add(1, Ordering::AcqRel);
        if prev > u64::from(u32::MAX) {
            // Restore the counter so future callers also see Exhausted
            // rather than racing past us into a wrapped value. We don't
            // need atomicity with the racing increments — once any caller
            // observes `prev > u32::MAX`, the space is effectively dead.
            self.counter
                .store(u64::from(u32::MAX) + 1, Ordering::Release);
            return Err(SurrogateAllocError::Exhausted);
        }
        self.allocs_since_flush.fetch_add(1, Ordering::AcqRel);
        // Safe: prev <= u32::MAX is guaranteed by the check above.
        Ok(Surrogate::new(prev as u32))
    }

    /// Allocate `n` contiguous surrogates as an inclusive range.
    /// Returns `EmptyBatch` for `n == 0`, `Exhausted` if the batch
    /// would cross the `u32::MAX` boundary.
    pub fn alloc(&self, n: u32) -> Result<RangeInclusive<Surrogate>, SurrogateAllocError> {
        if n == 0 {
            return Err(SurrogateAllocError::EmptyBatch);
        }
        let prev = self.counter.fetch_add(u64::from(n), Ordering::AcqRel);
        let last = prev + u64::from(n) - 1;
        if last > u64::from(u32::MAX) {
            self.counter
                .store(u64::from(u32::MAX) + 1, Ordering::Release);
            return Err(SurrogateAllocError::Exhausted);
        }
        self.allocs_since_flush
            .fetch_add(u64::from(n), Ordering::AcqRel);
        Ok(Surrogate::new(prev as u32)..=Surrogate::new(last as u32))
    }

    /// Cluster-mode local allocation: hand out one surrogate from the
    /// node's currently-reserved batch, lock-free. Returns `None` when
    /// the batch is empty (`reserved_next >= reserved_end`), signalling
    /// the caller to reserve a fresh batch from the global watermark.
    ///
    /// Never touches `counter` — the global watermark only advances in
    /// `reserve_from_global` (via `SurrogateReserve` apply), so two
    /// concurrent calls draining the same batch are still disjoint, and
    /// the reserved range was already carved out of `G` cluster-wide.
    pub fn try_alloc_reserved(&self) -> Option<Surrogate> {
        let end = self.reserved_end.load(Ordering::Acquire);
        loop {
            let next = self.reserved_next.load(Ordering::Acquire);
            if next >= end {
                return None;
            }
            match self.reserved_next.compare_exchange_weak(
                next,
                next + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                // `next < end <= u32::MAX` (the reserved end is carved
                // by `reserve_from_global`, which caps it at `u32::MAX`),
                // so `next` fits a `u32` without loss.
                Ok(_) => return Some(Surrogate::new(next as u32)),
                Err(_) => continue,
            }
        }
    }

    /// Cluster-mode: true if the reserved batch still has capacity.
    /// Used by the assigner to skip a redundant reservation when another
    /// concurrent refill already installed a fresh batch.
    pub fn has_reserved(&self) -> bool {
        self.reserved_next.load(Ordering::Acquire) < self.reserved_end.load(Ordering::Acquire)
    }

    /// Cluster-mode: how many surrogates remain in the reserved batch
    /// (`reserved_end - reserved_next`, saturating at 0 when drained).
    ///
    /// Lock-free (interior atomics) so the background refiller can poll it
    /// cheaply on the hot path's behalf to drive threshold top-up. A stale
    /// read is harmless: it only ever triggers an *earlier* refill, never a
    /// missed one (the hot path also notifies on a genuinely empty draw).
    pub fn remaining_reserved(&self) -> u64 {
        let end = self.reserved_end.load(Ordering::Acquire);
        let next = self.reserved_next.load(Ordering::Acquire);
        end.saturating_sub(next)
    }

    /// Cluster-mode: install a freshly-reserved `[start, end)` batch as
    /// the node's local allocation pool. Called from
    /// `SurrogateAssigner::complete_reservation` on the owning node ONLY when
    /// a live pending waiter for the reservation exists — i.e. during a
    /// genuine in-process reservation, never during metadata-log replay (which
    /// has no waiters). This gating is what stops a restart from re-installing
    /// a partly-consumed pre-crash batch and re-handing-out its surrogates.
    /// Replaces any (typically already-drained) prior batch — a non-empty
    /// prior batch's tail is abandoned, which is gap-tolerant by design.
    pub fn set_reserved_batch(&self, start: u32, end: u32) {
        // Store `end` first, then `next`: a concurrent `try_alloc_reserved`
        // that observes the new `next` will also observe the matching (or
        // larger) `end`, never a stale smaller one.
        self.reserved_end.store(u64::from(end), Ordering::Release);
        self.reserved_next
            .store(u64::from(start), Ordering::Release);
    }

    /// Cluster-mode: deterministically advance the global watermark
    /// `G` (the `counter`) by `batch_size` and return the carved
    /// `[start, end)` range. Every node runs this in the same Raft log
    /// order against an identical `G`, so all nodes compute the same
    /// disjoint ranges for the same sequence of reservations.
    ///
    /// Uses the SAME no-wrap `u32::MAX` discipline as `alloc_one` /
    /// `alloc`: a reservation whose exclusive end would exceed `u32::MAX`
    /// returns `Exhausted` and does not wrap; the counter is pinned at
    /// `u32::MAX + 1` so subsequent reservations also observe `Exhausted`.
    /// (The exclusive bound must be losslessly representable as a `u32`,
    /// so the boundary `end == u32::MAX + 1` is rejected too; the lost
    /// final partial batch is gap-tolerant by design.)
    pub fn reserve_from_global(&self, batch_size: u32) -> Result<(u32, u32), SurrogateAllocError> {
        if batch_size == 0 {
            return Err(SurrogateAllocError::EmptyBatch);
        }
        let start = self
            .counter
            .fetch_add(u64::from(batch_size), Ordering::AcqRel);
        let end = start + u64::from(batch_size);
        // `end` is the exclusive upper bound, returned as a `u32`. We
        // require `end <= u32::MAX` (not `u32::MAX + 1`) so the bound is
        // losslessly representable: a batch that would push `end` to
        // exactly `u32::MAX + 1` is rejected as `Exhausted` rather than
        // wrapping the exclusive bound to `0`. The lost final partial
        // batch is gap-tolerant by design (reserved tails are abandoned
        // on restart anyway). This is the SAME no-wrap discipline as
        // `alloc_one` / `alloc`: cross the ceiling → typed error, pin the
        // counter so every later reservation also sees `Exhausted`.
        if end > u64::from(u32::MAX) {
            self.counter
                .store(u64::from(u32::MAX) + 1, Ordering::Release);
            return Err(SurrogateAllocError::Exhausted);
        }
        // Safe: `start < end <= u32::MAX`, both fit u32.
        Ok((start as u32, end as u32))
    }

    /// Advance the global watermark `G` for the `SurrogateReserve` at
    /// metadata Raft log `raft_index`, **exactly once** across restarts.
    ///
    /// Returns `Ok(Some((start, end)))` on the first application of this
    /// index (carving a fresh disjoint range out of `G`), and `Ok(None)`
    /// if this index was already applied — i.e. it is `<=` the persisted
    /// applied-reserve cursor (full metadata-log replay after restart, or a
    /// duplicate delivery). On `None` the caller MUST skip: do not advance
    /// `G`, do not persist, do not install a batch.
    ///
    /// This is the replay-idempotency wrapper around the otherwise
    /// non-idempotent [`reserve_from_global`] `fetch_add`. It is
    /// deterministic given the same ordered sequence of `raft_index` values
    /// and the same seeded `G`/cursor, so every node folds the identical set
    /// of reservations into `G` and arrives at the identical watermark.
    ///
    /// Uses interior atomics only (no `&mut self`), so the Raft applier can
    /// call it under a registry read guard without risking the allocation
    /// path's lock re-entry.
    pub fn reserve_at_index(
        &self,
        raft_index: u64,
        batch_size: u32,
    ) -> Result<Option<(u32, u32)>, SurrogateAllocError> {
        // Already folded into the seeded/advanced `G` — replay or duplicate.
        if raft_index <= self.last_reserve_index.load(Ordering::Acquire) {
            return Ok(None);
        }
        let (start, end) = self.reserve_from_global(batch_size)?;
        // Record that this index is now reflected in `G`. A later store
        // wins monotonically because the applier delivers indices in
        // strictly increasing order; the load-guard above already rejected
        // anything `<=` the current cursor, so this only ever advances it.
        self.last_reserve_index.store(raft_index, Ordering::Release);
        Ok(Some((start, end)))
    }

    /// Cluster-mode: the highest metadata Raft log index whose
    /// `SurrogateReserve` has been folded into `G`. Persisted alongside the
    /// hwm so restart replay can skip already-applied reservations.
    pub fn last_reserve_index(&self) -> u64 {
        self.last_reserve_index.load(Ordering::Acquire)
    }

    /// Highest surrogate ever issued — `0` if no allocations yet.
    pub fn current_hwm(&self) -> u32 {
        let next = self.counter.load(Ordering::Acquire);
        // `next` is `hwm + 1` on a healthy registry; saturate at u32::MAX
        // for the exhausted case where `next == u32::MAX as u64 + 1`.
        next.saturating_sub(1).min(u64::from(u32::MAX)) as u32
    }

    /// True if the periodic-flush thresholds (ops or elapsed) are tripped.
    pub fn should_flush(&self) -> bool {
        if self.allocs_since_flush.load(Ordering::Acquire) >= FLUSH_OPS_THRESHOLD {
            return true;
        }
        if let Ok(last) = self.last_flush_at.lock() {
            return last.elapsed() >= FLUSH_ELAPSED_THRESHOLD;
        }
        false
    }

    /// Idempotently raise the high-watermark to at least `new_hwm`.
    /// Used by WAL replay: each replayed `SurrogateAlloc` record advances
    /// the in-memory counter so post-restart allocations cannot collide
    /// with pre-crash ones. Never lowers — a request to lower is a
    /// no-op rather than an error, because WAL replay can legitimately
    /// see records below the catalog's already-flushed hwm (the registry
    /// is seeded from the catalog before replay walks the older records).
    pub fn restore_hwm(&self, new_hwm: u32) -> Result<(), SurrogateAllocError> {
        let target = u64::from(new_hwm) + 1;
        let mut current = self.counter.load(Ordering::Acquire);
        loop {
            if target <= current {
                return Ok(());
            }
            match self.counter.compare_exchange_weak(
                current,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Persist the current high-watermark and reset flush counters.
    ///
    /// Idempotent: calling on an unmodified registry just rewrites the
    /// same hwm.
    pub fn flush(&self, persist: &dyn SurrogateHwmPersist) -> Result<(), SurrogateAllocError> {
        let hwm = self.current_hwm();
        persist
            .checkpoint(hwm)
            .map_err(|e| SurrogateAllocError::FlushFailed {
                detail: e.to_string(),
            })?;
        self.allocs_since_flush.store(0, Ordering::Release);
        if let Ok(mut guard) = self.last_flush_at.lock() {
            *guard = Instant::now();
        }
        Ok(())
    }

    /// Test-only: force the elapsed-flush trigger to fire on the next
    /// `should_flush` call by rewinding the wall-clock anchor.
    #[cfg(test)]
    fn rewind_flush_clock(&self, by: Duration) {
        if let Ok(mut guard) = self.last_flush_at.lock()
            && let Some(earlier) = guard.checked_sub(by)
        {
            *guard = earlier;
        }
    }
}

impl Default for SurrogateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl From<SurrogateAllocError> for crate::Error {
    fn from(e: SurrogateAllocError) -> Self {
        match e {
            SurrogateAllocError::Exhausted => crate::Error::Internal {
                detail: "surrogate space exhausted (u32::MAX reached)".into(),
            },
            SurrogateAllocError::EmptyBatch => crate::Error::BadRequest {
                detail: "surrogate batch size 0 is not allowed".into(),
            },
            SurrogateAllocError::FlushFailed { detail } => crate::Error::Storage {
                engine: "surrogate".into(),
                detail,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use super::*;

    /// In-memory persist for tests — captures the latest checkpoint.
    struct MemPersist {
        last: std::sync::Mutex<Option<u32>>,
        calls: AtomicU32,
    }

    impl MemPersist {
        fn new() -> Self {
            Self {
                last: std::sync::Mutex::new(None),
                calls: AtomicU32::new(0),
            }
        }

        fn last(&self) -> Option<u32> {
            *self.last.lock().unwrap()
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::Acquire)
        }
    }

    impl SurrogateHwmPersist for MemPersist {
        fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
            *self.last.lock().unwrap() = Some(hwm);
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn load(&self) -> crate::Result<u32> {
            Ok(self.last().unwrap_or(0))
        }
    }

    #[test]
    fn monotonic_10k() {
        let reg = SurrogateRegistry::new();
        let mut prev = 0u32;
        for _ in 0..10_000 {
            let s = reg.alloc_one().unwrap().as_u32();
            assert!(s > prev, "expected monotonic, got {prev} then {s}");
            prev = s;
        }
        assert_eq!(reg.current_hwm(), 10_000);
    }

    #[test]
    fn batch_alloc_returns_range_then_advances() {
        let reg = SurrogateRegistry::new();
        let range = reg.alloc(100).unwrap();
        assert_eq!(*range.start(), Surrogate::new(1));
        assert_eq!(*range.end(), Surrogate::new(100));
        // Length of an inclusive range over u32-equivalents:
        let count = (range.end().as_u32() - range.start().as_u32() + 1) as usize;
        assert_eq!(count, 100);
        let next = reg.alloc_one().unwrap();
        assert_eq!(next, Surrogate::new(101));
    }

    #[test]
    fn batch_alloc_zero_rejected() {
        let reg = SurrogateRegistry::new();
        assert!(matches!(reg.alloc(0), Err(SurrogateAllocError::EmptyBatch)));
    }

    #[test]
    fn restart_survives_hwm() {
        let reg = SurrogateRegistry::from_persisted_hwm(5000);
        let s = reg.alloc_one().unwrap();
        assert_eq!(s, Surrogate::new(5001));
        assert_eq!(reg.current_hwm(), 5001);
    }

    #[test]
    fn concurrent_16x1000_unique() {
        let reg = Arc::new(SurrogateRegistry::new());
        let mut handles = Vec::with_capacity(16);
        for _ in 0..16 {
            let r = reg.clone();
            handles.push(std::thread::spawn(move || {
                let mut local = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    local.push(r.alloc_one().unwrap());
                }
                local
            }));
        }
        let mut all = Vec::with_capacity(16_000);
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 16_000, "expected 16000 unique surrogates");
        assert!(reg.current_hwm() >= 16_000);
    }

    #[test]
    fn overflow_surfaces_typed_error() {
        // Bootstrap right at the edge: counter = u32::MAX, so the next
        // alloc returns Surrogate(u32::MAX), and the one after fails.
        let reg = SurrogateRegistry::from_persisted_hwm(u32::MAX - 1);
        let last = reg.alloc_one().unwrap();
        assert_eq!(last, Surrogate::new(u32::MAX));
        let err = reg.alloc_one().unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
        // Subsequent calls also exhausted — counter does not wrap.
        assert!(matches!(
            reg.alloc_one().unwrap_err(),
            SurrogateAllocError::Exhausted
        ));
    }

    #[test]
    fn batch_overflow_surfaces_typed_error() {
        let reg = SurrogateRegistry::from_persisted_hwm(u32::MAX - 5);
        let err = reg.alloc(100).unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
    }

    #[test]
    fn flush_threshold_ops() {
        let reg = SurrogateRegistry::new();
        assert!(!reg.should_flush(), "fresh registry should not flush yet");
        for _ in 0..(FLUSH_OPS_THRESHOLD - 1) {
            let _ = reg.alloc_one().unwrap();
        }
        assert!(!reg.should_flush(), "below ops threshold should not flush");
        let _ = reg.alloc_one().unwrap();
        assert!(reg.should_flush(), "at ops threshold should flush");

        let persist = MemPersist::new();
        reg.flush(&persist).unwrap();
        assert_eq!(persist.calls(), 1);
        assert_eq!(persist.last(), Some(FLUSH_OPS_THRESHOLD as u32));
        assert!(!reg.should_flush(), "post-flush should clear ops");
    }

    #[test]
    fn flush_threshold_elapsed() {
        let reg = SurrogateRegistry::new();
        let _ = reg.alloc_one().unwrap();
        assert!(!reg.should_flush());
        reg.rewind_flush_clock(FLUSH_ELAPSED_THRESHOLD * 2);
        assert!(reg.should_flush(), "rewound clock should fire elapsed");
        let persist = MemPersist::new();
        reg.flush(&persist).unwrap();
        assert!(!reg.should_flush(), "post-flush should reset clock");
    }

    #[test]
    fn flush_idempotent_on_empty_registry() {
        let reg = SurrogateRegistry::new();
        let persist = MemPersist::new();
        reg.flush(&persist).unwrap();
        reg.flush(&persist).unwrap();
        assert_eq!(persist.calls(), 2);
        assert_eq!(persist.last(), Some(0));
    }

    #[test]
    fn reserve_from_global_carves_disjoint_advancing_ranges() {
        let reg = SurrogateRegistry::new();
        // First reservation starts at the fresh-registry next (1).
        let (s0, e0) = reg.reserve_from_global(10).unwrap();
        assert_eq!((s0, e0), (1, 11));
        // Second reservation is strictly disjoint and follows on.
        let (s1, e1) = reg.reserve_from_global(5).unwrap();
        assert_eq!((s1, e1), (11, 16));
        // Ranges are disjoint and monotonically advancing.
        assert!(e0 <= s1, "ranges must not overlap");
        // `reserve_from_global` advances the global watermark itself.
        assert_eq!(reg.current_hwm(), 15);

        // Zero batch is rejected, like `alloc`.
        assert!(matches!(
            reg.reserve_from_global(0),
            Err(SurrogateAllocError::EmptyBatch)
        ));
    }

    #[test]
    fn reserve_at_index_advances_once_then_skips_replay() {
        let reg = SurrogateRegistry::new();
        // First application of index 10: advances `G`, returns the range.
        let first = reg.reserve_at_index(10, 4).unwrap();
        assert_eq!(first, Some((1, 5)));
        assert_eq!(reg.current_hwm(), 4);
        assert_eq!(reg.last_reserve_index(), 10);

        // Re-applying the SAME index (full-log replay / duplicate delivery)
        // is a no-op: returns None and does NOT advance `G`.
        let replay = reg.reserve_at_index(10, 4).unwrap();
        assert_eq!(replay, None);
        assert_eq!(reg.current_hwm(), 4, "replay must not advance G");
        assert_eq!(reg.last_reserve_index(), 10);

        // An even-older index (e.g. replay walking from index 1) is also
        // skipped — anything <= the cursor is already folded into `G`.
        assert_eq!(reg.reserve_at_index(7, 4).unwrap(), None);
        assert_eq!(reg.current_hwm(), 4);

        // A genuinely newer index advances `G` exactly once more.
        let next = reg.reserve_at_index(11, 4).unwrap();
        assert_eq!(next, Some((5, 9)));
        assert_eq!(reg.current_hwm(), 8);
        assert_eq!(reg.last_reserve_index(), 11);
    }

    #[test]
    fn from_persisted_seeds_reserve_cursor_so_replay_is_skipped() {
        // Simulate a restart: G seeded to 8 (hwm), cursor seeded to 11.
        // Replaying every historical reservation with index <= 11 is a
        // no-op — no double-count.
        let reg = SurrogateRegistry::from_persisted(8, 11);
        assert_eq!(reg.current_hwm(), 8);
        assert_eq!(reg.reserve_at_index(10, 4).unwrap(), None);
        assert_eq!(reg.reserve_at_index(11, 4).unwrap(), None);
        assert_eq!(
            reg.current_hwm(),
            8,
            "replay below seeded cursor must not advance G"
        );
        // The first post-persist reservation (index 12) applies.
        assert_eq!(reg.reserve_at_index(12, 4).unwrap(), Some((9, 13)));
        assert_eq!(reg.current_hwm(), 12);
    }

    #[test]
    fn try_alloc_reserved_drains_exact_range_then_none() {
        let reg = SurrogateRegistry::new();
        let (start, end) = reg.reserve_from_global(4).unwrap();
        reg.set_reserved_batch(start, end);

        // Drains exactly [start, end) in order.
        let mut got = Vec::new();
        while let Some(s) = reg.try_alloc_reserved() {
            got.push(s.as_u32());
        }
        let expect: Vec<u32> = (start..end).collect();
        assert_eq!(got, expect);
        // Exhausted batch returns None and stays None.
        assert!(reg.try_alloc_reserved().is_none());
        assert!(reg.try_alloc_reserved().is_none());
    }

    #[test]
    fn empty_registry_has_no_reserved_batch() {
        let reg = SurrogateRegistry::new();
        // No `set_reserved_batch` call yet → nothing to hand out.
        assert!(reg.try_alloc_reserved().is_none());
    }

    #[test]
    fn reserve_from_global_overflow_surfaces_typed_error() {
        // Leave fewer than a full batch below the ceiling so the
        // reservation crosses it and must error rather than wrap.
        let reg = SurrogateRegistry::from_persisted_hwm(u32::MAX - 5);
        let err = reg.reserve_from_global(100).unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
        // Counter is pinned: subsequent reservations also exhausted.
        assert!(matches!(
            reg.reserve_from_global(1),
            Err(SurrogateAllocError::Exhausted)
        ));
    }

    #[test]
    fn current_hwm_tracks_allocs() {
        let reg = SurrogateRegistry::new();
        assert_eq!(reg.current_hwm(), 0);
        let _ = reg.alloc_one().unwrap();
        assert_eq!(reg.current_hwm(), 1);
        let _ = reg.alloc(10).unwrap();
        assert_eq!(reg.current_hwm(), 11);
    }
}
