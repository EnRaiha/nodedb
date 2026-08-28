// SPDX-License-Identifier: BUSL-1.1

//! Cross-node HiLo surrogate allocation. The global watermark `G` only
//! ever advances via `reserve_at_index` (replay-idempotent), driven by
//! `SurrogateReserve` Raft-apply in identical log order on every node.
//! Never exposes `alloc_one`/`alloc`.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Surrogate;

use super::error::SurrogateAllocError;
use super::watermark::Watermark;

/// Cluster (Raft-HiLo) surrogate counter.
pub struct ClusterCounter {
    /// Global watermark `G`.
    watermark: Watermark,
    /// Reserved batch — next surrogate to hand out locally.
    reserved_next: AtomicU64,
    /// Reserved batch — exclusive upper bound `[start, end)`.
    reserved_end: AtomicU64,
    /// Highest metadata Raft log index already folded into `G`.
    last_reserve_index: AtomicU64,
}

impl ClusterCounter {
    pub(super) fn new(hwm: u32, reserve_index: u64) -> Self {
        Self {
            watermark: Watermark::new(hwm),
            reserved_next: AtomicU64::new(0),
            reserved_end: AtomicU64::new(0),
            last_reserve_index: AtomicU64::new(reserve_index),
        }
    }

    /// Hand out one surrogate from the reserved batch, lock-free.
    /// `None` means the batch is empty — reserve a fresh one.
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
                Ok(_) => return Some(Surrogate::new(next as u32)),
                Err(_) => continue,
            }
        }
    }

    /// True if the reserved batch still has capacity.
    pub fn has_reserved(&self) -> bool {
        self.reserved_next.load(Ordering::Acquire) < self.reserved_end.load(Ordering::Acquire)
    }

    /// Surrogates remaining in the reserved batch, saturating at 0.
    pub fn remaining_reserved(&self) -> u64 {
        let end = self.reserved_end.load(Ordering::Acquire);
        let next = self.reserved_next.load(Ordering::Acquire);
        end.saturating_sub(next)
    }

    /// Install a freshly-reserved `[start, end)` batch as the local pool.
    pub fn set_reserved_batch(&self, start: u32, end: u32) {
        self.reserved_end.store(u64::from(end), Ordering::Release);
        self.reserved_next
            .store(u64::from(start), Ordering::Release);
    }

    /// Deterministically advance `G` by `batch_size`, returning the
    /// carved `[start, end)` range.
    pub fn reserve_from_global(&self, batch_size: u32) -> Result<(u32, u32), SurrogateAllocError> {
        if batch_size == 0 {
            return Err(SurrogateAllocError::EmptyBatch);
        }
        let start = self.watermark.fetch_add_raw(u64::from(batch_size));
        let end = start + u64::from(batch_size);
        if end > u64::from(u32::MAX) {
            self.watermark.pin_exhausted();
            return Err(SurrogateAllocError::Exhausted);
        }
        Ok((start as u32, end as u32))
    }

    /// Advance `G` for the `SurrogateReserve` at `raft_index`, exactly
    /// once across restarts. `Ok(None)` means already applied — the
    /// caller must skip: no `G` advance, no persist, no batch install.
    pub fn reserve_at_index(
        &self,
        raft_index: u64,
        batch_size: u32,
    ) -> Result<Option<(u32, u32)>, SurrogateAllocError> {
        if raft_index <= self.last_reserve_index.load(Ordering::Acquire) {
            return Ok(None);
        }
        let (start, end) = self.reserve_from_global(batch_size)?;
        self.last_reserve_index.store(raft_index, Ordering::Release);
        Ok(Some((start, end)))
    }

    /// Highest metadata Raft log index folded into `G`.
    pub fn last_reserve_index(&self) -> u64 {
        self.last_reserve_index.load(Ordering::Acquire)
    }

    /// Highest surrogate ever issued — `0` if none yet.
    pub fn current_hwm(&self) -> u32 {
        self.watermark.current_hwm()
    }

    /// Idempotently raise the high-watermark to at least `new_hwm`.
    pub fn restore_hwm(&self, new_hwm: u32) {
        self.watermark.restore(new_hwm);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_from_global_carves_disjoint_advancing_ranges() {
        let c = ClusterCounter::new(0, 0);
        let (s0, e0) = c.reserve_from_global(10).unwrap();
        assert_eq!((s0, e0), (1, 11));
        let (s1, e1) = c.reserve_from_global(5).unwrap();
        assert_eq!((s1, e1), (11, 16));
        assert!(e0 <= s1, "ranges must not overlap");
        assert_eq!(c.current_hwm(), 15);
        assert!(matches!(
            c.reserve_from_global(0),
            Err(SurrogateAllocError::EmptyBatch)
        ));
    }

    #[test]
    fn reserve_at_index_advances_once_then_skips_replay() {
        let c = ClusterCounter::new(0, 0);
        let first = c.reserve_at_index(10, 4).unwrap();
        assert_eq!(first, Some((1, 5)));
        assert_eq!(c.current_hwm(), 4);
        assert_eq!(c.last_reserve_index(), 10);

        let replay = c.reserve_at_index(10, 4).unwrap();
        assert_eq!(replay, None);
        assert_eq!(c.current_hwm(), 4, "replay must not advance G");
        assert_eq!(c.last_reserve_index(), 10);

        assert_eq!(c.reserve_at_index(7, 4).unwrap(), None);
        assert_eq!(c.current_hwm(), 4);

        let next = c.reserve_at_index(11, 4).unwrap();
        assert_eq!(next, Some((5, 9)));
        assert_eq!(c.current_hwm(), 8);
        assert_eq!(c.last_reserve_index(), 11);
    }

    #[test]
    fn from_persisted_seeds_reserve_cursor_so_replay_is_skipped() {
        let c = ClusterCounter::new(8, 11);
        assert_eq!(c.current_hwm(), 8);
        assert_eq!(c.reserve_at_index(10, 4).unwrap(), None);
        assert_eq!(c.reserve_at_index(11, 4).unwrap(), None);
        assert_eq!(
            c.current_hwm(),
            8,
            "replay below seeded cursor must not advance G"
        );
        assert_eq!(c.reserve_at_index(12, 4).unwrap(), Some((9, 13)));
        assert_eq!(c.current_hwm(), 12);
    }

    #[test]
    fn try_alloc_reserved_drains_exact_range_then_none() {
        let c = ClusterCounter::new(0, 0);
        let (start, end) = c.reserve_from_global(4).unwrap();
        c.set_reserved_batch(start, end);

        let mut got = Vec::new();
        while let Some(s) = c.try_alloc_reserved() {
            got.push(s.as_u32());
        }
        let expect: Vec<u32> = (start..end).collect();
        assert_eq!(got, expect);
        assert!(c.try_alloc_reserved().is_none());
        assert!(c.try_alloc_reserved().is_none());
    }

    #[test]
    fn empty_registry_has_no_reserved_batch() {
        let c = ClusterCounter::new(0, 0);
        assert!(c.try_alloc_reserved().is_none());
    }

    #[test]
    fn reserve_from_global_overflow_surfaces_typed_error() {
        let c = ClusterCounter::new(u32::MAX - 5, 0);
        let err = c.reserve_from_global(100).unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
        assert!(matches!(
            c.reserve_from_global(1),
            Err(SurrogateAllocError::Exhausted)
        ));
    }
}
