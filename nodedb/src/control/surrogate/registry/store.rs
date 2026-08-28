// SPDX-License-Identifier: BUSL-1.1

//! `SurrogateRegistry` — thread-safe monotonic surrogate allocator.
//! Owns flush-cadence bookkeeping generically; allocation itself goes
//! through whichever `SurrogateRegistryMode` this node was built with.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::super::persist::SurrogateHwmPersist;
use super::cluster::ClusterCounter;
use super::consts::{FLUSH_ELAPSED_THRESHOLD, FLUSH_OPS_THRESHOLD};
use super::error::{SurrogateAllocError, SurrogatePromotionError};
use super::local::LocalCounter;
use super::mode::SurrogateRegistryMode;

/// Thread-safe surrogate allocator. See module docs.
pub struct SurrogateRegistry {
    mode: SurrogateRegistryMode,
    /// Allocations since the last flush. `Local`-mode bookkeeping only —
    /// `Cluster` mode is persisted by the `SurrogateReserve` apply path.
    allocs_since_flush: AtomicU64,
    /// Wall-clock anchor for the elapsed-time flush trigger.
    last_flush_at: Mutex<Instant>,
}

impl SurrogateRegistry {
    /// Create an empty `Local` registry — first allocation returns
    /// `Surrogate(1)`.
    pub fn new() -> Self {
        Self::from_persisted_hwm(0)
    }

    /// Restore a `Local` registry from a persisted high-watermark. Next
    /// allocation returns `hwm + 1`.
    pub fn from_persisted_hwm(hwm: u32) -> Self {
        Self {
            mode: SurrogateRegistryMode::Local(LocalCounter::new(hwm)),
            allocs_since_flush: AtomicU64::new(0),
            last_flush_at: Mutex::new(Instant::now()),
        }
    }

    /// Restore a `Cluster` registry from a persisted high-watermark and
    /// applied-reserve cursor. `Cluster` vs `Local` is a static,
    /// deployment-time choice made by the caller — never inferred here.
    pub fn from_persisted_cluster(hwm: u32, reserve_index: u64) -> Self {
        Self {
            mode: SurrogateRegistryMode::Cluster(ClusterCounter::new(hwm, reserve_index)),
            allocs_since_flush: AtomicU64::new(0),
            last_flush_at: Mutex::new(Instant::now()),
        }
    }

    /// The counter backing this registry. Callers match on this to reach
    /// mode-specific methods (see `assign/cluster_reserve.rs`).
    pub fn mode(&self) -> &SurrogateRegistryMode {
        &self.mode
    }

    /// Highest surrogate ever issued — `0` if no allocations yet.
    pub fn current_hwm(&self) -> u32 {
        match &self.mode {
            SurrogateRegistryMode::Local(c) => c.current_hwm(),
            SurrogateRegistryMode::Cluster(c) => c.current_hwm(),
        }
    }

    /// Idempotently raise the high-watermark to at least `new_hwm`.
    /// Valid in either mode — a `bind()` or WAL/Raft replay can advance
    /// the floor regardless of how this node allocates fresh surrogates.
    pub fn restore_hwm(&self, new_hwm: u32) -> Result<(), SurrogateAllocError> {
        match &self.mode {
            SurrogateRegistryMode::Local(c) => c.restore_hwm(new_hwm),
            SurrogateRegistryMode::Cluster(c) => c.restore_hwm(new_hwm),
        }
        Ok(())
    }

    /// Record `n` local allocations against the flush-cadence counters.
    /// Called by the hot path after a successful `Local` draw only.
    pub(crate) fn record_allocs(&self, n: u64) {
        self.allocs_since_flush.fetch_add(n, Ordering::AcqRel);
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

    /// Persist the current high-watermark and reset flush counters.
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

    /// Promote a `Local` registry into `Cluster` mode. Always fails
    /// today: the barrier that would flush this node's local hwm into
    /// `G` before promotion is safe does not exist yet. A typed error
    /// here, not a silent switch, is deliberate — see follow-up work.
    pub fn promote_to_cluster(&mut self) -> Result<(), SurrogatePromotionError> {
        match &self.mode {
            SurrogateRegistryMode::Cluster(_) => Ok(()),
            SurrogateRegistryMode::Local(_) => Err(SurrogatePromotionError::BarrierNotImplemented),
        }
    }

    /// Test-only: force the elapsed-flush trigger to fire on the next
    /// `should_flush` call by rewinding the wall-clock anchor.
    #[cfg(test)]
    fn rewind_flush_clock(&self, by: std::time::Duration) {
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

#[cfg(test)]
mod tests {
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

    /// Allocate one surrogate on a `Local` registry and feed the
    /// flush-counter bookkeeping the hot path feeds it in production.
    fn alloc_local_one(reg: &SurrogateRegistry) -> nodedb_types::Surrogate {
        let SurrogateRegistryMode::Local(local) = reg.mode() else {
            panic!("expected Local mode");
        };
        let s = local.alloc_one().unwrap();
        reg.record_allocs(1);
        s
    }

    #[test]
    fn current_hwm_tracks_allocs() {
        let reg = SurrogateRegistry::new();
        assert_eq!(reg.current_hwm(), 0);
        let _ = alloc_local_one(&reg);
        assert_eq!(reg.current_hwm(), 1);
    }

    #[test]
    fn flush_threshold_ops() {
        let reg = SurrogateRegistry::new();
        assert!(!reg.should_flush(), "fresh registry should not flush yet");
        for _ in 0..(FLUSH_OPS_THRESHOLD - 1) {
            let _ = alloc_local_one(&reg);
        }
        assert!(!reg.should_flush(), "below ops threshold should not flush");
        let _ = alloc_local_one(&reg);
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
        let _ = alloc_local_one(&reg);
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
    fn restore_hwm_works_in_either_mode() {
        let local = SurrogateRegistry::new();
        local.restore_hwm(500).unwrap();
        assert_eq!(local.current_hwm(), 500);

        let cluster = SurrogateRegistry::from_persisted_cluster(0, 0);
        cluster.restore_hwm(500).unwrap();
        assert_eq!(cluster.current_hwm(), 500);
    }

    #[test]
    fn promote_local_to_cluster_is_a_typed_error() {
        let mut reg = SurrogateRegistry::new();
        let err = reg.promote_to_cluster().unwrap_err();
        assert!(matches!(
            err,
            SurrogatePromotionError::BarrierNotImplemented
        ));

        let mut already_cluster = SurrogateRegistry::from_persisted_cluster(0, 0);
        assert!(already_cluster.promote_to_cluster().is_ok());
    }

    /// The invariant this split makes impossible to express on ordinary
    /// cluster-mode code: `ClusterCounter` has no `alloc_one`/`alloc`, so
    /// two cluster nodes can never independently bump `G` with a local,
    /// non-replicated increment. This test reproduces the OLD defect's
    /// shape directly on the counters: if a `ClusterCounter`'s watermark
    /// is ever seeded from a value a `LocalCounter` produced out-of-band
    /// (the only residual path — via `restore_hwm`, used for carried-
    /// surrogate binds), two nodes replaying the identical
    /// `reserve_at_index(index, batch)` diverge and mint colliding
    /// surrogate ranges for the same `raft_index`.
    #[test]
    fn divergent_local_alloc_then_identical_replay_mints_colliding_ranges() {
        let node_a = ClusterCounter::new(0, 0);
        let node_b = ClusterCounter::new(0, 0);

        let leaked_local = LocalCounter::new(0);
        let _ = leaked_local.alloc_one().unwrap();
        node_a.restore_hwm(leaked_local.current_hwm());

        let carved_a = node_a.reserve_at_index(1, 10).unwrap().unwrap();
        let carved_b = node_b.reserve_at_index(1, 10).unwrap().unwrap();

        assert_ne!(
            carved_a, carved_b,
            "diverged watermark ⇒ same raft_index mints colliding surrogate ranges"
        );
    }
}
