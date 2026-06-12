// SPDX-License-Identifier: Apache-2.0

//! Per-engine memory budget tracking.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Decrement `counter` by `size`, clamping at zero.
///
/// Plain `fetch_sub` wraps on underflow — a counter that has been
/// released past zero by another path (e.g. a legacy `MemoryGovernor::release`
/// draining a budget while a `ReservationToken` is still alive) would
/// otherwise jump to ~`usize::MAX`, which every utilization reader then
/// interprets as 100 % → permanent Emergency pressure. All release sites
/// must go through this so an over-release is at worst a saturated zero,
/// never a wrapped maximum.
pub(crate) fn atomic_saturating_sub(counter: &AtomicUsize, size: usize) {
    if size == 0 {
        return;
    }
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let new_val = current.saturating_sub(size);
        match counter.compare_exchange_weak(current, new_val, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// A memory budget for a single engine.
///
/// Tracks current allocation against a configurable limit using atomic
/// counters (safe to read from any thread — metrics exporter, governor, etc.).
///
/// The `allocated` counter is stored behind an `Arc` so that
/// [`ReservationToken`](crate::reservation_token::ReservationToken) can hold a
/// reference to it without requiring a back-reference to the governor.
#[derive(Debug)]
pub struct Budget {
    /// Hard limit in bytes. Allocations beyond this are rejected.
    limit: AtomicUsize,

    /// Current allocated bytes. Arc-wrapped so tokens can release on drop.
    allocated: Arc<AtomicUsize>,

    /// Peak allocated bytes (high-water mark).
    peak: AtomicUsize,

    /// Number of times an allocation was rejected due to budget exhaustion.
    rejection_count: AtomicUsize,

    /// Number of `release(size)` calls where `size > current` — the
    /// accounting-drift symptom. Saturating the per-engine counter
    /// to zero hides the drift from `allocated()`, so this counter
    /// is the only observable signal that the call-site is
    /// over-releasing.
    over_release_count: AtomicUsize,
}

impl Budget {
    /// Create a new budget with the given limit.
    pub fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            allocated: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            rejection_count: AtomicUsize::new(0),
            over_release_count: AtomicUsize::new(0),
        }
    }

    /// Try to reserve `size` bytes from this budget.
    ///
    /// Returns `true` if the reservation succeeded, `false` if it would
    /// exceed the limit.
    pub fn try_reserve(&self, size: usize) -> bool {
        let limit = self.limit.load(Ordering::Relaxed);

        // CAS loop to atomically check and increment.
        loop {
            let current = self.allocated.load(Ordering::Relaxed);
            if current + size > limit {
                self.rejection_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }

            match self.allocated.compare_exchange_weak(
                current,
                current + size,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Update peak if necessary.
                    let new_allocated = current + size;
                    let mut peak = self.peak.load(Ordering::Relaxed);
                    while new_allocated > peak {
                        match self.peak.compare_exchange_weak(
                            peak,
                            new_allocated,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => peak = actual,
                        }
                    }
                    return true;
                }
                Err(_) => continue, // Retry CAS.
            }
        }
    }

    /// Try to reserve `size` bytes and return a shared `Arc` to the allocated
    /// counter so the caller can decrement it on drop.
    ///
    /// Returns `Some(arc)` on success, `None` on budget exhaustion.
    pub fn try_reserve_arc(&self, size: usize) -> Option<Arc<AtomicUsize>> {
        if self.try_reserve(size) {
            Some(Arc::clone(&self.allocated))
        } else {
            None
        }
    }

    /// Release `size` bytes back to the budget.
    ///
    /// Saturates to zero if `size` exceeds the current allocation (which can
    /// happen when data is replayed from WAL without a matching reservation).
    pub fn release(&self, size: usize) {
        loop {
            let current = self.allocated.load(Ordering::Acquire);
            let new_val = current.saturating_sub(size);
            match self.allocated.compare_exchange_weak(
                current,
                new_val,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if size > current {
                        self.over_release_count.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            released = size,
                            allocated = current,
                            "memory release exceeds allocation (WAL replay or accounting drift)"
                        );
                    }
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    /// Current allocated bytes.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Hard limit in bytes.
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Number of over-release events observed on this budget. A
    /// non-zero value is the smoking-gun signal that some call-site
    /// is releasing more bytes than it reserved. Per-engine
    /// `allocated()` saturates to zero on over-release, so this
    /// counter is the only post-hoc observable.
    pub fn over_release_count(&self) -> usize {
        self.over_release_count.load(Ordering::Relaxed)
    }

    /// Remaining bytes available.
    pub fn available(&self) -> usize {
        let limit = self.limit();
        let allocated = self.allocated();
        limit.saturating_sub(allocated)
    }

    /// Utilization as a percentage (0-100).
    ///
    /// Computed in `u128` so a corrupted (e.g. underflow-wrapped near
    /// `usize::MAX`) `allocated` clamps to 100 % rather than panicking on
    /// `allocated * 100` overflow — a panic here is taken inside the Data
    /// Plane core loop (`apply_spsc_pressure` → `snapshot`) and escalates
    /// to a DEGRADED core.
    pub fn utilization_percent(&self) -> u8 {
        let limit = self.limit();
        if limit == 0 {
            return 100;
        }
        let allocated = self.allocated() as u128;
        ((allocated * 100) / limit as u128).min(100) as u8
    }

    /// Peak allocation (high-water mark).
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Number of rejected allocation attempts.
    pub fn rejections(&self) -> usize {
        self.rejection_count.load(Ordering::Relaxed)
    }

    /// Update the limit dynamically (for rebalancing).
    ///
    /// The new limit must be >= current allocation. If it's less, the limit
    /// is set to the current allocation (no immediate eviction).
    pub fn set_limit(&self, new_limit: usize) {
        let allocated = self.allocated();
        let effective = new_limit.max(allocated);
        self.limit.store(effective, Ordering::Release);
    }

    /// Reset all counters (for testing).
    #[cfg(test)]
    pub fn reset(&self) {
        self.allocated.store(0, Ordering::Relaxed);
        self.peak.store(0, Ordering::Relaxed);
        self.rejection_count.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utilization_and_available_never_panic_on_corrupted_allocated() {
        // `allocated` is the sum of `try_reserve`/`release` deltas. An
        // unbalanced call site — or a `ReservationToken` drop that
        // `fetch_sub`s past zero — leaves it wrapped near `usize::MAX`.
        // `utilization_percent` must not panic computing `allocated * 100`
        // (it does today in debug builds: `attempt to multiply with
        // overflow`, taken inside the Data Plane core loop via
        // `apply_spsc_pressure → snapshot`, which the panic watchdog then
        // escalates to a DEGRADED core). In release the same multiply
        // wraps and reports a garbage level. A budget reader must be
        // robust to a corrupted counter: report a clamped 100 % (so the
        // pressure detector at least errs toward Emergency, not toward
        // "idle") and never crash.
        let budget = Budget::new(1024);
        budget.allocated.store(usize::MAX, Ordering::Relaxed);

        assert_eq!(
            budget.utilization_percent(),
            100,
            "a wrapped/over-large allocated counter must clamp to 100%, not \
             panic on `allocated * 100` and not wrap to a small percentage"
        );
        assert_eq!(
            budget.available(),
            0,
            "no capacity is available when allocated has run past the limit"
        );
    }

    #[test]
    fn reserve_within_limit() {
        let budget = Budget::new(1024);
        assert!(budget.try_reserve(512));
        assert_eq!(budget.allocated(), 512);
        assert_eq!(budget.available(), 512);
        assert_eq!(budget.utilization_percent(), 50);
    }

    #[test]
    fn reserve_at_limit() {
        let budget = Budget::new(1024);
        assert!(budget.try_reserve(1024));
        assert!(!budget.try_reserve(1));
        assert_eq!(budget.rejections(), 1);
    }

    #[test]
    fn reserve_exceeds_limit() {
        let budget = Budget::new(100);
        assert!(!budget.try_reserve(101));
        assert_eq!(budget.allocated(), 0);
        assert_eq!(budget.rejections(), 1);
    }

    #[test]
    fn release_frees_capacity() {
        let budget = Budget::new(1024);
        assert!(budget.try_reserve(512));
        assert!(budget.try_reserve(512));
        assert!(!budget.try_reserve(1));

        budget.release(256);
        assert!(budget.try_reserve(256));
    }

    #[test]
    fn peak_tracks_high_water_mark() {
        let budget = Budget::new(1024);
        budget.try_reserve(800);
        budget.release(500);
        budget.try_reserve(100);

        assert_eq!(budget.peak(), 800);
        assert_eq!(budget.allocated(), 400);
    }

    #[test]
    fn dynamic_limit_adjustment() {
        let budget = Budget::new(1024);
        budget.try_reserve(600);

        // Increase limit.
        budget.set_limit(2048);
        assert_eq!(budget.limit(), 2048);
        assert!(budget.try_reserve(1000));

        // Decrease limit — but not below current allocation.
        budget.set_limit(100);
        assert_eq!(budget.limit(), 1600); // max(100, 1600 allocated)
    }

    #[test]
    fn try_reserve_arc_returns_shared_counter() {
        let budget = Budget::new(1024);
        let arc = budget.try_reserve_arc(512).expect("within budget");
        assert_eq!(arc.load(Ordering::Relaxed), 512);
        // Release via the arc.
        arc.fetch_sub(512, Ordering::Relaxed);
        assert_eq!(budget.allocated(), 0);
    }

    #[test]
    fn concurrent_reserves() {
        use std::sync::Arc;
        use std::thread;

        let budget = Arc::new(Budget::new(10_000));
        let mut handles = Vec::new();

        for _ in 0..10 {
            let b = Arc::clone(&budget);
            handles.push(thread::spawn(move || {
                let mut reserved = 0;
                for _ in 0..100 {
                    if b.try_reserve(10) {
                        reserved += 10;
                    }
                }
                reserved
            }));
        }

        let total_reserved: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // All 1000 reservations of 10 bytes should succeed (10 * 100 * 10 = 10000).
        assert_eq!(total_reserved, 10_000);
        assert_eq!(budget.allocated(), 10_000);
    }
}
