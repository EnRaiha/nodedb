// SPDX-License-Identifier: BUSL-1.1

//! `ProducerIdAllocator` — thread-safe monotonic `u64` allocator for
//! per-Lite-client producer identifiers.
//!
//! ## Counter semantics
//!
//! The internal `AtomicU64` stores the **next** id to be handed out.
//! `alloc_one()` does `fetch_add(1, AcqRel)`; the returned value is the
//! previously stored counter (the id the caller now owns).  After every
//! allocation, `current_hwm()` returns the highest producer-id ever
//! issued — equivalently `counter - 1`.
//!
//! ## Restart semantics
//!
//! `from_persisted_hwm(hwm)` initialises `counter = hwm + 1`.  On a fresh
//! database the persisted hwm is `0` and the first allocation returns `1`
//! (id `0` is reserved as "unallocated").  After a restart with persisted
//! hwm `50`, the first allocation returns `51`.
//!
//! ## Durability contract (Stage 1)
//!
//! In this stage the allocator persists its hwm to the `_system.sync_producer_hwm`
//! redb table via the `ProducerHwmPersist` trait, matching exactly how
//! `SurrogateRegistry` uses `SurrogateHwmPersist`.  Durable WAL-HWM record
//! emission (the `RecordType::SurrogateAlloc` analogue) and Raft replication
//! of the hwm are Stage-5 follow-ups, declared here explicitly so the gap
//! is visible in code rather than implicit.
//!
//! ## Width
//!
//! Producer-ids are `u64` (vs surrogates' `u32`) because they are allocated
//! per-Lite-client rather than per-row; the space is far sparser and a `u64`
//! counter will never realistically wrap.  Exhaustion is still detected and
//! surfaced as a typed error rather than wrapping silently.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Periodic flush trigger: every N allocations.
pub const PRODUCER_FLUSH_OPS_THRESHOLD: u64 = 256;

/// Periodic flush trigger: every T elapsed since the last flush.
pub const PRODUCER_FLUSH_ELAPSED_THRESHOLD: Duration = Duration::from_millis(500);

/// Allocation errors.  Wired into `crate::Error` via `From` at the bottom of
/// this file.
#[derive(Debug, thiserror::Error)]
pub enum ProducerAllocError {
    #[error("producer-id space exhausted (u64::MAX reached)")]
    Exhausted,

    #[error("producer-id flush failed: {detail}")]
    FlushFailed { detail: String },
}

/// Pluggable persistence boundary for the producer-id hwm.
///
/// Tests substitute an in-memory store; production wires the
/// `SystemCatalog`-backed implementation in `crate::control::sync_producer::persist`.
pub trait ProducerHwmPersist: Send + Sync {
    /// Persist the current high-watermark.  Called by `ProducerIdAllocator::flush`
    /// whenever the periodic-flush thresholds are tripped.
    fn checkpoint(&self, hwm: u64) -> crate::Result<()>;

    /// Load the persisted high-watermark, or `0` if none has been recorded yet.
    fn load(&self) -> crate::Result<u64>;
}

/// Thread-safe monotonic producer-id allocator.
///
/// The `Mutex<Instant>` for `last_flush_at` is uncontended on the hot path
/// (`alloc_one` only touches atomics); the lock is taken only by
/// `should_flush` and `flush`, which run at most once per ~500 ms or per
/// 256 allocations.
pub struct ProducerIdAllocator {
    /// Next producer-id to hand out.  Starts at 1 on a fresh allocator, or
    /// `persisted_hwm + 1` on restart.
    counter: AtomicU64,
    /// Allocations since the last flush.  Reset by `flush()`.
    allocs_since_flush: AtomicU64,
    /// Wall-clock anchor for the elapsed-time flush trigger.
    last_flush_at: Mutex<Instant>,
}

impl ProducerIdAllocator {
    /// Create a fresh allocator — first allocation returns `1`.
    pub fn new() -> Self {
        Self::from_persisted_hwm(0)
    }

    /// Restore from a persisted high-watermark.  Next allocation returns
    /// `hwm + 1`.  `hwm == 0` is equivalent to `new()`.
    pub fn from_persisted_hwm(hwm: u64) -> Self {
        Self {
            counter: AtomicU64::new(hwm.saturating_add(1)),
            allocs_since_flush: AtomicU64::new(0),
            last_flush_at: Mutex::new(Instant::now()),
        }
    }

    /// Allocate a single producer-id.  Returns `Exhausted` if `u64::MAX`
    /// has been reached.
    pub fn alloc_one(&self) -> Result<u64, ProducerAllocError> {
        let prev = self.counter.fetch_add(1, Ordering::AcqRel);
        if prev == u64::MAX {
            // Restore so future callers also see Exhausted rather than
            // racing past into a wrapped 0.
            self.counter.store(u64::MAX, Ordering::Release);
            return Err(ProducerAllocError::Exhausted);
        }
        self.allocs_since_flush.fetch_add(1, Ordering::AcqRel);
        Ok(prev)
    }

    /// Highest producer-id ever issued — `0` if no allocations yet.
    pub fn current_hwm(&self) -> u64 {
        let next = self.counter.load(Ordering::Acquire);
        next.saturating_sub(1)
    }

    /// True if the periodic-flush thresholds (ops or elapsed) are tripped.
    pub fn should_flush(&self) -> bool {
        if self.allocs_since_flush.load(Ordering::Acquire) >= PRODUCER_FLUSH_OPS_THRESHOLD {
            return true;
        }
        if let Ok(last) = self.last_flush_at.lock() {
            return last.elapsed() >= PRODUCER_FLUSH_ELAPSED_THRESHOLD;
        }
        false
    }

    /// Idempotently raise the high-watermark to at least `new_hwm`.
    ///
    /// Used at boot time so post-restart allocations cannot collide with
    /// pre-crash ones (mirrors `SurrogateRegistry::restore_hwm`).
    pub fn restore_hwm(&self, new_hwm: u64) -> Result<(), ProducerAllocError> {
        let target = new_hwm.saturating_add(1);
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
    /// Idempotent: calling on an unmodified allocator just rewrites the same hwm.
    pub fn flush(&self, persist: &dyn ProducerHwmPersist) -> Result<(), ProducerAllocError> {
        let hwm = self.current_hwm();
        persist
            .checkpoint(hwm)
            .map_err(|e| ProducerAllocError::FlushFailed {
                detail: e.to_string(),
            })?;
        self.allocs_since_flush.store(0, Ordering::Release);
        if let Ok(mut guard) = self.last_flush_at.lock() {
            *guard = Instant::now();
        }
        Ok(())
    }

    /// Test-only: rewind the wall-clock anchor so the elapsed flush trigger
    /// fires on the next `should_flush` call.
    #[cfg(test)]
    fn rewind_flush_clock(&self, by: Duration) {
        if let Ok(mut guard) = self.last_flush_at.lock()
            && let Some(earlier) = guard.checked_sub(by)
        {
            *guard = earlier;
        }
    }
}

impl Default for ProducerIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl From<ProducerAllocError> for crate::Error {
    fn from(e: ProducerAllocError) -> Self {
        match e {
            ProducerAllocError::Exhausted => crate::Error::Internal {
                detail: "producer-id space exhausted (u64::MAX reached)".into(),
            },
            ProducerAllocError::FlushFailed { detail } => crate::Error::Storage {
                engine: "sync_producer".into(),
                detail,
            },
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use super::*;

    /// In-memory persist for tests.
    struct MemPersist {
        last: std::sync::Mutex<Option<u64>>,
        calls: AtomicU64,
    }

    impl MemPersist {
        fn new() -> Self {
            Self {
                last: std::sync::Mutex::new(None),
                calls: AtomicU64::new(0),
            }
        }

        fn last(&self) -> Option<u64> {
            *self.last.lock().unwrap()
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Acquire)
        }
    }

    impl ProducerHwmPersist for MemPersist {
        fn checkpoint(&self, hwm: u64) -> crate::Result<()> {
            *self.last.lock().unwrap() = Some(hwm);
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn load(&self) -> crate::Result<u64> {
            Ok(self.last().unwrap_or(0))
        }
    }

    #[test]
    fn first_alloc_returns_one() {
        let a = ProducerIdAllocator::new();
        assert_eq!(a.alloc_one().unwrap(), 1);
        assert_eq!(a.current_hwm(), 1);
    }

    #[test]
    fn monotonic_10k() {
        let a = ProducerIdAllocator::new();
        let mut prev = 0u64;
        for _ in 0..10_000 {
            let id = a.alloc_one().unwrap();
            assert!(id > prev, "expected monotonic, got {prev} then {id}");
            prev = id;
        }
        assert_eq!(a.current_hwm(), 10_000);
    }

    #[test]
    fn restart_survives_hwm() {
        let a = ProducerIdAllocator::from_persisted_hwm(5000);
        assert_eq!(a.alloc_one().unwrap(), 5001);
        assert_eq!(a.current_hwm(), 5001);
    }

    #[test]
    fn concurrent_16x1000_unique() {
        let a = Arc::new(ProducerIdAllocator::new());
        let mut handles = Vec::with_capacity(16);
        for _ in 0..16 {
            let r = a.clone();
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
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 16_000, "expected 16000 unique producer-ids");
        assert!(a.current_hwm() >= 16_000);
    }

    #[test]
    fn flush_threshold_ops() {
        let a = ProducerIdAllocator::new();
        assert!(!a.should_flush());
        for _ in 0..(PRODUCER_FLUSH_OPS_THRESHOLD - 1) {
            a.alloc_one().unwrap();
        }
        assert!(!a.should_flush(), "below threshold");
        a.alloc_one().unwrap();
        assert!(a.should_flush(), "at threshold");
        let p = MemPersist::new();
        a.flush(&p).unwrap();
        assert_eq!(p.calls(), 1);
        assert_eq!(p.last(), Some(PRODUCER_FLUSH_OPS_THRESHOLD));
        assert!(!a.should_flush(), "post-flush");
    }

    #[test]
    fn flush_threshold_elapsed() {
        let a = ProducerIdAllocator::new();
        a.alloc_one().unwrap();
        assert!(!a.should_flush());
        a.rewind_flush_clock(PRODUCER_FLUSH_ELAPSED_THRESHOLD * 2);
        assert!(a.should_flush(), "rewound clock fires elapsed");
        let p = MemPersist::new();
        a.flush(&p).unwrap();
        assert!(!a.should_flush(), "post-flush resets clock");
    }

    #[test]
    fn restore_hwm_never_lowers() {
        let a = ProducerIdAllocator::from_persisted_hwm(100);
        a.restore_hwm(50).unwrap();
        assert_eq!(a.current_hwm(), 100);
        a.restore_hwm(200).unwrap();
        assert_eq!(a.current_hwm(), 200);
    }
}
