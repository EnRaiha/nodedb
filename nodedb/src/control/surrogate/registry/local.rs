// SPDX-License-Identifier: BUSL-1.1

//! Single-node surrogate allocation via a local `AtomicU64` — no Raft
//! round-trip. Exposes only `alloc_one`/`alloc`; never the cluster
//! HiLo reservation methods.

use std::ops::RangeInclusive;

use nodedb_types::Surrogate;

use super::error::SurrogateAllocError;
use super::watermark::Watermark;

/// Local (non-replicated) surrogate counter.
pub struct LocalCounter {
    watermark: Watermark,
}

impl LocalCounter {
    pub(super) fn new(hwm: u32) -> Self {
        Self {
            watermark: Watermark::new(hwm),
        }
    }

    /// Allocate one surrogate. `Exhausted` if the u32 space is full.
    pub fn alloc_one(&self) -> Result<Surrogate, SurrogateAllocError> {
        let prev = self.watermark.fetch_add_raw(1);
        if prev > u64::from(u32::MAX) {
            self.watermark.pin_exhausted();
            return Err(SurrogateAllocError::Exhausted);
        }
        Ok(Surrogate::new(prev as u32))
    }

    /// Allocate `n` contiguous surrogates as an inclusive range.
    pub fn alloc(&self, n: u32) -> Result<RangeInclusive<Surrogate>, SurrogateAllocError> {
        if n == 0 {
            return Err(SurrogateAllocError::EmptyBatch);
        }
        let prev = self.watermark.fetch_add_raw(u64::from(n));
        let last = prev + u64::from(n) - 1;
        if last > u64::from(u32::MAX) {
            self.watermark.pin_exhausted();
            return Err(SurrogateAllocError::Exhausted);
        }
        Ok(Surrogate::new(prev as u32)..=Surrogate::new(last as u32))
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
    use std::sync::Arc;

    use super::*;

    #[test]
    fn monotonic_10k() {
        let c = LocalCounter::new(0);
        let mut prev = 0u32;
        for _ in 0..10_000 {
            let s = c.alloc_one().unwrap().as_u32();
            assert!(s > prev, "expected monotonic, got {prev} then {s}");
            prev = s;
        }
        assert_eq!(c.current_hwm(), 10_000);
    }

    #[test]
    fn batch_alloc_returns_range_then_advances() {
        let c = LocalCounter::new(0);
        let range = c.alloc(100).unwrap();
        assert_eq!(*range.start(), Surrogate::new(1));
        assert_eq!(*range.end(), Surrogate::new(100));
        let count = (range.end().as_u32() - range.start().as_u32() + 1) as usize;
        assert_eq!(count, 100);
        let next = c.alloc_one().unwrap();
        assert_eq!(next, Surrogate::new(101));
    }

    #[test]
    fn batch_alloc_zero_rejected() {
        let c = LocalCounter::new(0);
        assert!(matches!(c.alloc(0), Err(SurrogateAllocError::EmptyBatch)));
    }

    #[test]
    fn restart_survives_hwm() {
        let c = LocalCounter::new(5000);
        let s = c.alloc_one().unwrap();
        assert_eq!(s, Surrogate::new(5001));
        assert_eq!(c.current_hwm(), 5001);
    }

    #[test]
    fn concurrent_16x1000_unique() {
        let c = Arc::new(LocalCounter::new(0));
        let mut handles = Vec::with_capacity(16);
        for _ in 0..16 {
            let r = c.clone();
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
        assert!(c.current_hwm() >= 16_000);
    }

    #[test]
    fn overflow_surfaces_typed_error() {
        let c = LocalCounter::new(u32::MAX - 1);
        let last = c.alloc_one().unwrap();
        assert_eq!(last, Surrogate::new(u32::MAX));
        let err = c.alloc_one().unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
        assert!(matches!(
            c.alloc_one().unwrap_err(),
            SurrogateAllocError::Exhausted
        ));
    }

    #[test]
    fn batch_overflow_surfaces_typed_error() {
        let c = LocalCounter::new(u32::MAX - 5);
        let err = c.alloc(100).unwrap_err();
        assert!(matches!(err, SurrogateAllocError::Exhausted));
    }

    #[test]
    fn current_hwm_tracks_allocs() {
        let c = LocalCounter::new(0);
        assert_eq!(c.current_hwm(), 0);
        let _ = c.alloc_one().unwrap();
        assert_eq!(c.current_hwm(), 1);
        let _ = c.alloc(10).unwrap();
        assert_eq!(c.current_hwm(), 11);
    }
}
