// SPDX-License-Identifier: BUSL-1.1

//! Overflow-safe `u32`-space atomic counter shared by `LocalCounter`
//! and `ClusterCounter`. Callers apply their own boundary formula and
//! call `pin_exhausted` on overflow.

use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct Watermark(AtomicU64);

impl Watermark {
    pub(super) fn new(hwm: u32) -> Self {
        Self(AtomicU64::new(u64::from(hwm) + 1))
    }

    /// Advance by `n`, returning the pre-advance value.
    pub(super) fn fetch_add_raw(&self, n: u64) -> u64 {
        self.0.fetch_add(n, Ordering::AcqRel)
    }

    /// Pin at `u32::MAX + 1` so later callers also see `Exhausted`.
    pub(super) fn pin_exhausted(&self) {
        self.0.store(u64::from(u32::MAX) + 1, Ordering::Release);
    }

    pub(super) fn current_hwm(&self) -> u32 {
        let next = self.0.load(Ordering::Acquire);
        next.saturating_sub(1).min(u64::from(u32::MAX)) as u32
    }

    /// Idempotently raise the watermark to at least `new_hwm`.
    pub(super) fn restore(&self, new_hwm: u32) {
        let target = u64::from(new_hwm) + 1;
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            if target <= current {
                return;
            }
            match self
                .0
                .compare_exchange_weak(current, target, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}
