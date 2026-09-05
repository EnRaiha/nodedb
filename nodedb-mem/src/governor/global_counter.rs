// SPDX-License-Identifier: Apache-2.0

//! Shared atomic global usage tracker for [`MemoryGovernor`](super::core::MemoryGovernor).

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::over_release::OverRelease;

/// Separate from the governor so a `ReservationToken` holds an `Arc` to the
/// counter alone.
pub struct GlobalCounter {
    pub(crate) allocated: AtomicUsize,
    pub(crate) ceiling: usize,
    /// Per-layer over-release counters. Every releaser already holds this
    /// `Arc`, so both drop paths reach them without extra plumbing.
    pub(crate) over_release: OverRelease,
}

impl GlobalCounter {
    pub(crate) fn new(ceiling: usize) -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            ceiling,
            over_release: OverRelease::new(),
        }
    }
}

impl std::fmt::Debug for GlobalCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalCounter")
            .field("allocated", &self.allocated.load(Ordering::Relaxed))
            .field("ceiling", &self.ceiling)
            .field("over_release", &self.over_release)
            .finish()
    }
}
