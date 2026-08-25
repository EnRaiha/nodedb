// SPDX-License-Identifier: BUSL-1.1

//! Injectable wall-clock source for lease expiry checks.
//!
//! Lease expiry is a *duration* question — "have N nanoseconds passed?" — so it
//! must be measured against real wall time, never against a Hybrid Logical
//! Clock. An HLC only advances on a local event or an inbound message, so on an
//! idle cluster it physically freezes; comparing a lease's `expires_at` against
//! `HlcClock::peek()` would find every lease unexpired and reinstate the wedge
//! (see PR #246 / `drain_propose.rs`).
//!
//! The [`WallClock`] trait lets production code read the real clock while tests
//! drive expiry deterministically with a [`MockClock`] instead of mocking
//! `SystemTime` or depending on `HlcClock::peek`.

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

/// A source of nanoseconds since the Unix epoch.
pub trait WallClock {
    fn now_ns(&self) -> u64;
}

/// Reads the real system clock.
pub struct RealWallClock;

impl WallClock for RealWallClock {
    fn now_ns(&self) -> u64 {
        crate::control::lease::wall_now_ns()
    }
}

/// Deterministic clock for tests. Set it explicitly so expiry assertions do not
/// depend on `SystemTime` or on `HlcClock::peek` (which never advances on its
/// own and would make every lease look unexpired).
#[cfg(test)]
pub struct MockClock {
    now: AtomicU64,
}

#[cfg(test)]
impl MockClock {
    pub fn new(ns: u64) -> Self {
        Self {
            now: AtomicU64::new(ns),
        }
    }

    pub fn set(&self, ns: u64) {
        self.now.store(ns, Ordering::Relaxed);
    }
}

#[cfg(test)]
impl WallClock for MockClock {
    fn now_ns(&self) -> u64 {
        self.now.load(Ordering::Relaxed)
    }
}
