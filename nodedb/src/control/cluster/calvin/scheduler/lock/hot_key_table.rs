// SPDX-License-Identifier: BUSL-1.1

//! CP-local hot-key detection for Calvin read reservations.
//!
//! Tracks, per lock key, how often a transaction that read that key
//! subsequently aborted on a serialization / OCC conflict. A key crossing the
//! abort threshold within a rolling time window is "hot" — a later pass uses
//! [`HotKeyTable::is_hot`] to decide whether to reserve that key at read time.
//! This is a pure scheduling heuristic: it is never replicated and never
//! influences a committed outcome, only whether a future read reserves.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::lock_key::LockKey;

/// Rolling window over which aborts are counted before the count decays.
const HOT_KEY_WINDOW: Duration = Duration::from_secs(10);
/// Aborts within one window at which a key is considered hot.
const HOT_KEY_ABORT_THRESHOLD: u32 = 3;

struct AbortCounter {
    count: u32,
    window_start: Instant,
}

/// Global, CP-local hot-key detector. NOT replicated, NOT deterministic — a
/// pure scheduling hint. Uses a wall-clock rolling window so a key cools down
/// when the workload's contention moves. `HashMap` (not `BTreeMap`)
/// deliberately: this table must never be relied on for deterministic
/// iteration.
pub struct HotKeyTable {
    counts: HashMap<LockKey, AbortCounter>,
    window: Duration,
    threshold: u32,
}

impl HotKeyTable {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            window: HOT_KEY_WINDOW,
            threshold: HOT_KEY_ABORT_THRESHOLD,
        }
    }

    /// Record that a txn reading `key` aborted. `now` is passed in (caller
    /// reads the clock) so tests are deterministic.
    pub fn record_abort(&mut self, key: &LockKey, now: Instant) {
        let e = self.counts.entry(key.clone()).or_insert(AbortCounter {
            count: 0,
            window_start: now,
        });
        if now.duration_since(e.window_start) > self.window {
            e.count = 0;
            e.window_start = now;
        }
        e.count = e.count.saturating_add(1);
    }

    /// Whether `key` has reached the abort threshold within the current
    /// window.
    pub fn is_hot(&self, key: &LockKey, now: Instant) -> bool {
        match self.counts.get(key) {
            Some(e) if now.duration_since(e.window_start) <= self.window => {
                e.count >= self.threshold
            }
            _ => false,
        }
    }
}

impl Default for HotKeyTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn key() -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from("docs"),
            surrogate: 42,
        }
    }

    #[test]
    fn below_threshold_is_not_hot() {
        let mut table = HotKeyTable::new();
        let now = Instant::now();
        let k = key();
        table.record_abort(&k, now);
        table.record_abort(&k, now + Duration::from_secs(1));
        assert!(!table.is_hot(&k, now + Duration::from_secs(1)));
    }

    #[test]
    fn at_threshold_is_hot() {
        let mut table = HotKeyTable::new();
        let now = Instant::now();
        let k = key();
        table.record_abort(&k, now);
        table.record_abort(&k, now + Duration::from_secs(1));
        table.record_abort(&k, now + Duration::from_secs(2));
        assert!(table.is_hot(&k, now + Duration::from_secs(2)));
    }

    #[test]
    fn window_expiry_resets_to_not_hot() {
        let mut table = HotKeyTable::new();
        let now = Instant::now();
        let k = key();
        table.record_abort(&k, now);
        table.record_abort(&k, now + Duration::from_secs(1));
        table.record_abort(&k, now + Duration::from_secs(2));
        assert!(table.is_hot(&k, now + Duration::from_secs(2)));

        // Past the window: the next abort resets the counter to 1.
        let later = now + HOT_KEY_WINDOW + Duration::from_secs(1);
        table.record_abort(&k, later);
        assert!(!table.is_hot(&k, later));
    }

    #[test]
    fn fresh_key_is_not_hot() {
        let table = HotKeyTable::new();
        let now = Instant::now();
        assert!(!table.is_hot(&key(), now));
    }
}
