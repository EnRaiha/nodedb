// SPDX-License-Identifier: BUSL-1.1

//! Resolve-miss observability: per-tenant counters + per-connection
//! spike detector.
//!
//! Two separate jobs:
//!
//! - `TenantMissCounters` — monotonic counter keyed by `TenantId`. Scraped
//!   by `/metrics` for Grafana; feeds the `session_handle.resolve_miss_total`
//!   tagged counter.
//! - `PerConnectionSpikeDetector` — sliding-window detector keyed by
//!   connection. When more than `threshold` misses land inside `window` on
//!   a single connection, the detector fires once (`Fired`) and then stays
//!   quiet until the window empties again — no audit-event storms.
//!
//! A fingerprint-rejected resolution counts as a miss here too. From the
//! caller's perspective it's indistinguishable from an unknown handle;
//! observability should reflect that.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::types::TenantId;

#[derive(Default)]
pub(super) struct TenantMissCounters {
    by_tenant: RwLock<HashMap<TenantId, AtomicU64>>,
}

impl TenantMissCounters {
    pub(super) fn increment(&self, tenant_id: TenantId) {
        // Fast path: reader lock + existing entry.
        {
            let map = self.by_tenant.read();
            if let Some(counter) = map.get(&tenant_id) {
                counter.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Slow path: insert.
        let mut map = self.by_tenant.write();
        map.entry(tenant_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn get(&self, tenant_id: TenantId) -> u64 {
        let map = self.by_tenant.read();
        map.get(&tenant_id)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// Outcome of recording a miss into the per-connection spike detector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SpikeOutcome {
    /// Within threshold; nothing to do.
    Quiet,
    /// Threshold crossed; caller should emit a single audit event.
    Fired,
}

pub(super) struct PerConnectionSpikeDetector {
    threshold: u32,
    window: Duration,
    state: RwLock<HashMap<String, ConnectionSpikeState>>,
}

struct ConnectionSpikeState {
    misses: VecDeque<Instant>,
    /// True while the spike is still active — prevents re-firing audit
    /// events on every subsequent miss. Cleared when the window empties.
    fired: bool,
}

impl PerConnectionSpikeDetector {
    pub(super) fn new(threshold: u32, window: Duration) -> Self {
        Self {
            threshold,
            window,
            state: RwLock::new(HashMap::new()),
        }
    }

    pub(super) fn record(&self, conn_key: &str) -> SpikeOutcome {
        self.record_at(conn_key, Instant::now())
    }

    pub(super) fn record_at(&self, conn_key: &str, now: Instant) -> SpikeOutcome {
        let mut state = self.state.write();
        let cutoff = now.checked_sub(self.window).unwrap_or(now);

        let entry = state
            .entry(conn_key.to_string())
            .or_insert_with(|| ConnectionSpikeState {
                misses: VecDeque::new(),
                fired: false,
            });

        while let Some(front) = entry.misses.front() {
            if *front < cutoff {
                entry.misses.pop_front();
            } else {
                break;
            }
        }
        entry.misses.push_back(now);

        let count = entry.misses.len() as u32;
        if count < self.threshold {
            // Dropped below threshold after eviction — re-arm for the next spike.
            entry.fired = false;
            return SpikeOutcome::Quiet;
        }

        if entry.fired {
            SpikeOutcome::Quiet
        } else {
            entry.fired = true;
            SpikeOutcome::Fired
        }
    }

    pub(super) fn forget(&self, conn_key: &str) {
        let mut state = self.state.write();
        state.remove(conn_key);
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    #[test]
    fn counter_increments_per_tenant() {
        let c = TenantMissCounters::default();
        c.increment(TenantId::new(1));
        c.increment(TenantId::new(1));
        c.increment(TenantId::new(2));
        assert_eq!(c.get(TenantId::new(1)), 2);
        assert_eq!(c.get(TenantId::new(2)), 1);
        assert_eq!(c.get(TenantId::new(3)), 0);
    }

    #[test]
    fn miss_counter_and_spike_detector_survive_panics_while_write_locked() {
        let counters = TenantMissCounters::default();
        let tenant_id = TenantId::new(9);
        counters.increment(tenant_id);
        let counter_panic = catch_unwind(AssertUnwindSafe(|| {
            let _map = counters.by_tenant.write();
            panic!("simulated interrupted miss-counter update");
        }));
        assert!(counter_panic.is_err());
        counters.increment(tenant_id);
        assert_eq!(counters.get(tenant_id), 2);

        let detector = PerConnectionSpikeDetector::new(2, Duration::from_secs(60));
        let now = Instant::now();
        assert_eq!(detector.record_at("conn", now), SpikeOutcome::Quiet);
        let detector_panic = catch_unwind(AssertUnwindSafe(|| {
            let _map = detector.state.write();
            panic!("simulated interrupted spike-detector update");
        }));
        assert!(detector_panic.is_err());
        // The earlier miss remains counted, so the threshold is still enforced.
        assert_eq!(
            detector.record_at("conn", now + Duration::from_millis(1)),
            SpikeOutcome::Fired
        );
        // Subsequent mutation can clear the connection-local state.
        detector.forget("conn");
        assert_eq!(
            detector.record_at("conn", now + Duration::from_millis(2)),
            SpikeOutcome::Quiet
        );
    }

    #[test]
    fn spike_fires_once_per_sustained_spike() {
        let d = PerConnectionSpikeDetector::new(3, Duration::from_secs(60));
        assert_eq!(d.record("c"), SpikeOutcome::Quiet);
        assert_eq!(d.record("c"), SpikeOutcome::Quiet);
        assert_eq!(d.record("c"), SpikeOutcome::Fired);
        // Subsequent misses in the same active spike stay quiet.
        assert_eq!(d.record("c"), SpikeOutcome::Quiet);
        assert_eq!(d.record("c"), SpikeOutcome::Quiet);
    }

    #[test]
    fn spike_rearms_after_window_empties() {
        let d = PerConnectionSpikeDetector::new(2, Duration::from_millis(50));
        let t0 = Instant::now();
        d.record_at("c", t0);
        assert_eq!(d.record_at("c", t0), SpikeOutcome::Fired);
        // Let the window elapse fully.
        let later = t0 + Duration::from_millis(200);
        // First record after the gap: window is empty → evicts both, pushes 1.
        assert_eq!(d.record_at("c", later), SpikeOutcome::Quiet);
        // Build up another spike.
        assert_eq!(
            d.record_at("c", later + Duration::from_millis(1)),
            SpikeOutcome::Fired
        );
    }
}
