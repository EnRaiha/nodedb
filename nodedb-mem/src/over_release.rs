// SPDX-License-Identifier: Apache-2.0

//! Over-release detection on the live reservation-release path.
//!
//! [`atomic_saturating_sub`](crate::budget::atomic_saturating_sub) clamps a
//! below-zero release to zero so a wrapped counter never reads as 100 %
//! utilization. The clamp hides the drift from every `allocated()` reader —
//! [`OverRelease`] is the counter that keeps it visible, split per budget
//! layer so an operator can tell which layer's release accounting drifted.

use std::sync::atomic::{AtomicUsize, Ordering};

use nodedb_types::{DatabaseId, TenantId};

use crate::engine::EngineId;

/// The reservation a release belongs to, carried into the warning so a
/// reader can attribute drift to a database, tenant, and engine.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReleaseIdentity {
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub engine: EngineId,
}

/// A budget layer in the four-level reservation hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Global,
    Database,
    Tenant,
    Engine,
}

/// Per-layer over-release counters shared by every releaser through
/// `Arc<GlobalCounter>`.
///
/// A nonzero counter means some call site released more bytes than that
/// layer ever held — the "memory release exceeds allocation" symptom, now
/// counted at the site that actually releases memory instead of at a dead
/// code path.
#[derive(Debug, Default)]
pub struct OverRelease {
    global: AtomicUsize,
    database: AtomicUsize,
    tenant: AtomicUsize,
    engine: AtomicUsize,
}

impl OverRelease {
    /// Build a fresh set of counters, all zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an over-release event against `layer`. `shortfall` is a
    /// nonzero test, not a byte total — the counter counts events, and
    /// never accumulates the shortfall bytes themselves. A zero shortfall
    /// is a no-op.
    pub fn record(&self, layer: Layer, shortfall: usize) {
        if shortfall == 0 {
            return;
        }
        self.counter(layer).fetch_add(1, Ordering::Relaxed);
    }

    /// Number of over-release events recorded against the global layer.
    pub fn global(&self) -> usize {
        self.global.load(Ordering::Relaxed)
    }

    /// Number of over-release events recorded against the database layer.
    pub fn database(&self) -> usize {
        self.database.load(Ordering::Relaxed)
    }

    /// Number of over-release events recorded against the tenant layer.
    pub fn tenant(&self) -> usize {
        self.tenant.load(Ordering::Relaxed)
    }

    /// Number of over-release events recorded against the engine layer.
    pub fn engine(&self) -> usize {
        self.engine.load(Ordering::Relaxed)
    }

    /// Total over-release events across every layer.
    ///
    /// Sums four independent relaxed loads — not a single atomic read, so
    /// a concurrent `record` on another layer can land between them. The
    /// count settles once all releasers finish; it is not a point-in-time
    /// snapshot under concurrent load.
    pub fn total(&self) -> usize {
        self.global() + self.database() + self.tenant() + self.engine()
    }

    fn counter(&self, layer: Layer) -> &AtomicUsize {
        match layer {
            Layer::Global => &self.global,
            Layer::Database => &self.database,
            Layer::Tenant => &self.tenant,
            Layer::Engine => &self.engine,
        }
    }
}

/// Release `size` bytes from one layer's counter and record any shortfall.
///
/// The shared call site `ReservationToken::drop` and `ReserveScope::drop`
/// both use to release one layer of the four-level hierarchy. Returns the
/// shortfall so a caller that also warns (`ReservationToken::drop`) does
/// not need to recompute it.
pub(crate) fn release_layer(
    counter: &AtomicUsize,
    size: usize,
    layer: Layer,
    over_release: &OverRelease,
    id: ReleaseIdentity,
) -> usize {
    let shortfall = crate::budget::atomic_saturating_sub(counter, size);
    if shortfall == 0 {
        return 0;
    }
    over_release.record(layer, shortfall);
    tracing::warn!(
        layer = ?layer,
        requested = size,
        shortfall,
        db = ?id.db,
        tenant = ?id.tenant,
        engine = ?id.engine,
        "memory release exceeds allocation (WAL replay or accounting drift)"
    );
    shortfall
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_ignores_zero_shortfall() {
        let counters = OverRelease::new();
        counters.record(Layer::Engine, 0);
        assert_eq!(counters.engine(), 0);
        assert_eq!(counters.total(), 0);
    }

    #[test]
    fn record_counts_events_not_bytes() {
        let counters = OverRelease::new();
        counters.record(Layer::Engine, 10);
        counters.record(Layer::Engine, 9999);
        assert_eq!(
            counters.engine(),
            2,
            "record counts the number of over-release events, not shortfall bytes"
        );
    }

    #[test]
    fn each_layer_tracks_independently() {
        let counters = OverRelease::new();
        counters.record(Layer::Global, 1);
        counters.record(Layer::Database, 1);
        counters.record(Layer::Tenant, 1);
        counters.record(Layer::Engine, 1);

        assert_eq!(counters.global(), 1);
        assert_eq!(counters.database(), 1);
        assert_eq!(counters.tenant(), 1);
        assert_eq!(counters.engine(), 1);
        assert_eq!(counters.total(), 4);
    }
}
