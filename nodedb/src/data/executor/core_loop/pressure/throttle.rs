// SPDX-License-Identifier: BUSL-1.1

//! The per-core SPSC intake throttle: pressure inputs in, one level out.

use std::sync::Arc;

use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController};
use nodedb_mem::PressureLevel;

use super::level::ThrottleLevel;
use super::metrics::ThrottleMetrics;

/// Consecutive calm ticks before a throttled core returns to full intake.
///
/// Damping is asymmetric. Entering a throttle applies at once, as does any
/// release that leaves one in place. Only dropping the last of it waits.
const THROTTLE_RELEASE_TICKS: u32 = 8;

/// Current intake throttle of one Data Plane core.
///
/// The level is a function of the pressure observed on this tick, so
/// sustained pressure settles at one level and holds there.
pub(crate) struct SpscThrottle {
    /// Level in force right now.
    level: ThrottleLevel,

    /// Consecutive observations of [`ThrottleLevel::Full`] while `level` is
    /// something stricter. Reset by any observation that is not `Full`.
    calm_ticks: u32,

    /// Utilization state machine for the outbound response ring. Owns the
    /// enter/exit threshold hysteresis.
    response_ring: BackpressureController,

    /// Shared with every other core and with the Prometheus handler.
    metrics: Arc<ThrottleMetrics>,
}

impl SpscThrottle {
    /// A core at full intake, on a private metrics instance until the
    /// bootstrap hands over the shared one.
    pub(crate) fn new() -> Self {
        let metrics = Arc::new(ThrottleMetrics::new());
        metrics.record_transition(None, ThrottleLevel::Full);
        Self {
            level: ThrottleLevel::Full,
            calm_ticks: 0,
            response_ring: BackpressureController::new(BackpressureConfig::default()),
            metrics,
        }
    }

    /// Adopt the process-wide metrics, carrying this core's level across so
    /// the gauge counts every live core once.
    pub(crate) fn adopt_metrics(&mut self, metrics: Arc<ThrottleMetrics>) {
        if Arc::ptr_eq(&self.metrics, &metrics) {
            return;
        }
        self.metrics.record_departure(self.level);
        metrics.record_transition(None, self.level);
        self.metrics = metrics;
    }

    /// SPSC drain batch size for this tick.
    pub(crate) fn read_depth(&self) -> usize {
        self.level.read_depth()
    }

    /// Whether the core takes in no new requests this tick.
    pub(crate) fn suspends_reads(&self) -> bool {
        self.level.suspends_reads()
    }

    /// Level in force right now.
    pub(crate) fn level(&self) -> ThrottleLevel {
        self.level
    }

    /// Fold this tick's pressure inputs into the level.
    ///
    /// `memory` is the worst engine budget pressure on this core,
    /// `response_utilization` the response ring's occupancy percentage.
    /// Returns the new level on a change, so the caller logs transitions only.
    pub(crate) fn observe(
        &mut self,
        memory: PressureLevel,
        response_utilization: u8,
    ) -> Option<ThrottleLevel> {
        self.response_ring.update(response_utilization);
        let observed =
            ThrottleLevel::from(memory).max(ThrottleLevel::from(self.response_ring.state()));

        let target = if observed == ThrottleLevel::Full && self.level != ThrottleLevel::Full {
            self.calm_ticks = self.calm_ticks.saturating_add(1);
            if self.calm_ticks >= THROTTLE_RELEASE_TICKS {
                ThrottleLevel::Full
            } else {
                self.level
            }
        } else {
            self.calm_ticks = 0;
            observed
        };

        if target == self.level {
            return None;
        }
        self.metrics.record_transition(Some(self.level), target);
        self.level = target;
        self.calm_ticks = 0;
        Some(target)
    }
}

/// A core that goes away stops being counted, so the gauge tracks live cores.
impl Drop for SpscThrottle {
    fn drop(&mut self) {
        self.metrics.record_departure(self.level);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_bridge::backpressure::PressureState;

    use super::*;

    /// Response-ring utilization that the controller reads as calm.
    const CALM: u8 = 0;
    /// Response-ring utilization above the throttle-enter threshold.
    const RING_THROTTLED: u8 = 90;
    /// Response-ring utilization above the suspend-enter threshold.
    const RING_SUSPENDED: u8 = 97;

    fn throttle() -> SpscThrottle {
        SpscThrottle::new()
    }

    fn settle(t: &mut SpscThrottle, memory: PressureLevel, utilization: u8, ticks: u32) {
        for _ in 0..ticks {
            t.observe(memory, utilization);
        }
    }

    #[test]
    fn sustained_critical_memory_holds_one_throttle_step() {
        let mut t = throttle();
        for tick in 1..=32 {
            t.observe(PressureLevel::Critical, CALM);
            assert_eq!(
                t.level(),
                ThrottleLevel::Throttled,
                "tick {tick}: a level that persists holds its depth"
            );
        }
    }

    #[test]
    fn escalation_applies_on_the_first_tick() {
        let mut t = throttle();
        assert_eq!(
            t.observe(PressureLevel::Emergency, CALM),
            Some(ThrottleLevel::Suspended)
        );
    }

    #[test]
    fn partial_release_applies_on_the_first_tick() {
        let mut t = throttle();
        t.observe(PressureLevel::Emergency, CALM);
        assert_eq!(
            t.observe(PressureLevel::Critical, CALM),
            Some(ThrottleLevel::Throttled),
            "a release that leaves a throttle in place needs no damping"
        );
    }

    #[test]
    fn full_release_waits_for_sustained_calm() {
        let mut t = throttle();
        t.observe(PressureLevel::Critical, CALM);

        for _ in 0..(THROTTLE_RELEASE_TICKS - 1) {
            assert_eq!(t.observe(PressureLevel::Normal, CALM), None);
            assert_eq!(t.level(), ThrottleLevel::Throttled);
        }
        assert_eq!(
            t.observe(PressureLevel::Normal, CALM),
            Some(ThrottleLevel::Full)
        );
    }

    #[test]
    fn one_pressured_tick_restarts_the_release_window() {
        let mut t = throttle();
        t.observe(PressureLevel::Critical, CALM);
        settle(
            &mut t,
            PressureLevel::Normal,
            CALM,
            THROTTLE_RELEASE_TICKS - 1,
        );
        t.observe(PressureLevel::Critical, CALM);

        settle(
            &mut t,
            PressureLevel::Normal,
            CALM,
            THROTTLE_RELEASE_TICKS - 1,
        );
        assert_eq!(
            t.level(),
            ThrottleLevel::Throttled,
            "the window restarts from zero after pressure returns"
        );
        t.observe(PressureLevel::Normal, CALM);
        assert_eq!(t.level(), ThrottleLevel::Full);
    }

    #[test]
    fn response_ring_saturation_throttles_a_core_with_calm_memory() {
        let mut t = throttle();
        assert_eq!(
            t.observe(PressureLevel::Normal, RING_THROTTLED),
            Some(ThrottleLevel::Throttled)
        );
        assert_eq!(
            t.observe(PressureLevel::Normal, RING_SUSPENDED),
            Some(ThrottleLevel::Suspended)
        );
    }

    #[test]
    fn a_drained_response_ring_does_not_relax_memory_pressure() {
        let mut t = throttle();
        settle(&mut t, PressureLevel::Emergency, CALM, 1);
        settle(
            &mut t,
            PressureLevel::Emergency,
            CALM,
            THROTTLE_RELEASE_TICKS * 2,
        );
        assert_eq!(t.level(), ThrottleLevel::Suspended);
    }

    #[test]
    fn response_ring_release_respects_controller_hysteresis() {
        let mut t = throttle();
        t.observe(PressureLevel::Normal, RING_THROTTLED);
        // Still above the controller's throttle-exit threshold: the ring has
        // not drained enough to count as calm, so the window never opens.
        settle(
            &mut t,
            PressureLevel::Normal,
            80,
            THROTTLE_RELEASE_TICKS * 2,
        );
        assert_eq!(t.level(), ThrottleLevel::Throttled);

        settle(&mut t, PressureLevel::Normal, CALM, THROTTLE_RELEASE_TICKS);
        assert_eq!(t.level(), ThrottleLevel::Full);
    }

    #[test]
    fn adopting_shared_metrics_moves_the_core_between_gauges() {
        let mut t = throttle();
        t.observe(PressureLevel::Critical, CALM);
        let private = Arc::clone(&t.metrics);

        let shared = Arc::new(ThrottleMetrics::new());
        t.adopt_metrics(Arc::clone(&shared));

        assert_eq!(private.cores_at(ThrottleLevel::Throttled), 0);
        assert_eq!(shared.cores_at(ThrottleLevel::Throttled), 1);
    }

    #[test]
    fn a_dropped_core_leaves_the_gauge() {
        let shared = Arc::new(ThrottleMetrics::new());
        {
            let mut t = throttle();
            t.adopt_metrics(Arc::clone(&shared));
            assert_eq!(shared.cores_at(ThrottleLevel::Full), 1);
        }
        assert_eq!(shared.cores_at(ThrottleLevel::Full), 0);
    }

    #[test]
    fn adopting_the_same_metrics_twice_counts_the_core_once() {
        let mut t = throttle();
        let shared = Arc::new(ThrottleMetrics::new());
        t.adopt_metrics(Arc::clone(&shared));
        t.adopt_metrics(Arc::clone(&shared));

        assert_eq!(shared.cores_at(ThrottleLevel::Full), 1);
    }

    #[test]
    fn queue_state_maps_onto_the_same_levels_as_memory() {
        assert_eq!(
            ThrottleLevel::from(PressureState::Throttled),
            ThrottleLevel::from(PressureLevel::Critical)
        );
        assert_eq!(
            ThrottleLevel::from(PressureState::Suspended),
            ThrottleLevel::from(PressureLevel::Emergency)
        );
    }
}
