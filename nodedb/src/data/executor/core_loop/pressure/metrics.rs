// SPDX-License-Identifier: BUSL-1.1

//! Cross-core telemetry for the SPSC intake throttle.
//!
//! Every Data Plane core writes its own transitions into one shared
//! instance. All fields are atomic: the Prometheus handler reads them without
//! crossing the plane boundary, and the tick path takes no lock.

use std::sync::atomic::{AtomicU64, Ordering};

use super::level::ThrottleLevel;

/// Per-level throttle counters, aggregated across all cores.
#[derive(Debug, Default)]
pub struct ThrottleMetrics {
    /// Cores currently at each level (gauge). A core moves itself between
    /// levels, so the three values sum to the live core count.
    cores_at_level: [AtomicU64; ThrottleLevel::ALL.len()],

    /// Transitions into each level (counter).
    transitions_into_level: [AtomicU64; ThrottleLevel::ALL.len()],
}

impl ThrottleMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a core entering `to` from `from`.
    ///
    /// `from` is `None` for a core's first level — nothing to decrement.
    pub(super) fn record_transition(&self, from: Option<ThrottleLevel>, to: ThrottleLevel) {
        if let Some(from) = from {
            self.cores_at_level[from.index()].fetch_sub(1, Ordering::Relaxed);
        }
        self.cores_at_level[to.index()].fetch_add(1, Ordering::Relaxed);
        self.transitions_into_level[to.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a core leaving this instance, so an adopted-away core leaves
    /// no phantom behind.
    pub(super) fn record_departure(&self, from: ThrottleLevel) {
        self.cores_at_level[from.index()].fetch_sub(1, Ordering::Relaxed);
    }

    /// Cores currently at `level`.
    pub fn cores_at(&self, level: ThrottleLevel) -> u64 {
        self.cores_at_level[level.index()].load(Ordering::Relaxed)
    }

    /// Transitions into `level` since startup.
    pub fn transitions_into(&self, level: ThrottleLevel) -> u64 {
        self.transitions_into_level[level.index()].load(Ordering::Relaxed)
    }

    /// Emit Prometheus text for the throttle gauges and counters.
    pub fn write_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;

        let _ = out.write_str(
            "# HELP nodedb_spsc_throttle_cores Data Plane cores currently at each SPSC intake throttle level\n\
             # TYPE nodedb_spsc_throttle_cores gauge\n",
        );
        for level in ThrottleLevel::ALL {
            let _ = writeln!(
                out,
                r#"nodedb_spsc_throttle_cores{{level="{}"}} {}"#,
                level.label(),
                self.cores_at(level)
            );
        }

        let _ = out.write_str(
            "# HELP nodedb_spsc_throttle_transitions_total Transitions into each SPSC intake throttle level\n\
             # TYPE nodedb_spsc_throttle_transitions_total counter\n",
        );
        for level in ThrottleLevel::ALL {
            let _ = writeln!(
                out,
                r#"nodedb_spsc_throttle_transitions_total{{level="{}"}} {}"#,
                level.label(),
                self.transitions_into(level)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_moves_the_gauge_and_bumps_the_counter() {
        let m = ThrottleMetrics::new();
        m.record_transition(None, ThrottleLevel::Full);
        m.record_transition(Some(ThrottleLevel::Full), ThrottleLevel::Throttled);

        assert_eq!(m.cores_at(ThrottleLevel::Full), 0);
        assert_eq!(m.cores_at(ThrottleLevel::Throttled), 1);
        assert_eq!(m.transitions_into(ThrottleLevel::Throttled), 1);
    }

    #[test]
    fn gauge_sums_to_the_core_count() {
        let m = ThrottleMetrics::new();
        for _ in 0..4 {
            m.record_transition(None, ThrottleLevel::Full);
        }
        m.record_transition(Some(ThrottleLevel::Full), ThrottleLevel::Suspended);

        let total: u64 = ThrottleLevel::ALL.iter().map(|l| m.cores_at(*l)).sum();
        assert_eq!(total, 4, "every core is counted at exactly one level");
    }

    #[test]
    fn departure_removes_the_core_from_the_gauge() {
        let m = ThrottleMetrics::new();
        m.record_transition(None, ThrottleLevel::Throttled);
        m.record_departure(ThrottleLevel::Throttled);

        assert_eq!(m.cores_at(ThrottleLevel::Throttled), 0);
    }

    #[test]
    fn prometheus_renders_every_level() {
        let m = ThrottleMetrics::new();
        m.record_transition(None, ThrottleLevel::Suspended);
        let mut out = String::new();
        m.write_prometheus(&mut out);

        assert!(out.contains(r#"nodedb_spsc_throttle_cores{level="suspended"} 1"#));
        assert!(out.contains(r#"nodedb_spsc_throttle_cores{level="full"} 0"#));
        assert!(out.contains(r#"nodedb_spsc_throttle_transitions_total{level="suspended"} 1"#));
    }
}
