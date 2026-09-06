// SPDX-License-Identifier: BUSL-1.1

//! Per-tick entry point for the SPSC intake throttle.

use tracing::{info, warn};

use super::level::ThrottleLevel;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Fold this tick's pressure inputs into the core's intake throttle.
    ///
    /// Two inputs, combined by taking the more restrictive:
    ///
    /// - **Engine memory pressure** — worst budget across this core's engines.
    /// - **Response-ring utilization** — every request taken in owes a
    ///   response to that ring.
    ///
    /// The inbound request ring is not an input: a full request ring calls
    /// for a faster drain. Called once per tick, before `drain_requests`.
    pub fn apply_spsc_pressure(&mut self) {
        let memory = self.governor.worst_engine_pressure();
        let response_utilization = self.response_tx.utilization();

        let Some(level) = self.throttle.observe(memory, response_utilization) else {
            return;
        };

        let core = self.core_id;
        let read_depth = level.read_depth();
        match level {
            ThrottleLevel::Full => info!(
                core,
                read_depth, "SPSC intake throttle released — full read depth restored"
            ),
            ThrottleLevel::Throttled | ThrottleLevel::Suspended => warn!(
                core,
                level = level.label(),
                read_depth,
                memory = ?memory,
                response_utilization,
                "SPSC intake throttled"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_mem::EngineId;

    use super::super::fixtures::{make_core, make_governor_at, push_request};
    use crate::data::executor::core_loop::pressure::SPSC_READ_DEPTH_THROTTLED;

    /// Engine utilization that lands in the governor's Critical band.
    const CRITICAL_PERCENT: u8 = 88;
    /// Engine utilization that lands in the governor's Emergency band.
    const EMERGENCY_PERCENT: u8 = 97;

    #[test]
    fn throttled_level_limits_the_drain_batch() {
        let (mut core, mut tx, _rx, _dir) = make_core();
        for id in 0..40 {
            push_request(&mut tx, id);
        }
        core.governor = make_governor_at(EngineId::Vector, CRITICAL_PERCENT);

        core.apply_spsc_pressure();
        core.drain_requests();

        assert_eq!(
            core.pending_count(),
            SPSC_READ_DEPTH_THROTTLED,
            "a throttled core drains exactly the throttled depth"
        );
    }

    #[test]
    fn suspended_level_drains_nothing() {
        let (mut core, mut tx, _rx, _dir) = make_core();
        push_request(&mut tx, 1);
        core.governor = make_governor_at(EngineId::Vector, EMERGENCY_PERCENT);

        core.apply_spsc_pressure();
        core.drain_requests();

        assert_eq!(
            core.pending_count(),
            0,
            "a suspended core takes in no new requests"
        );
    }

    #[test]
    fn a_calm_core_drains_the_full_batch() {
        let (mut core, mut tx, _rx, _dir) = make_core();
        for id in 0..40 {
            push_request(&mut tx, id);
        }

        core.apply_spsc_pressure();
        core.drain_requests();

        assert_eq!(core.pending_count(), 40);
    }
}
