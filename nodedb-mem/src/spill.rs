// SPDX-License-Identifier: Apache-2.0

//! Cooperative spill controller for per-core arena overflow.
//!
//! When a Data Plane core's arena utilization exceeds 90%, the spill
//! controller triggers eviction of coldest unpinned allocations to a
//! shared mmap overflow region. This prevents asymmetric OOM while
//! preserving the zero-lock TPC property.

/// Action the spill controller recommends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillAction {
    /// No action needed — utilization is healthy.
    None,
    /// Begin spilling coldest unpinned allocations to overflow.
    BeginSpill,
    /// Continue spilling (utilization still above threshold).
    ContinueSpill,
    /// Utilization dropped below restore threshold — can restore spilled data.
    BeginRestore,
}

/// Spill controller configuration.
#[derive(Debug, Clone, Copy)]
pub struct SpillConfig {
    /// Utilization threshold to begin spilling (default: 90%).
    pub spill_threshold: u8,
    /// Utilization threshold below which restoration can begin (default: 75%).
    pub restore_threshold: u8,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            spill_threshold: 90,
            restore_threshold: 75,
        }
    }
}

/// Per-core spill controller.
///
/// Call `check(utilization_percent)` periodically from the core's housekeeping
/// loop. The controller tracks state transitions and returns the recommended action.
///
/// The controller implements a simple state machine:
/// - Normal (utilization < spill_threshold) → None
/// - Spilling (spill_threshold ≤ utilization) → BeginSpill, then ContinueSpill
/// - Restoring (utilization < restore_threshold while spilling) → BeginRestore, then None
///
/// Not `Send` or `Sync` — it's single-core owned.
pub struct SpillController {
    config: SpillConfig,
    /// Whether spill mode is currently active.
    spilling: bool,
    /// Counter: number of spill cycles initiated.
    spill_count: u64,
    /// Counter: number of entries spilled in current cycle.
    entries_spilled: u64,
}

impl SpillController {
    /// Create a new spill controller with the given configuration.
    pub fn new(config: SpillConfig) -> Self {
        Self {
            config,
            spilling: false,
            spill_count: 0,
            entries_spilled: 0,
        }
    }

    /// Check utilization and return the recommended action.
    ///
    /// Call this periodically from the core's housekeeping loop.
    /// `utilization` should be a percentage 0-100.
    pub fn check(&mut self, utilization: u8) -> SpillAction {
        match (self.spilling, utilization >= self.config.spill_threshold) {
            // Not spilling, utilization below threshold.
            (false, false) => SpillAction::None,
            // Not spilling, crossed into danger zone.
            (false, true) => {
                self.spilling = true;
                self.spill_count += 1;
                self.entries_spilled = 0;
                SpillAction::BeginSpill
            }
            // Spilling, still above threshold.
            (true, true) => SpillAction::ContinueSpill,
            // Spilling, dropped to restore threshold or below.
            (true, false) if utilization <= self.config.restore_threshold => {
                self.spilling = false;
                SpillAction::BeginRestore
            }
            // Spilling, but still above restore threshold (wait).
            (true, false) => SpillAction::None,
        }
    }

    /// Whether spill mode is currently active.
    pub fn is_spilling(&self) -> bool {
        self.spilling
    }

    /// Total number of spill cycles initiated.
    pub fn spill_count(&self) -> u64 {
        self.spill_count
    }

    /// Number of entries spilled in the current cycle.
    pub fn entries_spilled(&self) -> u64 {
        self.entries_spilled
    }

    /// Record that `count` entries were spilled in this cycle.
    pub fn record_spill(&mut self, count: u64) {
        self.entries_spilled += count;
    }

    /// Reset the current cycle's spill counters (but keep total spill_count).
    pub fn reset_cycle(&mut self) {
        self.entries_spilled = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = SpillConfig::default();
        assert_eq!(config.spill_threshold, 90);
        assert_eq!(config.restore_threshold, 75);
    }

    #[test]
    fn state_none_below_threshold() {
        let mut controller = SpillController::new(SpillConfig::default());
        assert_eq!(controller.check(50), SpillAction::None);
        assert!(!controller.is_spilling());
    }

    #[test]
    fn state_begin_spill_at_threshold() {
        let mut controller = SpillController::new(SpillConfig::default());
        assert_eq!(controller.check(90), SpillAction::BeginSpill);
        assert!(controller.is_spilling());
        assert_eq!(controller.spill_count(), 1);
        assert_eq!(controller.entries_spilled(), 0);
    }

    #[test]
    fn state_continue_spill_above_threshold() {
        let mut controller = SpillController::new(SpillConfig::default());
        controller.check(90);
        assert_eq!(controller.check(92), SpillAction::ContinueSpill);
        assert!(controller.is_spilling());
    }

    #[test]
    fn state_begin_restore_below_threshold() {
        let mut controller = SpillController::new(SpillConfig::default());
        controller.check(90);
        assert_eq!(controller.check(74), SpillAction::BeginRestore);
        assert!(!controller.is_spilling());
    }

    #[test]
    fn state_wait_between_thresholds() {
        let mut controller = SpillController::new(SpillConfig::default());
        controller.check(90);
        // Utilization drops to 80% — above restore threshold (75%), should wait.
        assert_eq!(controller.check(80), SpillAction::None);
        assert!(controller.is_spilling());
    }

    #[test]
    fn record_spill() {
        let mut controller = SpillController::new(SpillConfig::default());
        controller.check(90);
        controller.record_spill(10);
        assert_eq!(controller.entries_spilled(), 10);
        controller.record_spill(5);
        assert_eq!(controller.entries_spilled(), 15);
    }

    #[test]
    fn reset_cycle() {
        let mut controller = SpillController::new(SpillConfig::default());
        controller.check(90);
        controller.record_spill(20);
        assert_eq!(controller.spill_count(), 1);
        assert_eq!(controller.entries_spilled(), 20);
        controller.reset_cycle();
        assert_eq!(controller.spill_count(), 1); // Total count unchanged
        assert_eq!(controller.entries_spilled(), 0); // Cycle reset
    }

    #[test]
    fn multiple_cycles() {
        let mut controller = SpillController::new(SpillConfig::default());

        // First cycle.
        controller.check(90);
        controller.record_spill(10);
        assert_eq!(controller.spill_count(), 1);

        // Drop below restore threshold.
        controller.check(74);
        assert!(!controller.is_spilling());

        // Rise again.
        controller.check(91);
        assert_eq!(controller.spill_count(), 2);
        assert_eq!(controller.entries_spilled(), 0); // Reset from previous restore
    }

    #[test]
    fn threshold_boundary() {
        let config = SpillConfig {
            spill_threshold: 80,
            restore_threshold: 60,
        };
        let mut controller = SpillController::new(config);

        // Exactly at spill threshold.
        assert_eq!(controller.check(80), SpillAction::BeginSpill);
        assert!(controller.is_spilling());

        // Just below spill threshold, above restore threshold.
        assert_eq!(controller.check(79), SpillAction::None);
        assert!(controller.is_spilling());

        // Exactly at restore threshold.
        assert_eq!(controller.check(60), SpillAction::BeginRestore);
        assert!(!controller.is_spilling());

        // Below restore threshold.
        assert_eq!(controller.check(59), SpillAction::None);
        assert!(!controller.is_spilling());
    }
}
