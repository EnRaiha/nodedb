// SPDX-License-Identifier: Apache-2.0

//! Memory pressure levels for gradual backpressure.
//!
//! Instead of binary accept/reject, the governor reports pressure levels
//! so callers can take graduated action:
//!
//! - **Normal**: full speed, no restrictions.
//! - **Warning**: start spilling large allocations to disk, reduce batch sizes.
//! - **Critical**: reject new allocations, force spill of in-progress work.
//!
//! Pressure is computed per-engine and globally.

/// Memory pressure level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    /// Below 70% utilization. No restrictions.
    Normal,
    /// 70-85% utilization. Caller should prefer smaller allocations,
    /// start spilling large temporary buffers to disk.
    Warning,
    /// 85-95% utilization. Caller must spill or backpressure.
    /// New large allocations should be rejected.
    Critical,
    /// Over 95% utilization. Emergency: only essential allocations permitted.
    /// All non-essential work must stop.
    Emergency,
}

/// Thresholds for pressure level transitions (percentage of budget).
#[derive(Debug, Clone, Copy)]
pub struct PressureThresholds {
    /// Enter Warning at this utilization (default: 70%).
    pub warning: u8,
    /// Enter Critical at this utilization (default: 85%).
    pub critical: u8,
    /// Enter Emergency at this utilization (default: 95%).
    pub emergency: u8,
}

impl Default for PressureThresholds {
    fn default() -> Self {
        Self {
            warning: 70,
            critical: 85,
            emergency: 95,
        }
    }
}

impl PressureThresholds {
    /// Compute the pressure level for a given utilization percentage.
    pub fn level_for(&self, utilization_percent: u8) -> PressureLevel {
        if utilization_percent >= self.emergency {
            PressureLevel::Emergency
        } else if utilization_percent >= self.critical {
            PressureLevel::Critical
        } else if utilization_percent >= self.warning {
            PressureLevel::Warning
        } else {
            PressureLevel::Normal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_thresholds() {
        let t = PressureThresholds::default();
        assert_eq!(t.level_for(0), PressureLevel::Normal);
        assert_eq!(t.level_for(69), PressureLevel::Normal);
        assert_eq!(t.level_for(70), PressureLevel::Warning);
        assert_eq!(t.level_for(84), PressureLevel::Warning);
        assert_eq!(t.level_for(85), PressureLevel::Critical);
        assert_eq!(t.level_for(94), PressureLevel::Critical);
        assert_eq!(t.level_for(95), PressureLevel::Emergency);
        assert_eq!(t.level_for(100), PressureLevel::Emergency);
    }

    #[test]
    fn pressure_ordering() {
        assert!(PressureLevel::Normal < PressureLevel::Warning);
        assert!(PressureLevel::Warning < PressureLevel::Critical);
        assert!(PressureLevel::Critical < PressureLevel::Emergency);
    }
}
