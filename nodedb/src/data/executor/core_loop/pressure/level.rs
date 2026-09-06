// SPDX-License-Identifier: BUSL-1.1

//! The effective intake throttle of one Data Plane core.
//!
//! Engine memory pressure and response-ring utilization both map onto
//! [`ThrottleLevel`]. A core runs at the more restrictive of the two, and
//! every throttle knob is a pure function of that level.

use nodedb_bridge::backpressure::PressureState;
use nodedb_mem::PressureLevel;

/// SPSC drain batch size at [`ThrottleLevel::Full`].
pub const SPSC_READ_DEPTH_NORMAL: usize = 64;

/// SPSC drain batch size at [`ThrottleLevel::Throttled`].
pub const SPSC_READ_DEPTH_THROTTLED: usize = SPSC_READ_DEPTH_NORMAL / 2;

/// Reported floor at [`ThrottleLevel::Suspended`]. Intake stops there, so no
/// drain uses it as a batch size.
pub const SPSC_READ_DEPTH_SUSPENDED: usize = 1;

/// How restrictive a core's request intake currently is.
///
/// `Ord` is the combination rule: the core runs at the maximum over its
/// inputs, so no input relaxes a throttle another input still demands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ThrottleLevel {
    /// Full-speed intake — drain the baseline batch every tick.
    Full,
    /// Reduced intake, so in-flight work and the response ring catch up.
    Throttled,
    /// No new intake until the level drops. In-flight tasks still answer.
    Suspended,
}

impl ThrottleLevel {
    /// Every level, least to most restrictive, ordered by [`Self::index`].
    pub const ALL: [Self; 3] = [Self::Full, Self::Throttled, Self::Suspended];

    /// SPSC drain batch size at this level.
    pub const fn read_depth(self) -> usize {
        match self {
            Self::Full => SPSC_READ_DEPTH_NORMAL,
            Self::Throttled => SPSC_READ_DEPTH_THROTTLED,
            Self::Suspended => SPSC_READ_DEPTH_SUSPENDED,
        }
    }

    /// Whether the core stops draining new requests at this level.
    pub const fn suspends_reads(self) -> bool {
        matches!(self, Self::Suspended)
    }

    /// Stable metric label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Throttled => "throttled",
            Self::Suspended => "suspended",
        }
    }

    /// Position in [`Self::ALL`], for indexing per-level metric arrays.
    pub const fn index(self) -> usize {
        match self {
            Self::Full => 0,
            Self::Throttled => 1,
            Self::Suspended => 2,
        }
    }
}

/// `Warning` is informational: the core keeps full intake until the budget
/// is at risk.
impl From<PressureLevel> for ThrottleLevel {
    fn from(level: PressureLevel) -> Self {
        match level {
            PressureLevel::Normal | PressureLevel::Warning => Self::Full,
            PressureLevel::Critical => Self::Throttled,
            PressureLevel::Emergency => Self::Suspended,
        }
    }
}

/// Every request taken in owes a response to this ring, so a ring the
/// Control Plane drains too slowly is a reason to take in less.
impl From<PressureState> for ThrottleLevel {
    fn from(state: PressureState) -> Self {
        match state {
            PressureState::Normal => Self::Full,
            PressureState::Throttled => Self::Throttled,
            PressureState::Suspended => Self::Suspended,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_order_least_to_most_restrictive() {
        assert!(ThrottleLevel::Full < ThrottleLevel::Throttled);
        assert!(ThrottleLevel::Throttled < ThrottleLevel::Suspended);
    }

    #[test]
    fn combination_takes_the_most_restrictive_input() {
        let memory = ThrottleLevel::from(PressureLevel::Normal);
        let queue = ThrottleLevel::from(PressureState::Suspended);
        assert_eq!(
            memory.max(queue),
            ThrottleLevel::Suspended,
            "a calm memory budget must not relax a saturated response ring"
        );
    }

    #[test]
    fn depth_is_one_fixed_step_between_levels() {
        assert_eq!(ThrottleLevel::Full.read_depth(), SPSC_READ_DEPTH_NORMAL);
        assert_eq!(
            ThrottleLevel::Throttled.read_depth(),
            SPSC_READ_DEPTH_NORMAL / 2
        );
        assert_eq!(ThrottleLevel::Suspended.read_depth(), 1);
    }

    #[test]
    fn only_suspended_stops_intake() {
        assert!(!ThrottleLevel::Full.suspends_reads());
        assert!(!ThrottleLevel::Throttled.suspends_reads());
        assert!(ThrottleLevel::Suspended.suspends_reads());
    }

    #[test]
    fn index_matches_position_in_all() {
        for (i, level) in ThrottleLevel::ALL.iter().enumerate() {
            assert_eq!(level.index(), i);
        }
    }

    #[test]
    fn warning_memory_pressure_keeps_full_intake() {
        assert_eq!(
            ThrottleLevel::from(PressureLevel::Warning),
            ThrottleLevel::Full
        );
    }
}
