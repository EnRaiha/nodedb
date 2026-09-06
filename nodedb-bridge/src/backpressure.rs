// SPDX-License-Identifier: BUSL-1.1

//! Adaptive backpressure controller.
//!
//! Tracks one SPSC ring's utilization and drives state transitions. The
//! ring's writer owns the controller and decides what each state means for it.
//!
//! ```text
//! ┌──────────┐   >85%   ┌──────────────┐  >95%  ┌───────────┐
//! │  Normal  │ ───────→ │  Throttled   │ ──────→│ Suspended │
//! └──────────┘          └──────────────┘        └───────────┘
//!      ↑    <75%              ↑    <85%               │
//!      └────────────────────  └───────────────────────┘
//! ```
//!
//! - **Normal**: the writer proceeds at full speed.
//! - **Throttled**: the writer slows down, so the reader can catch up.
//! - **Suspended**: the writer stops adding load until the ring drains.
//!
//! Hysteresis (10% gap) prevents oscillation at threshold boundaries.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Backpressure thresholds (percentage of queue capacity).
#[derive(Debug, Clone, Copy)]
pub struct BackpressureConfig {
    /// Enter Throttled state when utilization exceeds this (default: 85%).
    pub throttle_enter: u8,
    /// Exit Throttled back to Normal when utilization drops below this (default: 75%).
    pub throttle_exit: u8,
    /// Enter Suspended state when utilization exceeds this (default: 95%).
    pub suspend_enter: u8,
    /// Exit Suspended back to Throttled when utilization drops below this (default: 85%).
    pub suspend_exit: u8,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            throttle_enter: 85,
            throttle_exit: 75,
            suspend_enter: 95,
            suspend_exit: 85,
        }
    }
}

/// Current backpressure state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PressureState {
    /// Full speed — all I/O permitted.
    Normal = 0,
    /// Reduced rate — the writer limits how much new load it adds.
    Throttled = 1,
    /// No new load — the writer waits for the ring to drain.
    Suspended = 2,
}

impl PressureState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Normal,
            1 => Self::Throttled,
            _ => Self::Suspended,
        }
    }
}

/// Tracks backpressure state and transition counts.
///
/// Called by the ring's writer to update state. The writer reads the current
/// state to decide how much new load to add.
pub struct BackpressureController {
    config: BackpressureConfig,

    /// Current state (atomic for cross-thread reads).
    state: AtomicU8,

    /// Number of transitions into Throttled.
    throttle_count: AtomicU64,

    /// Number of transitions into Suspended.
    suspend_count: AtomicU64,
}

impl BackpressureController {
    /// Create a new controller with the given config.
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            state: AtomicU8::new(PressureState::Normal as u8),
            throttle_count: AtomicU64::new(0),
            suspend_count: AtomicU64::new(0),
        }
    }

    /// Update the backpressure state based on current queue utilization.
    ///
    /// Returns the new state if a transition occurred, or `None` if unchanged.
    /// The caller SHOULD emit a tracing event on transitions.
    pub fn update(&self, utilization_percent: u8) -> Option<PressureState> {
        let current = PressureState::from_u8(self.state.load(Ordering::Relaxed));

        let new_state = match current {
            PressureState::Normal => {
                if utilization_percent >= self.config.suspend_enter {
                    PressureState::Suspended
                } else if utilization_percent >= self.config.throttle_enter {
                    PressureState::Throttled
                } else {
                    return None;
                }
            }
            PressureState::Throttled => {
                if utilization_percent >= self.config.suspend_enter {
                    PressureState::Suspended
                } else if utilization_percent < self.config.throttle_exit {
                    PressureState::Normal
                } else {
                    return None;
                }
            }
            PressureState::Suspended => {
                if utilization_percent < self.config.suspend_exit {
                    if utilization_percent < self.config.throttle_exit {
                        PressureState::Normal
                    } else {
                        PressureState::Throttled
                    }
                } else {
                    return None;
                }
            }
        };

        self.state.store(new_state as u8, Ordering::Release);

        match new_state {
            PressureState::Throttled => {
                self.throttle_count.fetch_add(1, Ordering::Relaxed);
            }
            PressureState::Suspended => {
                self.suspend_count.fetch_add(1, Ordering::Relaxed);
            }
            PressureState::Normal => {}
        }

        Some(new_state)
    }

    /// Current backpressure state.
    pub fn state(&self) -> PressureState {
        PressureState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Number of times the controller transitioned into Throttled.
    pub fn throttle_transitions(&self) -> u64 {
        self.throttle_count.load(Ordering::Relaxed)
    }

    /// Number of times the controller transitioned into Suspended.
    pub fn suspend_transitions(&self) -> u64 {
        self.suspend_count.load(Ordering::Relaxed)
    }
}

impl Default for BackpressureController {
    fn default() -> Self {
        Self::new(BackpressureConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_normal() {
        let bp = BackpressureController::default();
        assert_eq!(bp.state(), PressureState::Normal);
    }

    #[test]
    fn normal_to_throttled_at_85() {
        let bp = BackpressureController::default();
        assert!(bp.update(84).is_none());
        assert_eq!(bp.update(85), Some(PressureState::Throttled));
        assert_eq!(bp.state(), PressureState::Throttled);
        assert_eq!(bp.throttle_transitions(), 1);
    }

    #[test]
    fn throttled_to_suspended_at_95() {
        let bp = BackpressureController::default();
        bp.update(85); // → Throttled
        assert_eq!(bp.update(95), Some(PressureState::Suspended));
        assert_eq!(bp.state(), PressureState::Suspended);
        assert_eq!(bp.suspend_transitions(), 1);
    }

    #[test]
    fn hysteresis_prevents_oscillation() {
        let bp = BackpressureController::default();

        // Enter throttled.
        bp.update(86);
        assert_eq!(bp.state(), PressureState::Throttled);

        // Drop to 80% — still above exit threshold (75%), stays throttled.
        assert!(bp.update(80).is_none());
        assert_eq!(bp.state(), PressureState::Throttled);

        // Drop to 74% — below exit threshold, returns to normal.
        assert_eq!(bp.update(74), Some(PressureState::Normal));
        assert_eq!(bp.state(), PressureState::Normal);
    }

    #[test]
    fn suspended_exits_through_throttled() {
        let bp = BackpressureController::default();

        // Normal → Suspended (jump past 95%).
        bp.update(96);
        assert_eq!(bp.state(), PressureState::Suspended);

        // Drop to 84% — below suspend_exit but above throttle_exit.
        assert_eq!(bp.update(84), Some(PressureState::Throttled));
        assert_eq!(bp.state(), PressureState::Throttled);
    }

    #[test]
    fn suspended_exits_to_normal_if_low_enough() {
        let bp = BackpressureController::default();

        bp.update(96); // → Suspended
        // Drop dramatically below throttle_exit.
        assert_eq!(bp.update(50), Some(PressureState::Normal));
        assert_eq!(bp.state(), PressureState::Normal);
    }

    #[test]
    fn normal_jumps_directly_to_suspended() {
        let bp = BackpressureController::default();
        assert_eq!(bp.update(96), Some(PressureState::Suspended));
        assert_eq!(bp.suspend_transitions(), 1);
        assert_eq!(bp.throttle_transitions(), 0);
    }

    #[test]
    fn no_transition_when_stable() {
        let bp = BackpressureController::default();
        assert!(bp.update(50).is_none());
        assert!(bp.update(60).is_none());
        assert!(bp.update(70).is_none());
    }
}
