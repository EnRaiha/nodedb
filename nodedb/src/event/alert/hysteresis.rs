// SPDX-License-Identifier: BUSL-1.1

//! Per-group hysteresis state machine for alert rules.
//!
//! Prevents flapping: a brief dip below threshold doesn't clear an active alert,
//! and a brief spike above threshold doesn't fire a cleared alert.
//!
//! State machine per (alert_name, group_key):
//! - Condition true  → increment consecutive_fire, reset consecutive_recover
//! - consecutive_fire >= fire_after AND Cleared → FIRE, set Active
//! - Condition false → increment consecutive_recover, reset consecutive_fire
//! - consecutive_recover >= recover_after AND Active → CLEAR, set Cleared

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::AlertStatus;

/// Per-group alert state.
#[derive(Debug, Clone)]
pub struct AlertGroupState {
    /// Current alert status.
    pub status: AlertStatus,
    /// Consecutive evaluation windows where condition was true.
    pub consecutive_fire: u32,
    /// Consecutive evaluation windows where condition was false.
    pub consecutive_recover: u32,
    /// Timestamp (ms) when the alert last transitioned to Active.
    pub fired_at: Option<u64>,
    /// Timestamp (ms) when the alert last transitioned to Cleared.
    pub cleared_at: Option<u64>,
    /// Last evaluated aggregate value.
    pub last_value: Option<f64>,
}

impl AlertGroupState {
    fn new() -> Self {
        Self {
            status: AlertStatus::Cleared,
            consecutive_fire: 0,
            consecutive_recover: 0,
            fired_at: None,
            cleared_at: None,
            last_value: None,
        }
    }
}

/// Result of evaluating one group's condition against hysteresis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HysteresisTransition {
    /// No state change.
    NoChange,
    /// Transitioned from Cleared → Active (fire notification).
    Fired,
    /// Transitioned from Active → Cleared (recovery notification).
    Recovered,
}

/// Parameters for [`HysteresisManager::evaluate`].
#[derive(Debug, Clone, Copy)]
pub struct EvaluateParams<'a> {
    /// Tenant scope for the alert's state key.
    pub tenant_id: u64,
    /// Alert rule name.
    pub alert_name: &'a str,
    /// Group key (e.g. a device/series identifier) within the alert.
    pub group_key: &'a str,
    /// Whether the alert condition evaluated true this window.
    pub condition_met: bool,
    /// The aggregate value that was evaluated.
    pub value: f64,
    /// Consecutive true windows required before firing.
    pub fire_after: u32,
    /// Consecutive false windows required before recovering.
    pub recover_after: u32,
    /// Timestamp (ms) of this evaluation.
    pub now_ms: u64,
}

/// Manages per-(alert, group) hysteresis state.
///
/// Thread-safe for access from the eval loop. State is in-memory only
/// (crash recovery: re-evaluate from data on startup, alerts re-converge
/// within fire_after windows).
pub struct HysteresisManager {
    /// Key: (tenant_id, alert_name, group_key) → state.
    states: RwLock<HashMap<(u64, String, String), AlertGroupState>>,
}

impl HysteresisManager {
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Evaluate a condition result for a group and return any state transition.
    pub fn evaluate(&self, params: EvaluateParams<'_>) -> HysteresisTransition {
        let EvaluateParams {
            tenant_id,
            alert_name,
            group_key,
            condition_met,
            value,
            fire_after,
            recover_after,
            now_ms,
        } = params;

        let key = (tenant_id, alert_name.to_string(), group_key.to_string());
        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        let state = states.entry(key).or_insert_with(AlertGroupState::new);
        state.last_value = Some(value);

        if condition_met {
            state.consecutive_fire += 1;
            state.consecutive_recover = 0;

            if state.consecutive_fire >= fire_after && state.status == AlertStatus::Cleared {
                state.status = AlertStatus::Active;
                state.fired_at = Some(now_ms);
                return HysteresisTransition::Fired;
            }
        } else {
            state.consecutive_recover += 1;
            state.consecutive_fire = 0;

            if state.consecutive_recover >= recover_after && state.status == AlertStatus::Active {
                state.status = AlertStatus::Cleared;
                state.cleared_at = Some(now_ms);
                return HysteresisTransition::Recovered;
            }
        }

        HysteresisTransition::NoChange
    }

    /// Get the current state for a specific group.
    pub fn get_state(
        &self,
        tenant_id: u64,
        alert_name: &str,
        group_key: &str,
    ) -> Option<AlertGroupState> {
        let key = (tenant_id, alert_name.to_string(), group_key.to_string());
        self.states
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .cloned()
    }

    /// List all group states for an alert (for SHOW ALERT STATUS).
    pub fn list_states(&self, tenant_id: u64, alert_name: &str) -> Vec<(String, AlertGroupState)> {
        let prefix = (tenant_id, alert_name.to_string());
        self.states
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|((t, a, _), _)| *t == prefix.0 && a == &prefix.1)
            .map(|((_, _, g), s)| (g.clone(), s.clone()))
            .collect()
    }

    /// Remove all state for an alert (on DROP ALERT).
    pub fn remove_alert(&self, tenant_id: u64, alert_name: &str) {
        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        states.retain(|(t, a, _), _| !(*t == tenant_id && a == alert_name));
    }
}

impl Default for HysteresisManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_after_consecutive_windows() {
        let mgr = HysteresisManager::new();

        // fire_after = 3, need 3 consecutive true evaluations.
        let r1 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "alert1",
            group_key: "g1",
            condition_met: true,
            value: 91.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 1000,
        });
        assert_eq!(r1, HysteresisTransition::NoChange);

        let r2 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "alert1",
            group_key: "g1",
            condition_met: true,
            value: 92.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 2000,
        });
        assert_eq!(r2, HysteresisTransition::NoChange);

        let r3 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "alert1",
            group_key: "g1",
            condition_met: true,
            value: 93.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 3000,
        });
        assert_eq!(r3, HysteresisTransition::Fired);

        // Already Active, consecutive true should not re-fire.
        let r4 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "alert1",
            group_key: "g1",
            condition_met: true,
            value: 94.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 4000,
        });
        assert_eq!(r4, HysteresisTransition::NoChange);
    }

    #[test]
    fn recovers_after_consecutive_false() {
        let mgr = HysteresisManager::new();

        // Fire first.
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: true,
            value: 91.0,
            fire_after: 1,
            recover_after: 2,
            now_ms: 1000,
        });
        let _fired = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: true,
            value: 92.0,
            fire_after: 1,
            recover_after: 2,
            now_ms: 2000,
        });
        // fire_after=1, so first true fires.
        assert_eq!(
            mgr.evaluate(EvaluateParams {
                tenant_id: 1,
                alert_name: "a",
                group_key: "g",
                condition_met: true,
                value: 93.0,
                fire_after: 1,
                recover_after: 2,
                now_ms: 1000,
            }),
            HysteresisTransition::NoChange
        );

        // Now recover: need 2 consecutive false.
        let r1 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: false,
            value: 89.0,
            fire_after: 1,
            recover_after: 2,
            now_ms: 3000,
        });
        assert_eq!(r1, HysteresisTransition::NoChange);

        let r2 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: false,
            value: 88.0,
            fire_after: 1,
            recover_after: 2,
            now_ms: 4000,
        });
        assert_eq!(r2, HysteresisTransition::Recovered);
    }

    #[test]
    fn interrupted_fire_resets() {
        let mgr = HysteresisManager::new();

        // fire_after=3: 2 true, then 1 false, then 2 true → should not fire.
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: true,
            value: 91.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 1000,
        });
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: true,
            value: 92.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 2000,
        });
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: false,
            value: 89.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 3000,
        }); // resets consecutive_fire
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: true,
            value: 91.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 4000,
        });
        let r = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g",
            condition_met: true,
            value: 92.0,
            fire_after: 3,
            recover_after: 2,
            now_ms: 5000,
        });
        assert_eq!(r, HysteresisTransition::NoChange); // Only 2 consecutive, not 3.
    }

    #[test]
    fn independent_groups() {
        let mgr = HysteresisManager::new();

        let r1 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "device-1",
            condition_met: true,
            value: 91.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });
        assert_eq!(r1, HysteresisTransition::Fired);

        // Different group should be independent.
        let r2 = mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "device-2",
            condition_met: false,
            value: 80.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });
        assert_eq!(r2, HysteresisTransition::NoChange); // Cleared, false → no change.

        // device-1 still Active.
        let state = mgr.get_state(1, "a", "device-1").unwrap();
        assert_eq!(state.status, AlertStatus::Active);
    }

    #[test]
    fn list_states_for_alert() {
        let mgr = HysteresisManager::new();
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g1",
            condition_met: true,
            value: 91.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g2",
            condition_met: true,
            value: 92.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "b",
            group_key: "g3",
            condition_met: true,
            value: 93.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });

        let states = mgr.list_states(1, "a");
        assert_eq!(states.len(), 2);
    }

    #[test]
    fn remove_alert_clears_all_groups() {
        let mgr = HysteresisManager::new();
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g1",
            condition_met: true,
            value: 91.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });
        mgr.evaluate(EvaluateParams {
            tenant_id: 1,
            alert_name: "a",
            group_key: "g2",
            condition_met: true,
            value: 92.0,
            fire_after: 1,
            recover_after: 1,
            now_ms: 1000,
        });
        mgr.remove_alert(1, "a");
        assert!(mgr.list_states(1, "a").is_empty());
    }
}
