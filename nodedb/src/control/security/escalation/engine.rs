// SPDX-License-Identifier: BUSL-1.1

//! [`EscalationEngine`] — counts violations per user and reports the moment a
//! threshold turns into an account-status verdict.
//!
//! The engine is deliberately a pure decision component: it owns counters and
//! thresholds, never the account record. Turning a verdict into a durable,
//! replicated status is [`super::violation::record_auth_violation`]'s job.
//!
//! Two pieces of state, with different lifetimes on purpose:
//!
//! * The rolling **violation timestamps** are process-local. They are
//!   sub-threshold evidence inside a bounded window, written on every
//!   attributed auth failure; persisting them would put an
//!   unauthenticated-triggerable disk write on the auth path for no
//!   enforcement gain.
//! * The **suspension count** — the rung of the ban ladder a user has reached
//!   — is not. It is written only when an escalation actually fires, which is
//!   already a durable event, and is restored via [`EscalationEngine::hydrate_suspensions`]
//!   from the persisted auth-user records at startup. Without that, a ban
//!   requiring N suspensions would be unreachable across restarts.

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use tracing::info;

use super::config::EscalationConfig;
use crate::control::security::auth_context::AuthStatus;

/// A fired escalation: the new account status plus the suspension count that
/// produced it, so the caller can persist both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Escalation {
    /// The status the account must be moved to.
    pub status: AuthStatus,
    /// Total suspensions this account has accumulated, including this one.
    pub suspensions: u32,
}

/// Per-user violation tracker.
struct ViolationTracker {
    /// Timestamps of recent violations (for rolling window).
    violations: Vec<u64>,
    /// Total number of times this user has been suspended.
    suspension_count: u32,
}

/// Auto-escalation engine.
pub struct EscalationEngine {
    config: EscalationConfig,
    state: RwLock<TrackerMap>,
}

/// Bounded `user -> tracker` map with FIFO eviction of the oldest tracked
/// user once `max_tracked_users` is reached.
#[derive(Default)]
struct TrackerMap {
    trackers: HashMap<String, ViolationTracker>,
    order: VecDeque<String>,
}

impl TrackerMap {
    /// Insert a fresh tracker for `user_id`, evicting the oldest tracked user
    /// if that would exceed `max_users`. Returns `false` when tracking is
    /// disabled outright (`max_users == 0`), so no map entry is created.
    fn admit(&mut self, user_id: &str, max_users: usize, suspension_count: u32) -> bool {
        if max_users == 0 {
            return false;
        }
        while self.order.len() >= max_users {
            match self.order.pop_front() {
                Some(evicted) => {
                    self.trackers.remove(&evicted);
                }
                None => break,
            }
        }
        self.trackers.insert(
            user_id.to_string(),
            ViolationTracker {
                violations: Vec::new(),
                suspension_count,
            },
        );
        self.order.push_back(user_id.to_string());
        true
    }

    fn remove(&mut self, user_id: &str) {
        self.trackers.remove(user_id);
        self.order.retain(|id| id != user_id);
    }
}

impl EscalationEngine {
    pub fn new(config: EscalationConfig) -> Self {
        Self {
            config,
            state: RwLock::new(TrackerMap::default()),
        }
    }

    /// The configuration this engine was built with.
    pub fn config(&self) -> &EscalationConfig {
        &self.config
    }

    /// Whether the operator enabled auto-escalation.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled && self.config.suspend_after_violations > 0
    }

    /// Restore the durable suspension count for a user, so the ban ladder
    /// survives a restart. Called once per persisted auth-user record while
    /// shared state is being opened, before any request is served.
    pub fn hydrate_suspensions(&self, user_id: &str, suspensions: u32) {
        if suspensions == 0 {
            return;
        }
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        if let Some(tracker) = state.trackers.get_mut(user_id) {
            tracker.suspension_count = tracker.suspension_count.max(suspensions);
            return;
        }
        state.admit(user_id, self.config.max_tracked_users, suspensions);
    }

    /// Record a violation for a user (auth failure, authorization refusal).
    ///
    /// Returns the escalation if one fired, or `None` if no change.
    pub fn record_violation(&self, user_id: &str) -> Option<Escalation> {
        if !self.is_enabled() {
            return None;
        }

        let now = now_secs();
        let window_start = if self.config.violation_window_secs > 0 {
            now.saturating_sub(self.config.violation_window_secs)
        } else {
            0
        };

        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        if !state.trackers.contains_key(user_id)
            && !state.admit(user_id, self.config.max_tracked_users, 0)
        {
            return None;
        }
        let tracker = state.trackers.get_mut(user_id)?;

        // Prune old violations outside the window.
        if window_start > 0 {
            tracker.violations.retain(|&ts| ts >= window_start);
        }

        tracker.violations.push(now);

        // Check suspension threshold.
        if tracker.violations.len() as u32 >= self.config.suspend_after_violations {
            tracker.violations.clear(); // Reset after escalation.
            tracker.suspension_count = tracker.suspension_count.saturating_add(1);
            let suspensions = tracker.suspension_count;

            // Check ban threshold.
            if self.config.ban_after_suspensions > 0
                && suspensions >= self.config.ban_after_suspensions
            {
                info!(user_id = %user_id, suspensions, "auto-ban triggered");
                return Some(Escalation {
                    status: AuthStatus::Banned,
                    suspensions,
                });
            }

            info!(
                user_id = %user_id,
                violations = self.config.suspend_after_violations,
                "auto-suspend triggered"
            );
            return Some(Escalation {
                status: AuthStatus::Suspended,
                suspensions,
            });
        }

        None
    }

    /// Get current violation count for a user.
    pub fn violation_count(&self, user_id: &str) -> u32 {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        state
            .trackers
            .get(user_id)
            .map(|t| t.violations.len() as u32)
            .unwrap_or(0)
    }

    /// Number of users currently tracked. Test/observability accessor.
    pub fn tracked_users(&self) -> usize {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        state.trackers.len()
    }

    /// Reset violations for a user (e.g., after admin review).
    pub fn reset(&self, user_id: &str) {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.remove(user_id);
    }
}

impl Default for EscalationEngine {
    fn default() -> Self {
        Self::new(EscalationConfig::default())
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(suspend: u32, ban: u32, window: u64) -> EscalationConfig {
        EscalationConfig {
            enabled: true,
            suspend_after_violations: suspend,
            ban_after_suspensions: ban,
            violation_window_secs: window,
            ..Default::default()
        }
    }

    #[test]
    fn disabled_by_default() {
        let engine = EscalationEngine::default();
        assert!(!engine.is_enabled());
        for _ in 0..100 {
            assert!(engine.record_violation("u1").is_none());
        }
        assert_eq!(engine.tracked_users(), 0);
    }

    #[test]
    fn no_escalation_below_threshold() {
        let engine = EscalationEngine::new(config(5, 3, 3600));
        for _ in 0..4 {
            assert!(engine.record_violation("u1").is_none());
        }
    }

    #[test]
    fn auto_suspend_at_threshold() {
        let engine = EscalationEngine::new(config(3, 3, 0));

        assert!(engine.record_violation("u1").is_none());
        assert!(engine.record_violation("u1").is_none());
        assert_eq!(
            engine.record_violation("u1"),
            Some(Escalation {
                status: AuthStatus::Suspended,
                suspensions: 1,
            })
        );
    }

    #[test]
    fn auto_ban_after_repeated_suspensions() {
        let engine = EscalationEngine::new(config(2, 2, 0));

        // First suspension.
        engine.record_violation("u1");
        assert_eq!(
            engine.record_violation("u1").map(|e| e.status),
            Some(AuthStatus::Suspended)
        );

        // Second suspension → ban.
        engine.record_violation("u1");
        assert_eq!(
            engine.record_violation("u1"),
            Some(Escalation {
                status: AuthStatus::Banned,
                suspensions: 2,
            })
        );
    }

    #[test]
    fn violations_outside_the_window_do_not_accumulate() {
        let engine = EscalationEngine::new(config(3, 3, 60));

        // Two violations timestamped an hour ago fall outside the 60s window
        // and are pruned before the live one is counted.
        {
            let mut state = engine.state.write().expect("tracker lock");
            state.admit("u1", engine.config.max_tracked_users, 0);
            let stale = now_secs().saturating_sub(3_600);
            let tracker = state.trackers.get_mut("u1").expect("tracker present");
            tracker.violations = vec![stale, stale + 1];
        }

        assert!(engine.record_violation("u1").is_none());
        assert_eq!(
            engine.violation_count("u1"),
            1,
            "stale violations must be pruned, not counted"
        );
    }

    #[test]
    fn disabled_when_threshold_is_zero() {
        let engine = EscalationEngine::new(EscalationConfig {
            enabled: true,
            suspend_after_violations: 0,
            ..Default::default()
        });

        for _ in 0..100 {
            assert!(engine.record_violation("u1").is_none());
        }
    }

    #[test]
    fn reset_clears_violations() {
        let engine = EscalationEngine::new(config(3, 3, 3600));

        engine.record_violation("u1");
        engine.record_violation("u1");
        assert_eq!(engine.violation_count("u1"), 2);

        engine.reset("u1");
        assert_eq!(engine.violation_count("u1"), 0);
        assert_eq!(engine.tracked_users(), 0);
    }

    #[test]
    fn tracked_user_map_is_bounded() {
        let engine = EscalationEngine::new(EscalationConfig {
            enabled: true,
            max_tracked_users: 8,
            ..config(5, 3, 3600)
        });

        for i in 0..1_000 {
            engine.record_violation(&format!("u{i}"));
        }

        assert_eq!(engine.tracked_users(), 8);
        assert_eq!(engine.violation_count("u0"), 0, "oldest user was evicted");
        assert_eq!(engine.violation_count("u999"), 1);
    }

    #[test]
    fn zero_max_tracked_users_tracks_nothing() {
        let engine = EscalationEngine::new(EscalationConfig {
            enabled: true,
            max_tracked_users: 0,
            ..config(1, 1, 0)
        });

        assert!(engine.record_violation("u1").is_none());
        assert_eq!(engine.tracked_users(), 0);
    }

    #[test]
    fn hydrated_suspensions_complete_the_ban_ladder() {
        let engine = EscalationEngine::new(config(2, 3, 0));
        // Two suspensions already survived a restart on the auth-user record.
        engine.hydrate_suspensions("u1", 2);

        engine.record_violation("u1");
        assert_eq!(
            engine.record_violation("u1"),
            Some(Escalation {
                status: AuthStatus::Banned,
                suspensions: 3,
            }),
            "the third suspension must ban even though the first two \
             were counted before the restart"
        );
    }
}
