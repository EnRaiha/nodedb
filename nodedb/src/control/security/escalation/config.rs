// SPDX-License-Identifier: BUSL-1.1

//! [`EscalationConfig`] — operator-facing knobs for auto-escalation, reached
//! from the `[auth.escalation]` section of the server config.

use serde::{Deserialize, Serialize};

/// Auto-escalation configuration.
///
/// Escalation is **off** unless [`EscalationConfig::enabled`] is set, and the
/// default is deliberately `false` for two reasons:
///
/// * The default thresholds punish ordinary mistakes. Ten failed
///   authentications inside one hour auto-suspends the account — a user who
///   fat-fingers a password, or a misconfigured client retrying with a stale
///   credential, reaches that in seconds. Three such suspensions ban the
///   account outright, and the verdict is durable, so recovering from it takes
///   an operator.
/// * A violation is attributed to the account the request names, and an
///   attacker chooses that name. With escalation on, anyone who knows a
///   username can spend ten failed logins to suspend that user and thirty to
///   ban them. The existing per-user lockout accepts a bounded, self-expiring
///   version of that trade-off; a permanent ban is not something to opt an
///   operator into silently.
///
/// Turning it on is therefore an explicit decision, taken together with
/// thresholds and a window tuned for that deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationConfig {
    /// Master switch. When `false` (the default) violations are still
    /// audited, but no account is ever suspended or banned automatically.
    #[serde(default)]
    pub enabled: bool,
    /// Number of violations before auto-suspend. 0 = disabled.
    #[serde(default = "default_suspend_threshold")]
    pub suspend_after_violations: u32,
    /// Number of suspensions before auto-ban. 0 = disabled.
    #[serde(default = "default_ban_threshold")]
    pub ban_after_suspensions: u32,
    /// Window in seconds for counting violations (rolling). 0 = lifetime.
    #[serde(default = "default_window")]
    pub violation_window_secs: u64,
    /// Maximum number of distinct users whose in-flight violation counts are
    /// retained. The map is written on every attributed auth failure, so it is
    /// bounded rather than allowed to grow with the user population; the
    /// oldest tracked user is evicted once the bound is reached. Evicting a
    /// user loses only sub-threshold evidence — the suspension count that
    /// drives the ban ladder is persisted on the auth-user record.
    #[serde(default = "default_max_tracked_users")]
    pub max_tracked_users: usize,
}

fn default_suspend_threshold() -> u32 {
    10
}
fn default_ban_threshold() -> u32 {
    3
}
fn default_window() -> u64 {
    3600
}
fn default_max_tracked_users() -> usize {
    10_000
}

impl Default for EscalationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            suspend_after_violations: default_suspend_threshold(),
            ban_after_suspensions: default_ban_threshold(),
            violation_window_secs: default_window(),
            max_tracked_users: default_max_tracked_users(),
        }
    }
}
