// SPDX-License-Identifier: BUSL-1.1

//! [`RiskConfig`] — operator-facing knobs for adaptive-auth risk scoring,
//! reached from the `[auth.risk]` section of the server config.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Risk scoring configuration.
///
/// Scoring is **off** unless [`RiskConfig::enabled`] is set. The default
/// weights score an ordinary first request from a user at `new_ip` (0.15) +
/// `device_not_trusted` (0.20) = 0.35, which is already above the default
/// allow threshold of 0.30 — i.e. defaulting this on would put every normal
/// request into the step-up band. Enabling it is therefore an explicit
/// operator decision, taken together with weights and thresholds tuned for
/// that deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Master switch. When `false` (the default) no request is scored,
    /// `$auth.risk_score` stays unresolvable, and no request is refused.
    #[serde(default)]
    pub enabled: bool,
    /// Weight for each signal (0.0 - 1.0). Total score is sum of triggered weights.
    #[serde(default = "default_weights")]
    pub weights: HashMap<String, f64>,
    /// Score threshold: at or below this → allow (default: 0.3).
    #[serde(default = "default_allow_threshold")]
    pub allow_threshold: f64,
    /// Score threshold: at or above this → deny (default: 0.7).
    #[serde(default = "default_deny_threshold")]
    pub deny_threshold: f64,
    /// Maximum number of distinct users whose known IPs are retained for
    /// "new IP" detection. The cache is consulted on every scored request,
    /// so it is bounded rather than allowed to grow with the user population.
    #[serde(default = "default_max_tracked_users")]
    pub max_tracked_users: usize,
    // Score between allow and deny → step-up MFA required.
}

fn default_weights() -> HashMap<String, f64> {
    let mut m = HashMap::new();
    m.insert("new_ip".into(), 0.15);
    m.insert("new_country".into(), 0.25);
    m.insert("impossible_travel".into(), 0.40);
    m.insert("unusual_time".into(), 0.10);
    m.insert("high_privilege".into(), 0.10);
    m.insert("device_not_trusted".into(), 0.20);
    m
}

fn default_allow_threshold() -> f64 {
    0.3
}

fn default_deny_threshold() -> f64 {
    0.7
}

fn default_max_tracked_users() -> usize {
    10_000
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            weights: default_weights(),
            allow_threshold: default_allow_threshold(),
            deny_threshold: default_deny_threshold(),
            max_tracked_users: default_max_tracked_users(),
        }
    }
}

/// Risk assessment result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskDecision {
    /// Score at or below `allow_threshold` — proceed normally.
    Allow,
    /// Score between the thresholds — require step-up MFA.
    StepUpMfa,
    /// Score at or above `deny_threshold` — deny access.
    Deny,
}
