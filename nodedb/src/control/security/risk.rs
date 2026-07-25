// SPDX-License-Identifier: BUSL-1.1

//! Risk scoring: combine signals into a score, expose as `$auth.risk_score` in RLS.
//!
//! Signals: new_ip, new_country, impossible_travel, unusual_time,
//! high_privilege, device_not_trusted.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// Risk scoring configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Weight for each signal (0.0 - 1.0). Total score is sum of triggered weights.
    #[serde(default = "default_weights")]
    pub weights: HashMap<String, f64>,
    /// Score threshold: below this → allow (default: 0.3).
    #[serde(default = "default_allow_threshold")]
    pub allow_threshold: f64,
    /// Score threshold: above this → deny (default: 0.7).
    #[serde(default = "default_deny_threshold")]
    pub deny_threshold: f64,
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

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            weights: default_weights(),
            allow_threshold: default_allow_threshold(),
            deny_threshold: default_deny_threshold(),
        }
    }
}

/// Risk assessment result.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RiskDecision {
    /// Score below allow_threshold — proceed normally.
    Allow,
    /// Score between thresholds — require step-up MFA.
    StepUpMfa,
    /// Score above deny_threshold — deny access.
    Deny,
}

/// Risk scorer: evaluates signals and produces a score.
pub struct RiskScorer {
    config: RiskConfig,
    /// Per-user known IPs (for "new_ip" detection).
    known_ips: RwLock<HashMap<String, Vec<String>>>,
}

impl RiskScorer {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            known_ips: RwLock::new(HashMap::new()),
        }
    }

    /// Score a request based on context signals.
    ///
    /// Returns (score, decision, triggered_signals).
    pub fn score(
        &self,
        user_id: &str,
        client_ip: &str,
        auth_ctx: &super::auth_context::AuthContext,
    ) -> (f64, RiskDecision, Vec<String>) {
        let mut total = 0.0_f64;
        let mut signals = Vec::new();

        // Signal: new_ip.
        if self.is_new_ip(user_id, client_ip)
            && let Some(&w) = self.config.weights.get("new_ip")
        {
            total += w;
            signals.push("new_ip".into());
        }

        // Signal: unusual_time (outside 06:00-22:00 local).
        let hour = current_hour();
        if !(6..22).contains(&hour)
            && let Some(&w) = self.config.weights.get("unusual_time")
        {
            total += w;
            signals.push("unusual_time".into());
        }

        // Signal: high_privilege (superuser or tenant_admin).
        if (auth_ctx.is_superuser() || auth_ctx.roles.iter().any(|r| r == "tenant_admin"))
            && let Some(&w) = self.config.weights.get("high_privilege")
        {
            total += w;
            signals.push("high_privilege".into());
        }

        // Signal: device_not_trusted.
        if auth_ctx
            .metadata
            .get("device_trusted")
            .is_none_or(|v| v != "true")
            && let Some(&w) = self.config.weights.get("device_not_trusted")
        {
            total += w;
            signals.push("device_not_trusted".into());
        }

        // Record this IP as known for future requests.
        self.record_ip(user_id, client_ip);

        let decision = if total <= self.config.allow_threshold {
            RiskDecision::Allow
        } else if total >= self.config.deny_threshold {
            RiskDecision::Deny
        } else {
            RiskDecision::StepUpMfa
        };

        (total, decision, signals)
    }

    /// Check if this IP is new for the user.
    fn is_new_ip(&self, user_id: &str, ip: &str) -> bool {
        let known = self.known_ips.read().unwrap_or_else(|p| p.into_inner());
        known
            .get(user_id)
            .is_none_or(|ips| !ips.contains(&ip.to_string()))
    }

    /// Record an IP as known for a user.
    fn record_ip(&self, user_id: &str, ip: &str) {
        let mut known = self.known_ips.write().unwrap_or_else(|p| p.into_inner());
        let ips = known.entry(user_id.into()).or_default();
        if !ips.contains(&ip.to_string()) {
            // Keep max 50 IPs per user.
            if ips.len() >= 50 {
                ips.remove(0);
            }
            ips.push(ip.to_string());
        }
    }
}

impl Default for RiskScorer {
    fn default() -> Self {
        Self::new(RiskConfig::default())
    }
}

fn current_hour() -> u8 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    ((secs % 86_400) / 3600) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ip_triggers() {
        let scorer = RiskScorer::default();
        let auth = crate::control::security::auth_context::AuthContext::from_identity(
            &crate::control::security::identity::AuthenticatedIdentity::new_regular(
                1,
                "alice",
                crate::types::TenantId::new(1),
                crate::control::security::identity::AuthMethod::ApiKey,
                vec![crate::control::security::identity::Role::ReadWrite],
                None,
                crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![
                    nodedb_types::id::DatabaseId::DEFAULT,
                ]),
            ),
            "test".into(),
        );

        let (score1, _, signals1) = scorer.score("u1", "10.0.0.1", &auth);
        assert!(signals1.contains(&"new_ip".into()));
        assert!(score1 > 0.0);

        // Second request from same IP — not new anymore.
        let (_, _, signals2) = scorer.score("u1", "10.0.0.1", &auth);
        assert!(!signals2.contains(&"new_ip".into()));
    }

    #[test]
    fn high_privilege_triggers() {
        let scorer = RiskScorer::default();
        let auth = crate::control::security::auth_context::AuthContext::from_identity(
            &crate::control::security::identity::AuthenticatedIdentity::new_internal_service(
                1,
                "admin",
                crate::types::TenantId::new(1),
                vec![crate::control::security::identity::Role::Superuser],
                true,
                None,
                crate::control::security::identity::DatabaseSet::All,
            ),
            "test".into(),
        );

        let (_, _, signals) = scorer.score("admin", "10.0.0.1", &auth);
        assert!(signals.contains(&"high_privilege".into()));
    }

    #[test]
    fn thresholds() {
        let config = RiskConfig {
            allow_threshold: 0.1,
            deny_threshold: 0.5,
            ..Default::default()
        };
        let scorer = RiskScorer::new(config);
        let auth = crate::control::security::auth_context::AuthContext::from_identity(
            &crate::control::security::identity::AuthenticatedIdentity::new_regular(
                1,
                "test",
                crate::types::TenantId::new(1),
                crate::control::security::identity::AuthMethod::ApiKey,
                vec![],
                None,
                crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![
                    nodedb_types::id::DatabaseId::DEFAULT,
                ]),
            ),
            "test".into(),
        );

        // First request: new_ip + device_not_trusted = 0.15 + 0.20 = 0.35 → StepUpMfa
        let (_, decision, _) = scorer.score("u1", "10.0.0.1", &auth);
        assert_eq!(decision, RiskDecision::StepUpMfa);
    }
}
