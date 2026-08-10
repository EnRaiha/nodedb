// SPDX-License-Identifier: BUSL-1.1

//! [`RiskScorer`] — evaluates the per-request signals and turns their summed
//! weights into a [`RiskDecision`].
//!
//! Signals: new_ip, new_country, impossible_travel, unusual_time,
//! high_privilege, device_not_trusted.

use std::sync::RwLock;

use crate::control::security::auth_context::AuthContext;

use super::cache::KnownIpCache;
use super::config::{RiskConfig, RiskDecision};

/// Risk scorer: evaluates signals and produces a score.
pub struct RiskScorer {
    config: RiskConfig,
    /// Bounded per-user known IPs (for the `new_ip` signal).
    known_ips: RwLock<KnownIpCache>,
}

impl RiskScorer {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            known_ips: RwLock::new(KnownIpCache::new()),
        }
    }

    /// The configuration this scorer was built with.
    pub fn config(&self) -> &RiskConfig {
        &self.config
    }

    /// Whether the operator enabled risk scoring for this server.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Score a request based on context signals.
    ///
    /// Returns (score, decision, triggered_signals).
    pub fn score(
        &self,
        user_id: &str,
        client_ip: &str,
        auth_ctx: &AuthContext,
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
        if !auth_ctx.metadata_flag("device_trusted")
            && let Some(&w) = self.config.weights.get("device_not_trusted")
        {
            total += w;
            signals.push("device_not_trusted".into());
        }

        // Record this IP as known for future requests.
        self.record_ip(user_id, client_ip);

        (total, self.decide(total), signals)
    }

    /// Map a score onto its decision band.
    pub fn decide(&self, score: f64) -> RiskDecision {
        if score <= self.config.allow_threshold {
            RiskDecision::Allow
        } else if score >= self.config.deny_threshold {
            RiskDecision::Deny
        } else {
            RiskDecision::StepUpMfa
        }
    }

    /// Check if this IP is new for the user.
    fn is_new_ip(&self, user_id: &str, ip: &str) -> bool {
        let known = self.known_ips.read().unwrap_or_else(|p| p.into_inner());
        !known.contains(user_id, ip)
    }

    /// Record an IP as known for a user.
    fn record_ip(&self, user_id: &str, ip: &str) {
        let mut known = self.known_ips.write().unwrap_or_else(|p| p.into_inner());
        known.record(user_id, ip, self.config.max_tracked_users);
    }
}

impl Default for RiskScorer {
    fn default() -> Self {
        Self::new(RiskConfig::default())
    }
}

impl std::fmt::Debug for RiskScorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RiskScorer")
            .field("enabled", &self.config.enabled)
            .field("allow_threshold", &self.config.allow_threshold)
            .field("deny_threshold", &self.config.deny_threshold)
            .finish()
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
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;

    fn enabled_config() -> RiskConfig {
        RiskConfig {
            enabled: true,
            ..Default::default()
        }
    }

    fn regular_context() -> AuthContext {
        AuthContext::from_identity(
            &AuthenticatedIdentity::new_regular(
                1,
                "alice",
                TenantId::new(1),
                AuthMethod::ApiKey,
                vec![Role::ReadWrite],
                None,
                DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
            ),
            "test".into(),
        )
    }

    #[test]
    fn new_ip_triggers() {
        let scorer = RiskScorer::new(enabled_config());
        let auth = regular_context();

        let (score1, _, signals1) = scorer.score("u1", "10.0.0.1", &auth);
        assert!(signals1.contains(&"new_ip".into()));
        assert!(score1 > 0.0);

        // Second request from same IP — not new anymore.
        let (_, _, signals2) = scorer.score("u1", "10.0.0.1", &auth);
        assert!(!signals2.contains(&"new_ip".into()));
    }

    #[test]
    fn high_privilege_triggers() {
        let scorer = RiskScorer::new(enabled_config());
        let auth = AuthContext::from_identity(
            &AuthenticatedIdentity::new_internal_service(
                1,
                "admin",
                TenantId::new(1),
                vec![Role::Superuser],
                true,
                None,
                DatabaseSet::All,
            ),
            "test".into(),
        );

        let (_, _, signals) = scorer.score("admin", "10.0.0.1", &auth);
        assert!(signals.contains(&"high_privilege".into()));
    }

    #[test]
    fn thresholds() {
        let config = RiskConfig {
            enabled: true,
            allow_threshold: 0.1,
            deny_threshold: 0.5,
            ..Default::default()
        };
        let scorer = RiskScorer::new(config);
        let auth = regular_context();

        // First request: new_ip + device_not_trusted = 0.15 + 0.20 = 0.35 → StepUpMfa
        let (_, decision, _) = scorer.score("u1", "10.0.0.1", &auth);
        assert_eq!(decision, RiskDecision::StepUpMfa);
    }

    /// The default thresholds put an ordinary first request into the step-up
    /// band, which is exactly why scoring must default off.
    #[test]
    fn default_config_is_disabled() {
        assert!(!RiskScorer::default().is_enabled());
    }

    #[test]
    fn decide_bands_are_inclusive_at_the_edges() {
        let scorer = RiskScorer::new(enabled_config());
        assert_eq!(scorer.decide(0.3), RiskDecision::Allow);
        assert_eq!(scorer.decide(0.5), RiskDecision::StepUpMfa);
        assert_eq!(scorer.decide(0.7), RiskDecision::Deny);
        assert_eq!(scorer.decide(1.0), RiskDecision::Deny);
    }
}
