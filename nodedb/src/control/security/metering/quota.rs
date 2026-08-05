// SPDX-License-Identifier: BUSL-1.1

//! Usage quota enforcement: hard (block), soft (warn), throttle, overage.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Default cap on the number of distinct `"{scope_name}:{grantee_id}"` keys
/// tracked in `QuotaManager::usage`. This is a process-lifetime store with no
/// caller currently invoking `reset_period()`, so it must be bounded; once at
/// capacity, new grantee keys are refused (existing ones keep accumulating)
/// and the refusal is surfaced via `dropped_usage_entries()` plus a one-time
/// `tracing::warn!`.
pub const DEFAULT_MAX_TRACKED_GRANTEES: usize = 100_000;

/// Quota enforcement mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaEnforcement {
    /// Block requests when quota exceeded.
    Hard,
    /// Log warning but allow requests.
    Soft,
    /// Throttle (reduce rate limit) when nearing quota.
    Throttle,
    /// Allow overage with per-token billing.
    Overage,
}

/// A quota definition attached to a scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaDefinition {
    /// Scope this quota applies to.
    pub scope_name: String,
    /// Maximum tokens per period.
    pub max_tokens: u64,
    /// Period in seconds (e.g., 2592000 = 30 days).
    pub period_secs: u64,
    /// Enforcement mode.
    pub enforcement: QuotaEnforcement,
    /// Warning threshold (0.0-1.0). Default: 0.8 (80%).
    pub warning_threshold: f64,
}

/// Quota status for a user/org.
#[derive(Debug, Clone)]
pub struct QuotaStatus {
    pub scope_name: String,
    pub max_tokens: u64,
    pub used_tokens: u64,
    pub remaining: u64,
    pub pct_used: f64,
    pub enforcement: QuotaEnforcement,
    pub exceeded: bool,
    pub warning: bool,
}

/// Quota manager: tracks usage against quota definitions.
pub struct QuotaManager {
    /// scope_name → quota definition.
    quotas: RwLock<HashMap<String, QuotaDefinition>>,
    /// "{scope_name}:{grantee_id}" → tokens used in current period.
    usage: RwLock<HashMap<String, u64>>,
    /// Cap on distinct keys in `usage`.
    max_tracked_grantees: usize,
    /// Count of new grantee keys refused because `usage` was at capacity.
    dropped_usage_entries: AtomicU64,
    /// Ensures the capacity warning is logged once, not once per dropped call.
    warned_capacity: AtomicBool,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self::with_bounds(DEFAULT_MAX_TRACKED_GRANTEES)
    }

    /// Construct with an explicit cap on distinct `"{scope}:{grantee}"` keys.
    pub fn with_bounds(max_tracked_grantees: usize) -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            max_tracked_grantees,
            dropped_usage_entries: AtomicU64::new(0),
            warned_capacity: AtomicBool::new(false),
        }
    }

    /// Define or update a quota for a scope.
    pub fn define_quota(&self, quota: QuotaDefinition) {
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        quotas.insert(quota.scope_name.clone(), quota);
    }

    /// Remove a quota definition.
    pub fn remove_quota(&self, scope_name: &str) -> bool {
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        quotas.remove(scope_name).is_some()
    }

    /// Record token usage against a quota.
    ///
    /// If `usage` is already at `max_tracked_grantees` and `grantee_id` is
    /// new for `scope_name`, the update is refused (rather than growing the
    /// map unboundedly) and surfaced via `dropped_usage_entries()` plus a
    /// one-time warning. Existing grantee keys always keep accumulating.
    pub fn record_usage(&self, scope_name: &str, grantee_id: &str, tokens: u64) {
        let key = format!("{scope_name}:{grantee_id}");
        let dropped = {
            let mut usage = self.usage.write().unwrap_or_else(|p| p.into_inner());
            if let Some(v) = usage.get_mut(&key) {
                *v += tokens;
                false
            } else if usage.len() < self.max_tracked_grantees {
                usage.insert(key, tokens);
                false
            } else {
                true
            }
        };

        if dropped {
            self.dropped_usage_entries.fetch_add(1, Ordering::Relaxed);
            if !self.warned_capacity.swap(true, Ordering::Relaxed) {
                warn!(
                    scope = %scope_name,
                    cap = self.max_tracked_grantees,
                    "quota usage tracking at capacity — new grantee entries are no longer \
                     being tracked (existing entries keep updating); see dropped_usage_entries()"
                );
            }
        }
    }

    /// Count of new grantee keys refused since `usage` hit capacity.
    pub fn dropped_usage_entries(&self) -> u64 {
        self.dropped_usage_entries.load(Ordering::Relaxed)
    }

    /// The configured cap on distinct `"{scope}:{grantee}"` keys (see
    /// `max_tracked_quota_grantees` on `MeteringConfig`). Exposed so
    /// observability surfaces can report drop counts alongside the ceiling
    /// that produced them.
    pub fn max_tracked_grantees(&self) -> usize {
        self.max_tracked_grantees
    }

    /// Check if a request should be allowed based on quota.
    ///
    /// Returns `Ok(())` if allowed, `Err` with quota status if blocked.
    pub fn check_quota(
        &self,
        scope_name: &str,
        grantee_id: &str,
        additional_tokens: u64,
    ) -> Result<(), QuotaStatus> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        let Some(quota) = quotas.get(scope_name) else {
            return Ok(()); // No quota defined → allow.
        };

        let key = format!("{scope_name}:{grantee_id}");
        let usage = self.usage.read().unwrap_or_else(|p| p.into_inner());
        let used = *usage.get(&key).unwrap_or(&0);
        let projected = used + additional_tokens;

        let pct = if quota.max_tokens > 0 {
            used as f64 / quota.max_tokens as f64
        } else {
            0.0
        };

        let status = QuotaStatus {
            scope_name: scope_name.into(),
            max_tokens: quota.max_tokens,
            used_tokens: used,
            remaining: quota.max_tokens.saturating_sub(used),
            pct_used: pct,
            enforcement: quota.enforcement,
            exceeded: projected > quota.max_tokens,
            warning: pct >= quota.warning_threshold,
        };

        if status.warning && !status.exceeded {
            warn!(
                scope = %scope_name,
                grantee = %grantee_id,
                pct = format!("{:.0}%", pct * 100.0),
                "quota warning threshold reached"
            );
        }

        if status.exceeded {
            match quota.enforcement {
                QuotaEnforcement::Hard => return Err(status),
                QuotaEnforcement::Soft => {
                    warn!(scope = %scope_name, "quota exceeded (soft enforcement — allowing)");
                }
                QuotaEnforcement::Throttle => {
                    // Caller should reduce rate limit.
                    info!(scope = %scope_name, "quota exceeded — throttling");
                }
                QuotaEnforcement::Overage => {
                    info!(scope = %scope_name, "quota exceeded — overage billing");
                }
            }
        }

        Ok(())
    }

    /// Get quota status for a user/org.
    pub fn get_status(&self, scope_name: &str, grantee_id: &str) -> Option<QuotaStatus> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        let quota = quotas.get(scope_name)?;

        let key = format!("{scope_name}:{grantee_id}");
        let usage = self.usage.read().unwrap_or_else(|p| p.into_inner());
        let used = *usage.get(&key).unwrap_or(&0);

        let pct = if quota.max_tokens > 0 {
            used as f64 / quota.max_tokens as f64
        } else {
            0.0
        };

        Some(QuotaStatus {
            scope_name: scope_name.into(),
            max_tokens: quota.max_tokens,
            used_tokens: used,
            remaining: quota.max_tokens.saturating_sub(used),
            pct_used: pct,
            enforcement: quota.enforcement,
            exceeded: used > quota.max_tokens,
            warning: pct >= quota.warning_threshold,
        })
    }

    /// List all quota definitions.
    pub fn list_quotas(&self) -> Vec<QuotaDefinition> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        quotas.values().cloned().collect()
    }

    /// Reset usage counters for a new billing period.
    pub fn reset_period(&self, scope_name: &str) {
        let prefix = format!("{scope_name}:");
        let mut usage = self.usage.write().unwrap_or_else(|p| p.into_inner());
        usage.retain(|k, _| !k.starts_with(&prefix));
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_quota_blocks() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "free".into(),
            max_tokens: 100,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        });

        // Use 90 tokens.
        mgr.record_usage("free", "u1", 90);
        assert!(mgr.check_quota("free", "u1", 5).is_ok());

        // Try to use 20 more → exceeds 100.
        assert!(mgr.check_quota("free", "u1", 20).is_err());
    }

    #[test]
    fn soft_quota_allows() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "free".into(),
            max_tokens: 100,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Soft,
            warning_threshold: 0.8,
        });

        mgr.record_usage("free", "u1", 200);
        assert!(mgr.check_quota("free", "u1", 1).is_ok()); // Soft = allow.
    }

    #[test]
    fn no_quota_allows_all() {
        let mgr = QuotaManager::new();
        assert!(mgr.check_quota("nonexistent", "u1", 999999).is_ok());
    }

    #[test]
    fn quota_status() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "pro".into(),
            max_tokens: 1000,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        });
        mgr.record_usage("pro", "u1", 500);

        let status = mgr.get_status("pro", "u1").unwrap();
        assert_eq!(status.used_tokens, 500);
        assert_eq!(status.remaining, 500);
        assert!(!status.exceeded);
        assert!(!status.warning);
    }

    #[test]
    fn reset_period_clears() {
        let mgr = QuotaManager::new();
        mgr.record_usage("free", "u1", 100);
        mgr.reset_period("free");

        let usage = mgr.usage.read().unwrap();
        assert!(!usage.contains_key("free:u1"));
    }

    #[test]
    fn usage_map_is_bounded_and_overflow_is_observable() {
        let mgr = QuotaManager::with_bounds(2);

        mgr.record_usage("free", "u1", 10);
        mgr.record_usage("free", "u2", 10);
        assert_eq!(mgr.dropped_usage_entries(), 0);
        assert_eq!(mgr.usage.read().unwrap().len(), 2);

        // A third distinct grantee exceeds the cap of 2.
        mgr.record_usage("free", "u3", 10);
        assert_eq!(mgr.dropped_usage_entries(), 1);
        assert_eq!(mgr.usage.read().unwrap().len(), 2); // Map did not grow.
        assert!(!mgr.usage.read().unwrap().contains_key("free:u3"));

        // Existing grantee keys keep updating past the cap being hit.
        mgr.record_usage("free", "u1", 5);
        assert_eq!(*mgr.usage.read().unwrap().get("free:u1").unwrap(), 15);
        assert_eq!(mgr.dropped_usage_entries(), 1); // No new drop for an existing key.

        // Further distinct grantees keep incrementing the drop counter.
        mgr.record_usage("free", "u4", 10);
        assert_eq!(mgr.dropped_usage_entries(), 2);
    }
}
