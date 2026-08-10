// SPDX-License-Identifier: BUSL-1.1

//! Usage quota enforcement: hard (block), soft (warn), throttle, overage.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::control::security::catalog::types::{StoredScopeQuota, SystemCatalog};

/// Default cap on the number of distinct `"{scope_name}:{grantee_id}"` keys
/// tracked in `QuotaManager::usage`. Lazy rollover (see
/// [`QuotaManager::record_usage`] / [`QuotaManager::get_status`]) bounds
/// usage *within* a scope's period, but the number of distinct grantee keys
/// tracked *between* rollovers is unbounded without this cap; once at
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

impl QuotaEnforcement {
    /// The stored spelling, and the one `SHOW QUOTAS` displays.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Soft => "soft",
            Self::Throttle => "throttle",
            Self::Overage => "overage",
        }
    }

    /// Parse an operator- or catalog-supplied mode, case-insensitively.
    ///
    /// An unrecognised mode is an error, never a silent fallback to the
    /// permissive mode: `ENFORCEMENT HRAD` must be a syntax error, not an
    /// unenforced cap the operator believes is blocking.
    pub fn parse(text: &str) -> crate::Result<Self> {
        match text.to_ascii_lowercase().as_str() {
            "hard" => Ok(Self::Hard),
            "soft" => Ok(Self::Soft),
            "throttle" => Ok(Self::Throttle),
            "overage" => Ok(Self::Overage),
            other => Err(crate::Error::BadRequest {
                detail: format!(
                    "unknown quota enforcement '{other}': expected HARD, SOFT, THROTTLE, or OVERAGE"
                ),
            }),
        }
    }
}

impl QuotaDefinition {
    /// Convert to its persisted form.
    pub fn to_stored(&self) -> StoredScopeQuota {
        StoredScopeQuota {
            scope_name: self.scope_name.clone(),
            max_tokens: self.max_tokens,
            period_secs: self.period_secs,
            enforcement: self.enforcement.as_str().to_string(),
            warning_threshold: self.warning_threshold,
        }
    }

    /// Rebuild from its persisted form.
    pub fn from_stored(stored: StoredScopeQuota) -> crate::Result<Self> {
        Ok(Self {
            scope_name: stored.scope_name,
            max_tokens: stored.max_tokens,
            period_secs: stored.period_secs,
            enforcement: QuotaEnforcement::parse(&stored.enforcement)?,
            warning_threshold: stored.warning_threshold,
        })
    }
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
    /// scope_name → unix-seconds timestamp the current period started at.
    /// Populated lazily by [`Self::rollover_if_due`] the first time it
    /// observes a given scope, so a quota defined mid-period doesn't roll
    /// over immediately on the next access. Bounded by the same admin-defined
    /// scope namespace as `quotas` — a scope only gets an entry once a quota
    /// is defined for it, and quota definitions are admin-authored, not
    /// request-driven, so this cannot grow unboundedly from request traffic.
    period_starts: RwLock<HashMap<String, u64>>,
    /// Cap on distinct keys in `usage`.
    max_tracked_grantees: usize,
    /// Count of new grantee keys refused because `usage` was at capacity.
    dropped_usage_entries: AtomicU64,
    /// Ensures the capacity warning is logged once, not once per dropped call.
    warned_capacity: AtomicBool,
    /// Catalog backing the `quotas` map, when this manager owns one.
    ///
    /// Definitions are admin-authored catalog objects; without persistence
    /// every restart would silently lift every cap.
    ///
    /// Consumption (`usage`) deliberately does NOT live here. A definition is
    /// an operator's stated ceiling and has to outlive the process; the usage
    /// counter is a rolling per-period tally that would put a catalog write on
    /// every single dispatch. The accepted consequence is bounded and
    /// explicit: a restart forgives whatever the current period had already
    /// consumed. Closing that gap means periodically checkpointing the
    /// counters, never a write-through on the request path.
    catalog: Option<SystemCatalog>,
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
            period_starts: RwLock::new(HashMap::new()),
            max_tracked_grantees,
            dropped_usage_entries: AtomicU64::new(0),
            warned_capacity: AtomicBool::new(false),
            catalog: None,
        }
    }

    /// Construct a catalog-backed manager and populate it from that catalog.
    pub fn open(max_tracked_grantees: usize, catalog: &SystemCatalog) -> crate::Result<Self> {
        let mut manager = Self::with_bounds(max_tracked_grantees);
        manager.catalog = Some(catalog.clone());
        manager.load_from(catalog)?;
        Ok(manager)
    }

    /// Replace the in-memory definitions with everything stored in `catalog`.
    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored = catalog.load_all_scope_quotas()?;
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        quotas.clear();
        for record in stored {
            let definition = QuotaDefinition::from_stored(record)?;
            quotas.insert(definition.scope_name.clone(), definition);
        }
        if !quotas.is_empty() {
            info!(count = quotas.len(), "scope quotas loaded from catalog");
        }
        Ok(())
    }

    /// Define or update a quota for a scope.
    ///
    /// Persistence happens first: a definition cached but not stored would
    /// report a cap the next restart does not have.
    pub fn define_quota(&self, quota: QuotaDefinition) -> crate::Result<()> {
        if let Some(ref catalog) = self.catalog {
            catalog.put_scope_quota(&quota.to_stored())?;
        }
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        quotas.insert(quota.scope_name.clone(), quota);
        Ok(())
    }

    /// Remove a quota definition, reporting whether one was present.
    ///
    /// As with [`Self::define_quota`], a catalog failure is fatal to the whole
    /// removal rather than leaving the cache and the catalog disagreeing about
    /// whether a cap still applies.
    pub fn remove_quota(&self, scope_name: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_scope_quota(scope_name)?;
        }
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        Ok(quotas.remove(scope_name).is_some())
    }

    /// Record token usage against a quota.
    ///
    /// `now_secs` rolls the scope's period over first (see
    /// [`Self::rollover_if_due`]) so usage is always recorded against the
    /// current period, never a stale one that should already have reset.
    /// Callers must read the clock once, before taking any lock, and pass
    /// the result in — this keeps every wall-clock read outside lock scope.
    ///
    /// If `usage` is already at `max_tracked_grantees` and `grantee_id` is
    /// new for `scope_name`, the update is refused (rather than growing the
    /// map unboundedly) and surfaced via `dropped_usage_entries()` plus a
    /// one-time warning. Existing grantee keys always keep accumulating.
    pub fn record_usage(&self, scope_name: &str, grantee_id: &str, tokens: u64, now_secs: u64) {
        self.rollover_if_due(scope_name, now_secs);
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
    /// `now_secs` rolls the scope's period over first — see
    /// [`Self::record_usage`] for the clock-read contract this shares.
    ///
    /// Returns `Ok(())` if allowed, `Err` with quota status if blocked.
    pub fn check_quota(
        &self,
        scope_name: &str,
        grantee_id: &str,
        additional_tokens: u64,
        now_secs: u64,
    ) -> Result<(), QuotaStatus> {
        self.rollover_if_due(scope_name, now_secs);
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
    ///
    /// `now_secs` rolls the scope's period over first — see
    /// [`Self::record_usage`] for the clock-read contract this shares. A
    /// status read must never observe usage from a period that should
    /// already have expired just because nothing happened to write to it.
    pub fn get_status(
        &self,
        scope_name: &str,
        grantee_id: &str,
        now_secs: u64,
    ) -> Option<QuotaStatus> {
        self.rollover_if_due(scope_name, now_secs);
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

    /// List all quota definitions, ordered by scope name so `SHOW QUOTAS`
    /// renders the same order on every call rather than a hash order that
    /// shifts between runs.
    pub fn list_quotas(&self) -> Vec<QuotaDefinition> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        let mut all: Vec<_> = quotas.values().cloned().collect();
        all.sort_by(|a, b| a.scope_name.cmp(&b.scope_name));
        all
    }

    /// Reset usage counters for a new billing period.
    pub fn reset_period(&self, scope_name: &str) {
        let prefix = format!("{scope_name}:");
        let mut usage = self.usage.write().unwrap_or_else(|p| p.into_inner());
        usage.retain(|k, _| !k.starts_with(&prefix));
    }

    /// Roll `scope_name`'s usage period over if `now_secs` has crossed the
    /// quota's `period_secs` boundary since the period last started.
    ///
    /// Lazy rollover on access — called from every `usage` reader/writer
    /// ([`Self::record_usage`], [`Self::check_quota`], [`Self::get_status`])
    /// instead of a periodic background sweep. A sweep can only fire on its
    /// own tick interval, which silently rounds any `period_secs` shorter
    /// than that interval up to the interval itself (a 10-second quota
    /// resetting every 60 seconds gives six times the configured
    /// allowance). Checking the boundary at the moment of access makes
    /// every `period_secs` value exact, with no coupling to an interval at
    /// all.
    ///
    /// No-op for a scope with no `QuotaDefinition` — there is no period to
    /// roll over. The first observation of a defined scope anchors
    /// `period_starts` at `now_secs` without rolling over, so a quota
    /// defined mid-period doesn't reset immediately on its first access.
    fn rollover_if_due(&self, scope_name: &str, now_secs: u64) {
        let period_secs = {
            let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
            match quotas.get(scope_name) {
                Some(quota) => quota.period_secs,
                None => return,
            }
        };

        let due = {
            let mut starts = self
                .period_starts
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let start = *starts.entry(scope_name.to_string()).or_insert(now_secs);
            if now_secs.saturating_sub(start) >= period_secs {
                starts.insert(scope_name.to_string(), now_secs);
                true
            } else {
                false
            }
        };

        if due {
            self.reset_period(scope_name);
        }
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
        })
        .expect("define quota in test");

        // Use 90 tokens.
        mgr.record_usage("free", "u1", 90, 1_000);
        assert!(mgr.check_quota("free", "u1", 5, 1_000).is_ok());

        // Try to use 20 more → exceeds 100.
        assert!(mgr.check_quota("free", "u1", 20, 1_000).is_err());
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
        })
        .expect("define quota in test");

        mgr.record_usage("free", "u1", 200, 1_000);
        assert!(mgr.check_quota("free", "u1", 1, 1_000).is_ok()); // Soft = allow.
    }

    #[test]
    fn no_quota_allows_all() {
        let mgr = QuotaManager::new();
        assert!(mgr.check_quota("nonexistent", "u1", 999999, 0).is_ok());
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
        })
        .expect("define quota in test");
        mgr.record_usage("pro", "u1", 500, 1_000);

        let status = mgr.get_status("pro", "u1", 1_000).unwrap();
        assert_eq!(status.used_tokens, 500);
        assert_eq!(status.remaining, 500);
        assert!(!status.exceeded);
        assert!(!status.warning);
    }

    #[test]
    fn reset_period_clears() {
        let mgr = QuotaManager::new();
        mgr.record_usage("free", "u1", 100, 0);
        mgr.reset_period("free");

        let usage = mgr.usage.read().unwrap();
        assert!(!usage.contains_key("free:u1"));
    }

    /// The defect this rollover design fixes: the old implementation only
    /// reset on a periodic 60-second sweep, so any `period_secs` under 60
    /// silently rounded up to the sweep interval — a 10-second period
    /// resets at ~60s and the caller gets six times the configured
    /// allowance. Lazy rollover checks the boundary on every access instead,
    /// so a sub-60s period rolls over exactly when it elapses, not on the
    /// next sweep tick. This test fails against a sweep-based
    /// implementation with a 60-second tick: at `now=1_011` (11 seconds
    /// after the period started at 1_000) a sweep would not have fired yet.
    #[test]
    fn lazy_rollover_is_exact_for_sub_sixty_second_periods() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "free".into(),
            max_tokens: 100,
            period_secs: 10,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        })
        .expect("define quota in test");

        // First access establishes the period start — nothing has "elapsed"
        // yet (0 seconds), so usage is untouched.
        mgr.record_usage("free", "u1", 50, 1_000);
        assert_eq!(*mgr.usage.read().unwrap().get("free:u1").unwrap(), 50);

        // An access before period_secs has elapsed leaves usage untouched.
        assert_eq!(mgr.get_status("free", "u1", 1_005).unwrap().used_tokens, 50);

        // Once period_secs has elapsed since the period start, the very
        // next access — whether a read or a write — rolls the period over
        // before doing anything else.
        assert_eq!(mgr.get_status("free", "u1", 1_011).unwrap().used_tokens, 0);
    }

    /// `record_usage` itself must also observe the rollover, not just
    /// `get_status` — a write into an already-expired period must land in
    /// the fresh one, not accumulate into the stale one.
    #[test]
    fn record_usage_rolls_over_before_recording() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "free".into(),
            max_tokens: 100,
            period_secs: 10,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        })
        .expect("define quota in test");

        mgr.record_usage("free", "u1", 90, 1_000);
        // Past the period boundary — this write must land in a fresh period.
        mgr.record_usage("free", "u1", 5, 1_011);

        assert_eq!(
            mgr.get_status("free", "u1", 1_011).unwrap().used_tokens,
            5,
            "stale-period usage must not carry over into the new period"
        );
    }

    #[test]
    fn usage_map_is_bounded_and_overflow_is_observable() {
        let mgr = QuotaManager::with_bounds(2);

        mgr.record_usage("free", "u1", 10, 0);
        mgr.record_usage("free", "u2", 10, 0);
        assert_eq!(mgr.dropped_usage_entries(), 0);
        assert_eq!(mgr.usage.read().unwrap().len(), 2);

        // A third distinct grantee exceeds the cap of 2.
        mgr.record_usage("free", "u3", 10, 0);
        assert_eq!(mgr.dropped_usage_entries(), 1);
        assert_eq!(mgr.usage.read().unwrap().len(), 2); // Map did not grow.
        assert!(!mgr.usage.read().unwrap().contains_key("free:u3"));

        // Existing grantee keys keep updating past the cap being hit.
        mgr.record_usage("free", "u1", 5, 0);
        assert_eq!(*mgr.usage.read().unwrap().get("free:u1").unwrap(), 15);
        assert_eq!(mgr.dropped_usage_entries(), 1); // No new drop for an existing key.

        // Further distinct grantees keep incrementing the drop counter.
        mgr.record_usage("free", "u4", 10, 0);
        assert_eq!(mgr.dropped_usage_entries(), 2);
    }
}
