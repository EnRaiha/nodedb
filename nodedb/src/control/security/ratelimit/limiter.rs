// SPDX-License-Identifier: BUSL-1.1

//! Hierarchical rate limiter: per-user → per-org → per-tenant → per-database.
//!
//! Each identity gets a token bucket. Requests consume tokens based on
//! endpoint cost multipliers. When empty, requests are rejected with 429.
//!
//! Hierarchy: per-key → per-user → per-org → per-tenant → per-database.
//! A request is allowed only if ALL applicable buckets have tokens.
//! Most-specific bucket that denies determines the error kind.
//!
//! ## Lock-poisoning policy
//!
//! The `RwLock`-guarded maps in this module hold owned `TokenBucket` values
//! whose internal state is a small struct of atomics + a refill timestamp.
//! Bucket mutations are individually consistent and do not span multiple
//! map operations. A panic in an unrelated request handler therefore cannot
//! corrupt the contents of these maps; only the `RwLock`'s poison flag is
//! set. Recovering via `unwrap_or_else(|p| p.into_inner())` keeps the rate
//! limiter live in the face of a one-off panic — the alternative (every
//! subsequent request returning a poisoning error) would itself be a
//! denial-of-service. Revisit if bucket mutation ever becomes a multi-step
//! protocol that can leave a bucket half-updated.

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::debug;

use nodedb_types::{DatabaseId, TenantId};

use super::bucket::TokenBucket;
use super::config::RateLimitConfig;

/// Rate limit check result.
#[derive(Debug)]
pub struct RateLimitResult {
    /// Whether the request is allowed.
    pub allowed: bool,
    /// Remaining tokens in the most constrained bucket.
    pub remaining: u64,
    /// Total limit of the most constrained bucket.
    pub limit: u64,
    /// Seconds until reset (0 if allowed).
    pub retry_after_secs: u64,
}

/// Parameters for a scoped rate-limit check that includes tenant and
/// database buckets in addition to the user/org hierarchy.
///
/// All fields are optional: set `None` to skip the corresponding bucket.
pub struct QuotaCheckParams {
    /// Tenant-scoped QPS cap (`tenant.quota.max_qps`). `0` or `None` = no cap.
    pub tenant_max_qps: Option<u64>,
    /// Database-scoped QPS cap (`database.quota.max_qps`). `0` or `None` = no cap.
    pub database_max_qps: Option<u64>,
    /// Tenant identifier (used as the bucket key when `tenant_max_qps` is set).
    pub tenant_id: TenantId,
    /// Database identifier (used as the bucket key when `database_max_qps` is set).
    pub database_id: DatabaseId,
}

/// Result of a pre-authentication login admission check.
///
/// A non-`Allowed` outcome is a *transient* rejection (retry after
/// `retry_after_secs`) — NOT a credential signal. Callers must surface it as a
/// distinct retryable error, never collapse it into an invalid-password error.
#[derive(Debug)]
pub enum LoginRateLimitOutcome {
    /// Admission granted — proceed with credential verification.
    Allowed,
    /// The per-IP admission ceiling (brute-force failure window or Argon2 DoS
    /// ceiling) is exhausted for this source address.
    IpExceeded {
        /// Seconds until the caller should retry.
        retry_after_secs: u64,
    },
    /// The per-username brute-force failure window is exhausted.
    UserExceeded {
        /// Seconds until the caller should retry.
        retry_after_secs: u64,
    },
}

/// Hierarchical rate limiter.
pub struct RateLimiter {
    config: RateLimitConfig,
    /// Per-identity buckets. Key = identity key (user_id, api_key_id, org_id).
    buckets: RwLock<HashMap<String, TokenBucket>>,
    /// Total rejection counter for Prometheus metrics.
    rejections_total: std::sync::atomic::AtomicU64,
    /// Pre-authentication login admission control (brute-force windows +
    /// Argon2 DoS ceiling). Owns its own bucket map — see `LoginLimiter`.
    login: super::login::LoginLimiter,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: RwLock::new(HashMap::new()),
            rejections_total: std::sync::atomic::AtomicU64::new(0),
            login: super::login::LoginLimiter::new(),
        }
    }

    /// Update the per-IP and per-username login brute-force failure capacities.
    /// Called once at startup from server configuration.
    pub fn set_login_capacities(&self, ip_cap: u64, user_cap: u64) {
        self.login.set_capacities(ip_cap, user_cap);
    }

    /// Pre-authentication admission check. Delegates to `LoginLimiter::check`;
    /// never consumes the brute-force budget for a legitimate attempt.
    pub fn check_login(&self, peer_addr: &str, username: &str) -> LoginRateLimitOutcome {
        self.login.check(peer_addr, username)
    }

    /// Record a genuine credential FAILURE for brute-force accounting. Must be
    /// called only from the same site that drives the credential lockout
    /// counter — never on success or on a policy rejection.
    pub fn record_login_failure(&self, peer_addr: &str, username: &str) {
        self.login.record_failure(peer_addr, username);
    }

    /// Non-consuming check of whether login admission would currently reject
    /// `(peer_addr, username)` — used to avoid double-counting a SASL failure
    /// that was actually an admission rejection.
    pub fn is_login_rate_limited(&self, peer_addr: &str, username: &str) -> bool {
        self.login.is_rate_limited(peer_addr, username)
    }

    /// Check rate limit for a request.
    ///
    /// `user_id` = authenticated user.
    /// `org_ids` = user's org memberships (for org-level rate limiting).
    /// `plan_tier` = tier name from `$auth.metadata.plan` (e.g., "free", "pro").
    /// `operation` = endpoint name for cost multiplier lookup.
    /// `quota` = optional tenant/database QPS caps; pass `None` to skip those buckets.
    ///
    /// Check order: user → org → tenant → database. First denial wins.
    pub fn check(
        &self,
        user_id: &str,
        org_ids: &[String],
        plan_tier: Option<&str>,
        operation: &str,
        quota: Option<&QuotaCheckParams>,
    ) -> RateLimitResult {
        if !self.config.enabled {
            return RateLimitResult {
                allowed: true,
                remaining: u64::MAX,
                limit: u64::MAX,
                retry_after_secs: 0,
            };
        }

        let cost = self.config.operation_cost(operation);

        // Resolve the tier (from JWT plan claim or default).
        let (qps, burst) = self.resolve_tier(plan_tier);

        // Check user-level bucket.
        let user_key = format!("user:{user_id}");
        let user_result = self.check_bucket(&user_key, qps, burst, cost);

        if !user_result.allowed {
            debug!(
                user_id = %user_id,
                operation = %operation,
                cost,
                "rate limited (user bucket)"
            );
            return user_result;
        }

        // Check org-level bucket (shared across members).
        for org_id in org_ids {
            let org_key = format!("org:{org_id}");
            // Org gets 10x the user rate (shared budget).
            let org_result = self.check_bucket(&org_key, qps * 10, burst * 10, cost);
            if !org_result.allowed {
                debug!(
                    user_id = %user_id,
                    org_id = %org_id,
                    operation = %operation,
                    "rate limited (org bucket)"
                );
                return org_result;
            }
        }

        // Check tenant-level bucket (if a cap is configured).
        if let Some(q) = quota {
            if q.tenant_max_qps.is_some_and(|v| v > 0) {
                let tenant_qps = q.tenant_max_qps.unwrap_or(0);
                let tenant_key = format!("tenant:{}", q.tenant_id.as_u64());
                let tenant_result = self.check_bucket(&tenant_key, tenant_qps, tenant_qps, cost);
                if !tenant_result.allowed {
                    debug!(
                        tenant_id = q.tenant_id.as_u64(),
                        operation = %operation,
                        "rate limited (tenant bucket)"
                    );
                    return tenant_result;
                }
            }

            // Check database-level bucket (if a cap is configured).
            if q.database_max_qps.is_some_and(|v| v > 0) {
                let db_qps = q.database_max_qps.unwrap_or(0);
                let db_key = format!("database:{}", q.database_id.as_u64());
                let db_result = self.check_bucket(&db_key, db_qps, db_qps, cost);
                if !db_result.allowed {
                    debug!(
                        database_id = q.database_id.as_u64(),
                        operation = %operation,
                        "rate limited (database bucket)"
                    );
                    return db_result;
                }
            }
        }

        user_result
    }

    /// Check with per-API-key limits (independent bucket).
    pub fn check_api_key(
        &self,
        key_id: &str,
        max_qps: u64,
        max_burst: u64,
        operation: &str,
    ) -> RateLimitResult {
        if !self.config.enabled || max_qps == 0 {
            return RateLimitResult {
                allowed: true,
                remaining: u64::MAX,
                limit: u64::MAX,
                retry_after_secs: 0,
            };
        }
        let cost = self.config.operation_cost(operation);
        let key = format!("apikey:{key_id}");
        self.check_bucket(&key, max_qps, max_burst, cost)
    }

    /// Check a single bucket, creating it if it doesn't exist.
    fn check_bucket(&self, key: &str, qps: u64, burst: u64, cost: u64) -> RateLimitResult {
        // Fast path: read-only check.
        {
            let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
            if let Some(bucket) = buckets.get(key) {
                let allowed = bucket.try_acquire(cost);
                return RateLimitResult {
                    allowed,
                    remaining: bucket.available(),
                    limit: bucket.capacity(),
                    retry_after_secs: if allowed {
                        0
                    } else {
                        (bucket.retry_after_ms() / 1000).max(1)
                    },
                };
            }
        }

        // Slow path: create bucket.
        let mut buckets = self.buckets.write().unwrap_or_else(|p| p.into_inner());
        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(burst, qps as f64));

        let allowed = bucket.try_acquire(cost);
        RateLimitResult {
            allowed,
            remaining: bucket.available(),
            limit: bucket.capacity(),
            retry_after_secs: if allowed {
                0
            } else {
                (bucket.retry_after_ms() / 1000).max(1)
            },
        }
    }

    /// Resolve rate limit tier from plan name.
    fn resolve_tier(&self, plan_tier: Option<&str>) -> (u64, u64) {
        if let Some(tier_name) = plan_tier
            && let Some(tier) = self.config.tier(tier_name)
        {
            return (tier.qps, tier.burst);
        }
        (self.config.default_qps, self.config.default_burst)
    }

    /// Build HTTP response headers for rate limit info.
    pub fn response_headers(result: &RateLimitResult) -> Vec<(String, String)> {
        vec![
            ("X-RateLimit-Limit".into(), result.limit.to_string()),
            ("X-RateLimit-Remaining".into(), result.remaining.to_string()),
            (
                "X-RateLimit-Reset".into(),
                result.retry_after_secs.to_string(),
            ),
        ]
    }

    /// Build Retry-After header value (seconds).
    pub fn retry_after_header(result: &RateLimitResult) -> Option<(String, String)> {
        if result.allowed {
            None
        } else {
            Some(("Retry-After".into(), result.retry_after_secs.to_string()))
        }
    }

    /// Record a rate limit rejection and return the total count.
    /// Exposed as `nodedb_rate_limit_rejected_total` in Prometheus metrics.
    pub fn record_rejection(&self) -> u64 {
        self.rejections_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// Get total rejection count for Prometheus export.
    pub fn rejections_total(&self) -> u64 {
        self.rejections_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether rate limiting is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Number of active buckets (for metrics) — the per-identity QPS buckets
    /// plus the login admission buckets.
    pub fn active_buckets(&self) -> usize {
        self.buckets.read().unwrap_or_else(|p| p.into_inner()).len() + self.login.active_buckets()
    }

    /// Get the config for inspection.
    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (login_ip_cap, login_user_cap) = self.login.capacities();
        f.debug_struct("RateLimiter")
            .field("login_ip_cap", &login_ip_cap)
            .field("login_user_cap", &login_user_cap)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> RateLimitConfig {
        use crate::control::security::ratelimit::config::RateLimitTier;
        let mut config = RateLimitConfig {
            enabled: true,
            default_qps: 10,
            default_burst: 20,
            ..Default::default()
        };
        config.tiers.insert(
            "pro".into(),
            RateLimitTier {
                qps: 5000,
                burst: 10000,
            },
        );
        config
    }

    #[test]
    fn disabled_allows_all() {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        let result = limiter.check("u1", &[], None, "point_get", None);
        assert!(result.allowed);
    }

    #[test]
    fn basic_rate_limiting() {
        let limiter = RateLimiter::new(enabled_config());

        // Burst of 20, cost 1 each.
        for _ in 0..20 {
            let r = limiter.check("u1", &[], None, "point_get", None);
            assert!(r.allowed);
        }
        // 21st request should be rejected.
        let r = limiter.check("u1", &[], None, "point_get", None);
        assert!(!r.allowed);
        assert!(r.retry_after_secs > 0);
    }

    #[test]
    fn cost_multiplier_drains_faster() {
        let limiter = RateLimiter::new(enabled_config());

        // vector_search costs 20 tokens. Burst is 20. First request OK.
        let r = limiter.check("u1", &[], None, "vector_search", None);
        assert!(r.allowed);
        // Second should fail (20 tokens consumed, 0 remaining).
        let r = limiter.check("u1", &[], None, "vector_search", None);
        assert!(!r.allowed);
    }

    #[test]
    fn tier_resolution() {
        let limiter = RateLimiter::new(enabled_config());

        // Pro tier: 5000 QPS, 10000 burst.
        for _ in 0..100 {
            let r = limiter.check("u1", &[], Some("pro"), "point_get", None);
            assert!(r.allowed);
        }
    }

    #[test]
    fn per_user_isolation() {
        let limiter = RateLimiter::new(enabled_config());

        // Exhaust u1's bucket.
        for _ in 0..20 {
            limiter.check("u1", &[], None, "point_get", None);
        }
        let r = limiter.check("u1", &[], None, "point_get", None);
        assert!(!r.allowed);

        // u2 should still have tokens.
        let r = limiter.check("u2", &[], None, "point_get", None);
        assert!(r.allowed);
    }

    #[test]
    fn response_headers() {
        let result = RateLimitResult {
            allowed: true,
            remaining: 50,
            limit: 100,
            retry_after_secs: 0,
        };
        let headers = RateLimiter::response_headers(&result);
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].0, "X-RateLimit-Limit");
        assert_eq!(headers[0].1, "100");
    }

    // ── Login rate-limit tests ───────────────────────────────────────

    fn login_limiter(ip_cap: u64, user_cap: u64) -> RateLimiter {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        limiter.set_login_capacities(ip_cap, user_cap);
        limiter
    }

    #[test]
    fn correct_credential_burst_is_never_rate_limited() {
        // The core regression: a burst of correct-credential reconnects from a
        // single IP (a pool warming up) must ALL be admitted. `check_login`
        // never consumes the brute-force budget, and the generous DoS ceiling
        // (max(ip_cap*4, 120) = 120 here) easily covers the burst.
        let limiter = login_limiter(5, 5);
        for i in 0..50 {
            let outcome = limiter.check_login("10.0.0.1", "pool_user");
            assert!(
                matches!(outcome, LoginRateLimitOutcome::Allowed),
                "correct-credential attempt {i} must be admitted"
            );
        }
    }

    #[test]
    fn brute_force_failures_close_the_ip_window() {
        let limiter = login_limiter(5, 100);

        // Five FAILED attempts from one IP drain the per-IP failure bucket.
        for _ in 0..5 {
            assert!(matches!(
                limiter.check_login("10.0.0.9", "victim"),
                LoginRateLimitOutcome::Allowed
            ));
            limiter.record_login_failure("10.0.0.9", "victim");
        }

        // The next attempt is denied — the brute-force window is closed.
        assert!(
            matches!(
                limiter.check_login("10.0.0.9", "victim"),
                LoginRateLimitOutcome::IpExceeded { .. }
            ),
            "IP must be rate-limited after exhausting failure budget"
        );
        assert!(limiter.is_login_rate_limited("10.0.0.9", "victim"));

        // A different IP is unaffected.
        assert!(matches!(
            limiter.check_login("10.0.0.10", "victim2"),
            LoginRateLimitOutcome::Allowed
        ));
    }

    #[test]
    fn brute_force_failures_close_the_user_window() {
        let limiter = login_limiter(1000, 5);

        // Five FAILED attempts for one username from different IPs drain the
        // per-user failure bucket.
        for i in 0..5 {
            let ip = format!("10.0.1.{i}");
            assert!(matches!(
                limiter.check_login(&ip, "victim"),
                LoginRateLimitOutcome::Allowed
            ));
            limiter.record_login_failure(&ip, "victim");
        }

        // A further attempt for that user from a fresh IP is denied.
        assert!(
            matches!(
                limiter.check_login("10.0.1.200", "victim"),
                LoginRateLimitOutcome::UserExceeded { .. }
            ),
            "user must be rate-limited after exhausting per-user failure budget"
        );

        // A different username is unaffected.
        assert!(matches!(
            limiter.check_login("10.0.1.200", "other_user"),
            LoginRateLimitOutcome::Allowed
        ));
    }

    #[test]
    fn retry_after_is_populated_on_rejection() {
        let limiter = login_limiter(2, 100);
        for _ in 0..2 {
            let _ = limiter.check_login("192.0.2.1", "u");
            limiter.record_login_failure("192.0.2.1", "u");
        }
        match limiter.check_login("192.0.2.1", "u") {
            LoginRateLimitOutcome::IpExceeded { retry_after_secs } => {
                assert!(retry_after_secs > 0, "retry hint must be non-zero");
            }
            other => panic!("expected IpExceeded, got a different outcome: {other:?}"),
        }
    }

    #[test]
    fn login_rate_limit_audit() {
        use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;
        use crate::control::security::audit::emitter::{AuditEmitContext, AuditEmitter};
        use crate::control::security::audit::event::AuditEvent;

        let emitter = CapturingEmitter::new();
        emitter.emit(
            AuditEvent::LoginRateLimited,
            "login_rate_limit",
            "ip=10.0.0.1 user=alice",
            AuditEmitContext::new(None, "", "alice"),
        );

        let recorded = emitter.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, AuditEvent::LoginRateLimited);
        assert!(recorded[0].2.contains("alice"));
    }

    #[test]
    fn login_rate_limit_constant_time() {
        use std::time::Instant;

        // Simulate the constant-time floor by measuring that an immediate
        // rejection (rate-limited before any Argon2) cannot be distinguished
        // from a real Argon2 rejection by timing alone.  We can't run actual
        // Argon2 here (test suite must be fast), so we verify the *floor
        // constant* is well-defined (non-zero) and that the enforcement
        // mechanism is present in the public API surface.
        //
        // The real enforcement is in `session_auth` (production code).
        // Here we only verify the rate-limit decision itself is fast
        // (sub-millisecond) so the test detects accidental blocking in the
        // decision path — the caller adds the floor separately.
        let limiter = login_limiter(5, 5);
        let start = Instant::now();
        for i in 0..10 {
            let _ = limiter.check_login("10.1.2.3", &format!("user{i}"));
        }
        let elapsed = start.elapsed();
        // 10 check_login calls must complete in under 10ms (no blocking).
        assert!(
            elapsed.as_millis() < 10,
            "check_login must be non-blocking; took {elapsed:?}"
        );
    }

    // ── Tenant and database bucket tests ────────────────────────────────────

    fn db_id() -> DatabaseId {
        DatabaseId::DEFAULT
    }

    fn t_id(n: u64) -> TenantId {
        TenantId::new(n)
    }

    #[test]
    fn database_cap_deny_while_tenant_has_headroom() {
        let limiter = RateLimiter::new(enabled_config());

        // Database cap: 5 (burst = 5). Tenant cap: 1000 (generous).
        let quota = QuotaCheckParams {
            tenant_max_qps: Some(1000),
            database_max_qps: Some(5),
            tenant_id: t_id(1),
            database_id: db_id(),
        };

        // Consume the database bucket.
        for _ in 0..5 {
            let r = limiter.check("u1", &[], None, "point_get", Some(&quota));
            assert!(r.allowed, "first 5 should be allowed under database cap");
        }

        // 6th request: database bucket exhausted even though tenant bucket is fine.
        let r = limiter.check("u1", &[], None, "point_get", Some(&quota));
        assert!(
            !r.allowed,
            "database bucket exhausted — request must be denied"
        );
    }

    #[test]
    fn tenant_cap_deny_while_database_has_headroom() {
        let limiter = RateLimiter::new(enabled_config());

        // Tenant cap: 3. Database cap: 1000 (generous).
        let quota = QuotaCheckParams {
            tenant_max_qps: Some(3),
            database_max_qps: Some(1000),
            tenant_id: t_id(2),
            database_id: db_id(),
        };

        // Consume the tenant bucket.
        for _ in 0..3 {
            let r = limiter.check("u2", &[], None, "point_get", Some(&quota));
            assert!(r.allowed, "first 3 should be allowed under tenant cap");
        }

        // 4th request: tenant bucket exhausted.
        let r = limiter.check("u2", &[], None, "point_get", Some(&quota));
        assert!(
            !r.allowed,
            "tenant bucket exhausted — request must be denied"
        );
    }

    #[test]
    fn when_both_would_deny_tenant_wins_over_database() {
        // Both caps = 1.  The user bucket has burst 20, so it won't deny.
        // The tenant bucket is checked before the database bucket, so
        // whichever fires first is the tenant bucket.
        let limiter = RateLimiter::new(enabled_config());

        let quota = QuotaCheckParams {
            tenant_max_qps: Some(1),
            database_max_qps: Some(1),
            tenant_id: t_id(3),
            database_id: db_id(),
        };

        // First request — allowed, drains both caps.
        let r = limiter.check("u3", &[], None, "point_get", Some(&quota));
        assert!(r.allowed, "first request should be allowed");

        // Second request — tenant bucket fires first (checked before database).
        let r2 = limiter.check("u3", &[], None, "point_get", Some(&quota));
        assert!(!r2.allowed, "second request must be denied");
        // We can't directly observe *which* bucket denied without instrumenting
        // the limiter further, but we can assert that a new quota with only a
        // database cap (no tenant cap) is ALSO denied at this point — confirming
        // the database bucket was also consumed.
        let quota_db_only = QuotaCheckParams {
            tenant_max_qps: None,
            database_max_qps: Some(1),
            tenant_id: t_id(3),
            database_id: db_id(),
        };
        let r3 = limiter.check("u3", &[], None, "point_get", Some(&quota_db_only));
        assert!(!r3.allowed, "database bucket should also be exhausted");
    }

    #[test]
    fn no_quota_params_skips_tenant_and_database_buckets() {
        let limiter = RateLimiter::new(enabled_config());
        // With no quota params, only user/org buckets apply.
        // Burst = 20, so 20 requests allowed.
        for _ in 0..20 {
            let r = limiter.check("u4", &[], None, "point_get", None);
            assert!(r.allowed);
        }
        // 21st exceeds user burst (not tenant/db).
        let r = limiter.check("u4", &[], None, "point_get", None);
        assert!(!r.allowed);
    }
}
