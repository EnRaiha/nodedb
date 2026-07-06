// SPDX-License-Identifier: BUSL-1.1

//! Pre-authentication login admission control.
//!
//! Separated from the general per-identity QPS limiter because it has a
//! distinct correctness model: the brute-force budget must be driven by
//! *failed* verifies, never by attempt volume, so a burst of correct-credential
//! reconnects (a connection pool warming up) is never rejected. Alongside that,
//! a generous per-IP ceiling bounds the CPU an attacker can spend forcing
//! expensive password verifications (Argon2 DoS).
//!
//! Owns its own bucket map so per-IP login keys — which are attacker-influenced
//! and therefore unbounded — can be evicted independently of the (bounded)
//! per-tenant QPS buckets.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use super::bucket::TokenBucket;
use super::limiter::LoginRateLimitOutcome;

/// Cap on the number of distinct login buckets kept resident. When the map
/// grows past this, buckets that have fully refilled (and therefore carry no
/// outstanding penalty — dropping one is indistinguishable from never having
/// created it) are evicted on the next write-locked insert. This bounds the
/// per-IP / per-user bucket map so long-uptime daemons don't accumulate an
/// unbounded set of idle entries.
const MAX_TRACKED_BUCKETS: usize = 100_000;

/// Login admission limiter: brute-force failure windows + Argon2 DoS ceiling.
pub(super) struct LoginLimiter {
    /// Login-specific token buckets, keyed by
    /// `login_fail_ip:{addr}` / `login_fail_user:{user}` / `login_dos_ip:{addr}`.
    buckets: RwLock<HashMap<String, TokenBucket>>,
    /// Maximum FAILED login attempts per IP per minute before the brute-force
    /// window closes for that source (0 = disabled). Consumed only by genuine
    /// credential failures, so correct-credential bursts never trip it.
    ip_cap: AtomicU64,
    /// Maximum FAILED login attempts per username per minute before the
    /// brute-force window closes for that account (0 = disabled).
    user_cap: AtomicU64,
}

impl LoginLimiter {
    pub(super) fn new() -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
            ip_cap: AtomicU64::new(30),
            user_cap: AtomicU64::new(10),
        }
    }

    /// Update the per-IP and per-username brute-force failure capacities.
    /// Takes effect for new buckets created after this call.
    pub(super) fn set_capacities(&self, ip_cap: u64, user_cap: u64) {
        self.ip_cap.store(ip_cap, Ordering::Relaxed);
        self.user_cap.store(user_cap, Ordering::Relaxed);
    }

    /// Current `(ip_cap, user_cap)` for inspection / metrics.
    pub(super) fn capacities(&self) -> (u64, u64) {
        (
            self.ip_cap.load(Ordering::Relaxed),
            self.user_cap.load(Ordering::Relaxed),
        )
    }

    /// Pre-authentication admission check.
    ///
    /// Neither branch consumes the brute-force budget for a legitimate attempt:
    ///
    /// 1. **Brute-force gate (peek only).** The per-IP `login_fail_ip:{addr}`
    ///    and per-user `login_fail_user:{username}` buckets are *peeked*
    ///    (non-consuming). They only ever drain via
    ///    [`record_failure`](Self::record_failure) on a genuine credential
    ///    failure, so a burst of correct-credential reconnects never empties
    ///    them. A drained bucket means a prior burst of *failed* attempts — the
    ///    actual brute-force window — and admission is denied until it refills.
    /// 2. **Argon2 DoS ceiling (consume).** A generous per-IP
    ///    `login_dos_ip:{addr}` bucket is consumed one token per attempt,
    ///    bounding the number of expensive verifications an attacker can force
    ///    from one source regardless of credential correctness. Its capacity is
    ///    `max(ip_cap * 4, 120)` per minute — generous enough for legitimate
    ///    pool bursts, low enough to bound a flood.
    pub(super) fn check(&self, peer_addr: &str, username: &str) -> LoginRateLimitOutcome {
        let ip_cap = self.ip_cap.load(Ordering::Relaxed);
        // 0-cap means login rate limiting is disabled entirely.
        if ip_cap == 0 {
            return LoginRateLimitOutcome::Allowed;
        }
        let user_cap = self.user_cap.load(Ordering::Relaxed);

        // 1. Brute-force gate: peek the failure buckets without consuming.
        if let Some(retry_after_secs) = self.peek_empty(&format!("login_fail_ip:{peer_addr}")) {
            return LoginRateLimitOutcome::IpExceeded { retry_after_secs };
        }
        if user_cap > 0
            && !username.is_empty()
            && let Some(retry_after_secs) = self.peek_empty(&format!("login_fail_user:{username}"))
        {
            return LoginRateLimitOutcome::UserExceeded { retry_after_secs };
        }

        // 2. Argon2 DoS ceiling: consume one token per attempt.
        let dos_cap = Self::dos_ceiling(ip_cap);
        let dos_rate = (dos_cap as f64) / 60.0;
        if let Some(retry_after_secs) =
            self.consume(&format!("login_dos_ip:{peer_addr}"), dos_cap, dos_rate)
        {
            return LoginRateLimitOutcome::IpExceeded { retry_after_secs };
        }

        LoginRateLimitOutcome::Allowed
    }

    /// Record a genuine credential FAILURE for brute-force accounting.
    ///
    /// Consumes one token from the per-IP and (when supplied) per-username
    /// failure buckets. Must be called from the *same* place that drives the
    /// credential lockout counter — only after a real wrong-credential verify,
    /// never on success and never on a policy rejection (which may carry a
    /// correct password). Draining these buckets closes the brute-force window
    /// consulted by [`check`](Self::check).
    pub(super) fn record_failure(&self, peer_addr: &str, username: &str) {
        let ip_cap = self.ip_cap.load(Ordering::Relaxed);
        if ip_cap == 0 {
            return;
        }
        let ip_rate = (ip_cap as f64) / 60.0;
        let _ = self.consume(&format!("login_fail_ip:{peer_addr}"), ip_cap, ip_rate);

        let user_cap = self.user_cap.load(Ordering::Relaxed);
        if user_cap > 0 && !username.is_empty() {
            let user_rate = (user_cap as f64) / 60.0;
            let _ = self.consume(&format!("login_fail_user:{username}"), user_cap, user_rate);
        }
    }

    /// Non-consuming check of whether [`check`](Self::check) would currently
    /// reject `(peer_addr, username)`.
    ///
    /// Used by the pgwire SCRAM failure arm to avoid double-counting: a SASL
    /// failure actually caused by admission rejection (not a wrong client proof)
    /// must not move the brute-force / lockout counters.
    pub(super) fn is_rate_limited(&self, peer_addr: &str, username: &str) -> bool {
        let ip_cap = self.ip_cap.load(Ordering::Relaxed);
        if ip_cap == 0 {
            return false;
        }
        if self
            .peek_empty(&format!("login_fail_ip:{peer_addr}"))
            .is_some()
        {
            return true;
        }
        let user_cap = self.user_cap.load(Ordering::Relaxed);
        if user_cap > 0
            && !username.is_empty()
            && self
                .peek_empty(&format!("login_fail_user:{username}"))
                .is_some()
        {
            return true;
        }
        self.peek_empty(&format!("login_dos_ip:{peer_addr}"))
            .is_some()
    }

    /// Number of resident login buckets (for metrics).
    pub(super) fn active_buckets(&self) -> usize {
        self.buckets.read().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Generous per-IP Argon2 DoS ceiling (attempts per minute) derived from the
    /// brute-force failure cap: high enough for a legitimate connection-pool
    /// burst, low enough to bound the CPU an attacker can spend on password
    /// verification from one source.
    fn dos_ceiling(ip_cap: u64) -> u64 {
        ip_cap.saturating_mul(4).max(120)
    }

    /// Peek a bucket without consuming a token.
    ///
    /// Returns `Some(retry_after_secs)` when the bucket exists and is currently
    /// empty, `None` otherwise (including when the bucket has never been
    /// created — an untouched identity is never rate-limited).
    fn peek_empty(&self, key: &str) -> Option<u64> {
        let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
        let bucket = buckets.get(key)?;
        if bucket.available() < 1 {
            Some((bucket.retry_after_ms() / 1000).max(1))
        } else {
            None
        }
    }

    /// Consume one token from a login bucket, creating it if absent.
    ///
    /// Returns `None` when a token was acquired (allowed) or
    /// `Some(retry_after_secs)` when the bucket was empty (rejected).
    fn consume(&self, key: &str, capacity: u64, rate_per_sec: f64) -> Option<u64> {
        // Fast path: read-only check.
        {
            let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
            if let Some(bucket) = buckets.get(key) {
                return if bucket.try_acquire(1) {
                    None
                } else {
                    Some((bucket.retry_after_ms() / 1000).max(1))
                };
            }
        }
        // Slow path: create bucket, evicting fully-refilled idle buckets first
        // if the map has grown past its resident cap.
        let mut buckets = self.buckets.write().unwrap_or_else(|p| p.into_inner());
        if buckets.len() > MAX_TRACKED_BUCKETS {
            buckets.retain(|_, b| b.available() < b.capacity());
        }
        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(capacity, rate_per_sec));
        if bucket.try_acquire(1) {
            None
        } else {
            Some((bucket.retry_after_ms() / 1000).max(1))
        }
    }
}

impl Default for LoginLimiter {
    fn default() -> Self {
        Self::new()
    }
}
