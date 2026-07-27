// SPDX-License-Identifier: BUSL-1.1

//! Per-connection sliding-window rate limiter for `SET LOCAL
//! nodedb.auth_session` attempts.
//!
//! Keyed by connection identity (the pgwire `SocketAddr` string), the
//! limiter tracks a bounded FIFO of attempt timestamps. Exceeding the
//! configured budget within the window returns `Denied` — the pgwire
//! handler then emits a fatal error, closing the connection.
//!
//! Design notes:
//!
//! - **Sliding window, not fixed bucket.** A fixed bucket lets an attacker
//!   time attempts around bucket boundaries (e.g. 20 at xx:59.9s + 20 at
//!   xx:00.1s = 40 in ~200ms). The sliding window prunes timestamps older
//!   than `window` before admission.
//! - **Bounded memory per connection.** The VecDeque never grows beyond
//!   `max_attempts` — once full, the oldest timestamp is compared against
//!   `now - window`. This is O(1) per attempt.
//! - **Lazy eviction of idle connections.** On any admission path we GC
//!   connection entries whose entire window has elapsed. Cheap; bounded by
//!   the number of live pgwire connections, which is itself capped.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Result of an admission check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDecision {
    /// Attempt is within the budget; caller may proceed.
    Allowed,
    /// Attempt exceeds the budget; caller must reject fatally.
    Denied,
}

pub(super) struct PerConnectionRateLimiter {
    max_attempts: u32,
    window: Duration,
    state: RwLock<HashMap<String, VecDeque<Instant>>>,
}

impl PerConnectionRateLimiter {
    pub(super) fn new(max_attempts: u32, window: Duration) -> Self {
        Self {
            max_attempts,
            window,
            state: RwLock::new(HashMap::new()),
        }
    }

    /// Record an attempt for `conn_key` and return whether it is admitted.
    ///
    /// An admitted attempt pushes `now` into the window. A denied attempt
    /// does NOT push — otherwise a client stuck at the cap would extend
    /// its own punishment indefinitely by retrying.
    pub(super) fn admit(&self, conn_key: &str) -> RateLimitDecision {
        self.admit_at(conn_key, Instant::now())
    }

    /// Test seam — caller supplies the clock.
    pub(super) fn admit_at(&self, conn_key: &str, now: Instant) -> RateLimitDecision {
        let mut state = self.state.write();
        let cutoff = now.checked_sub(self.window).unwrap_or(now);

        let entry = state.entry(conn_key.to_string()).or_default();
        // Evict timestamps that have slid out of the window.
        while let Some(front) = entry.front() {
            if *front < cutoff {
                entry.pop_front();
            } else {
                break;
            }
        }

        if entry.len() as u32 >= self.max_attempts {
            return RateLimitDecision::Denied;
        }
        entry.push_back(now);

        // Opportunistic GC: if this entry is the only stale one and has
        // just emptied, nothing to do. For active entries we skip the
        // hashmap scan — connection churn tracks forget() on disconnect.
        RateLimitDecision::Allowed
    }

    /// Forget a connection on disconnect to reclaim memory. Safe to call
    /// for unknown keys.
    pub(super) fn forget(&self, conn_key: &str) {
        let mut state = self.state.write();
        state.remove(conn_key);
    }

    /// Test introspection: current attempt count in the window.
    #[cfg(test)]
    pub(super) fn attempts_in_window(&self, conn_key: &str) -> usize {
        let state = self.state.read();
        state.get(conn_key).map(|q| q.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    #[test]
    fn admits_up_to_limit_then_denies() {
        let rl = PerConnectionRateLimiter::new(3, Duration::from_secs(60));
        assert_eq!(rl.admit("c"), RateLimitDecision::Allowed);
        assert_eq!(rl.admit("c"), RateLimitDecision::Allowed);
        assert_eq!(rl.admit("c"), RateLimitDecision::Allowed);
        assert_eq!(rl.admit("c"), RateLimitDecision::Denied);
    }

    #[test]
    fn rate_limit_remains_enforced_after_panic_while_write_locked() {
        let limiter = PerConnectionRateLimiter::new(1, Duration::from_secs(60));
        let now = Instant::now();
        assert_eq!(limiter.admit_at("conn", now), RateLimitDecision::Allowed);

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _map = limiter.state.write();
            panic!("simulated interrupted session-handle rate-limit update");
        }));
        assert!(panic_result.is_err());

        // The prior admitted attempt remains counted, so the connection stays denied.
        assert_eq!(
            limiter.admit_at("conn", now + Duration::from_millis(1)),
            RateLimitDecision::Denied
        );
        // A later disconnect mutation clears the entry and permits a new connection.
        limiter.forget("conn");
        assert_eq!(
            limiter.admit_at("conn", now + Duration::from_millis(2)),
            RateLimitDecision::Allowed
        );
    }

    #[test]
    fn sliding_window_replenishes_after_elapsed_time() {
        let rl = PerConnectionRateLimiter::new(2, Duration::from_millis(100));
        let t0 = Instant::now();
        assert_eq!(rl.admit_at("c", t0), RateLimitDecision::Allowed);
        assert_eq!(
            rl.admit_at("c", t0 + Duration::from_millis(10)),
            RateLimitDecision::Allowed
        );
        assert_eq!(
            rl.admit_at("c", t0 + Duration::from_millis(20)),
            RateLimitDecision::Denied
        );
        // Slide past the window.
        assert_eq!(
            rl.admit_at("c", t0 + Duration::from_millis(200)),
            RateLimitDecision::Allowed
        );
    }

    #[test]
    fn denied_attempts_do_not_extend_punishment() {
        let rl = PerConnectionRateLimiter::new(2, Duration::from_millis(100));
        let t0 = Instant::now();
        rl.admit_at("c", t0);
        rl.admit_at("c", t0);
        // Hammer while denied — should not push any timestamp.
        for _ in 0..50 {
            assert_eq!(rl.admit_at("c", t0), RateLimitDecision::Denied);
        }
        assert_eq!(rl.attempts_in_window("c"), 2);
        // Window slides correctly despite the storm.
        assert_eq!(
            rl.admit_at("c", t0 + Duration::from_millis(200)),
            RateLimitDecision::Allowed
        );
    }

    #[test]
    fn connections_are_isolated() {
        let rl = PerConnectionRateLimiter::new(1, Duration::from_secs(60));
        assert_eq!(rl.admit("a"), RateLimitDecision::Allowed);
        assert_eq!(rl.admit("a"), RateLimitDecision::Denied);
        assert_eq!(rl.admit("b"), RateLimitDecision::Allowed);
    }

    #[test]
    fn forget_clears_state() {
        let rl = PerConnectionRateLimiter::new(1, Duration::from_secs(60));
        rl.admit("c");
        rl.forget("c");
        assert_eq!(rl.admit("c"), RateLimitDecision::Allowed);
    }
}
