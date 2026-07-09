// SPDX-License-Identifier: BUSL-1.1

//! Circuit-breaker for OLLP per-predicate-class retry storms.
//!
//! States:
//!   Closed → HalfOpen → Open → HalfOpen → Closed
//!
//! The `TokenWindow` tracks (success, retry) events in a sliding window.
//! When `retry / (retry + success) >= threshold_pct / 100` the circuit
//! trips to Open. After `open_duration` it transitions to HalfOpen; one
//! probe submission is allowed.  After `close_successes` consecutive
//! successes in HalfOpen, the circuit closes.
//!
//! `Instant::now()` is used for circuit timers (observability / off-WAL
//! path only).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

// ── TokenWindow ────────────────────────────────────────────────────────────────

/// A sliding-window event tracker that records (success, retry) pairs.
struct TokenWindow {
    /// Events: `true` = retry, `false` = success.
    events: VecDeque<(Instant, bool)>,
    window: Duration,
    capacity: usize,
}

impl TokenWindow {
    fn new(capacity: usize, window: Duration) -> Self {
        Self {
            events: VecDeque::new(),
            window,
            capacity,
        }
    }

    /// Record an event and evict expired ones.
    fn record(&mut self, is_retry: bool) {
        // no-determinism: OLLP circuit breaker observability, not Calvin WAL data
        let now = Instant::now();
        self.evict_old(now);
        // If at capacity, evict the oldest.
        if self.events.len() >= self.capacity {
            self.events.pop_front();
        }
        self.events.push_back((now, is_retry));
    }

    /// Retry ratio within the window: `retries / total`.  Returns 0.0 if empty.
    fn retry_ratio(&self) -> f64 {
        let total = self.events.len();
        if total == 0 {
            return 0.0;
        }
        let retries = self.events.iter().filter(|(_, r)| *r).count();
        retries as f64 / total as f64
    }

    fn evict_old(&mut self, now: Instant) {
        while let Some(&(ts, _)) = self.events.front() {
            if now.duration_since(ts) > self.window {
                self.events.pop_front();
            } else {
                break;
            }
        }
    }
}

// ── CircuitState ───────────────────────────────────────────────────────────────

/// Circuit breaker state.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CircuitState {
    /// Normal operation — submissions allowed.
    Closed,
    /// One probe submission allowed; observing recovery.
    HalfOpen,
    /// All submissions rejected.
    Open,
}

// ── CircuitBreaker ─────────────────────────────────────────────────────────────

/// Per-predicate-class circuit breaker.
pub struct CircuitBreaker {
    state: CircuitState,
    window: TokenWindow,
    threshold_pct: u8,
    open_duration: Duration,
    close_successes: u32,
    /// Wall-clock time when the circuit entered Open state.
    ///
    /// `Instant::now()` — observability / off-WAL path only.
    opened_at: Option<Instant>,
    /// Consecutive successes recorded in HalfOpen state.
    half_open_successes: u32,
}

impl CircuitBreaker {
    pub fn new(
        capacity: usize,
        window: Duration,
        threshold_pct: u8,
        open_duration: Duration,
        close_successes: u32,
    ) -> Self {
        Self {
            state: CircuitState::Closed,
            window: TokenWindow::new(capacity, window),
            threshold_pct,
            open_duration,
            close_successes,
            opened_at: None,
            half_open_successes: 0,
        }
    }

    /// Current circuit state (re-evaluates Open → HalfOpen transition).
    ///
    /// `Instant::now()` — observability / off-WAL path only.
    pub fn state(&mut self) -> CircuitState {
        if self.state == CircuitState::Open
            && let Some(opened) = self.opened_at
            // no-determinism: OLLP circuit state timeout check, not WAL data
            && Instant::now().duration_since(opened) >= self.open_duration
        {
            self.state = CircuitState::HalfOpen;
            self.half_open_successes = 0;
        }
        self.state
    }

    /// Record a successful submission.
    pub fn record_success(&mut self) {
        self.window.record(false);
        if self.state == CircuitState::HalfOpen {
            self.half_open_successes += 1;
            if self.half_open_successes >= self.close_successes {
                self.state = CircuitState::Closed;
                self.opened_at = None;
            }
        }
    }

    /// Record a retry (OLLP verification failed).
    ///
    /// If the retry ratio in the window exceeds `threshold_pct`, the
    /// circuit trips to Open.
    pub fn record_retry(&mut self) {
        self.window.record(true);
        if self.state == CircuitState::Closed {
            let ratio = self.window.retry_ratio();
            let threshold = f64::from(self.threshold_pct) / 100.0;
            if ratio >= threshold {
                self.state = CircuitState::Open;
                // no-determinism: OLLP circuit trip timestamp, not Calvin WAL data
                self.opened_at = Some(Instant::now());
            }
        } else if self.state == CircuitState::HalfOpen {
            // Retry in HalfOpen: trip back to Open immediately.
            self.state = CircuitState::Open;
            // no-determinism: OLLP circuit trip timestamp, not Calvin WAL data
            self.opened_at = Some(Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_breaker() -> CircuitBreaker {
        CircuitBreaker::new(
            256,
            Duration::from_secs(60),
            50,                       // 50%
            Duration::from_millis(1), // short for tests
            4,
        )
    }

    #[test]
    fn circuit_breaker_opens_at_50pct_retry_ratio() {
        let mut cb = make_breaker();
        // 5 successes + 5 retries = 50% → should trip at the 6th retry
        // when the window has 10 events with 5/10 retries.
        // Actually with threshold = 50%, ratio >= 0.5 trips: 5/10 = 0.5.
        for _ in 0..5 {
            cb.record_success();
        }
        // At 4 retries: 4/9 = 0.44 < 0.5 — still closed.
        for _ in 0..4 {
            cb.record_retry();
        }
        assert_eq!(cb.state(), CircuitState::Closed);
        // 5th retry: 5/10 = 0.5 >= 0.5 → open.
        cb.record_retry();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn circuit_breaker_half_opens_after_window() {
        // The open window must comfortably exceed the time it takes to force the
        // breaker open, or the window elapses mid-setup under parallel test load
        // and the breaker reads HalfOpen before the Open assertion runs.
        const OPEN_WINDOW: Duration = Duration::from_millis(100);
        let mut cb = CircuitBreaker::new(256, Duration::from_secs(60), 50, OPEN_WINDOW, 4);
        // Force open.
        for _ in 0..10 {
            cb.record_retry();
        }
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(OPEN_WINDOW + Duration::from_millis(50));

        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn circuit_breaker_closes_after_4_successes() {
        let mut cb = CircuitBreaker::new(
            256,
            Duration::from_secs(60),
            50,
            Duration::from_millis(1),
            4,
        );
        // Force open.
        for _ in 0..10 {
            cb.record_retry();
        }
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // 3 successes: still HalfOpen.
        for _ in 0..3 {
            cb.record_success();
        }
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // 4th success: closes.
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
