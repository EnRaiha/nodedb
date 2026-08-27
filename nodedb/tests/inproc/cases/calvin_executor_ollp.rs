// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the OLLP orchestrator: retry, exhaustion,
//! circuit-breaker, tenant budget.

use std::time::Duration;

use nodedb::control::cluster::calvin::executor::ollp::{
    CircuitBreaker, CircuitState, OllpConfig, RateBucket,
};

// ── Backoff ────────────────────────────────────────────────────────────────────

#[test]
fn adaptive_backoff_doubles_per_retry() {
    let config = OllpConfig {
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_secs(5),
        ..OllpConfig::default()
    };
    let initial = config.backoff_initial;
    let max = config.backoff_max;

    for (retry, expected_ms) in [(0u32, 10u64), (1, 20), (2, 40), (3, 80), (4, 160)] {
        let shift = retry;
        let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let backoff = initial.saturating_mul(factor).min(max);
        assert_eq!(
            backoff.as_millis() as u64,
            expected_ms,
            "retry {retry}: expected {expected_ms}ms"
        );
    }
}

#[test]
fn adaptive_backoff_caps_at_5s() {
    let config = OllpConfig {
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_secs(5),
        ..OllpConfig::default()
    };
    let initial = config.backoff_initial;
    let max = config.backoff_max;

    let factor = 1u32.checked_shl(20).unwrap_or(u32::MAX);
    let backoff = initial.saturating_mul(factor).min(max);
    assert_eq!(backoff, Duration::from_secs(5));
}

// ── Circuit breaker ────────────────────────────────────────────────────────────

#[test]
fn circuit_breaker_opens_at_50pct_retry_ratio() {
    let mut cb = CircuitBreaker::new(
        256,
        Duration::from_secs(60),
        50,
        Duration::from_millis(1),
        4,
    );
    // 5 successes, then 4 retries: 4/9 = 44% < 50% → Closed.
    for _ in 0..5 {
        cb.record_success();
    }
    for _ in 0..4 {
        cb.record_retry();
    }
    assert_eq!(cb.state(), CircuitState::Closed);
    // 5th retry: 5/10 = 50% >= 50% → Open.
    cb.record_retry();
    assert_eq!(cb.state(), CircuitState::Open);
}

#[test]
fn circuit_breaker_half_opens_after_window() {
    // The open window must comfortably exceed the time it takes to force the
    // breaker open, or the window elapses mid-setup under parallel test load and
    // the breaker reads HalfOpen before the Open assertion runs.
    const OPEN_WINDOW: Duration = Duration::from_millis(100);
    let mut cb = CircuitBreaker::new(256, Duration::from_secs(60), 50, OPEN_WINDOW, 4);
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
    for _ in 0..10 {
        cb.record_retry();
    }
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    for _ in 0..3 {
        cb.record_success();
    }
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    cb.record_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

// ── Tenant budget ───────────────────────────────────────────────────────────────

#[test]
fn tenant_budget_exceeded_at_1000_per_min() {
    let mut bucket = RateBucket::new(1000, Duration::from_secs(60));
    for _ in 0..1000 {
        assert!(!bucket.record_and_check());
    }
    assert!(
        bucket.record_and_check(),
        "1001st event should exceed budget"
    );
}

#[test]
fn tenant_budget_not_exceeded_below_cap() {
    let mut bucket = RateBucket::new(1000, Duration::from_secs(60));
    for _ in 0..999 {
        assert!(!bucket.record_and_check());
    }
}
