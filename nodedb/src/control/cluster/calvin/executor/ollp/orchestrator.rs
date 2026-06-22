// SPDX-License-Identifier: BUSL-1.1

//! OLLP retry orchestrator.
//!
//! Orchestrates the optimistic retry loop for dependent-read Calvin
//! transactions.  The caller provides a `tx_builder` closure that builds a
//! `TxClass` from a fresh optimistic pre-execution result; the orchestrator
//! submits it to the sequencer inbox and retries on `OllpRetryRequired` up
//! to `ollp_max_retries` times with adaptive backoff.
//!
//! # Circuit breaker and tenant budget
//!
//! Per-predicate-class circuit breakers and per-tenant sliding-window
//! budgets are checked before each retry attempt.
//!
//! # Determinism note
//!
//! `Instant::now()` is used for circuit-breaker timers and rate-bucket
//! windows only (observability; off-WAL path).  No `SystemTime::now()`,
//! no `Uuid::new_v4()`, no unseeded RNG.
//!
//! All maps in this module use `BTreeMap` (determinism contract).

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::sleep;

use nodedb_cluster::calvin::sequencer::inbox::Inbox;
use nodedb_cluster::calvin::types::TxClass;
use nodedb_types::TenantId;

use super::circuit_breaker::{CircuitBreaker, CircuitState};
use super::config::OllpConfig;
use super::error::OllpError;
use super::metrics::{
    CIRCUIT_CLOSED, CIRCUIT_HALF_OPEN, CIRCUIT_OPEN, OUTCOME_CIRCUIT_OPEN, OUTCOME_RETRIED,
    OUTCOME_SUCCEEDED, OllpMetrics,
};
use super::rate_bucket::RateBucket;
use crate::control::planner::calvin::submit::RoutedAssignment;
use nodedb_cluster::error::CalvinError;

// ── OllpRetryRequired ─────────────────────────────────────────────────────────

/// Opaque status returned by the SPSC response path when the active executor
/// detects that the declared predicate no longer matches the current engine
/// state.
///
/// The orchestrator interprets this status and retries.
pub const OLLP_RETRY_REQUIRED_CODE: u16 = 0x_CAAD; // Calvin Adaptive, needs re-Dispatch

// ── OllpOrchestrator ─────────────────────────────────────────────────────────

/// Orchestrates OLLP retry loops with adaptive backoff, circuit breaking,
/// and per-tenant budgets.
///
/// One `OllpOrchestrator` is shared (Arc'd) across all OLLP-eligible
/// connections on the Control Plane.
pub struct OllpOrchestrator {
    config: OllpConfig,
    metrics: OllpMetrics,
    /// Per-predicate-class circuit breakers.
    /// `BTreeMap` for determinism.
    circuit_breakers: std::sync::Mutex<BTreeMap<u64, CircuitBreaker>>,
    /// Per-tenant retry rate buckets.
    /// `BTreeMap` for determinism.
    tenant_budgets: std::sync::Mutex<BTreeMap<u64, RateBucket>>,
}

impl OllpOrchestrator {
    /// Create a new orchestrator with the given config.
    pub fn new(config: OllpConfig) -> Self {
        Self {
            config,
            metrics: OllpMetrics::default(),
            circuit_breakers: std::sync::Mutex::new(BTreeMap::new()),
            tenant_budgets: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// Circuit-breaker + tenant-budget admission gate shared by both submit
    /// variants.
    ///
    /// Checks the per-predicate circuit state (recording the state metric),
    /// returns `Err(OllpError::CircuitOpen)` if the circuit is open, builds
    /// `TxClass` via `tx_builder`, and ensures the per-tenant rate bucket
    /// exists.  On success returns the built `TxClass` ready to submit.
    fn check_and_build(
        &self,
        predicate_class: u64,
        tenant_id: TenantId,
        tx_builder: impl Fn() -> Result<TxClass, CalvinError>,
    ) -> Result<TxClass, OllpError> {
        // Check circuit state first — denies submission entirely when open.
        let circuit_state = {
            let mut breakers = self
                .circuit_breakers
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let cb = breakers.entry(predicate_class).or_insert_with(|| {
                CircuitBreaker::new(
                    self.config.circuit_capacity,
                    self.config.circuit_window,
                    self.config.circuit_threshold_pct,
                    self.config.circuit_open_duration,
                    self.config.circuit_close_successes,
                )
            });
            let state = cb.state();
            let state_code = match state {
                CircuitState::Closed => CIRCUIT_CLOSED,
                CircuitState::HalfOpen => CIRCUIT_HALF_OPEN,
                CircuitState::Open => CIRCUIT_OPEN,
            };
            self.metrics
                .record_circuit_state(predicate_class, state_code);
            state
        };

        if circuit_state == CircuitState::Open {
            self.metrics
                .record_retry_outcome(predicate_class, OUTCOME_CIRCUIT_OPEN);
            return Err(OllpError::CircuitOpen { predicate_class });
        }

        // Build the txn class from the caller's closure.
        let tx_class = tx_builder().map_err(|e| match e {
            CalvinError::Sequencer(s) => OllpError::Sequencer(s),
            _other => OllpError::Sequencer(
                nodedb_cluster::calvin::sequencer::error::SequencerError::Unavailable,
            ),
        })?;

        // Ensure tenant budget bucket exists. Budget consumption happens in
        // `on_retry_required` since first-attempt admissions don't count.
        {
            let mut budgets = self
                .tenant_budgets
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            budgets.entry(tenant_id.as_u64()).or_insert_with(|| {
                RateBucket::new(
                    self.config.tenant_budget_per_minute,
                    Duration::from_secs(60),
                )
            });
        }

        Ok(tx_class)
    }

    /// Record a successful submit in the circuit window and emit the success
    /// metric.  Called by both submit variants after a successful delivery.
    fn record_submit_success(&self, predicate_class: u64) {
        {
            let mut breakers = self
                .circuit_breakers
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(cb) = breakers.get_mut(&predicate_class) {
                cb.record_success();
            }
        }
        self.metrics
            .record_retry_outcome(predicate_class, OUTCOME_SUCCEEDED);
    }

    /// Submit a dependent-read transaction with OLLP retry.
    ///
    /// `tx_builder` is called once per attempt to build a fresh `TxClass`
    /// from the latest optimistic pre-execution result.
    ///
    /// Returns the `inbox_seq` of the successfully submitted txn, or an
    /// `OllpError`.
    pub async fn submit_with_retry(
        &self,
        inbox: &Inbox,
        predicate_class: u64,
        tenant_id: TenantId,
        tx_builder: impl Fn() -> Result<TxClass, CalvinError>,
    ) -> Result<u64, OllpError> {
        // Single-attempt admission gate. Retries are driven externally by the
        // SPSC response handler calling `on_retry_required` followed by another
        // `submit_with_retry` call from the SQL layer with a fresh `tx_builder`
        // closure; the budget + circuit checks below run on every attempt
        // because they are stored on `self` and persist across calls.
        let tx_class = self.check_and_build(predicate_class, tenant_id, tx_builder)?;

        // Submit to the inbox.
        match inbox.submit(tx_class) {
            Ok(inbox_seq) => {
                self.record_submit_success(predicate_class);
                Ok(inbox_seq)
            }
            Err(e) => Err(OllpError::Sequencer(e)),
        }
    }

    /// Submit a dependent-read transaction with OLLP retry, delegating the
    /// actual submit step to an injected `submit_fn`.
    ///
    /// Sibling of [`Self::submit_with_retry`]: the circuit-breaker admission gate
    /// and per-tenant budget bookkeeping are identical (via [`Self::check_and_build`]),
    /// but instead of submitting to a local `Inbox` this calls `submit_fn(tx_class)`.
    /// That lets a non-leader coordinator run the dependent path by routing the submit
    /// to the sequencer-group leader (returning the leader-assigned
    /// [`RoutedAssignment`]) while still passing through this node's
    /// circuit-breaker / budget gate.
    ///
    /// On `Ok(assignment)` the per-predicate circuit success is recorded (same as
    /// [`Self::submit_with_retry`]) and the assignment is returned. On `Err` the
    /// underlying `OllpError` is returned verbatim (not re-mapped).
    pub async fn submit_with_retry_via<F, Fut>(
        &self,
        predicate_class: u64,
        tenant_id: TenantId,
        tx_builder: impl Fn() -> Result<TxClass, CalvinError>,
        submit_fn: F,
    ) -> Result<RoutedAssignment, OllpError>
    where
        F: Fn(TxClass) -> Fut,
        Fut: std::future::Future<Output = Result<RoutedAssignment, OllpError>>,
    {
        let tx_class = self.check_and_build(predicate_class, tenant_id, tx_builder)?;

        // Submit via the injected routed-submit closure.
        match submit_fn(tx_class).await {
            Ok(assignment) => {
                self.record_submit_success(predicate_class);
                Ok(assignment)
            }
            Err(ollp_err) => Err(ollp_err),
        }
    }

    /// Return the configured maximum retry count for OLLP transactions.
    pub fn ollp_max_retries(&self) -> u8 {
        self.config.ollp_max_retries.min(u8::MAX as u32) as u8
    }

    /// Emit OLLP metrics in Prometheus text exposition format.
    ///
    /// Called from the node's Prometheus scrape endpoint to include OLLP
    /// observability alongside the scheduler and sequencer metrics.
    pub fn render_prometheus(&self) -> String {
        self.metrics.render_prometheus()
    }

    /// Called by the SPSC response handler when `OllpRetryRequired` status
    /// is returned by the active executor.
    ///
    /// Applies adaptive backoff and records the retry in the circuit window.
    pub async fn on_retry_required(&self, predicate_class: u64, retry_count: u32) {
        // Record in circuit window.
        {
            let mut breakers = self
                .circuit_breakers
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let cb = breakers.entry(predicate_class).or_insert_with(|| {
                CircuitBreaker::new(
                    self.config.circuit_capacity,
                    self.config.circuit_window,
                    self.config.circuit_threshold_pct,
                    self.config.circuit_open_duration,
                    self.config.circuit_close_successes,
                )
            });
            cb.record_retry();
        }

        // Adaptive backoff: min(initial * 2^retry, max).
        let backoff = {
            let shift = retry_count.min(31);
            let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
            let multiplied = self.config.backoff_initial.saturating_mul(factor);
            multiplied.min(self.config.backoff_max)
        };

        let backoff_ms = backoff.as_millis() as u64;
        self.metrics.record_backoff_ms(predicate_class, backoff_ms);
        self.metrics
            .record_retry_outcome(predicate_class, OUTCOME_RETRIED);

        sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn adaptive_backoff_doubles_per_retry() {
        let config = OllpConfig {
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_secs(5),
            ..OllpConfig::default()
        };
        let initial = config.backoff_initial;
        let max = config.backoff_max;

        // retry 0 → 10 ms * 2^0 = 10 ms
        let shift = 0u32;
        let factor0 = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let backoff0 = initial.saturating_mul(factor0).min(max);
        assert_eq!(backoff0, Duration::from_millis(10));

        // retry 1 → 10 ms * 2^1 = 20 ms
        let shift = 1u32;
        let factor1 = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let backoff1 = initial.saturating_mul(factor1).min(max);
        assert_eq!(backoff1, Duration::from_millis(20));

        // retry 2 → 10 ms * 2^2 = 40 ms
        let shift = 2u32;
        let factor2 = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let backoff2 = initial.saturating_mul(factor2).min(max);
        assert_eq!(backoff2, Duration::from_millis(40));
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

        // retry 20 → 10ms * 2^20 = 10485760 ms >> 5000 ms cap.
        let shift = 20u32;
        let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let backoff = initial.saturating_mul(factor).min(max);
        assert_eq!(backoff, Duration::from_secs(5));
    }

    #[test]
    fn tenant_budget_exceeded_at_1000_per_min() {
        let mut bucket = RateBucket::new(1000, Duration::from_secs(60));
        // First 1000 should NOT exceed.
        for _ in 0..1000 {
            assert!(!bucket.record_and_check());
        }
        // 1001st should exceed.
        assert!(bucket.record_and_check());
    }
}
