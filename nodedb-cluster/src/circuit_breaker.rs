// SPDX-License-Identifier: BUSL-1.1

//! Per-peer circuit breaker and retry policy for RPC resilience.
//!
//! The circuit breaker prevents sending RPCs to peers that are consistently
//! failing, saving resources and enabling faster failure detection.
//!
//! States: Closed → Open → HalfOpen → Closed (or back to Open).
//!
//! The retry policy controls exponential backoff for transient failures.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::error::{ClusterError, Result};

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Duration the circuit stays open before transitioning to half-open.
    pub cooldown: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown: Duration::from_secs(10),
        }
    }
}

/// Per-peer circuit breaker.
///
/// Thread-safe — all mutations go through an internal `RwLock`.
pub struct CircuitBreaker {
    peers: RwLock<HashMap<u64, PeerBreaker>>,
    config: CircuitBreakerConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — requests pass through.
    Closed,
    /// Peer is failing — fast-fail all requests until cooldown expires.
    Open,
    /// Cooldown expired — allow one probe request through.
    HalfOpen,
}

struct PeerBreaker {
    state: CircuitState,
    consecutive_failures: u32,
    last_state_change: Instant,
    /// When the half-open probe went out. `None` when none is out.
    probe_since: Option<Instant>,
}

impl PeerBreaker {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            last_state_change: Instant::now(),
            probe_since: None,
        }
    }
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Check if an RPC to this peer is allowed.
    ///
    /// Returns `Ok(())` if the circuit is closed or half-open (probe allowed).
    /// Returns `Err(CircuitOpen)` if the circuit is open and cooldown hasn't expired.
    pub fn check(&self, peer: u64) -> Result<()> {
        let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
        let breaker = peers.entry(peer).or_insert_with(PeerBreaker::new);
        let now = Instant::now();
        let refused = ClusterError::CircuitOpen {
            node_id: peer,
            failures: breaker.consecutive_failures,
        };

        // An open circuit refuses until its cooldown passes, then turns
        // half-open and lets exactly one probe through; other requests are
        // refused while it is out. A probe that never reports, because its
        // caller was dropped, stops counting after one cooldown, and the next
        // request goes out as the probe instead. So the circuit can always
        // recover.
        match breaker.state {
            CircuitState::Closed => Ok(()),
            CircuitState::HalfOpen => {
                let probe_out = breaker.probe_since.is_some_and(|since| {
                    now.saturating_duration_since(since) < self.config.cooldown
                });
                if probe_out {
                    return Err(refused);
                }
                breaker.probe_since = Some(now);
                Ok(())
            }
            CircuitState::Open => {
                if now.saturating_duration_since(breaker.last_state_change) < self.config.cooldown {
                    return Err(refused);
                }
                breaker.state = CircuitState::HalfOpen;
                breaker.last_state_change = now;
                breaker.probe_since = Some(now);
                Ok(())
            }
        }
    }

    /// Record a successful RPC to a peer. Resets the circuit to Closed.
    pub fn record_success(&self, peer: u64) {
        let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
        let breaker = peers.entry(peer).or_insert_with(PeerBreaker::new);
        breaker.consecutive_failures = 0;
        breaker.probe_since = None;
        if breaker.state != CircuitState::Closed {
            breaker.state = CircuitState::Closed;
            breaker.last_state_change = Instant::now();
        }
    }

    /// Record a failed RPC to a peer. May open the circuit.
    pub fn record_failure(&self, peer: u64) {
        let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
        let breaker = peers.entry(peer).or_insert_with(PeerBreaker::new);
        breaker.consecutive_failures += 1;

        match breaker.state {
            CircuitState::Closed => {
                if breaker.consecutive_failures >= self.config.failure_threshold {
                    breaker.state = CircuitState::Open;
                    breaker.last_state_change = Instant::now();
                }
            }
            CircuitState::HalfOpen => {
                // Probe failed → back to Open.
                breaker.state = CircuitState::Open;
                breaker.last_state_change = Instant::now();
                breaker.probe_since = None;
            }
            // A request that went out before the circuit opened and failed
            // since leaves the cooldown as it is. Restarting the cooldown on
            // every late failure would keep the circuit open for as long as
            // old requests keep failing.
            CircuitState::Open => {}
        }
    }

    /// Get the current circuit state for a peer (for observability).
    pub fn state(&self, peer: u64) -> CircuitState {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        peers
            .get(&peer)
            .map(|b| b.state)
            .unwrap_or(CircuitState::Closed)
    }

    /// Return the ids of every peer whose breaker is currently Open.
    ///
    /// Used by the reachability driver to find peers that need an
    /// active probe — without a periodic poke these peers never
    /// transition back to HalfOpen (no traffic → no `check()` call
    /// → no cooldown re-evaluation).
    pub fn open_peers(&self) -> Vec<u64> {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        peers
            .iter()
            .filter_map(|(id, b)| {
                if b.state == CircuitState::Open {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get consecutive failure count for a peer.
    pub fn failure_count(&self, peer: u64) -> u32 {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        peers
            .get(&peer)
            .map(|b| b.consecutive_failures)
            .unwrap_or(0)
    }

    /// Snapshot every tracked peer's breaker state for observability
    /// (`/cluster/debug/transport`). Sorted by peer id so output is
    /// deterministic.
    pub fn snapshot(&self) -> Vec<BreakerSnapshot> {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<BreakerSnapshot> = peers
            .iter()
            .map(|(id, b)| BreakerSnapshot {
                peer_id: *id,
                state: match b.state {
                    CircuitState::Closed => "closed",
                    CircuitState::Open => "open",
                    CircuitState::HalfOpen => "half_open",
                },
                consecutive_failures: b.consecutive_failures,
                seconds_in_state: b.last_state_change.elapsed().as_secs(),
            })
            .collect();
        out.sort_by_key(|s| s.peer_id);
        out
    }
}

/// Per-peer circuit-breaker snapshot emitted by
/// [`CircuitBreaker::snapshot`] for the `/cluster/debug/transport`
/// endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BreakerSnapshot {
    pub peer_id: u64,
    /// One of `"closed"`, `"open"`, `"half_open"`.
    pub state: &'static str,
    pub consecutive_failures: u32,
    pub seconds_in_state: u64,
}

// ── Retry policy ────────────────────────────────────────────────────

/// Retry policy with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the initial attempt).
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Multiplier applied to the delay after each retry.
    pub backoff_multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    /// Calculate the delay for a given attempt (0-indexed, attempt 0 = first retry).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return self.initial_delay;
        }
        let factor = self.backoff_multiplier.powi(attempt as i32);
        let delay_ms = self.initial_delay.as_millis() as f64 * factor;
        let capped = delay_ms.min(self.max_delay.as_millis() as f64);
        Duration::from_millis(capped as u64)
    }

    /// Determine if an error is retryable.
    ///
    /// Only transport errors (connection failures) are retried. Codec errors,
    /// circuit-open errors, shard timeouts, and application errors are not.
    pub fn is_retryable(err: &ClusterError) -> bool {
        match err {
            ClusterError::Transport { .. } => true,
            // A timed-out send may still have reached the peer, so resending
            // it can apply the request twice.
            ClusterError::ShardTimeout { .. } => false,
            ClusterError::Raft(_)
            | ClusterError::VShardNotMapped { .. }
            | ClusterError::GroupNotFound { .. }
            | ClusterError::LearnerNotCaughtUp { .. }
            | ClusterError::MigrationInProgress { .. }
            | ClusterError::MigrationPauseBudgetExceeded { .. }
            | ClusterError::NodeUnreachable { .. }
            | ClusterError::GhostNotFound { .. }
            | ClusterError::StreamTerminal { .. }
            | ClusterError::Storage { .. }
            | ClusterError::DataPlane { .. }
            | ClusterError::Codec { .. }
            | ClusterError::UnsupportedWireVersion { .. }
            | ClusterError::CircuitOpen { .. }
            | ClusterError::JoinGroupDisappeared { .. }
            | ClusterError::JoinCommitTimeout { .. }
            | ClusterError::ReadIndexNotLeader { .. }
            | ClusterError::ReadIndexTimeout { .. }
            | ClusterError::Config { .. }
            | ClusterError::MigrationCheckpoint(_)
            | ClusterError::MigrationRecovery(_)
            | ClusterError::WrongOwner { .. }
            | ClusterError::Calvin(_)
            | ClusterError::SnapshotCrcMismatch { .. }
            | ClusterError::SnapshotOffsetRegression { .. }
            | ClusterError::PartialSnapshotCorrupt { .. }
            | ClusterError::PartialSnapshotCleanupFailed { .. }
            | ClusterError::SnapshotApplyFailed { .. }
            | ClusterError::Mirror(_)
            | ClusterError::BspBarrier(_)
            | ClusterError::VectorGather(_)
            | ClusterError::SpatialGather(_)
            | ClusterError::Bm25Gather(_)
            | ClusterError::TsGather(_)
            | ClusterError::ShufflePush(_)
            | ClusterError::RemoteUntyped { .. }
            | ClusterError::ShardExecution { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circuit_starts_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(cb.state(42), CircuitState::Closed);
        cb.check(42).unwrap(); // Should succeed.
    }

    #[test]
    fn circuit_opens_after_threshold() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            cooldown: Duration::from_secs(60),
        });

        cb.check(1).unwrap();
        cb.record_failure(1);
        cb.record_failure(1);
        assert_eq!(cb.state(1), CircuitState::Closed);

        cb.record_failure(1); // 3rd failure → opens.
        assert_eq!(cb.state(1), CircuitState::Open);
        assert_eq!(cb.failure_count(1), 3);

        // Further checks should fail.
        let err = cb.check(1).unwrap_err();
        assert!(err.to_string().contains("circuit open"), "{err}");
    }

    #[test]
    fn circuit_half_open_after_cooldown() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_millis(10),
        });

        cb.record_failure(1); // Opens immediately (threshold=1).
        assert_eq!(cb.state(1), CircuitState::Open);

        // Wait for cooldown.
        std::thread::sleep(Duration::from_millis(15));

        // Check should transition to HalfOpen and allow the probe.
        cb.check(1).unwrap();
        assert_eq!(cb.state(1), CircuitState::HalfOpen);
    }

    #[test]
    fn half_open_lets_one_probe_through_until_it_reports_or_expires() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_millis(500),
        });
        cb.record_failure(1);
        std::thread::sleep(Duration::from_millis(550));
        cb.check(1).expect("the probe goes out");
        assert!(cb.check(1).is_err(), "a second request waits for the probe");
        // The probe never reports: after one cooldown the next request probes.
        std::thread::sleep(Duration::from_millis(550));
        cb.check(1).expect("a lost probe is replaced");
        cb.record_success(1);
        assert_eq!(cb.state(1), CircuitState::Closed);
    }

    #[test]
    fn late_failures_do_not_extend_an_open_circuit() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_millis(400),
        });
        cb.record_failure(1);
        std::thread::sleep(Duration::from_millis(300));
        // A request sent before the circuit opened fails now.
        cb.record_failure(1);
        std::thread::sleep(Duration::from_millis(150));
        cb.check(1)
            .expect("the cooldown ran from the opening failure");
        assert_eq!(cb.state(1), CircuitState::HalfOpen);
    }

    #[test]
    fn half_open_success_closes_circuit() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_millis(5),
        });

        cb.record_failure(1);
        std::thread::sleep(Duration::from_millis(10));
        cb.check(1).unwrap(); // → HalfOpen

        cb.record_success(1);
        assert_eq!(cb.state(1), CircuitState::Closed);
        assert_eq!(cb.failure_count(1), 0);
    }

    #[test]
    fn half_open_failure_reopens_circuit() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_millis(5),
        });

        cb.record_failure(1);
        std::thread::sleep(Duration::from_millis(10));
        cb.check(1).unwrap(); // → HalfOpen

        cb.record_failure(1); // Probe failed → back to Open.
        assert_eq!(cb.state(1), CircuitState::Open);
    }

    #[test]
    fn success_resets_failure_count() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 5,
            cooldown: Duration::from_secs(60),
        });

        cb.record_failure(1);
        cb.record_failure(1);
        assert_eq!(cb.failure_count(1), 2);

        cb.record_success(1);
        assert_eq!(cb.failure_count(1), 0);
        assert_eq!(cb.state(1), CircuitState::Closed);
    }

    #[test]
    fn retry_delay_exponential_backoff() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
        };

        assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(800));
    }

    #[test]
    fn retry_delay_capped_at_max() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 10.0,
        };

        // 1 * 10^2 = 100s → capped at 5s.
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(5));
    }

    #[test]
    fn retryable_errors() {
        assert!(RetryPolicy::is_retryable(&ClusterError::Transport {
            detail: "timeout".into(),
        }));
        assert!(!RetryPolicy::is_retryable(&ClusterError::Codec {
            detail: "bad crc".into(),
        }));
        assert!(!RetryPolicy::is_retryable(&ClusterError::CircuitOpen {
            node_id: 1,
            failures: 5,
        }));
        assert!(!RetryPolicy::is_retryable(&ClusterError::NodeUnreachable {
            node_id: 1
        }));
    }
}
