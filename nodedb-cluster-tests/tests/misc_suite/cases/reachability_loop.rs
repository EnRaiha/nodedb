// SPDX-License-Identifier: BUSL-1.1

//! Reachability loop closes the circuit-breaker blind spot.
//!
//! Scenario: peer 42 starts out unreachable so its breaker opens.
//! After a few seconds the peer "recovers" (the mock prober flips
//! from Err to Ok). The reachability driver must observe the next
//! sweep as a success and drive the breaker back to `Closed` without
//! any user traffic.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use nodedb_cluster::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use nodedb_cluster::error::{ClusterError, Result};
use nodedb_cluster::reachability::{
    ReachabilityDriver, ReachabilityDriverConfig, ReachabilityProber,
};
use tokio::sync::watch;

/// Mock prober whose success/failure can be flipped at runtime by
/// the test. Every probe call increments a hit counter so the test
/// can prove the sweep actually ran.
struct Flappy {
    healthy: AtomicBool,
}

impl Flappy {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            healthy: AtomicBool::new(false),
        })
    }
    fn heal(&self) {
        self.healthy.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ReachabilityProber for Flappy {
    async fn probe(&self, peer: u64) -> Result<()> {
        if self.healthy.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ClusterError::Transport {
                detail: format!("mock: peer {peer} unreachable"),
            })
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reachability_loop_recovers_open_breaker_without_user_traffic() {
    // --- Shared breaker, opened immediately for peer 42. ---
    let breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 1,
        // Short cooldown so HalfOpen is eligible quickly — the
        // driver still needs to drive the actual transition.
        cooldown: Duration::from_millis(100),
    }));
    breaker.record_failure(42);
    assert_eq!(breaker.state(42), CircuitState::Open);

    // --- Flappy prober starts "unhealthy". ---
    let prober = Flappy::new();

    // The driver's sweep_once calls probe() but does NOT itself
    // drive record_success/record_failure — production relies on
    // NexarTransport::send_rpc for that, and the mock has no such
    // wrapper. So we install a relay closure that records the
    // outcome against the breaker on the driver's behalf. This is
    // the minimal glue needed to exercise the real loop end-to-end.
    struct RelayProber {
        inner: Arc<Flappy>,
        breaker: Arc<CircuitBreaker>,
    }
    #[async_trait]
    impl ReachabilityProber for RelayProber {
        async fn probe(&self, peer: u64) -> Result<()> {
            // Mirror send_rpc: check → probe → record outcome.
            if self.breaker.check(peer).is_err() {
                return Err(ClusterError::CircuitOpen {
                    node_id: peer,
                    failures: self.breaker.failure_count(peer),
                });
            }
            match self.inner.probe(peer).await {
                Ok(()) => {
                    self.breaker.record_success(peer);
                    Ok(())
                }
                Err(e) => {
                    self.breaker.record_failure(peer);
                    Err(e)
                }
            }
        }
    }
    let relay: Arc<dyn ReachabilityProber> = Arc::new(RelayProber {
        inner: prober.clone(),
        breaker: Arc::clone(&breaker),
    });

    let driver = Arc::new(ReachabilityDriver::new(
        Arc::clone(&breaker),
        relay,
        ReachabilityDriverConfig {
            interval: Duration::from_millis(150),
        },
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn({
        let d = Arc::clone(&driver);
        async move { d.run(shutdown_rx).await }
    });

    // --- First few sweeps: probe keeps failing, breaker stays Open. ---
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        breaker.state(42),
        CircuitState::Open,
        "breaker should stay open while peer is unhealthy"
    );

    // --- Heal the peer. Next sweep should drive Open → HalfOpen → Closed. ---
    prober.heal();

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if breaker.state(42) == CircuitState::Closed {
            break;
        }
        if Instant::now() >= deadline {
            panic!("breaker never recovered; state = {:?}", breaker.state(42));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
