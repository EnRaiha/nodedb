// SPDX-License-Identifier: BUSL-1.1

//! Shared test doubles for rebalancer integration tests.
//!
//! Used by `elastic_scaling.rs` and `elastic_scaling_churn.rs` to avoid
//! duplicating the mock provider, dispatcher, and helper constructor.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use nodedb_cluster::error::Result;
use nodedb_cluster::rebalance::PlannedMove;
use nodedb_cluster::rebalancer::{LoadMetrics, LoadMetricsProvider, MigrationDispatcher};

pub struct DynamicProvider {
    metrics: Mutex<Vec<LoadMetrics>>,
}

impl DynamicProvider {
    pub fn new(initial: Vec<LoadMetrics>) -> Arc<Self> {
        Arc::new(Self {
            metrics: Mutex::new(initial),
        })
    }

    pub fn push(&self, m: LoadMetrics) {
        self.metrics.lock().unwrap().push(m);
    }
}

#[async_trait]
impl LoadMetricsProvider for DynamicProvider {
    async fn snapshot(&self) -> Result<Vec<LoadMetrics>> {
        Ok(self.metrics.lock().unwrap().clone())
    }
}

pub struct RecordingDispatcher {
    calls: Mutex<Vec<PlannedMove>>,
    any_call: AtomicBool,
}

impl RecordingDispatcher {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            any_call: AtomicBool::new(false),
        })
    }

    pub fn snapshot(&self) -> Vec<PlannedMove> {
        self.calls.lock().unwrap().clone()
    }

    pub fn fired(&self) -> bool {
        self.any_call.load(Ordering::SeqCst)
    }

    pub fn reset_fired(&self) {
        self.any_call.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl MigrationDispatcher for RecordingDispatcher {
    async fn dispatch(&self, mv: PlannedMove) -> Result<()> {
        self.calls.lock().unwrap().push(mv);
        self.any_call.store(true, Ordering::SeqCst);
        Ok(())
    }
}

pub fn lm(id: u64, v: u32, bytes_mib: u64, w: f64, r: f64) -> LoadMetrics {
    LoadMetrics {
        node_id: id,
        vshards_led: v,
        bytes_stored: bytes_mib * 1_048_576,
        writes_per_sec: w,
        reads_per_sec: r,
        qps_recent: 0.0,
        p95_latency_us: 0,
        cpu_utilization: 0.0,
    }
}

pub async fn wait_until<F: Fn() -> bool>(deadline: Duration, predicate: F) -> bool {
    let stop = Instant::now() + deadline;
    while Instant::now() < stop {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}
