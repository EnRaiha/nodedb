// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard delivery metrics.
//!
//! Atomic counters for observability: writes/sec, retry rate, DLQ depth,
//! duplicates dropped. All counters are monotonic (never reset).

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for cross-shard event delivery.
pub struct CrossShardMetrics {
    /// Total cross-shard writes sent (enqueued for delivery).
    pub writes_sent: AtomicU64,
    /// Total cross-shard writes successfully delivered (ACK received).
    pub writes_delivered: AtomicU64,
    /// Total cross-shard writes received (inbound on this node).
    pub writes_received: AtomicU64,
    /// Total retries attempted.
    pub retries: AtomicU64,
    /// Total events sent to DLQ after max retries.
    pub dlq_enqueued: AtomicU64,
    /// Total duplicate events dropped by dedup.
    pub duplicates_dropped: AtomicU64,
    /// Total delivery failures (transport errors, execution errors).
    pub delivery_failures: AtomicU64,
    /// Sum of delivery latencies in microseconds (for computing average).
    pub delivery_latency_us_sum: AtomicU64,
    /// Count of latency samples (for computing average).
    pub delivery_latency_samples: AtomicU64,
}

impl CrossShardMetrics {
    pub fn new() -> Self {
        Self {
            writes_sent: AtomicU64::new(0),
            writes_delivered: AtomicU64::new(0),
            writes_received: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            dlq_enqueued: AtomicU64::new(0),
            duplicates_dropped: AtomicU64::new(0),
            delivery_failures: AtomicU64::new(0),
            delivery_latency_us_sum: AtomicU64::new(0),
            delivery_latency_samples: AtomicU64::new(0),
        }
    }

    pub fn record_sent(&self) {
        self.writes_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delivered(&self, latency_us: u64) {
        self.writes_delivered.fetch_add(1, Ordering::Relaxed);
        self.delivery_latency_us_sum
            .fetch_add(latency_us, Ordering::Relaxed);
        self.delivery_latency_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_received(&self) {
        self.writes_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dlq(&self) {
        self.dlq_enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_duplicate(&self) {
        self.duplicates_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.delivery_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Average delivery latency in microseconds (0 if no samples).
    pub fn avg_latency_us(&self) -> u64 {
        let samples = self.delivery_latency_samples.load(Ordering::Relaxed);
        if samples == 0 {
            return 0;
        }
        self.delivery_latency_us_sum.load(Ordering::Relaxed) / samples
    }

    /// Snapshot all metrics for reporting.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            writes_sent: self.writes_sent.load(Ordering::Relaxed),
            writes_delivered: self.writes_delivered.load(Ordering::Relaxed),
            writes_received: self.writes_received.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            dlq_enqueued: self.dlq_enqueued.load(Ordering::Relaxed),
            duplicates_dropped: self.duplicates_dropped.load(Ordering::Relaxed),
            delivery_failures: self.delivery_failures.load(Ordering::Relaxed),
            avg_latency_us: self.avg_latency_us(),
        }
    }
}

impl Default for CrossShardMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time metrics snapshot for reporting.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub writes_sent: u64,
    pub writes_delivered: u64,
    pub writes_received: u64,
    pub retries: u64,
    pub dlq_enqueued: u64,
    pub duplicates_dropped: u64,
    pub delivery_failures: u64,
    pub avg_latency_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_basic() {
        let m = CrossShardMetrics::new();
        m.record_sent();
        m.record_sent();
        m.record_delivered(1000);
        m.record_retry();
        m.record_duplicate();
        m.record_dlq();
        m.record_failure();

        let snap = m.snapshot();
        assert_eq!(snap.writes_sent, 2);
        assert_eq!(snap.writes_delivered, 1);
        assert_eq!(snap.retries, 1);
        assert_eq!(snap.duplicates_dropped, 1);
        assert_eq!(snap.dlq_enqueued, 1);
        assert_eq!(snap.delivery_failures, 1);
        assert_eq!(snap.avg_latency_us, 1000);
    }

    #[test]
    fn avg_latency_multiple_samples() {
        let m = CrossShardMetrics::new();
        m.record_delivered(100);
        m.record_delivered(200);
        m.record_delivered(300);
        assert_eq!(m.avg_latency_us(), 200);
    }

    #[test]
    fn avg_latency_zero_samples() {
        let m = CrossShardMetrics::new();
        assert_eq!(m.avg_latency_us(), 0);
    }
}
