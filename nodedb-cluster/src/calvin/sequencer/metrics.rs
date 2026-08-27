// SPDX-License-Identifier: BUSL-1.1

//! Metrics for the Calvin sequencer service.
//!
//! [`SequencerMetrics`] tracks epoch and admission counters plus a per-conflict
//! context breakdown. All atomic fields can be read without locking from a
//! Prometheus render task. The `conflict_by_context` map uses a
//! `std::sync::Mutex<BTreeMap<...>>` for deterministic iteration order.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Key for the per-context conflict breakdown metric.
///
/// Tracks which `(tenant, engine, collection)` triples are hot for conflicts.
/// Uses `BTreeMap` so iteration order is deterministic (required for test
/// assertions and stable Prometheus output).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConflictKey {
    pub tenant: u64,
    pub engine: &'static str,
    pub collection: String,
}

/// Histogram bucket boundaries (ms) for epoch duration.
///
/// Used by `render_prometheus` to emit the `nodedb_sequencer_epoch_duration_ms`
/// histogram.
pub const EPOCH_DURATION_BUCKETS: &[u64] = &[1, 5, 10, 20, 50, 100, 250, 500, 1000];

/// Metrics for the sequencer epoch loop.
///
/// All atomic fields are `std::sync::atomic` so they can be read from a
/// separate Prometheus render task without acquiring `conflict_by_context`'s
/// lock.
pub struct SequencerMetrics {
    /// Total epochs proposed (including empty ones that were skipped).
    pub epochs_total: AtomicU64,
    /// Total txns admitted across all epochs.
    pub admitted_total: AtomicU64,
    /// Total txns rejected due to intra-batch write-set conflicts.
    pub rejected_conflict_total: AtomicU64,
    /// Total txns rejected because the plans blob exceeded the per-txn cap.
    pub rejected_txn_too_large: AtomicU64,
    /// Total txns rejected because the fan-out exceeded the vshard cap.
    pub rejected_fanout_too_wide: AtomicU64,
    /// Total txns rejected because the inbox was full (back-pressure).
    pub rejected_inbox_full: AtomicU64,
    /// Total txns rejected because this node is not the sequencer leader.
    pub rejected_not_leader: AtomicU64,
    /// Total txns rejected because the tenant's inbox quota was exceeded.
    pub rejected_tenant_quota_exceeded: AtomicU64,
    /// Per-context conflict breakdown: maps `(tenant, engine, collection)` to
    /// the number of conflict rejections observed for that key.
    pub conflict_by_context: Mutex<BTreeMap<ConflictKey, u64>>,
    /// Histogram bucket counts for `nodedb_sequencer_epoch_duration_ms`.
    ///
    /// Length == `EPOCH_DURATION_BUCKETS.len() + 1` (the last slot is the
    /// `+Inf` bucket / overflow).  Index `i` counts epochs whose duration
    /// fell in the range `(EPOCH_DURATION_BUCKETS[i-1], EPOCH_DURATION_BUCKETS[i]]`.
    pub epoch_duration_buckets: [AtomicU64; 10],
    /// Running sum of all epoch durations in ms (for the histogram `_sum`).
    pub epoch_duration_sum_ms: AtomicU64,
    /// Current inbox depth gauge (snapshot at Prometheus render time).
    ///
    /// Updated by the inbox on every submit (+1) and every drain (−1).
    pub inbox_depth: AtomicU64,
    /// Whether this node's sequencer holds a usable epoch seed.
    ///
    /// Written on every leader tick by `SequencerService::ensure_epoch_seeded`:
    /// `true` once the seed is derived, `false` while the sequencer group is
    /// still replaying or the state machine has halted. Stays `false` on a
    /// follower, whose service never mints. Read by the readiness probe to tell
    /// whether a Calvin submit landing here can actually be sequenced.
    pub epoch_seeded: AtomicBool,
}

impl SequencerMetrics {
    /// Construct a new zeroed metrics instance wrapped in an `Arc`.
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }

    /// Increment the conflict counter for `key`.
    pub fn record_conflict(&self, key: ConflictKey) {
        let mut map = self
            .conflict_by_context
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        *map.entry(key).or_insert(0) += 1;
    }

    /// Record one epoch's duration (ms) into the histogram.
    ///
    /// Increments the appropriate bucket and the running sum.
    pub fn record_epoch_duration_ms(&self, ms: u64) {
        self.epoch_duration_sum_ms.fetch_add(ms, Ordering::Relaxed);
        // Find the first bucket boundary >= ms; if none, use the +Inf slot (index 9).
        let bucket_idx = EPOCH_DURATION_BUCKETS
            .iter()
            .position(|&b| ms <= b)
            .unwrap_or(EPOCH_DURATION_BUCKETS.len());
        self.epoch_duration_buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Emit all metrics in Prometheus text exposition format.
    ///
    /// Mirrors the convention used in `crate::loop_metrics::LoopMetrics::render_prom`:
    /// each metric gets a `# HELP` line, a `# TYPE` line, and one or more
    /// sample lines.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();

        macro_rules! counter {
            ($name:expr, $help:expr, $value:expr) => {
                let _ = writeln!(out, "# HELP {} {}", $name, $help);
                let _ = writeln!(out, "# TYPE {} counter", $name);
                let _ = writeln!(out, "{} {}", $name, $value);
            };
        }

        counter!(
            "nodedb_sequencer_epochs_total",
            "Total epochs processed by the sequencer leader.",
            self.epochs_total.load(Ordering::Relaxed)
        );

        // Unified admitted/rejected counter with {outcome} label.
        let _ = writeln!(
            out,
            "# HELP nodedb_sequencer_admitted_txns_total \
             Total transactions processed by the sequencer, by outcome."
        );
        let _ = writeln!(out, "# TYPE nodedb_sequencer_admitted_txns_total counter");
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"admitted\"}} {}",
            self.admitted_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"rejected_conflict\"}} {}",
            self.rejected_conflict_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"rejected_inbox_full\"}} {}",
            self.rejected_inbox_full.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"rejected_txn_too_large\"}} {}",
            self.rejected_txn_too_large.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"rejected_fanout_too_wide\"}} {}",
            self.rejected_fanout_too_wide.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"rejected_tenant_quota\"}} {}",
            self.rejected_tenant_quota_exceeded.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_admitted_txns_total{{outcome=\"rejected_not_leader\"}} {}",
            self.rejected_not_leader.load(Ordering::Relaxed)
        );

        // Inbox depth gauge.
        let _ = writeln!(
            out,
            "# HELP nodedb_sequencer_inbox_depth Current number of transactions queued in the inbox."
        );
        let _ = writeln!(out, "# TYPE nodedb_sequencer_inbox_depth gauge");
        let _ = writeln!(
            out,
            "nodedb_sequencer_inbox_depth {}",
            self.inbox_depth.load(Ordering::Relaxed)
        );

        // Epoch-seed readiness gauge.
        let _ = writeln!(
            out,
            "# HELP nodedb_sequencer_epoch_seeded \
             1 when this node's sequencer holds a usable epoch seed, 0 otherwise."
        );
        let _ = writeln!(out, "# TYPE nodedb_sequencer_epoch_seeded gauge");
        let _ = writeln!(
            out,
            "nodedb_sequencer_epoch_seeded {}",
            u8::from(self.epoch_seeded.load(Ordering::Relaxed))
        );

        // Epoch duration histogram.
        let _ = writeln!(
            out,
            "# HELP nodedb_sequencer_epoch_duration_ms \
             Duration of each sequencer epoch tick in milliseconds."
        );
        let _ = writeln!(out, "# TYPE nodedb_sequencer_epoch_duration_ms histogram");
        let mut cumulative: u64 = 0;
        for (i, &boundary) in EPOCH_DURATION_BUCKETS.iter().enumerate() {
            cumulative += self.epoch_duration_buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "nodedb_sequencer_epoch_duration_ms_bucket{{le=\"{boundary}\"}} {cumulative}"
            );
        }
        // +Inf bucket.
        cumulative +=
            self.epoch_duration_buckets[EPOCH_DURATION_BUCKETS.len()].load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "nodedb_sequencer_epoch_duration_ms_bucket{{le=\"+Inf\"}} {cumulative}"
        );
        let _ = writeln!(
            out,
            "nodedb_sequencer_epoch_duration_ms_sum {}",
            self.epoch_duration_sum_ms.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "nodedb_sequencer_epoch_duration_ms_count {cumulative}");

        // Per-context conflict breakdown.
        let _ = writeln!(
            out,
            "# HELP nodedb_sequencer_conflict_by_context_total \
             Conflict rejections keyed by tenant, engine, and collection."
        );
        let _ = writeln!(
            out,
            "# TYPE nodedb_sequencer_conflict_by_context_total counter"
        );
        let map = self
            .conflict_by_context
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if map.is_empty() {
            let _ = writeln!(
                out,
                "nodedb_sequencer_conflict_by_context_total\
                 {{tenant=\"\",engine=\"\",collection=\"\"}} 0"
            );
        } else {
            for (key, count) in map.iter() {
                let _ = writeln!(
                    out,
                    "nodedb_sequencer_conflict_by_context_total\
                     {{tenant=\"{}\",engine=\"{}\",collection=\"{}\"}} {}",
                    key.tenant, key.engine, key.collection, count
                );
            }
        }

        out
    }
}

impl Default for SequencerMetrics {
    fn default() -> Self {
        Self {
            epochs_total: AtomicU64::new(0),
            admitted_total: AtomicU64::new(0),
            rejected_conflict_total: AtomicU64::new(0),
            rejected_txn_too_large: AtomicU64::new(0),
            rejected_fanout_too_wide: AtomicU64::new(0),
            rejected_inbox_full: AtomicU64::new(0),
            rejected_not_leader: AtomicU64::new(0),
            rejected_tenant_quota_exceeded: AtomicU64::new(0),
            conflict_by_context: Mutex::new(BTreeMap::new()),
            epoch_duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            epoch_duration_sum_ms: AtomicU64::new(0),
            inbox_depth: AtomicU64::new(0),
            epoch_seeded: AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_counters_increment() {
        let m = SequencerMetrics::default();

        m.rejected_txn_too_large.fetch_add(1, Ordering::Relaxed);
        m.rejected_fanout_too_wide.fetch_add(2, Ordering::Relaxed);
        m.rejected_inbox_full.fetch_add(3, Ordering::Relaxed);
        m.rejected_not_leader.fetch_add(4, Ordering::Relaxed);
        m.rejected_tenant_quota_exceeded
            .fetch_add(5, Ordering::Relaxed);

        assert_eq!(m.rejected_txn_too_large.load(Ordering::Relaxed), 1);
        assert_eq!(m.rejected_fanout_too_wide.load(Ordering::Relaxed), 2);
        assert_eq!(m.rejected_inbox_full.load(Ordering::Relaxed), 3);
        assert_eq!(m.rejected_not_leader.load(Ordering::Relaxed), 4);
        assert_eq!(m.rejected_tenant_quota_exceeded.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn record_conflict_increments_map() {
        let m = SequencerMetrics::default();
        let key = ConflictKey {
            tenant: 42,
            engine: "document",
            collection: "orders".to_owned(),
        };
        m.record_conflict(key.clone());
        m.record_conflict(key.clone());
        let map = m
            .conflict_by_context
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(*map.get(&key).unwrap(), 2);
    }

    #[test]
    fn render_prometheus_includes_conflict_labels() {
        let m = SequencerMetrics::default();
        m.record_conflict(ConflictKey {
            tenant: 1,
            engine: "kv",
            collection: "cache".to_owned(),
        });
        let output = m.render_prometheus();
        assert!(
            output.contains("tenant=\"1\",engine=\"kv\",collection=\"cache\""),
            "expected conflict label in output:\n{output}"
        );
    }

    #[test]
    fn render_prometheus_uses_nodedb_prefix() {
        let m = SequencerMetrics::default();
        let output = m.render_prometheus();
        // All metric names must start with nodedb_.
        for line in output.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            assert!(
                line.starts_with("nodedb_"),
                "metric line missing nodedb_ prefix: {line}"
            );
        }
    }

    #[test]
    fn epoch_duration_histogram_records_into_correct_bucket() {
        let m = SequencerMetrics::default();
        m.record_epoch_duration_ms(10); // bucket index 2 (<=10)
        m.record_epoch_duration_ms(1000); // bucket index 8 (<=1000)
        m.record_epoch_duration_ms(2000); // +Inf bucket index 9
        let output = m.render_prometheus();
        assert!(
            output.contains("nodedb_sequencer_epoch_duration_ms_bucket"),
            "histogram output missing: {output}"
        );
        assert!(
            output.contains("nodedb_sequencer_epoch_duration_ms_sum 3010"),
            "sum incorrect in: {output}"
        );
    }

    #[test]
    fn admitted_txns_total_has_outcome_labels() {
        let m = SequencerMetrics::default();
        m.admitted_total.fetch_add(5, Ordering::Relaxed);
        m.rejected_conflict_total.fetch_add(2, Ordering::Relaxed);
        let output = m.render_prometheus();
        assert!(
            output.contains("outcome=\"admitted\"} 5"),
            "admitted outcome missing: {output}"
        );
        assert!(
            output.contains("outcome=\"rejected_conflict\"} 2"),
            "rejected_conflict outcome missing: {output}"
        );
    }
}
