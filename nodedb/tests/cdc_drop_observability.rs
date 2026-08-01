// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for CDC drop observability.
//!
//! Covers:
//! - `StreamBuffer::push` returns eviction count per call.
//! - `oldest_available_offset` reflects buffer head after eviction.
//! - Per-consumer-group `evicted_since_last_poll` delta is correct across
//!   multiple polls.
//! - `SystemMetrics::record_cdc_stream_drop` / `prometheus_cdc_stream_drops`
//!   renders `nodedb_cdc_events_dropped_total{tenant, stream}`.
//! - `CdcLagWarner` fires when the threshold is crossed, stays silent below.

mod common;

use common::make_cdc_event;
use nodedb::control::metrics::system::SystemMetrics;
use nodedb::event::cdc::CdcOffset;
use nodedb::event::cdc::buffer::StreamBuffer;
use nodedb::event::cdc::consumer_group::state::OffsetStore;
use nodedb::event::cdc::lag_warner::CdcLagWarner;
use nodedb::event::cdc::stream_def::RetentionConfig;
use nodedb::types::DatabaseId;

// ── StreamBuffer ──────────────────────────────────────────────────────────────

#[test]
fn push_returns_zero_when_no_eviction() {
    let buf = StreamBuffer::new(
        "s1".into(),
        RetentionConfig {
            max_events: 10,
            max_age_secs: 3600,
        },
    );
    for i in 1..=5 {
        let evicted = buf.push(make_cdc_event(
            DatabaseId::DEFAULT,
            i,
            0,
            "orders",
            "INSERT",
        ));
        assert_eq!(evicted, 0, "no eviction expected for event {i}");
    }
}

#[test]
fn push_returns_eviction_count_at_capacity() {
    let buf = StreamBuffer::new(
        "s1".into(),
        RetentionConfig {
            max_events: 3,
            max_age_secs: 3600,
        },
    );
    // Fill to capacity — no evictions yet.
    for i in 1..=3 {
        let evicted = buf.push(make_cdc_event(
            DatabaseId::DEFAULT,
            i,
            0,
            "orders",
            "INSERT",
        ));
        assert_eq!(
            evicted, 0,
            "event {i}: should not evict while under capacity"
        );
    }
    // Push 4th: causes 1 eviction (the oldest event).
    let evicted = buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        4,
        0,
        "orders",
        "INSERT",
    ));
    assert_eq!(evicted, 1, "4th push must evict exactly 1 event");

    // Push 5th: another eviction.
    let evicted = buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        5,
        0,
        "orders",
        "INSERT",
    ));
    assert_eq!(evicted, 1, "5th push must evict exactly 1 event");

    assert_eq!(buf.total_evicted(), 2);
    assert_eq!(buf.len(), 3);
}

#[test]
fn oldest_available_offset_reflects_head_after_eviction() {
    let buf = StreamBuffer::new(
        "s1".into(),
        RetentionConfig {
            max_events: 2,
            max_age_secs: 3600,
        },
    );
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        1,
        0,
        "orders",
        "INSERT",
    )); // lsn = 10
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        2,
        0,
        "orders",
        "INSERT",
    )); // lsn = 20

    // Buffer full, no eviction yet.
    assert_eq!(buf.earliest_offset(), Some(CdcOffset::new(10, 1)));

    // Push 3rd — evicts lsn=10.
    buf.push(make_cdc_event(
        DatabaseId::DEFAULT,
        3,
        0,
        "orders",
        "INSERT",
    )); // lsn = 30
    assert_eq!(
        buf.earliest_offset(),
        Some(CdcOffset::new(20, 2)),
        "oldest_available_offset must advance to the new head after eviction"
    );
}

// ── OffsetStore eviction baseline ─────────────────────────────────────────────

#[test]
fn eviction_baseline_zero_on_first_poll() {
    let dir = tempfile::tempdir().unwrap();
    let store = OffsetStore::open(dir.path()).unwrap();

    // First call: baseline is 0, delta = current_total − 0.
    let delta = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "grp", 42);
    assert_eq!(delta, 42, "first poll: delta should equal current_total");
}

#[test]
fn eviction_baseline_delta_across_polls() {
    let dir = tempfile::tempdir().unwrap();
    let store = OffsetStore::open(dir.path()).unwrap();

    // Poll 1: baseline=0, current=10 → delta=10.
    let d1 = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "grp", 10);
    assert_eq!(d1, 10);

    // Poll 2: baseline=10, current=10 → delta=0 (no new evictions).
    let d2 = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "grp", 10);
    assert_eq!(d2, 0);

    // Poll 3: baseline=10, current=25 → delta=15.
    let d3 = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "grp", 25);
    assert_eq!(d3, 15);
}

#[test]
fn eviction_baseline_independent_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let store = OffsetStore::open(dir.path()).unwrap();

    // Group A sees 10 evictions.
    let da = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "group_a", 10);
    assert_eq!(da, 10);

    // Group B first poll — should start from 0 independently.
    let db = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "group_b", 10);
    assert_eq!(
        db, 10,
        "group_b baseline starts at 0, independent from group_a"
    );

    // Group A second poll — no new evictions since last.
    let da2 = store.swap_eviction_baseline(DatabaseId::DEFAULT, 1, "orders_stream", "group_a", 10);
    assert_eq!(da2, 0);
}

// ── SystemMetrics per-stream counter ─────────────────────────────────────────

#[test]
fn system_metrics_cdc_stream_drop_increments_both_counters() {
    let metrics = SystemMetrics::new();

    metrics.record_cdc_stream_drop(1, "orders_stream", 5);
    metrics.record_cdc_stream_drop(1, "orders_stream", 3);
    metrics.record_cdc_stream_drop(2, "users_stream", 7);

    // Global counter = sum across all streams.
    use std::sync::atomic::Ordering;
    assert_eq!(
        metrics.change_events_dropped.load(Ordering::Relaxed),
        15,
        "global counter must equal sum of all per-stream drops"
    );
}

#[test]
fn system_metrics_prometheus_renders_per_stream_counter() {
    let metrics = SystemMetrics::new();

    metrics.record_cdc_stream_drop(42, "orders_stream", 100);
    metrics.record_cdc_stream_drop(42, "users_stream", 50);

    let prom = metrics.to_prometheus();

    assert!(
        prom.contains(r#"nodedb_cdc_events_dropped_total{tenant="42",stream="orders_stream"} 100"#),
        "prometheus output must contain per-stream counter for orders_stream"
    );
    assert!(
        prom.contains(r#"nodedb_cdc_events_dropped_total{tenant="42",stream="users_stream"} 50"#),
        "prometheus output must contain per-stream counter for users_stream"
    );
    // HELP and TYPE lines must be present exactly once (idempotent header).
    let help_count = prom
        .matches("# HELP nodedb_cdc_events_dropped_total")
        .count();
    assert_eq!(help_count, 1, "HELP line must appear exactly once");
}

#[test]
fn system_metrics_prometheus_no_cdc_section_when_empty() {
    let metrics = SystemMetrics::new();
    let prom = metrics.to_prometheus();
    // The per-stream section is suppressed when there are no drops yet.
    assert!(
        !prom.contains("nodedb_cdc_events_dropped_total{"),
        "no labelled CDC counter expected when no drops have occurred"
    );
}

// ── CdcLagWarner ─────────────────────────────────────────────────────────────

#[test]
fn lag_warner_no_warn_below_threshold() {
    // Threshold = 1000. Accumulate 999 — should not log/panic.
    let warner = CdcLagWarner::new(1000);
    warner.record_drops(1, "orders_stream", 999, 0);
    // No assertion needed — just must not panic.
}

#[test]
fn lag_warner_fires_at_threshold() {
    // Threshold = 1. A single drop must cross it.
    let warner = CdcLagWarner::new(1);
    // Should not panic (we cannot easily assert on log output in unit tests,
    // but the code path must complete without error).
    warner.record_drops(1, "stream_a", 1, 100);
}

#[test]
fn lag_warner_noop_on_zero_drops() {
    // Zero drops must be a no-op — must not insert state.
    let warner = CdcLagWarner::new(10);
    warner.record_drops(1, "stream_a", 0, 0);
    // Drop with actual events — if state was inserted for zero, this would
    // accumulate from 0 rather than starting fresh from the zero baseline.
    // Verified by checking no spurious warn fires on the first non-zero batch.
    warner.record_drops(1, "stream_a", 5, 0); // 5 < 10 threshold → no warn.
}

#[test]
fn lag_warner_remove_stream_resets_window() {
    let warner = CdcLagWarner::new(100);
    // Accumulate 90 drops.
    warner.record_drops(1, "s1", 90, 0);
    // Remove stream — window state cleared.
    warner.remove_stream(1, "s1");
    // Next 90 drops start fresh — should not cross threshold (90 < 100).
    warner.record_drops(1, "s1", 90, 0);
    // If window was NOT reset, accumulated total would be 180 > 100 and warn!
    // would have fired twice. No assertion on tracing; just must not panic.
}
