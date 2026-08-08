// SPDX-License-Identifier: BUSL-1.1

//! Memory-budget bound for unbounded (no-LIMIT) timeseries raw scans.
//!
//! A SQL `SELECT * FROM <timeseries>` arrives as `TimeseriesOp::Scan` with
//! `limit == usize::MAX`, no aggregates, and `bucket_interval_ms == 0` — the
//! raw-scan path. It must return every row that fits the per-query memory
//! budget and surface a deterministic `ResourcesExhausted` error rather than
//! silently truncating when the result would exceed it. An explicit `limit = N`
//! is unaffected; aggregate / COUNT paths never reach the raw-scan handler.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use crate::helpers::*;

/// Build an ILP payload with `count` simple lines for `collection`, one row
/// per millisecond starting at `start_ts_ns`.
fn ilp_lines(collection: &str, count: usize, start_ts_ns: i64) -> String {
    let mut lines = String::new();
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000; // 1ms apart
        lines.push_str(&format!(
            "{collection},host=h{} v={}.0 {ts_ns}\n",
            i % 8,
            (i % 1000) as f64
        ));
    }
    lines
}

/// Ingest an ILP payload, asserting the write succeeded.
fn ingest(ctx: &mut TestCtx, collection: &str, payload: &str) {
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: payload.as_bytes().to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
}

/// A raw timeseries scan (no aggregation) with the given row `limit`.
fn ts_raw_scan(collection: &str, limit: usize) -> PhysicalPlan {
    PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: collection.to_string(),
        time_range: (0, i64::MAX),
        projection: Vec::new(),
        limit,
        filters: Vec::new(),
        sort_keys: Vec::new(),
        bucket_interval_ms: 0,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        gap_fill: String::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        computed_columns: Vec::new(),
    })
}

fn scan_rows(payload: &[u8]) -> Vec<serde_json::Value> {
    let json_str = nodedb::data::executor::response_codec::decode_payload_to_json(payload);
    serde_json::from_str(&json_str).unwrap_or_default()
}

/// A no-LIMIT raw scan over more than the historical 10k truncation point
/// returns EVERY row when the result fits the memory budget.
#[test]
fn ts_unbounded_scan_returns_all_rows_when_within_budget() {
    let mut ctx = make_ctx();

    let count = 12_000;
    ingest(&mut ctx, "wide_ts", &ilp_lines("wide_ts", count, 0));

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        ts_raw_scan("wide_ts", usize::MAX),
    );
    let rows = scan_rows(&payload);
    assert_eq!(
        rows.len(),
        count,
        "no-LIMIT raw scan must return all {count} rows, not the old 10k cap"
    );
    assert!(
        rows.len() > 10_000,
        "regression: result was truncated at/under the old 10k default"
    );
}

/// A no-LIMIT raw scan whose materialized result exceeds the memory budget
/// surfaces a deterministic `ResourcesExhausted` error — never a partial result.
#[test]
fn ts_unbounded_scan_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    // Tiny per-query scan budget so a modest collection trips it.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    ingest(&mut ctx, "big_ts", &ilp_lines("big_ts", 5_000, 0));

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        ts_raw_scan("big_ts", usize::MAX),
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "over-budget scan must surface an error"
    );
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "must surface the deterministic resource-exhausted error"
    );
}

/// An explicit `limit = N` still returns exactly `N` rows — the budget bound
/// only applies to unbounded scans and must not change explicit-limit behaviour.
#[test]
fn ts_explicit_limit_returns_exactly_n() {
    let mut ctx = make_ctx();

    ingest(&mut ctx, "limited_ts", &ilp_lines("limited_ts", 12_000, 0));

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        ts_raw_scan("limited_ts", 250),
    );
    let rows = scan_rows(&payload);
    assert_eq!(
        rows.len(),
        250,
        "explicit limit 250 must return exactly 250 rows"
    );
}
