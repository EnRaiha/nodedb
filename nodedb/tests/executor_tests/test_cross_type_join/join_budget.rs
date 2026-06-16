// SPDX-License-Identifier: BUSL-1.1

//! Tests for the join-side memory-budget guards and the removal of the
//! silent 50,000-row-per-side cap.
//!
//! Verifies two properties:
//! 1. **Completeness past the old cap** — an inner join whose matching row
//!    sits beyond index 50,000 IS returned when the memory budget allows it.
//! 2. **Deterministic error over budget** — when a join side exceeds
//!    `max_scan_result_bytes`, the join returns `ResourcesExhausted` (never a
//!    truncated success). Covers both left-side and right-side guards for hash,
//!    nested-loop, and sort-merge handlers.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan, QueryOp};

use crate::helpers::*;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Insert `count` KV entries (`k0`…`k{count-1}`) with tiny payloads.
/// Returns the key of the last entry inserted.
fn batch_kv(ctx: &mut TestCtx, collection: &str, count: usize) -> String {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
        .map(|i| {
            let key = format!("k{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            (key, value)
        })
        .collect();
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection: collection.into(),
            entries,
            ttl_ms: 0,
        }),
    );
    format!("k{}", count - 1)
}

// ── 1. Completeness past the old 50k cap ─────────────────────────────────────

/// An inner join whose probe row sits beyond the old 50,000-row cap is still
/// returned when the memory budget allows.
///
/// Strategy: insert 51,000 KV entries on the left side and one matching entry
/// whose key is `k50999` (the last one). The right side has a single entry
/// with key `k50999` as well. With the old cap the probe never reached that
/// row; after the fix the full scan completes and the join matches.
#[test]
fn hash_join_completeness_past_50k_cap() {
    let mut ctx = make_ctx();

    // Left: 51,000 entries — the matching key is the very last one.
    let match_key = batch_kv(&mut ctx, "left_large", 51_000);

    // Right: one entry with the same key.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "right_small".into(),
            key: match_key.as_bytes().to_vec(),
            value: b"match".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Hash join on the `key` field (the KV engine surfaces the key as `key`).
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "left_large".into(),
            right_collection: "right_small".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            // Use a large limit so the join itself doesn't cap results.
            limit: 1_000_000,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        1,
        "inner join must find the match that sits beyond index 50k; got {} rows",
        rows.len()
    );
}

/// Same completeness check for the sort-merge join handler.
#[test]
fn sort_merge_join_completeness_past_50k_cap() {
    let mut ctx = make_ctx();

    let match_key = batch_kv(&mut ctx, "smj_left", 51_000);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "smj_right".into(),
            key: match_key.as_bytes().to_vec(),
            value: b"smatch".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: "smj_left".into(),
            right_collection: "smj_right".into(),
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            pre_sorted: false,
        }),
    );

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        1,
        "sort-merge join must find the match that sits beyond index 50k; got {} rows",
        rows.len()
    );
}

/// Same completeness check for the nested-loop join handler.
#[test]
fn nested_loop_join_completeness_past_50k_cap() {
    let mut ctx = make_ctx();

    let match_key = batch_kv(&mut ctx, "nlj_left", 51_000);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "nlj_right".into(),
            key: match_key.as_bytes().to_vec(),
            value: b"nlmatch".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Nested-loop join with no condition = cross join; the limit gates the output.
    // Use a limit large enough to let the single match through.
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nlj_left".into(),
            right_collection: "nlj_right".into(),
            // Serialize a ScanFilter on the key column to restrict to the match.
            condition: zerompk::to_msgpack_vec(&vec![ScanFilter {
                field: "nlj_left.key".into(),
                op: FilterOp::EqColumn,
                value: nodedb_types::Value::String("nlj_right.key".into()),
                clauses: Vec::new(),
                expr: None,
            }])
            .unwrap(),
            join_type: "inner".into(),
            limit: 1_000_000,
        }),
    );

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    // The cross-product of 51k × 1 filtered to matching keys = 1 row.
    assert_eq!(
        rows.len(),
        1,
        "nested-loop join must find the match that sits beyond index 50k; got {} rows",
        rows.len()
    );
}

// ── 2. Deterministic error when a side exceeds the byte budget ────────────────

/// Hash join: left side (probe) over budget → `ResourcesExhausted`.
/// This specifically exercises the newly-added left-side guard.
#[test]
fn hash_join_left_side_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    // Tiny budget: 256 bytes — easily exceeded by even a handful of rows.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    // Large left side; small right side.
    batch_kv(&mut ctx, "bgt_left", 500);
    batch_kv(&mut ctx, "bgt_right", 1);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "bgt_left".into(),
            right_collection: "bgt_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "over-budget left side must surface an error, not a partial result"
    );
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::ResourcesExhausted),
        "expected ResourcesExhausted, got {:?}",
        resp.error_code
    );
}

/// Hash join: right side (build) over budget → `ResourcesExhausted`.
#[test]
fn hash_join_right_side_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    // Small left, large right.
    batch_kv(&mut ctx, "bgtr_left", 1);
    batch_kv(&mut ctx, "bgtr_right", 500);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "bgtr_left".into(),
            right_collection: "bgtr_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "over-budget right side must surface an error"
    );
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::ResourcesExhausted),
        "expected ResourcesExhausted, got {:?}",
        resp.error_code
    );
}

/// Sort-merge join: left side over budget → `ResourcesExhausted`.
#[test]
fn sort_merge_join_left_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "smjb_left", 500);
    batch_kv(&mut ctx, "smjb_right", 1);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: "smjb_left".into(),
            right_collection: "smjb_right".into(),
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            pre_sorted: false,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::ResourcesExhausted),
        "sort-merge left over-budget must surface ResourcesExhausted"
    );
}

/// Sort-merge join: right side over budget → `ResourcesExhausted`.
#[test]
fn sort_merge_join_right_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "smjbr_left", 1);
    batch_kv(&mut ctx, "smjbr_right", 500);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: "smjbr_left".into(),
            right_collection: "smjbr_right".into(),
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            pre_sorted: false,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::ResourcesExhausted),
        "sort-merge right over-budget must surface ResourcesExhausted"
    );
}

/// Nested-loop join: left side over budget → `ResourcesExhausted`.
#[test]
fn nested_loop_join_left_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "nljb_left", 500);
    batch_kv(&mut ctx, "nljb_right", 1);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nljb_left".into(),
            right_collection: "nljb_right".into(),
            condition: Vec::new(),
            join_type: "inner".into(),
            limit: 1_000_000,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::ResourcesExhausted),
        "nested-loop left over-budget must surface ResourcesExhausted"
    );
}

/// Nested-loop join: right side over budget → `ResourcesExhausted`.
#[test]
fn nested_loop_join_right_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "nljbr_left", 1);
    batch_kv(&mut ctx, "nljbr_right", 500);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nljbr_left".into(),
            right_collection: "nljbr_right".into(),
            condition: Vec::new(),
            join_type: "inner".into(),
            limit: 1_000_000,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::ResourcesExhausted),
        "nested-loop right over-budget must surface ResourcesExhausted"
    );
}

/// Budget of 0 is unlimited — a large join must complete without error.
#[test]
fn join_budget_zero_is_unlimited() {
    let mut ctx = make_ctx();

    // Explicitly set budget to 0 (unlimited).
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 0,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "unlim_left", 500);
    batch_kv(&mut ctx, "unlim_right", 500);

    // Hash join: both sides large but budget = 0 → must succeed.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "unlim_left".into(),
            right_collection: "unlim_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );
}
