// SPDX-License-Identifier: BUSL-1.1

// Note: bypasses WebSocket transport and Origin infrastructure.
// Drives `nodedb_array::sync::gc::collapse_below` directly against an
// InMemoryOpLog + MockSnapshotSink.
//
// Phase H (Origin GC task / ack vector collection from Lite) is not yet wired.
// These tests exercise the pure GC logic layer in nodedb-array.

mod common;

use common::array_sync::{hlc, rep};
use nodedb_array::sync::ack::AckVector;
use nodedb_array::sync::gc::{MockSnapshotSink, collapse_below};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::op_codec;
use nodedb_array::sync::op_log::{InMemoryOpLog, OpLog};
use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;

fn make_op(array: &str, ms: u64, rid: u64) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms, rid),
            schema_hlc: hlc(1, 1),
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(ms as i64)],
        attrs: Some(vec![CellValue::Null]),
    }
}

fn dummy_snapshot(array: &str, frontier: Hlc) -> TileSnapshot {
    TileSnapshot {
        array: array.into(),
        coord_range: CoordRange {
            lo: vec![CoordValue::Int64(0)],
            hi: vec![CoordValue::Int64(100)],
        },
        tile_blob: vec![0xBB; 8],
        snapshot_hlc: frontier,
        schema_hlc: hlc(1, 1),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// With no acks in the vector, GC cannot determine a safe frontier and
/// must be a no-op: no snapshots written, no ops dropped.
#[test]
fn gc_with_no_acks_is_noop() {
    let log = InMemoryOpLog::new();
    log.append(&make_op("arr", 10, 1)).expect("append");

    let acks = AckVector::new();
    let sink = MockSnapshotSink::new();

    let report = collapse_below(&log, &acks, &sink, |_, _| Ok(None)).expect("collapse_below");

    assert_eq!(report.snapshots_written, 0, "no snapshots without acks");
    assert_eq!(report.ops_dropped, 0, "no ops dropped without acks");
    assert_eq!(report.frontier, Hlc::ZERO);
    assert_eq!(log.len().expect("len"), 1, "log must be unchanged");
}

/// Ops below the min-ack frontier are collapsed into snapshots and dropped.
/// Ops at or above the frontier survive.
#[test]
fn gc_collapses_ops_below_frontier() {
    let log = InMemoryOpLog::new();
    for ms in [10u64, 20, 30, 40] {
        log.append(&make_op("arr", ms, 1)).expect("append");
    }

    let mut acks = AckVector::new();
    acks.record(rep(1), hlc(25, 1)); // frontier = 25

    let sink = MockSnapshotSink::new();
    let report = collapse_below(&log, &acks, &sink, |array, frontier| {
        Ok(Some(dummy_snapshot(array, frontier)))
    })
    .expect("collapse_below");

    // Ops strictly < 25: ms=10, ms=20 → 2 ops dropped.
    assert_eq!(report.frontier, hlc(25, 1));
    assert_eq!(
        report.ops_dropped, 2,
        "ops at ms=10 and ms=20 must be dropped"
    );
    assert_eq!(
        report.snapshots_written, 1,
        "one snapshot written for 'arr'"
    );
    assert_eq!(log.len().expect("len"), 2, "ms=30 and ms=40 must remain");
}

/// Multiple acks from different replicas: the frontier is the minimum.
#[test]
fn gc_uses_min_ack_as_frontier() {
    let log = InMemoryOpLog::new();
    for ms in [10u64, 20, 30, 40, 50] {
        log.append(&make_op("arr", ms, 1)).expect("append");
    }

    let mut acks = AckVector::new();
    acks.record(rep(1), hlc(50, 1)); // replica 1 has seen up to 50
    acks.record(rep(2), hlc(30, 2)); // replica 2 has seen up to 30 → frontier = 30

    let sink = MockSnapshotSink::new();
    let report = collapse_below(&log, &acks, &sink, |array, frontier| {
        Ok(Some(dummy_snapshot(array, frontier)))
    })
    .expect("collapse_below");

    assert_eq!(report.frontier, hlc(30, 2), "frontier must be min-ack");
    // HLC lex order is (physical_ms, logical, replica_id). Ops live at replica
    // id 1 with frontier replica id 2, so (30,1) < (30,2) — three ops drop.
    assert_eq!(
        report.ops_dropped, 3,
        "ops at ms=10, 20, 30 (replica<2) drop"
    );
    assert_eq!(log.len().expect("len"), 2, "ms=40 and ms=50 survive");
}

/// After GC collapses ops into a snapshot, that snapshot contains all the
/// op data so a new peer can reconstruct state from it.
/// Exercises `MockSnapshotSink::into_snapshots` to verify snapshot content.
#[test]
fn gc_snapshot_contains_ops_for_new_peer_catchup() {
    // Build 3 ops for "gc_array".
    let schema_hlc_val = hlc(1, 1);
    let ops: Vec<ArrayOp> = (1u64..=3)
        .map(|i| ArrayOp {
            header: ArrayOpHeader {
                array: "gc_array".into(),
                hlc: hlc(i * 100, 1),
                schema_hlc: schema_hlc_val,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: (i * 100) as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(i as i64)],
            attrs: Some(vec![CellValue::Float64(i as f64)]),
        })
        .collect();

    let log = InMemoryOpLog::new();
    for op in &ops {
        log.append(op).expect("append");
    }

    // Encode all 3 ops into the snapshot blob (simulating the Origin snapshot builder).
    let blob = op_codec::encode_op_batch(&ops).expect("encode_op_batch");

    // Frontier covers all 3 ops.
    let frontier = hlc(400, 1);
    let mut acks = AckVector::new();
    acks.record(rep(1), frontier);

    let sink = MockSnapshotSink::new();
    collapse_below(&log, &acks, &sink, |array, f| {
        Ok(Some(TileSnapshot {
            array: array.into(),
            coord_range: CoordRange {
                lo: vec![CoordValue::Int64(0)],
                hi: vec![CoordValue::Int64(10)],
            },
            tile_blob: blob.clone(),
            snapshot_hlc: f,
            schema_hlc: schema_hlc_val,
        }))
    })
    .expect("collapse_below");

    // One snapshot produced.
    assert_eq!(sink.snapshot_count(), 1);
    let snapshots = sink.into_snapshots();
    let snap = &snapshots[0];

    // Verify new peer can decode the blob back to ops.
    let decoded_ops = op_codec::decode_op_batch(&snap.tile_blob).expect("decode_op_batch");
    assert_eq!(
        decoded_ops.len(),
        3,
        "snapshot blob must contain all 3 ops for new peer catch-up"
    );

    // Verify all 3 coords are in the decoded ops.
    let coords: Vec<i64> = decoded_ops
        .iter()
        .filter_map(|op| match op.coord.first() {
            Some(CoordValue::Int64(x)) => Some(*x),
            _ => None,
        })
        .collect();
    for i in 1i64..=3 {
        assert!(
            coords.contains(&i),
            "coord {i} must be present in snapshot ops"
        );
    }
}

/// Missing live state blocks GC so no array loses its only recoverable ops.
#[test]
fn gc_refuses_to_prune_when_an_array_has_no_live_state() {
    let log = InMemoryOpLog::new();
    log.append(&make_op("gone", 10, 1)).expect("append");
    log.append(&make_op("alive", 20, 1)).expect("append");

    let mut acks = AckVector::new();
    acks.record(rep(1), hlc(50, 1));

    let sink = MockSnapshotSink::new();
    let result = collapse_below(&log, &acks, &sink, |array, frontier| {
        if array == "alive" {
            Ok(Some(dummy_snapshot(array, frontier)))
        } else {
            Ok(None) // "gone" has no durable recovery snapshot
        }
    });

    assert!(result.is_err());
    assert_eq!(log.len().expect("len"), 2, "no operation may be pruned");
}

/// GC propagates snapshot errors without mutating the log.
#[test]
fn gc_propagates_snapshot_error_and_leaves_log_intact() {
    use nodedb_array::error::ArrayError;

    let log = InMemoryOpLog::new();
    log.append(&make_op("bad", 10, 1)).expect("append");

    let mut acks = AckVector::new();
    acks.record(rep(1), hlc(50, 1));

    let sink = MockSnapshotSink::new();
    let result = collapse_below(&log, &acks, &sink, |_, _| {
        Err(ArrayError::SegmentCorruption {
            detail: "simulated snapshot write failure".into(),
        })
    });

    assert!(result.is_err(), "GC must propagate snapshot errors");
    assert_eq!(
        log.len().expect("len"),
        1,
        "log must be unchanged when snapshot fails"
    );
}

/// Multiple distinct arrays each get their own snapshot on GC.
#[test]
fn gc_produces_one_snapshot_per_array() {
    let log = InMemoryOpLog::new();
    log.append(&make_op("a1", 10, 1)).expect("append");
    log.append(&make_op("a2", 20, 1)).expect("append");
    log.append(&make_op("a3", 30, 1)).expect("append");

    let mut acks = AckVector::new();
    acks.record(rep(1), hlc(100, 1));

    let sink = MockSnapshotSink::new();
    let report = collapse_below(&log, &acks, &sink, |array, frontier| {
        Ok(Some(dummy_snapshot(array, frontier)))
    })
    .expect("collapse_below");

    assert_eq!(
        report.snapshots_written, 3,
        "one snapshot per distinct array"
    );
    assert_eq!(report.ops_dropped, 3);
}

/// `AckVector` records are monotonic: a lower HLC cannot regress the watermark.
#[test]
fn ack_vector_watermark_is_monotonic() {
    let mut acks = AckVector::new();
    let h50 = hlc(50, 1);
    let h100 = hlc(100, 1);
    let h30 = hlc(30, 1);

    acks.record(rep(1), h100);
    acks.record(rep(1), h30); // must not regress
    acks.record(rep(1), h50); // still below h100

    assert_eq!(
        acks.min_ack_hlc(),
        Some(h100),
        "watermark must remain at max recorded HLC"
    );
}
