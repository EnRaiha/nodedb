// SPDX-License-Identifier: Apache-2.0

//! Log compaction: collapse ops below the min-ack frontier into snapshots.
//!
//! [`collapse_below`] is the pure GC entry point. It consults an
//! [`AckVector`] to find the min-ack frontier, writes one snapshot per
//! affected array via a [`SnapshotSink`], then drops the compacted ops from
//! the log. No I/O beyond the abstract traits.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::error::ArrayResult;
use crate::sync::ack::AckVector;
use crate::sync::hlc::Hlc;
use crate::sync::op_log::OpLog;
use crate::sync::snapshot::{SnapshotSink, TileSnapshot};

// ─── GcReport ────────────────────────────────────────────────────────────────

/// Summary produced by [`collapse_below`].
#[derive(Clone, Debug, PartialEq)]
pub struct GcReport {
    /// Number of arrays for which a snapshot was successfully written.
    pub snapshots_written: u64,
    /// Number of ops dropped from the log below the frontier.
    pub ops_dropped: u64,
    /// The GC frontier HLC used for this run.
    pub frontier: Hlc,
}

// ─── collapse_below ───────────────────────────────────────────────────────────

/// Compact ops below the min-ack frontier into snapshots, then prune the log.
///
/// Algorithm:
/// 1. Determine frontier = `acks.min_ack_hlc()`. If `None`, return early with
///    an empty report — GC must not proceed without knowing every peer's
///    progress.
/// 2. Collect distinct array names whose ops have `hlc < frontier`.
/// 3. For each such array, call `snapshot_for_array(name, frontier)`.
///    - `Some(snap)` → write via `sink.write_snapshot(&snap)` and increment
///      `snapshots_written`.
///    - `None` → fail without pruning: no operation may be discarded unless
///      a snapshot represents that array through the frontier.
///    - `Err` → propagate immediately; the log is **not** mutated.
/// 4. After all snapshots succeed, call `log.drop_below(frontier)` and record
///    the count in `ops_dropped`.
/// 5. Return the report.
pub fn collapse_below(
    log: &dyn OpLog,
    acks: &AckVector,
    sink: &dyn SnapshotSink,
    snapshot_for_array: impl Fn(&str, Hlc) -> ArrayResult<Option<TileSnapshot>>,
) -> ArrayResult<GcReport> {
    let frontier = match acks.min_ack_hlc() {
        None => {
            return Ok(GcReport {
                snapshots_written: 0,
                ops_dropped: 0,
                frontier: Hlc::ZERO,
            });
        }
        Some(h) => h,
    };

    // Collect distinct array names with ops below the frontier.
    let mut arrays_to_snapshot: HashSet<String> = HashSet::new();
    for item in log.scan_from(Hlc::ZERO)? {
        let op = item?;
        if op.header.hlc < frontier {
            arrays_to_snapshot.insert(op.header.array.clone());
        }
    }

    // Write snapshots — abort on the first error; do NOT mutate the log yet.
    let mut snapshots_written: u64 = 0;
    for array in &arrays_to_snapshot {
        let snap = snapshot_for_array(array, frontier)?.ok_or_else(|| {
            crate::error::ArrayError::SegmentCorruption {
                detail: format!(
                    "array GC refused to prune '{array}' without a snapshot through {frontier:?}"
                ),
            }
        })?;
        if snap.array != *array {
            return Err(crate::error::ArrayError::SegmentCorruption {
                detail: format!(
                    "array GC snapshot array mismatch: expected '{array}', got '{}'",
                    snap.array
                ),
            });
        }
        if snap.snapshot_hlc < frontier {
            return Err(crate::error::ArrayError::SegmentCorruption {
                detail: format!(
                    "array GC snapshot for '{array}' is stale: {:?} < {frontier:?}",
                    snap.snapshot_hlc
                ),
            });
        }
        sink.write_snapshot(&snap)?;
        snapshots_written += 1;
    }

    // All snapshots succeeded — now prune the log.
    let ops_dropped = log.drop_below(frontier)?;

    Ok(GcReport {
        snapshots_written,
        ops_dropped,
        frontier,
    })
}

// ─── MockSnapshotSink ────────────────────────────────────────────────────────

/// In-memory [`SnapshotSink`] suitable for tests in any crate that depends
/// on `nodedb-array`.
pub struct MockSnapshotSink {
    snapshots: Mutex<Vec<TileSnapshot>>,
}

impl MockSnapshotSink {
    /// Create an empty sink.
    pub fn new() -> Self {
        Self {
            snapshots: Mutex::new(Vec::new()),
        }
    }

    /// Return the number of snapshots written so far.
    pub fn snapshot_count(&self) -> usize {
        self.snapshots
            .lock()
            .expect("invariant: MockSnapshotSink mutex is not poisoned")
            .len()
    }

    /// Consume the sink, returning all written snapshots.
    pub fn into_snapshots(self) -> Vec<TileSnapshot> {
        self.snapshots
            .into_inner()
            .expect("invariant: MockSnapshotSink mutex is not poisoned")
    }
}

impl Default for MockSnapshotSink {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotSink for MockSnapshotSink {
    fn write_snapshot(&self, snapshot: &TileSnapshot) -> crate::error::ArrayResult<()> {
        self.snapshots
            .lock()
            .expect("invariant: MockSnapshotSink mutex is not poisoned")
            .push(snapshot.clone());
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ArrayError;
    use crate::sync::hlc::Hlc;
    use crate::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
    use crate::sync::op_log::InMemoryOpLog;
    use crate::sync::replica_id::ReplicaId;
    use crate::sync::snapshot::CoordRange;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;

    fn replica() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, replica()).unwrap()
    }

    fn make_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms),
                schema_hlc: hlc(1),
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
            tile_blob: vec![0xAA; 16],
            snapshot_hlc: frontier,
            schema_hlc: hlc(1),
        }
    }

    #[test]
    fn gc_with_no_acks_is_noop() {
        let log = InMemoryOpLog::new();
        log.append(&make_op("arr", 10)).unwrap();
        let acks = AckVector::new();
        let sink = MockSnapshotSink::new();

        let report = collapse_below(&log, &acks, &sink, |_, _| Ok(None)).unwrap();

        assert_eq!(report.snapshots_written, 0);
        assert_eq!(report.ops_dropped, 0);
        assert_eq!(report.frontier, Hlc::ZERO);
        assert_eq!(log.len().unwrap(), 1);
    }

    #[test]
    fn gc_collapses_below_min_ack() {
        let log = InMemoryOpLog::new();
        for ms in [10, 20, 30, 40] {
            log.append(&make_op("arr", ms)).unwrap();
        }
        let mut acks = AckVector::new();
        acks.record(replica(), hlc(25));

        let sink = MockSnapshotSink::new();
        let report = collapse_below(&log, &acks, &sink, |array, frontier| {
            Ok(Some(dummy_snapshot(array, frontier)))
        })
        .unwrap();

        // frontier = 25; ops at 10 and 20 are below it
        assert_eq!(report.frontier, hlc(25));
        assert_eq!(report.ops_dropped, 2);
        // One distinct array ("arr") had ops below frontier
        assert_eq!(report.snapshots_written, 1);
        assert_eq!(sink.snapshot_count(), 1);
        // Remaining ops: ms=25 (excluded by strict <), ms=30, ms=40
        // drop_below(25) drops strictly < 25 → drops 10, 20 → 2 remain (30 and 40)
        assert_eq!(log.len().unwrap(), 2);
    }

    #[test]
    fn gc_refuses_to_prune_when_any_array_has_no_live_snapshot() {
        let log = InMemoryOpLog::new();
        log.append(&make_op("alive", 10)).unwrap();
        log.append(&make_op("dead", 10)).unwrap();

        let mut acks = AckVector::new();
        acks.record(replica(), hlc(50));

        let sink = MockSnapshotSink::new();
        let result = collapse_below(&log, &acks, &sink, |array, frontier| {
            if array == "alive" {
                Ok(Some(dummy_snapshot(array, frontier)))
            } else {
                Ok(None)
            }
        });

        assert!(result.is_err());
        assert_eq!(log.len().unwrap(), 2);
    }

    #[test]
    fn gc_propagates_snapshot_error() {
        let log = InMemoryOpLog::new();
        log.append(&make_op("arr", 10)).unwrap();

        let mut acks = AckVector::new();
        acks.record(replica(), hlc(50));

        let sink = MockSnapshotSink::new();
        let result = collapse_below(&log, &acks, &sink, |_, _| {
            Err(ArrayError::SegmentCorruption {
                detail: "simulated snapshot error".into(),
            })
        });

        assert!(result.is_err());
        // Log must be unchanged.
        assert_eq!(log.len().unwrap(), 1);
    }

    #[test]
    fn gc_rejects_snapshot_for_wrong_array_without_pruning() {
        let log = InMemoryOpLog::new();
        log.append(&make_op("arr", 10)).unwrap();
        let mut acks = AckVector::new();
        acks.record(replica(), hlc(50));
        let sink = MockSnapshotSink::new();

        let result = collapse_below(&log, &acks, &sink, |_, frontier| {
            Ok(Some(dummy_snapshot("other", frontier)))
        });

        assert!(matches!(result, Err(ArrayError::SegmentCorruption { .. })));
        assert_eq!(log.len().unwrap(), 1);
        assert_eq!(sink.snapshot_count(), 0);
    }

    #[test]
    fn gc_min_ack_with_two_replicas() {
        let log = InMemoryOpLog::new();
        for ms in [10, 20, 30, 40] {
            log.append(&make_op("arr", ms)).unwrap();
        }

        let r2 = ReplicaId::new(2);
        let mut acks = AckVector::new();
        acks.record(replica(), hlc(50)); // replica 1 ack at 50
        acks.record(r2, hlc(30)); // replica 2 ack at 30 → frontier = 30

        let sink = MockSnapshotSink::new();
        let report = collapse_below(&log, &acks, &sink, |array, frontier| {
            Ok(Some(dummy_snapshot(array, frontier)))
        })
        .unwrap();

        assert_eq!(report.frontier, hlc(30));
        // Ops at 10, 20 are strictly < 30 → 2 dropped; 30, 40 survive.
        assert_eq!(report.ops_dropped, 2);
        assert_eq!(log.len().unwrap(), 2);
    }
}
