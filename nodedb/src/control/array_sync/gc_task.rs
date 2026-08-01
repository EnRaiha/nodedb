// SPDX-License-Identifier: BUSL-1.1

//! [`ArrayGcTask`] — periodic log compaction for array CRDT sync.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot};
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::DatabaseId;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::ack_registry::ArrayAckRegistry;
use super::op_log::OriginOpLog;
use super::snapshot_store::OriginSnapshotStore;
use crate::control::shutdown::{ShutdownReceiver, ShutdownWatch};

/// Default GC interval: 60 seconds.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Spawn the GC task and return a [`JoinHandle`].
pub fn spawn(
    op_log: Arc<OriginOpLog>,
    snapshots: Arc<OriginSnapshotStore>,
    ack_registry: Arc<ArrayAckRegistry>,
    array_snapshot_hlcs: super::scope::ArraySnapshotHlcs,
    shutdown: Arc<ShutdownWatch>,
    interval: Duration,
) -> Option<JoinHandle<()>> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        debug!("array_gc_task: no tokio runtime; skipping spawn (test or non-async context)");
        return None;
    };
    Some(handle.spawn(async move {
        let mut shutdown_rx: ShutdownReceiver = shutdown.subscribe();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    run_gc(&op_log, &snapshots, &ack_registry, &array_snapshot_hlcs);
                }
                _ = shutdown_rx.wait_cancelled() => {
                    debug!("array_gc_task: shutdown received — exiting");
                    return;
                }
            }
        }
    }))
}

/// Execute one GC pass across all known database/array scopes.
fn run_gc(
    op_log: &OriginOpLog,
    snapshots: &OriginSnapshotStore,
    ack_registry: &ArrayAckRegistry,
    array_snapshot_hlcs: &RwLock<HashMap<(DatabaseId, u64, String), Hlc>>,
) {
    let scopes = ack_registry.known_array_scopes();
    if scopes.is_empty() {
        debug!("array_gc_task: no arrays with acks — skipping");
        return;
    }

    for (database_id, tenant_id, array) in &scopes {
        let ack_vector = ack_registry.ack_vector_in_database(*database_id, *tenant_id, array);
        let Some(frontier) = ack_vector.min_ack_hlc() else {
            debug!(database = %database_id, array = %array, "array_gc_task: no min_ack for array — skipping");
            continue;
        };

        // Build and persist a replacement snapshot before deleting any log row.
        // A snapshot is an op batch, not a label for an earlier batch: recovery
        // must be able to replay it without consulting already-pruned history.
        let array_name = array.clone();
        let report = build_snapshot_through_frontier(
            op_log,
            snapshots,
            *database_id,
            *tenant_id,
            array,
            frontier,
        )
        .and_then(|snapshot| {
            snapshots
                .put_in_database(*database_id, *tenant_id, &snapshot)
                .map_err(|error| nodedb_array::error::ArrayError::SegmentCorruption {
                    detail: format!("array GC snapshot persist for '{array}': {error}"),
                })?;
            let ops_dropped =
                op_log.drop_array_below_in_database(*database_id, *tenant_id, array, frontier)?;
            Ok((ops_dropped, 1, frontier))
        });

        match report {
            Ok((ops_dropped, snapshots_written, frontier)) => {
                if ops_dropped > 0 || snapshots_written > 0 {
                    info!(
                        database = %database_id,
                        array = %array_name,
                        ops_dropped,
                        snapshots_written,
                        frontier = ?frontier,
                        "array_gc_task: GC run complete"
                    );
                }
                snapshots.delete_older_than_in_database(
                    *database_id,
                    *tenant_id,
                    &array_name,
                    frontier,
                );
                match array_snapshot_hlcs.write() {
                    Ok(mut map) => {
                        map.insert((*database_id, *tenant_id, array_name.clone()), frontier);
                    }
                    Err(e) => {
                        error!(
                            database = %database_id,
                            array = %array_name,
                            error = %e,
                            "array_gc_task: snapshot_hlc map poisoned"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    database = %database_id,
                    array = %array_name,
                    error = %e,
                    "array_gc_task: GC error — skipping this array"
                );
            }
        }
    }
}

/// Materialize one self-contained, structurally scoped op-batch snapshot.
///
/// The previous snapshot is decoded rather than relabelled, then merged with
/// every same-scope log operation through `frontier`. HLC is the op-log's
/// idempotency key, so a `BTreeMap` both orders and deduplicates the batch.
/// There is deliberately no empty/no-prior snapshot: without either a prior
/// range or a coordinate-bearing operation we cannot claim recoverable state.
fn build_snapshot_through_frontier(
    op_log: &OriginOpLog,
    snapshots: &OriginSnapshotStore,
    database_id: DatabaseId,
    tenant_id: u64,
    array: &str,
    frontier: Hlc,
) -> nodedb_array::error::ArrayResult<TileSnapshot> {
    let prior = snapshots.latest_for_array_in_database(database_id, tenant_id, array);
    let mut ops = BTreeMap::<Hlc, ArrayOp>::new();
    let mut coord_range = None;
    let mut schema_hlc = Hlc::ZERO;

    if let Some(snapshot) = prior {
        if snapshot.array != array {
            return Err(nodedb_array::error::ArrayError::SegmentCorruption {
                detail: format!(
                    "array GC snapshot scope mismatch: expected '{array}', got '{}'",
                    snapshot.array
                ),
            });
        }
        coord_range = Some(snapshot.coord_range);
        // A later snapshot can be present when ACK frontiers regress; its
        // range remains conservative, but its schema watermark is not part of
        // this frontier unless the decoded operations establish it.
        if snapshot.snapshot_hlc <= frontier {
            schema_hlc = snapshot.schema_hlc;
        }
        for op in op_codec::decode_op_batch(&snapshot.tile_blob)? {
            if op.header.array != array {
                return Err(nodedb_array::error::ArrayError::SegmentCorruption {
                    detail: format!(
                        "array GC snapshot for '{array}' contains op for '{}'",
                        op.header.array
                    ),
                });
            }
            if op.header.hlc <= frontier {
                schema_hlc = schema_hlc.max(op.header.schema_hlc);
                ops.insert(op.header.hlc, op);
            }
        }
    }

    for item in op_log.scan_range_in_database(database_id, tenant_id, array, Hlc::ZERO, frontier)? {
        let op = item?;
        if op.header.array != array {
            return Err(nodedb_array::error::ArrayError::SegmentCorruption {
                detail: format!(
                    "array GC log scope mismatch: expected '{array}', got '{}'",
                    op.header.array
                ),
            });
        }
        schema_hlc = schema_hlc.max(op.header.schema_hlc);
        ops.insert(op.header.hlc, op);
    }

    if ops.is_empty() {
        return Err(nodedb_array::error::ArrayError::SegmentCorruption {
            detail: format!(
                "array GC refused to advance '{array}' without recoverable snapshot content"
            ),
        });
    }
    for op in ops.values() {
        coord_range = Some(merge_coord_range(coord_range, &op.coord)?);
    }
    let coord_range =
        coord_range.ok_or_else(|| nodedb_array::error::ArrayError::SegmentCorruption {
            detail: format!(
                "array GC refused to advance '{array}' without recoverable snapshot content"
            ),
        })?;

    Ok(TileSnapshot {
        array: array.to_owned(),
        coord_range,
        tile_blob: op_codec::encode_op_batch(&ops.into_values().collect::<Vec<_>>())?,
        snapshot_hlc: frontier,
        schema_hlc,
    })
}

fn merge_coord_range(
    existing: Option<CoordRange>,
    coord: &[CoordValue],
) -> nodedb_array::error::ArrayResult<CoordRange> {
    let mut range = existing.unwrap_or_else(|| CoordRange {
        lo: coord.to_vec(),
        hi: coord.iter().map(exclusive_upper_bound).collect(),
    });
    if range.lo.len() != coord.len() || range.hi.len() != coord.len() {
        return Err(nodedb_array::error::ArrayError::SegmentCorruption {
            detail: "array GC encountered incompatible coordinate arity".into(),
        });
    }
    for (index, value) in coord.iter().enumerate() {
        if value < &range.lo[index] {
            range.lo[index] = value.clone();
        }
        let upper = exclusive_upper_bound(value);
        if upper > range.hi[index] {
            range.hi[index] = upper;
        }
    }
    Ok(range)
}

fn exclusive_upper_bound(value: &CoordValue) -> CoordValue {
    match value {
        CoordValue::Int64(value) => CoordValue::Int64(value.saturating_add(1)),
        CoordValue::TimestampMs(value) => CoordValue::TimestampMs(value.saturating_add(1)),
        CoordValue::Float64(value) => CoordValue::Float64(value.next_up()),
        // Appending NUL is the smallest lexicographically larger Rust string.
        CoordValue::String(value) => CoordValue::String(format!("{value}\0")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::op_codec;
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    fn snapshot(array: &str, ms: u64) -> TileSnapshot {
        TileSnapshot {
            array: array.to_owned(),
            coord_range: CoordRange {
                lo: vec![CoordValue::Int64(0)],
                hi: vec![CoordValue::Int64(1)],
            },
            tile_blob: Vec::new(),
            snapshot_hlc: hlc(ms),
            schema_hlc: hlc(1),
        }
    }

    fn op(array: &str, ms: u64, schema_ms: u64, coord: i64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms),
                schema_hlc: hlc(schema_ms),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(coord)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    /// The four in-memory stores one GC round operates on.
    struct Fixture {
        op_log: OriginOpLog,
        snapshots: Arc<OriginSnapshotStore>,
        acks: Arc<ArrayAckRegistry>,
        frontiers: RwLock<HashMap<crate::control::array_sync::ArrayScopeKey, Hlc>>,
    }

    fn setup() -> Fixture {
        Fixture {
            op_log: OriginOpLog::open_in_memory().unwrap(),
            snapshots: OriginSnapshotStore::open_in_memory().unwrap(),
            acks: ArrayAckRegistry::open_in_memory().unwrap(),
            frontiers: RwLock::new(HashMap::new()),
        }
    }

    #[test]
    fn no_snapshot_and_no_ops_does_not_advance_shared_frontier() {
        let Fixture {
            op_log,
            snapshots,
            acks,
            frontiers,
        } = setup();
        acks.record_in_database(DatabaseId::DEFAULT, 0, "arr", ReplicaId::new(1), hlc(50));
        frontiers
            .write()
            .unwrap()
            .insert((DatabaseId::DEFAULT, 0, "arr".into()), hlc(10));

        run_gc(&op_log, &snapshots, &acks, &frontiers);

        assert_eq!(
            frontiers
                .read()
                .unwrap()
                .get(&(DatabaseId::DEFAULT, 0, "arr".into())),
            Some(&hlc(10))
        );
    }

    #[test]
    fn no_prior_snapshot_recovers_all_ops_before_pruning() {
        let Fixture {
            op_log,
            snapshots,
            acks,
            frontiers,
        } = setup();
        let database_id = DatabaseId::new(42);
        op_log
            .append_in_database(database_id, 7, &op("arr", 10, 2, 3))
            .unwrap();
        op_log
            .append_in_database(database_id, 7, &op("arr", 20, 5, 9))
            .unwrap();
        acks.record_in_database(database_id, 7, "arr", ReplicaId::new(1), hlc(30));

        run_gc(&op_log, &snapshots, &acks, &frontiers);

        let snapshot = snapshots
            .latest_for_array_in_database(database_id, 7, "arr")
            .unwrap();
        assert_eq!(snapshot.snapshot_hlc, hlc(30));
        assert_eq!(snapshot.schema_hlc, hlc(5));
        assert_eq!(snapshot.coord_range.lo, vec![CoordValue::Int64(3)]);
        assert_eq!(snapshot.coord_range.hi, vec![CoordValue::Int64(10)]);
        assert_eq!(
            op_codec::decode_op_batch(&snapshot.tile_blob)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(op_log.len_in_database(database_id, 7).unwrap(), 0);
        assert_eq!(
            frontiers
                .read()
                .unwrap()
                .get(&(database_id, 7, "arr".into())),
            Some(&hlc(30))
        );
    }

    #[test]
    fn incremental_prior_snapshot_recovers_deduplicated_ops_before_pruning() {
        let Fixture {
            op_log,
            snapshots,
            acks,
            frontiers,
        } = setup();
        let database_id = DatabaseId::new(43);
        let prior_ops = vec![op("arr", 10, 2, 1), op("arr", 20, 3, 2)];
        snapshots
            .put_in_database(
                database_id,
                9,
                &TileSnapshot {
                    array: "arr".into(),
                    coord_range: CoordRange {
                        lo: vec![CoordValue::Int64(1)],
                        hi: vec![CoordValue::Int64(3)],
                    },
                    tile_blob: op_codec::encode_op_batch(&prior_ops).unwrap(),
                    snapshot_hlc: hlc(25),
                    schema_hlc: hlc(3),
                },
            )
            .unwrap();
        // The repeated HLC is intentionally present in the live log: GC must
        // deduplicate it with the recovered prior batch.
        op_log
            .append_in_database(database_id, 9, &op("arr", 20, 3, 2))
            .unwrap();
        op_log
            .append_in_database(database_id, 9, &op("arr", 30, 8, 8))
            .unwrap();
        acks.record_in_database(database_id, 9, "arr", ReplicaId::new(1), hlc(40));

        run_gc(&op_log, &snapshots, &acks, &frontiers);

        let snapshot = snapshots
            .latest_for_array_in_database(database_id, 9, "arr")
            .unwrap();
        let recovered = op_codec::decode_op_batch(&snapshot.tile_blob).unwrap();
        assert_eq!(snapshot.snapshot_hlc, hlc(40));
        assert_eq!(snapshot.schema_hlc, hlc(8));
        assert_eq!(snapshot.coord_range.lo, vec![CoordValue::Int64(1)]);
        assert_eq!(snapshot.coord_range.hi, vec![CoordValue::Int64(9)]);
        assert_eq!(
            recovered.iter().map(|op| op.header.hlc).collect::<Vec<_>>(),
            vec![hlc(10), hlc(20), hlc(30)]
        );
        assert_eq!(op_log.len_in_database(database_id, 9).unwrap(), 0);
    }

    #[test]
    fn stale_snapshot_and_no_ops_does_not_relabel_frontier() {
        let Fixture {
            op_log,
            snapshots,
            acks,
            frontiers,
        } = setup();
        snapshots.put(&snapshot("arr", 20)).unwrap();
        acks.record_in_database(DatabaseId::DEFAULT, 0, "arr", ReplicaId::new(1), hlc(50));
        frontiers
            .write()
            .unwrap()
            .insert((DatabaseId::DEFAULT, 0, "arr".into()), hlc(10));

        run_gc(&op_log, &snapshots, &acks, &frontiers);

        assert_eq!(
            frontiers
                .read()
                .unwrap()
                .get(&(DatabaseId::DEFAULT, 0, "arr".into())),
            Some(&hlc(10))
        );
        assert!(snapshots.get("arr", hlc(20)).is_some());
    }
}
