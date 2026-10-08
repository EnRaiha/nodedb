// SPDX-License-Identifier: BUSL-1.1

//! Plain columnar profile temporal-purge.
//!
//! Row-level audit purge on bitemporal plain-columnar collections.
//! Walks flushed segments (through the shared flushed-segment reader) and
//! the live memtable, groups rows by primary key, and marks every
//! *superseded* row whose `_ts_system` is below the cutoff in the
//! engine's per-segment delete bitmap.
//!
//! The single latest version per PK is always preserved — even if it is
//! itself below the cutoff — so "AS OF" reads beyond the cutoff can still
//! resolve each logical row's terminal state.
//!
//! A version's row index is its physical position in its segment or in the
//! memtable: the index the delete bitmap marks.

use std::collections::HashMap;

use nodedb_columnar::MutationEngine;
use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;
use nodedb_types::{DatabaseId, TenantId};

use crate::data::executor::core_loop::CoreLoop;

/// The read path the corruption report names.
const SITE: &str = "columnar_temporal_purge";

pub(super) struct RowVersion {
    pub seg_id: u64,
    pub row_idx: u32,
    pub pk_bytes: Vec<u8>,
    pub sys_ts: i64,
}

impl CoreLoop {
    /// See module docs. Returns the number of rows tombstoned in
    /// per-segment delete bitmaps. No WAL records are appended here; the
    /// caller is responsible for the `RecordType::TemporalPurge` audit
    /// record that covers the batch.
    ///
    /// `Err` when a segment or memtable row does not read, or a row carries
    /// no system time. Nothing is marked then.
    pub(super) fn plain_columnar_purge(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        let key = (database_id, tid, collection.to_string());
        let victims_per_seg = {
            let Some(engine) = self.columnar_engines.get(&key) else {
                return Ok(0);
            };
            if !engine.schema().is_bitemporal() {
                return Ok(0);
            }
            let versions = self.plain_columnar_row_versions(&key, engine)?;
            purge_victims(engine, versions, cutoff_system_ms)
        };
        if victims_per_seg.is_empty() {
            return Ok(0);
        }

        let engine_mut =
            self.columnar_engines
                .get_mut(&key)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("columnar engine for '{collection}' is gone mid-purge"),
                })?;
        let mut total = 0usize;
        for (seg_id, mut row_indices) in victims_per_seg {
            row_indices.sort_unstable();
            row_indices.dedup();
            total += row_indices.len();
            engine_mut
                .delete_bitmap_mut(seg_id)
                .mark_deleted_batch(&row_indices);
        }
        Ok(total)
    }

    /// Every row version of the collection at `key`. A flushed row is listed
    /// tombstoned or not. A memtable row is listed only when live.
    fn plain_columnar_row_versions(
        &self,
        key: &(DatabaseId, TenantId, String),
        engine: &MutationEngine,
    ) -> crate::Result<Vec<RowVersion>> {
        let collection = key.2.as_str();
        let schema = engine.schema();
        let ts_idx = schema
            .ts_system_idx()
            .ok_or_else(|| crate::Error::Storage {
                engine: "columnar".into(),
                detail: "bitemporal schema missing _ts_system column".into(),
            })?;
        let pk_indices: Vec<usize> = engine.pk_col_indices().to_vec();
        if pk_indices.is_empty() {
            return Err(crate::Error::Storage {
                engine: "columnar".into(),
                detail: "bitemporal collection without primary key columns".into(),
            });
        }
        // Decode the columns up to the last one a version needs.
        let column_count = pk_indices.iter().copied().fold(ts_idx, usize::max) + 1;

        let mut versions: Vec<RowVersion> = Vec::new();

        // Flushed segments: segment_id starts at 1 (memtable is id 0).
        let segments = self
            .columnar_flushed_segments
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            let seg_id = seg_idx as u64 + 1;
            let segment =
                self.decode_flushed_segment(collection, seg_id, seg_bytes, column_count, SITE)?;
            for row_idx in 0..segment.row_count() {
                let ts_cell = segment.cell(ts_idx, row_idx, &ColumnType::Int64)?;
                let sys_ts =
                    system_time(&ts_cell, collection, seg_id, row_idx).inspect_err(|e| {
                        crate::diag::columnar_segment_corrupt(e, collection, seg_id, "cell", SITE);
                    })?;
                let pk_cells = pk_indices
                    .iter()
                    .map(|&i| segment.cell(i, row_idx, &schema.columns[i].column_type))
                    .collect::<crate::Result<Vec<Value>>>()?;
                versions.push(RowVersion {
                    seg_id,
                    row_idx: row_index_u32(collection, seg_id, row_idx)?,
                    pk_bytes: version_pk_bytes(&pk_cells),
                    sys_ts,
                });
            }
        }

        // Memtable rows, each at its physical index.
        let memtable_seg_id = engine.memtable_segment_id();
        for row_idx in 0..engine.memtable().row_count() {
            // `None` is a row the memtable delete bitmap marks.
            let Some(row) = engine.get_memtable_row(row_idx)? else {
                continue;
            };
            let sys_ts = system_time(
                row.get(ts_idx).unwrap_or(&Value::Null),
                collection,
                memtable_seg_id,
                row_idx,
            )?;
            let pk_cells = pk_indices
                .iter()
                .map(|&i| {
                    row.get(i).cloned().ok_or_else(|| crate::Error::Internal {
                        detail: format!(
                            "columnar '{collection}': memtable row {row_idx} holds no \
                             primary-key cell {i}"
                        ),
                    })
                })
                .collect::<crate::Result<Vec<Value>>>()?;
            versions.push(RowVersion {
                seg_id: memtable_seg_id,
                row_idx: row_index_u32(collection, memtable_seg_id, row_idx)?,
                pk_bytes: version_pk_bytes(&pk_cells),
                sys_ts,
            });
        }
        Ok(versions)
    }
}

/// The row indices to tombstone, per segment: every version superseded by a
/// later one of its PK, below `cutoff_system_ms`, and not tombstoned yet.
fn purge_victims(
    engine: &MutationEngine,
    versions: Vec<RowVersion>,
    cutoff_system_ms: i64,
) -> HashMap<u64, Vec<u32>> {
    let mut latest: HashMap<&[u8], i64> = HashMap::new();
    for v in &versions {
        latest
            .entry(v.pk_bytes.as_slice())
            .and_modify(|cur| *cur = (*cur).max(v.sys_ts))
            .or_insert(v.sys_ts);
    }

    let mut victims_per_seg: HashMap<u64, Vec<u32>> = HashMap::new();
    for v in &versions {
        let lat = latest
            .get(v.pk_bytes.as_slice())
            .copied()
            .unwrap_or(v.sys_ts);
        if v.sys_ts < cutoff_system_ms && v.sys_ts < lat {
            let already = engine
                .delete_bitmap(v.seg_id)
                .is_some_and(|bm| bm.is_deleted(v.row_idx));
            if !already {
                victims_per_seg.entry(v.seg_id).or_default().push(v.row_idx);
            }
        }
    }
    victims_per_seg
}

/// The system time `_ts_system` holds. The column is a required `Int64`, so
/// any other cell is an error.
fn system_time(cell: &Value, collection: &str, seg_id: u64, row_idx: usize) -> crate::Result<i64> {
    match cell {
        Value::Integer(n) => Ok(*n),
        other => Err(crate::Error::SegmentCorrupted {
            detail: format!(
                "columnar '{collection}': row {row_idx} of segment {seg_id} holds {other:?} \
                 in _ts_system, not an integer system time"
            ),
        }),
    }
}

/// Encode a version's primary key from its PK cells, in PK column order.
/// The bytes match what the engine's PK index holds for the same row.
fn version_pk_bytes(cells: &[Value]) -> Vec<u8> {
    match cells {
        [single] => nodedb_columnar::pk_index::encode_pk(single),
        _ => {
            let refs: Vec<&Value> = cells.iter().collect();
            nodedb_columnar::pk_index::encode_composite_pk(&refs)
        }
    }
}

/// `row_idx` as the `u32` row index a delete bitmap marks.
fn row_index_u32(collection: &str, seg_id: u64, row_idx: usize) -> crate::Result<u32> {
    u32::try_from(row_idx).map_err(|_| crate::Error::Internal {
        detail: format!(
            "columnar '{collection}': row {row_idx} of segment {seg_id} is past the u32 row \
             index range"
        ),
    })
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::pk_index::encode_pk;
    use nodedb_types::columnar::{ColumnDef, ColumnarSchema};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("_ts_valid_from", ColumnType::Int64),
            ColumnDef::required("_ts_valid_until", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ])
        .expect("valid")
    }

    fn version(sys_ts: i64, id: i64) -> Vec<Value> {
        vec![
            Value::Integer(sys_ts),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(id),
        ]
    }

    /// A tombstoned memtable row ahead of the superseded version does not
    /// shift the index the purge marks.
    #[test]
    fn the_purge_marks_the_physical_row_behind_a_deleted_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut engine = MutationEngine::new("bt".to_string(), schema());
        // Physical rows: 0 = id 9 (deleted below), 1 = id 1 at 10, 2 = id 1 at 50.
        engine.insert(&version(5, 9)).expect("insert");
        engine.insert(&version(10, 1)).expect("insert");
        engine.insert(&version(50, 1)).expect("insert");
        engine.delete(&Value::Integer(9)).expect("delete");
        let memtable_seg = engine.memtable_segment_id();
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        core.columnar_engines.insert(key.clone(), engine);

        let purged = core
            .plain_columnar_purge(key.0, key.1, "bt", 100)
            .expect("purge");

        assert_eq!(purged, 1, "the version of id 1 at 10 is superseded");
        let engine = core.columnar_engines.get(&key).expect("engine");
        let bitmap = engine.delete_bitmap(memtable_seg).expect("bitmap");
        assert!(bitmap.is_deleted(0), "the deleted row stays deleted");
        assert!(bitmap.is_deleted(1), "the superseded version is marked");
        assert!(!bitmap.is_deleted(2), "the latest version stays live");
        assert_eq!(
            engine
                .pk_index()
                .get(&encode_pk(&Value::Integer(1)))
                .map(|loc| loc.row_index),
            Some(2)
        );
    }
}
