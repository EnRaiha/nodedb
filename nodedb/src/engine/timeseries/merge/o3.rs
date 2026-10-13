// SPDX-License-Identifier: BUSL-1.1

//! Out-of-order buffer merge into an existing sealed partition.

use std::path::Path;

use nodedb_types::timeseries::PartitionMeta;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::{
    ColumnarSegmentReader, ColumnarSegmentWriter, SegmentError,
};
use crate::engine::timeseries::o3_buffer::O3Row;

/// Incrementally merge O3 buffer rows into an existing sealed partition.
///
/// Copy-on-write: reads the existing partition, merges O3 rows in sorted order,
/// writes a new partition file, then atomically swaps via rename.
/// The old partition file is untouched until the swap succeeds — concurrent
/// queries read the old version until the swap completes.
///
/// ## Continuous-aggregate invariant
///
/// Every continuous aggregate records, by directory name, the sealed
/// partitions it has folded. A merge must keep that record exact:
///
/// - A merge moves rows and never changes them. The merged partition holds
///   exactly the union of its sources' rows.
/// - The merged directory name joins an aggregate's folded set only when
///   every source partition was already in it. The source names then leave
///   the set.
/// - A group where some aggregate has folded some sources and not others
///   does not merge. The maintenance tick folds the missing sources first.
/// - A rewrite that adds, removes, or changes rows in place is forbidden.
///   The folded set cannot express it, and the aggregate would count those
///   rows twice or lose them.
/// - The aggregate image is rewritten after the merge commits, before the
///   source directories are removed. A crash between the two leaves both
///   names on disk, and the image still names only the sources.
///
/// `kek` is the timeseries segment key: the partition is read and written
/// with it. `None` reads and writes plaintext.
pub fn merge_o3_into_partition(
    base_dir: &Path,
    partition_dir_name: &str,
    o3_rows: &[O3Row],
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> Result<PartitionMeta, SegmentError> {
    if o3_rows.is_empty() {
        return Err(SegmentError::Io("no O3 rows to merge".into()));
    }

    let partition_dir = base_dir.join(partition_dir_name);
    if !partition_dir.exists() {
        return Err(SegmentError::Io(format!(
            "partition {} does not exist",
            partition_dir.display()
        )));
    }

    // Read existing partition data.
    let existing_meta = ColumnarSegmentReader::read_meta(&partition_dir, kek)?;
    let existing_schema = ColumnarSegmentReader::read_schema(&partition_dir, kek)?;
    let ts_type = existing_schema
        .columns
        .get(existing_schema.timestamp_idx)
        .map(|(_, ty)| *ty)
        .ok_or_else(|| {
            SegmentError::Corrupt(format!(
                "partition {partition_dir_name} schema has no designated time column"
            ))
        })?;

    let ts_col = ColumnarSegmentReader::read_column(&partition_dir, "timestamp", ts_type, kek)?;
    let val_col =
        ColumnarSegmentReader::read_column(&partition_dir, "value", ColumnType::Float64, kek)?;

    let existing_ts = ts_col.as_timestamps();
    let existing_val = val_col.as_f64();

    // Merge: sorted merge of existing rows + O3 rows.
    let total_rows = existing_ts.len() + o3_rows.len();
    let mut merged_ts = Vec::with_capacity(total_rows);
    let mut merged_val = Vec::with_capacity(total_rows);

    let mut ei = 0usize; // existing index
    let mut oi = 0usize; // o3 index

    // O3 rows are already sorted by drain_for_partition.
    while ei < existing_ts.len() && oi < o3_rows.len() {
        if existing_ts[ei] <= o3_rows[oi].timestamp_ms {
            merged_ts.push(existing_ts[ei]);
            merged_val.push(existing_val[ei]);
            ei += 1;
        } else {
            merged_ts.push(o3_rows[oi].timestamp_ms);
            merged_val.push(o3_rows[oi].value);
            oi += 1;
        }
    }
    while ei < existing_ts.len() {
        merged_ts.push(existing_ts[ei]);
        merged_val.push(existing_val[ei]);
        ei += 1;
    }
    while oi < o3_rows.len() {
        merged_ts.push(o3_rows[oi].timestamp_ms);
        merged_val.push(o3_rows[oi].value);
        oi += 1;
    }

    // Write to a temporary partition directory.
    let tmp_name = format!("{partition_dir_name}.o3merge");
    let drain = crate::engine::timeseries::columnar_memtable::ColumnarDrainResult {
        columns: vec![
            ColumnData::Timestamp(merged_ts),
            ColumnData::Float64(merged_val),
        ],
        schema: existing_schema,
        symbol_dicts: std::collections::HashMap::new(),
        row_count: total_rows as u64,
        min_ts: existing_meta.min_ts.min(
            o3_rows
                .iter()
                .map(|r| r.timestamp_ms)
                .min()
                .unwrap_or(i64::MAX),
        ),
        max_ts: existing_meta.max_ts.max(
            o3_rows
                .iter()
                .map(|r| r.timestamp_ms)
                .max()
                .unwrap_or(i64::MIN),
        ),
        // O3 merge preserves the existing partition's system-time max;
        // O3Row does not carry `_ts_system` (O3 is a value-only buffer).
        max_system_ts: existing_meta.max_system_ts,
        series_row_counts: std::collections::HashMap::new(),
    };

    let writer = ColumnarSegmentWriter::new(base_dir);
    let new_meta = writer.write_partition(
        &tmp_name,
        &drain.view(),
        existing_meta.interval_ms,
        existing_meta.last_flushed_wal_lsn,
        kek,
    )?;

    // Durable directory swap: rename original → backup, tmp → original, then
    // fsync the parent directory so both renames are metadata-durable before
    // the backup is removed. All three names sit directly under `base_dir`.
    let backup_name = format!("{partition_dir_name}.old");
    let backup_dir = base_dir.join(&backup_name);
    nodedb_wal::segment::atomic_swap_dirs_fsync(
        base_dir,
        partition_dir_name,
        &backup_name,
        &tmp_name,
    )
    .map_err(|e| SegmentError::Io(format!("atomic dir swap: {e}")))?;
    std::fs::remove_dir_all(&backup_dir).map_err(|e| {
        SegmentError::Io(format!(
            "remove replaced partition {}: {e}",
            backup_dir.display()
        ))
    })?;

    Ok(new_meta)
}
