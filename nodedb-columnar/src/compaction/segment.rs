// SPDX-License-Identifier: Apache-2.0

//! Single-segment compaction: drop deleted rows from one segment, write a new one.

use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor};
use nodedb_types::columnar::ColumnarSchema;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::materialize_rows::extract::extract_row_value;
use crate::memtable::ColumnarMemtable;
use crate::reader::SegmentReader;
use crate::writer::SegmentWriter;

/// Default compaction threshold: compact when >20% of rows are deleted.
pub const DEFAULT_DELETE_RATIO_THRESHOLD: f64 = 0.2;

/// Result of a compaction operation.
pub struct CompactionResult {
    /// The new compacted segment bytes. `None` if all rows were deleted.
    pub segment: Option<Vec<u8>>,
    /// Number of live rows in the new segment.
    pub live_rows: usize,
    /// Number of rows removed (deleted).
    pub removed_rows: usize,
}

/// Compact a single segment by removing deleted rows.
///
/// Reads the segment, skips rows marked in the delete bitmap, and writes
/// a new segment with only live rows. Returns `None` segment if all rows
/// were deleted.
///
/// When `kek` is `Some`, the output segment is wrapped in an AES-256-GCM
/// SEGC envelope. The input segment must be plaintext (the caller is
/// responsible for decrypting before passing to this function).
///
/// `governor` is optional: when `Some`, working-buffer allocations are
/// tracked against the `Columnar` engine budget. Pass `None` in embedded
/// (Lite) deployments where no governor is configured.
pub fn compact_segment(
    segment_data: &[u8],
    deletes: &DeleteBitmap,
    schema: &ColumnarSchema,
    profile_tag: u8,
    governor: Option<&Arc<MemoryGovernor>>,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> Result<CompactionResult, ColumnarError> {
    let reader = SegmentReader::open(segment_data)?;
    let total_rows = reader.row_count() as usize;
    let deleted = deletes.deleted_count() as usize;
    let live = total_rows.saturating_sub(deleted);

    if live == 0 {
        return Ok(CompactionResult {
            segment: None,
            live_rows: 0,
            removed_rows: total_rows,
        });
    }

    // Read all columns without delete masking — we'll filter manually.
    let col_count = reader.column_count();
    // Reserve budget for the decoded-column pointer vec (each entry is a fat pointer).
    let _cols_guard = governor
        .map(|g| {
            g.reserve(
                EngineId::Columnar,
                col_count * std::mem::size_of::<usize>() * 3,
            )
        })
        .transpose()?;
    let mut decoded_cols = Vec::with_capacity(col_count);
    for i in 0..col_count {
        decoded_cols.push(reader.read_column(i)?);
    }

    // Build a new memtable with only live rows.
    let mut memtable = ColumnarMemtable::new(schema);
    let col_len = schema.columns.len();
    let _row_guard = governor
        .map(|g| {
            g.reserve(
                EngineId::Columnar,
                col_len * std::mem::size_of::<usize>() * 3,
            )
        })
        .transpose()?;
    let mut row_values = Vec::with_capacity(col_len);

    for row_idx in 0..total_rows {
        if deletes.is_deleted(row_idx as u32) {
            continue;
        }

        row_values.clear();
        for (col_idx, decoded) in decoded_cols.iter().enumerate() {
            let col = &schema.columns[col_idx];
            let value = extract_row_value(decoded, row_idx, &col.column_type, &col.name)?;
            row_values.push(value);
        }

        memtable.append_row(&row_values)?;
    }

    let (schema, columns, row_count) = memtable.drain();
    let writer = match governor {
        Some(g) => SegmentWriter::with_governor(profile_tag, Arc::clone(g)),
        None => SegmentWriter::new(profile_tag),
    };
    let new_segment = writer.write_segment(&schema, &columns, row_count, kek)?;

    Ok(CompactionResult {
        segment: Some(new_segment),
        live_rows: row_count,
        removed_rows: deleted,
    })
}
