// SPDX-License-Identifier: Apache-2.0

//! Single-segment compaction: drop deleted rows from one segment, write a new one.

use nodedb_mem::ScopedMemory;
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
/// `memory` is optional: when `Some`, working-buffer allocations are
/// tracked against the bound database, tenant, and engine budget. Pass
/// `None` in embedded (Lite) deployments where no governor is configured.
pub fn compact_segment(
    segment_data: &[u8],
    deletes: &DeleteBitmap,
    schema: &ColumnarSchema,
    profile_tag: u8,
    memory: Option<&ScopedMemory>,
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
    let _cols_guard = memory
        .map(|m| m.reserve(col_count * std::mem::size_of::<usize>() * 3))
        .transpose()?;
    let mut decoded_cols = Vec::with_capacity(col_count);
    for i in 0..col_count {
        decoded_cols.push(reader.read_column(i)?);
    }

    // Build a new memtable with only live rows.
    let mut memtable = ColumnarMemtable::new(schema);
    let col_len = schema.columns.len();
    let _row_guard = memory
        .map(|m| m.reserve(col_len * std::mem::size_of::<usize>() * 3))
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
    let writer = match memory {
        Some(m) => SegmentWriter::with_memory(profile_tag, m.clone()),
        None => SegmentWriter::new(profile_tag),
    };
    let new_segment = writer.write_segment(&schema, &columns, row_count, kek)?;

    Ok(CompactionResult {
        segment: Some(new_segment),
        live_rows: row_count,
        removed_rows: deleted,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use crate::delete_bitmap::DeleteBitmap;
    use crate::memtable::ColumnarMemtable;
    use crate::reader::{DecodedColumn, SegmentReader};
    use crate::writer::SegmentWriter;

    use super::compact_segment;

    fn test_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid")
    }

    fn write_segment(rows: usize) -> Vec<u8> {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..rows {
            mt.append_row(&[
                Value::Integer(i as i64),
                Value::String(format!("user_{i}")),
                if i % 3 == 0 {
                    Value::Null
                } else {
                    Value::Float(i as f64 * 0.5)
                },
            ])
            .expect("append");
        }
        let (schema, columns, row_count) = mt.drain();
        SegmentWriter::plain()
            .write_segment(&schema, &columns, row_count, None)
            .expect("write")
    }

    #[test]
    fn compact_removes_deleted_rows() {
        let segment = write_segment(100);
        let mut deletes = DeleteBitmap::new();

        // Delete rows 0, 10, 20, ..., 90 (10 rows).
        for i in (0..100).step_by(10) {
            deletes.mark_deleted(i);
        }

        let result =
            compact_segment(&segment, &deletes, &test_schema(), 0, None, None).expect("compact");

        assert_eq!(result.live_rows, 90);
        assert_eq!(result.removed_rows, 10);
        assert!(result.segment.is_some());

        // Verify the compacted segment has correct row count.
        let new_seg = result.segment.as_ref().expect("segment");
        let reader = SegmentReader::open(new_seg).expect("open");
        assert_eq!(reader.row_count(), 90);

        // Verify that deleted rows are gone: row 0 (id=0) was deleted,
        // so the first row should be id=1.
        let col = reader.read_column(0).expect("read id");
        match col {
            DecodedColumn::Int64 { values, valid } => {
                assert_eq!(values[0], 1); // First live row.
                assert!(valid[0]);
                // Row at index 8 should be id=9 (rows 0,10 deleted, so 1..9 = 9 rows, idx 8 = id 9).
                assert_eq!(values[8], 9);
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn compact_all_deleted() {
        let segment = write_segment(10);
        let mut deletes = DeleteBitmap::new();
        for i in 0..10 {
            deletes.mark_deleted(i);
        }

        let result =
            compact_segment(&segment, &deletes, &test_schema(), 0, None, None).expect("compact");

        assert_eq!(result.live_rows, 0);
        assert_eq!(result.removed_rows, 10);
        assert!(result.segment.is_none());
    }

    #[test]
    fn compact_no_deletes() {
        let segment = write_segment(50);
        let deletes = DeleteBitmap::new();

        let result =
            compact_segment(&segment, &deletes, &test_schema(), 0, None, None).expect("compact");

        assert_eq!(result.live_rows, 50);
        assert_eq!(result.removed_rows, 0);
        assert!(result.segment.is_some());
    }

    #[test]
    fn compact_preserves_string_data() {
        let segment = write_segment(20);
        let mut deletes = DeleteBitmap::new();
        deletes.mark_deleted(0); // Delete first row.

        let result =
            compact_segment(&segment, &deletes, &test_schema(), 0, None, None).expect("compact");
        let new_seg = result.segment.as_ref().expect("segment");
        let reader = SegmentReader::open(new_seg).expect("open");

        // Read the name column (string).
        let col = reader.read_column(1).expect("read name");
        match col {
            DecodedColumn::Binary {
                data,
                offsets,
                valid,
            } => {
                // First row should be "user_1" (user_0 was deleted).
                let start = offsets[0] as usize;
                let end = offsets[1] as usize;
                let first_name = std::str::from_utf8(&data[start..end]).expect("utf8");
                assert_eq!(first_name, "user_1");
                assert!(valid[0]);
            }
            _ => panic!("expected Binary"),
        }
    }
}
