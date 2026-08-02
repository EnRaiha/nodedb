// SPDX-License-Identifier: Apache-2.0

//! Materialize live (non-deleted) rows from a flushed segment blob.
//!
//! Used by the durable RESTORE re-issue path: a backed-up flushed segment is
//! decoded back into per-row `Value::Object`s so the rows can be replayed as a
//! normal columnar `Insert` (WAL- or Raft-durable) rather than installed into
//! in-memory-only state. Deleted rows are excluded — resurrecting them would be
//! a correctness regression.

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::reader::OwnedSegmentReader;

use super::extract::extract_row_value;

/// Decode the live rows of one flushed segment blob into `Value::Object`s,
/// paired with their per-row cross-engine surrogate.
///
/// - `blob` is a raw `NDBS` (plaintext) or `SEGC` (encrypted) segment.
///   Encryption is detected from the blob magic: a `SEGC` blob is decrypted
///   with `kek` (an error if `kek` is `None`); an `NDBS` blob is read directly.
/// - `deletes` is the segment's delete bitmap (empty when no rows were
///   deleted). Rows marked deleted are skipped.
/// - `row_surrogates` is the per-row surrogate sidecar for this segment
///   (parallel to segment row order). It may be empty (pre-surrogate snapshot)
///   or shorter than the row count, in which case the missing entries read as
///   `None`.
///
/// Returns `(Value::Object, Option<Surrogate>)` for every live row, in segment
/// row order. The object keys are the schema column names.
pub fn materialize_segment_live_rows(
    blob: &[u8],
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    schema: &ColumnarSchema,
    deletes: &DeleteBitmap,
    row_surrogates: &[Option<Surrogate>],
) -> Result<Vec<(Value, Option<Surrogate>)>, ColumnarError> {
    // `open_with_kek` is strict: it refuses a plaintext blob when a kek is
    // supplied and refuses an encrypted blob when no kek is supplied. Pick the
    // kek by the blob's own magic so both encrypted and plaintext segments in a
    // single restore decode correctly.
    let is_encrypted = blob.len() >= 4 && blob[0..4] == crate::encrypt::SEGC_MAGIC;
    let effective_kek = if is_encrypted { kek } else { None };
    let owned = OwnedSegmentReader::open_with_kek(blob, effective_kek)?;
    let reader = owned.reader();

    let total_rows = reader.row_count() as usize;
    let col_count = schema.columns.len();
    let col_indices: Vec<usize> = (0..col_count).collect();
    let decoded_cols = reader.read_columns(&col_indices, &[])?;

    let mut out = Vec::with_capacity(total_rows.saturating_sub(deletes.deleted_count() as usize));
    for row_idx in 0..total_rows {
        if !deletes.is_empty() && deletes.is_deleted(row_idx as u32) {
            continue;
        }
        let mut map = std::collections::HashMap::with_capacity(col_count);
        for (col_idx, decoded) in decoded_cols.iter().enumerate() {
            let col = &schema.columns[col_idx];
            let value = extract_row_value(decoded, row_idx, &col.column_type, &col.name)?;
            map.insert(col.name.clone(), value);
        }
        let surrogate = row_surrogates.get(row_idx).copied().flatten();
        out.push((Value::Object(map), surrogate));
    }
    Ok(out)
}
