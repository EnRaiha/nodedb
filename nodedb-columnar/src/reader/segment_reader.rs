// SPDX-License-Identifier: Apache-2.0

//! Segment reader: decode compressed columns from a segment into typed vectors,
//! with column projection and block predicate pushdown.
//!
//! # Block Wire Format
//!
//! Each block in a column is stored as `[compressed_len: u32 LE][compressed_data]`.
//!
//! The compressed_data structure depends on the column type:
//! - **Int64/Float64/Timestamp**: `[validity_bitmap][codec_compressed_values]`
//! - **Bool/Decimal/Uuid/Vector**: `[validity_bitmap][codec_compressed_bytes]`
//! - **String/Bytes/Geometry**: `[validity_bitmap][offset_len: u32][compressed_offsets][compressed_data]`

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::format::{HEADER_SIZE, SegmentFooter, SegmentHeader};
use crate::predicate::ScanPredicate;

use super::block_decode::{
    append_null_fill, decode_block, empty_decoded, infer_column_type, result_valid_len,
    result_valid_slice_mut,
};
use super::types::DecodedColumn;

fn checked_slice(data: &[u8], start: usize, len: usize) -> Result<&[u8], ColumnarError> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| ColumnarError::Corruption {
            segment_id: None,
            reason: "column block range overflow".into(),
            offset: None,
        })?;
    data.get(start..end).ok_or(ColumnarError::TruncatedSegment {
        expected: end,
        got: data.len(),
    })
}

/// Reads and decodes columns from a segment byte buffer.
///
/// Supports column projection (only decode requested columns) and block
/// predicate pushdown (skip blocks whose stats prove no match).
pub struct SegmentReader<'a> {
    pub(super) data: &'a [u8],
    pub(super) footer: SegmentFooter,
}

/// An owned segment reader that holds decrypted segment bytes.
///
/// Used when a segment was encrypted at rest: the decrypted plaintext is owned
/// by this struct, and `reader()` borrows from it for column decoding.
#[derive(Debug)]
pub struct OwnedSegmentReader {
    /// Decrypted plaintext segment bytes.
    plaintext: Vec<u8>,
    footer: SegmentFooter,
}

impl OwnedSegmentReader {
    fn from_plaintext(plaintext: Vec<u8>) -> Result<Self, ColumnarError> {
        SegmentHeader::from_bytes(&plaintext)?;
        let footer = SegmentFooter::from_segment_tail(&plaintext)?;
        Ok(Self { plaintext, footer })
    }

    /// Open a segment with optional at-rest decryption.
    ///
    /// - `kek = None` → requires a plaintext (`NDBS`) segment; returns
    ///   `Err(MissingKek)` if the blob starts with `SEGC`.
    /// - `kek = Some(key)` → requires an encrypted (`SEGC`) segment; decrypts
    ///   the blob, then parses the inner plaintext. Returns `Err(KekRequired)`
    ///   if the blob starts with `NDBS`.
    pub fn open_with_kek(
        blob: &[u8],
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    ) -> Result<Self, ColumnarError> {
        let is_encrypted = blob.len() >= 4 && blob[0..4] == crate::encrypt::SEGC_MAGIC;
        if is_encrypted {
            let key = kek.ok_or(ColumnarError::MissingKek)?;
            let plaintext = crate::encrypt::decrypt_segment(key, blob)?;
            Self::from_plaintext(plaintext)
        } else if kek.is_some() {
            Err(ColumnarError::KekRequired)
        } else {
            Self::from_plaintext(blob.to_vec())
        }
    }

    /// Borrow a `SegmentReader` over the owned plaintext bytes.
    pub fn reader(&self) -> SegmentReader<'_> {
        SegmentReader {
            data: &self.plaintext,
            footer: self.footer.clone(),
        }
    }

    /// Access the segment footer.
    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    /// Total row count in the segment.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }
}

impl<'a> SegmentReader<'a> {
    /// Open a plaintext segment from a byte buffer.
    ///
    /// Validates the `NDBS` header and footer CRC. If `data` starts with `SEGC`
    /// (an encrypted envelope) and `kek` is `None`, returns `Err(MissingKek)`.
    /// If `data` starts with `NDBS` (plaintext) and `kek` is `Some`, returns
    /// `Err(KekRequired)` — refusing to load unencrypted data when encryption
    /// is configured.
    ///
    /// To read an encrypted segment, use [`OwnedSegmentReader::open_with_kek`].
    pub fn open(data: &'a [u8]) -> Result<Self, ColumnarError> {
        if data.len() >= 4 && data[0..4] == crate::encrypt::SEGC_MAGIC {
            return Err(ColumnarError::MissingKek);
        }
        SegmentHeader::from_bytes(data)?;
        let footer = SegmentFooter::from_segment_tail(data)?;
        Ok(Self { data, footer })
    }

    /// Access the footer metadata.
    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    /// Total row count in the segment.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }

    /// Number of columns in the segment.
    pub fn column_count(&self) -> usize {
        self.footer.column_count as usize
    }

    /// Read a single column, decoding all blocks.
    ///
    /// `col_idx` is the column index in the footer's column metadata.
    pub fn read_column(&self, col_idx: usize) -> Result<DecodedColumn, ColumnarError> {
        self.read_column_filtered(col_idx, &[])
    }

    /// Read a single column with predicate pushdown.
    ///
    /// Blocks whose stats satisfy the predicates are skipped. For skipped
    /// blocks, null/zero-fill rows are emitted to preserve row alignment
    /// across projected columns.
    pub fn read_column_filtered(
        &self,
        col_idx: usize,
        predicates: &[ScanPredicate],
    ) -> Result<DecodedColumn, ColumnarError> {
        self.read_column_impl(col_idx, predicates, &DeleteBitmap::new())
    }

    /// Read multiple columns with shared predicate pushdown.
    ///
    /// All columns share the same block skip decisions so row alignment
    /// is maintained across the result set.
    pub fn read_columns(
        &self,
        col_indices: &[usize],
        predicates: &[ScanPredicate],
    ) -> Result<Vec<DecodedColumn>, ColumnarError> {
        col_indices
            .iter()
            .map(|&idx| self.read_column_filtered(idx, predicates))
            .collect()
    }

    /// Read a column with both predicate pushdown and delete bitmap masking.
    ///
    /// Deleted rows have their validity set to false in the output.
    /// Fully deleted blocks are skipped entirely (no decompression).
    pub fn read_column_with_deletes(
        &self,
        col_idx: usize,
        predicates: &[ScanPredicate],
        deletes: &DeleteBitmap,
    ) -> Result<DecodedColumn, ColumnarError> {
        self.read_column_impl(col_idx, predicates, deletes)
    }

    /// Read multiple columns with predicate pushdown and delete bitmap.
    pub fn read_columns_with_deletes(
        &self,
        col_indices: &[usize],
        predicates: &[ScanPredicate],
        deletes: &DeleteBitmap,
    ) -> Result<Vec<DecodedColumn>, ColumnarError> {
        col_indices
            .iter()
            .map(|&idx| self.read_column_with_deletes(idx, predicates, deletes))
            .collect()
    }

    /// Shared implementation for column reading with predicate pushdown and
    /// optional delete bitmap masking.
    fn read_column_impl(
        &self,
        col_idx: usize,
        predicates: &[ScanPredicate],
        deletes: &DeleteBitmap,
    ) -> Result<DecodedColumn, ColumnarError> {
        if col_idx >= self.footer.columns.len() {
            return Err(ColumnarError::ColumnOutOfRange {
                index: col_idx,
                count: self.footer.columns.len(),
            });
        }

        let col_meta = &self.footer.columns[col_idx];
        let my_preds: Vec<&ScanPredicate> =
            predicates.iter().filter(|p| p.col_idx == col_idx).collect();

        let col_offset =
            usize::try_from(col_meta.offset).map_err(|_| ColumnarError::Corruption {
                segment_id: None,
                reason: "column offset does not fit usize".into(),
                offset: None,
            })?;
        let col_start =
            HEADER_SIZE
                .checked_add(col_offset)
                .ok_or_else(|| ColumnarError::Corruption {
                    segment_id: None,
                    reason: "column offset range overflow".into(),
                    offset: None,
                })?;
        if col_start > self.data.len() {
            return Err(ColumnarError::TruncatedSegment {
                expected: col_start,
                got: self.data.len(),
            });
        }
        let mut cursor = col_start;
        let col_type = infer_column_type(col_meta);
        let mut result = empty_decoded(&col_type);
        let mut global_row: u32 = 0;

        for block_stat in &col_meta.block_stats {
            let block_row_count = block_stat.row_count;

            let header_end = cursor
                .checked_add(4)
                .ok_or_else(|| ColumnarError::Corruption {
                    segment_id: None,
                    reason: "column block header range overflow".into(),
                    offset: None,
                })?;
            let header =
                self.data
                    .get(cursor..header_end)
                    .ok_or(ColumnarError::TruncatedSegment {
                        expected: header_end,
                        got: self.data.len(),
                    })?;
            let block_len = usize::try_from(u32::from_le_bytes([
                header[0], header[1], header[2], header[3],
            ]))
            .map_err(|_| ColumnarError::Corruption {
                segment_id: None,
                reason: "column block length does not fit usize".into(),
                offset: None,
            })?;
            let block_data = checked_slice(self.data, header_end, block_len)?;
            cursor =
                header_end
                    .checked_add(block_len)
                    .ok_or_else(|| ColumnarError::Corruption {
                        segment_id: None,
                        reason: "column block range overflow".into(),
                        offset: None,
                    })?;

            // Skip via predicate pushdown.
            let pred_skip = my_preds.iter().any(|p| p.can_skip_block(block_stat));

            // Skip if entire block is deleted.
            let delete_skip =
                !deletes.is_empty() && deletes.is_block_fully_deleted(global_row, block_row_count);

            if pred_skip || delete_skip {
                append_null_fill(&mut result, block_row_count as usize);
                global_row += block_row_count;
                continue;
            }

            // Decode the block.
            let pre_len = result_valid_len(&result);
            decode_block(
                &mut result,
                block_data,
                &col_type,
                col_meta.codec,
                block_row_count as usize,
                col_meta.dictionary.as_deref(),
            )?;

            // Apply delete bitmap to the newly decoded rows.
            if !deletes.is_empty() {
                let valid_slice = result_valid_slice_mut(&mut result, pre_len);
                deletes.apply_to_validity(valid_slice, global_row);
            }

            global_row += block_row_count;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::checked_slice;
    use crate::error::ColumnarError;

    #[test]
    fn block_range_rejects_truncated_and_overflowed_lengths() {
        assert!(matches!(
            checked_slice(&[0; 4], 2, 4),
            Err(ColumnarError::TruncatedSegment {
                expected: 6,
                got: 4
            })
        ));
        assert!(matches!(
            checked_slice(&[], usize::MAX, 1),
            Err(ColumnarError::Corruption { .. })
        ));
    }
}
