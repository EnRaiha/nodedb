// SPDX-License-Identifier: BUSL-1.1

//! The one reader every flushed columnar segment read goes through.
//!
//! A segment that does not open, a column that does not decode, and a cell
//! that does not hold its declared type are all corruption. Each one returns
//! a typed error and files one corruption report here, where it is detected.
//! No caller skips a segment or a row it cannot read: a skipped row reads as
//! an absent row, which is a wrong answer, not a degraded one.

use nodedb_columnar::reader::DecodedColumn;
use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::decoded_col_to_value;

/// One flushed segment with every schema column decoded.
pub(in crate::data::executor) struct DecodedFlushedSegment<'a> {
    collection: &'a str,
    segment_id: u64,
    site: &'static str,
    row_count: usize,
    columns: Vec<DecodedColumn>,
}

impl CoreLoop {
    /// Open flushed segment `segment_id` of `collection` and decode its first
    /// `column_count` columns.
    ///
    /// The open goes through the quarantine registry when one is wired, so a
    /// repeated CRC failure quarantines the segment. `site` names the read
    /// path in the corruption report.
    pub(in crate::data::executor) fn decode_flushed_segment<'a>(
        &self,
        collection: &'a str,
        segment_id: u64,
        seg_bytes: &[u8],
        column_count: usize,
        site: &'static str,
    ) -> crate::Result<DecodedFlushedSegment<'a>> {
        let report = |err: crate::Error, stage: &'static str| {
            crate::diag::columnar_segment_corrupt(&err, collection, segment_id, stage, site);
            err
        };
        let opened = match &self.quarantine_registry {
            Some(reg) => crate::storage::quarantine::engines::open_segment_with_quarantine(
                reg,
                seg_bytes,
                collection,
                &segment_id.to_string(),
            )
            .map_err(crate::Error::from),
            None => nodedb_columnar::SegmentReader::open(seg_bytes).map_err(crate::Error::from),
        };
        let reader =
            opened.map_err(|e| report(segment_error(collection, segment_id, e), "open"))?;
        let columns = (0..column_count)
            .map(|col_idx| reader.read_column(col_idx))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                report(
                    segment_error(collection, segment_id, crate::Error::from(e)),
                    "column",
                )
            })?;
        let row_count = usize::try_from(reader.row_count()).map_err(|_| {
            report(
                crate::Error::SegmentCorrupted {
                    detail: format!(
                        "columnar segment {segment_id} of '{collection}' reports {} rows, \
                         more than this machine can address",
                        reader.row_count()
                    ),
                },
                "open",
            )
        })?;
        Ok(DecodedFlushedSegment {
            collection,
            segment_id,
            site,
            row_count,
            columns,
        })
    }
}

impl DecodedFlushedSegment<'_> {
    /// Rows the segment holds, tombstoned ones included.
    pub(in crate::data::executor) fn row_count(&self) -> usize {
        self.row_count
    }

    /// Row `row_idx`, each cell typed by its declared column.
    pub(in crate::data::executor) fn row(
        &self,
        schema: &ColumnarSchema,
        row_idx: usize,
    ) -> crate::Result<Vec<Value>> {
        self.columns
            .iter()
            .zip(&schema.columns)
            .map(|(col, def)| self.decode_cell(col, row_idx, &def.column_type))
            .collect()
    }

    /// Cell `row_idx` of column `col_idx`, typed by `declared`.
    pub(in crate::data::executor) fn cell(
        &self,
        col_idx: usize,
        row_idx: usize,
        declared: &ColumnType,
    ) -> crate::Result<Value> {
        let Some(col) = self.columns.get(col_idx) else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "columnar segment {} of '{}': column {col_idx} was not decoded",
                    self.segment_id, self.collection
                ),
            });
        };
        self.decode_cell(col, row_idx, declared)
    }

    fn decode_cell(
        &self,
        col: &DecodedColumn,
        row_idx: usize,
        declared: &ColumnType,
    ) -> crate::Result<Value> {
        decoded_col_to_value(col, row_idx, declared).map_err(|e| {
            let err = segment_error(self.collection, self.segment_id, e);
            crate::diag::columnar_segment_corrupt(
                &err,
                self.collection,
                self.segment_id,
                "cell",
                self.site,
            );
            err
        })
    }
}

/// `err` with the segment and collection named in its detail when it is a
/// corruption error, so the refusal says which segment is damaged.
fn segment_error(collection: &str, segment_id: u64, err: crate::Error) -> crate::Error {
    match err {
        crate::Error::SegmentCorrupted { detail } => crate::Error::SegmentCorrupted {
            detail: format!("columnar segment {segment_id} of '{collection}': {detail}"),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("doc", ColumnType::Json),
        ])
        .expect("valid")
    }

    #[test]
    fn unopenable_segment_bytes_are_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = make_core_with_dir(dir.path());
        let result = core.decode_flushed_segment("m", 1, &[], schema().columns.len(), "test");
        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("segment 1 of 'm'"))
        );
    }

    #[test]
    fn a_cell_that_does_not_fit_its_type_is_refused_as_corruption() {
        let segment = DecodedFlushedSegment {
            collection: "m",
            segment_id: 2,
            site: "test",
            row_count: 1,
            columns: vec![
                DecodedColumn::Int64 {
                    values: vec![1],
                    valid: vec![true],
                },
                DecodedColumn::Binary {
                    data: vec![0xC1],
                    offsets: vec![0, 1],
                    valid: vec![true],
                },
            ],
        };
        let result = segment.row(&schema(), 0);
        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("segment 2 of 'm'"))
        );
    }
}
