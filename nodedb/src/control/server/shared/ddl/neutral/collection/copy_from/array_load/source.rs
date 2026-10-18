// SPDX-License-Identifier: BUSL-1.1

//! The rows of one slice of an array bulk load's input, read one at a time:
//! a run of whole COPY records, or one native request's columns.

use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::ddl_ast::statement::CopyStdinFormat;
use nodedb_sql::types_array::ArrayInsertRow;
use nodedb_types::Value;

use super::decode::{RowError, check_header, decode_text_row, decode_value_row};
use super::records::{Fields, RecordReader};

/// How a COPY stream encodes its records.
#[derive(Clone, Copy)]
pub(crate) struct TextFormat {
    pub format: CopyStdinFormat,
    /// `None` takes the format's default: `,` for CSV, tab for text.
    pub delimiter: Option<char>,
    /// Whether the first record names the columns.
    pub header: bool,
}

impl TextFormat {
    /// The delimiter the records split on.
    pub(super) fn delimiter(&self) -> char {
        self.delimiter.unwrap_or(match self.format {
            CopyStdinFormat::Csv => ',',
            CopyStdinFormat::Text => '\t',
        })
    }

    /// The word a row error names the input by.
    pub(super) fn label(&self) -> &'static str {
        match self.format {
            CopyStdinFormat::Csv => "CSV",
            CopyStdinFormat::Text => "text",
        }
    }
}

/// Where a run of text records sits in its stream.
pub(super) struct TextSlice<'a, 'h> {
    pub src: &'a str,
    pub format: TextFormat,
    /// The 1-based number of the run's first record in the stream.
    pub first_record_no: usize,
    /// Whether the stream's header record is still unread. Reading it sets
    /// this to false.
    pub header_pending: &'h mut bool,
}

/// Reads the typed rows of one input slice in input order.
pub(super) enum RowSource<'a, 'v> {
    Text {
        reader: RecordReader<'a>,
        fields: Fields<'a>,
        view: &'v ArrayCatalogView,
    },
    Cells {
        dims: &'a [Vec<Value>],
        attrs: &'a [Vec<Value>],
        view: &'v ArrayCatalogView,
        next: usize,
        len: usize,
    },
}

impl<'a, 'v> RowSource<'a, 'v> {
    /// Open a run of whole text records against `view`. Reads and checks
    /// the header record when the stream's header is still unread.
    pub(super) fn text(
        slice: TextSlice<'a, '_>,
        view: &'v ArrayCatalogView,
    ) -> Result<Self, RowError> {
        let TextSlice {
            src,
            format,
            first_record_no,
            header_pending,
        } = slice;
        let mut reader =
            RecordReader::new(src, format.format, format.delimiter(), first_record_no)?;
        let mut fields = Fields::new();
        if *header_pending && reader.next_into(&mut fields)? {
            *header_pending = false;
            check_header(&fields, view, reader.record_no())?;
        }
        Ok(RowSource::Text {
            reader,
            fields,
            view,
        })
    }

    /// Open native request columns against `view`: one per dim, then one
    /// per attr, in schema order, all of one length.
    pub(super) fn cells(
        dims: &'a [Vec<Value>],
        attrs: &'a [Vec<Value>],
        view: &'v ArrayCatalogView,
    ) -> Result<Self, RowError> {
        if dims.len() != view.dims.len() || attrs.len() != view.attrs.len() {
            return Err(RowError::new(
                0,
                "22P02",
                format!(
                    "batch has {} dim and {} attr columns, array '{}' has {} and {}",
                    dims.len(),
                    attrs.len(),
                    view.name,
                    view.dims.len(),
                    view.attrs.len()
                ),
            ));
        }
        let len = dims.first().or(attrs.first()).map_or(0, Vec::len);
        if dims.iter().chain(attrs).any(|column| column.len() != len) {
            return Err(RowError::new(
                0,
                "22P02",
                "batch columns differ in length".to_string(),
            ));
        }
        Ok(RowSource::Cells {
            dims,
            attrs,
            view,
            next: 0,
            len,
        })
    }

    /// The 1-based number of the record after the last one read. Text only.
    /// A cells source answers 1.
    pub(super) fn next_record_no(&self) -> usize {
        match self {
            RowSource::Text { reader, .. } => reader.record_no() + 1,
            RowSource::Cells { .. } => 1,
        }
    }

    /// Whether a text source met the `\.` end-of-data line.
    pub(super) fn at_end_marker(&self) -> bool {
        match self {
            RowSource::Text { reader, .. } => reader.at_end_marker(),
            RowSource::Cells { .. } => false,
        }
    }

    /// The next row and its 1-based number. `None` at the end of the slice.
    pub(super) fn next_row(&mut self) -> Option<Result<(usize, ArrayInsertRow), RowError>> {
        match self {
            RowSource::Text {
                reader,
                fields,
                view,
            } => match reader.next_into(fields) {
                Ok(true) => {
                    let row = reader.record_no();
                    Some(decode_text_row(fields, view, row).map(|decoded| (row, decoded)))
                }
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            },
            RowSource::Cells {
                dims,
                attrs,
                view,
                next,
                len,
            } => {
                if *next >= *len {
                    return None;
                }
                let index = *next;
                *next += 1;
                Some(decode_value_row(dims, attrs, index, view).map(|decoded| (index + 1, decoded)))
            }
        }
    }
}
