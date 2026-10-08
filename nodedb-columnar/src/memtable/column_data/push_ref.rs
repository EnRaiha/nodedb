// SPDX-License-Identifier: Apache-2.0

//! Append borrowed values on `ColumnData` for zero-copy ingest.

use crate::error::ColumnarError;

use super::super::IngestValue;
use super::types::ColumnData;

impl ColumnData {
    /// Append a borrowed value (zero-copy for strings). Used by `ingest_row_refs`.
    pub(crate) fn push_ref(
        &mut self,
        value: &IngestValue<'_>,
        col_name: &str,
    ) -> Result<(), ColumnarError> {
        match (self, value) {
            (Self::Int64 { values, valid }, IngestValue::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Float64 { values, valid }, IngestValue::Null) => {
                values.push(0.0);
                Self::push_valid(valid, false);
            }
            (Self::Bool { values, valid }, IngestValue::Null) => {
                values.push(false);
                Self::push_valid(valid, false);
            }
            (Self::Timestamp { values, valid }, IngestValue::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::String { offsets, valid, .. }, IngestValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Bytes { offsets, valid, .. }, IngestValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Json { offsets, valid, .. }, IngestValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::DictEncoded { ids, valid, .. }, IngestValue::Null) => {
                ids.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Int64 { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, IngestValue::Float64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v as f64);
                Self::push_valid(valid, true);
            }
            (Self::Bool { values, valid }, IngestValue::Bool(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, IngestValue::Timestamp(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                IngestValue::Str(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                },
                IngestValue::Str(s),
            ) => {
                let id = if let Some(&existing) = reverse.get(*s) {
                    existing
                } else {
                    let new_id = dictionary.len() as u32;
                    dictionary.push((*s).to_string());
                    reverse.insert((*s).to_string(), new_id);
                    new_id
                };
                ids.push(id);
                Self::push_valid(valid, true);
            }
            (other, _) => {
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: other.type_name().to_string(),
                });
            }
        }
        Ok(())
    }
}
