// SPDX-License-Identifier: Apache-2.0

// Integer attr payload: the non-null values widened to `i64`, FastLanes
// encoded. FOR + bit-packing packs a narrow column by its value range, so a
// `UINT16` column costs the same bytes as an `INT64` column of equal values.

use nodedb_codec::error::CodecError;

use crate::error::{ArrayError, ArrayResult};
use crate::schema::attr_spec::AttrType;
use crate::types::cell_value::value::CellValue;

pub(super) fn codec_err(e: CodecError) -> ArrayError {
    ArrayError::SegmentCorruption {
        detail: format!("codec error: {e}"),
    }
}

/// Encode the integer values of `present`. Every value is an integer cell.
pub(super) fn encode_ints(present: &[&CellValue]) -> ArrayResult<Vec<u8>> {
    let ints = present
        .iter()
        .map(|v| {
            v.as_i64().ok_or_else(|| ArrayError::InvalidOp {
                detail: format!("integer attr column holds {v:?}"),
            })
        })
        .collect::<ArrayResult<Vec<i64>>>()?;
    nodedb_codec::fastlanes::encode(&ints).map_err(codec_err)
}

/// Decode `expected` integer cells of type `dtype`.
pub(super) fn decode_ints(
    body: &[u8],
    dtype: AttrType,
    expected: usize,
) -> ArrayResult<Vec<CellValue>> {
    let ints = nodedb_codec::fastlanes::decode(body).map_err(codec_err)?;
    if ints.len() != expected {
        return Err(ArrayError::SegmentCorruption {
            detail: format!(
                "attr col {}: {} values decoded, validity holds {expected}",
                dtype.sql_name(),
                ints.len()
            ),
        });
    }
    ints.into_iter()
        .map(|v| {
            CellValue::from_int_literal(v, dtype).map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("attr col: {e}"),
            })
        })
        .collect()
}
