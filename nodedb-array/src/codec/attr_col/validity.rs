// SPDX-License-Identifier: Apache-2.0

// Validity section of an attr column.
//
//   [u8 has_nulls]
//   [ceil(count / 8) bytes bitmap]   only when has_nulls == 1
//
// Bit `i` (LSB first within each byte) is set when row `i` holds a value.
// A column with no NULL writes no bitmap.

use crate::error::{ArrayError, ArrayResult};
use crate::types::cell_value::value::CellValue;

/// Write the validity section of `values` into `out`.
pub(super) fn encode_validity(values: &[CellValue], out: &mut Vec<u8>) {
    if !values.iter().any(CellValue::is_null) {
        out.push(0);
        return;
    }
    out.push(1);
    let mut bitmap = vec![0u8; values.len().div_ceil(8)];
    for (i, v) in values.iter().enumerate() {
        if !v.is_null() {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    out.extend_from_slice(&bitmap);
}

/// Read the validity section of a `count`-row column at `*pos`. Returns one
/// flag per row, `true` where the row holds a value.
pub(super) fn decode_validity(
    data: &[u8],
    pos: &mut usize,
    count: usize,
) -> ArrayResult<Vec<bool>> {
    let has_nulls = *data
        .get(*pos)
        .ok_or_else(|| ArrayError::SegmentCorruption {
            detail: "attr col: truncated validity flag".into(),
        })?;
    *pos += 1;
    match has_nulls {
        0 => Ok(vec![true; count]),
        1 => {
            let len = count.div_ceil(8);
            let bitmap =
                data.get(*pos..*pos + len)
                    .ok_or_else(|| ArrayError::SegmentCorruption {
                        detail: format!("attr col: validity bitmap needs {len} bytes"),
                    })?;
            *pos += len;
            Ok((0..count)
                .map(|i| bitmap[i / 8] & (1 << (i % 8)) != 0)
                .collect())
        }
        other => Err(ArrayError::SegmentCorruption {
            detail: format!("attr col: validity flag {other} is neither 0 nor 1"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_null_writes_no_bitmap() {
        let mut out = Vec::new();
        encode_validity(&[CellValue::Int64(1), CellValue::Int64(2)], &mut out);
        assert_eq!(out, vec![0]);
        let mut pos = 0;
        assert_eq!(
            decode_validity(&out, &mut pos, 2).unwrap(),
            vec![true, true]
        );
        assert_eq!(pos, 1);
    }

    #[test]
    fn nulls_round_trip_through_the_bitmap() {
        let values: Vec<CellValue> = (0..10)
            .map(|i| {
                if i % 3 == 0 {
                    CellValue::Null
                } else {
                    CellValue::Int64(i)
                }
            })
            .collect();
        let mut out = Vec::new();
        encode_validity(&values, &mut out);
        assert_eq!(out.len(), 1 + 2);
        let mut pos = 0;
        let flags = decode_validity(&out, &mut pos, values.len()).unwrap();
        let want: Vec<bool> = values.iter().map(|v| !v.is_null()).collect();
        assert_eq!(flags, want);
    }

    #[test]
    fn a_truncated_bitmap_is_corruption() {
        let mut pos = 0;
        assert!(decode_validity(&[1, 0], &mut pos, 20).is_err());
        let mut pos = 0;
        assert!(decode_validity(&[7], &mut pos, 1).is_err());
    }
}
