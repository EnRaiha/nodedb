// SPDX-License-Identifier: Apache-2.0

// Float attr payloads.
//
//   FLOAT64: Gorilla XOR over the non-null values.
//   FLOAT32: [u8 mode][body]. Mode 0 holds raw f32 LE values, mode 1 holds
//            32-bit Gorilla XOR. The encoder writes the smaller one.

use crate::error::{ArrayError, ArrayResult};
use crate::types::cell_value::value::CellValue;

use super::int::codec_err;

const F32_RAW: u8 = 0;
const F32_GORILLA: u8 = 1;

fn not_float(v: &CellValue, width: &str) -> ArrayError {
    ArrayError::InvalidOp {
        detail: format!("{width} attr column holds {v:?}"),
    }
}

fn count_mismatch(width: &str, got: usize, expected: usize) -> ArrayError {
    ArrayError::SegmentCorruption {
        detail: format!("attr col {width}: {got} values decoded, validity holds {expected}"),
    }
}

pub(super) fn encode_f64(present: &[&CellValue]) -> ArrayResult<Vec<u8>> {
    let floats = present
        .iter()
        .map(|v| match v {
            CellValue::Float64(f) => Ok(*f),
            other => Err(not_float(other, "FLOAT64")),
        })
        .collect::<ArrayResult<Vec<f64>>>()?;
    Ok(nodedb_codec::gorilla::encode_f64(&floats))
}

pub(super) fn decode_f64(body: &[u8], expected: usize) -> ArrayResult<Vec<CellValue>> {
    let floats = nodedb_codec::gorilla::decode_f64(body).map_err(codec_err)?;
    if floats.len() != expected {
        return Err(count_mismatch("FLOAT64", floats.len(), expected));
    }
    Ok(floats.into_iter().map(CellValue::Float64).collect())
}

pub(super) fn encode_f32(present: &[&CellValue]) -> ArrayResult<Vec<u8>> {
    let floats = present
        .iter()
        .map(|v| match v {
            CellValue::Float32(f) => Ok(*f),
            other => Err(not_float(other, "FLOAT32")),
        })
        .collect::<ArrayResult<Vec<f32>>>()?;
    let gorilla = nodedb_codec::gorilla_f32::encode_f32(&floats);
    let raw_len = floats.len() * 4;
    let mut out = Vec::with_capacity(1 + gorilla.len().min(raw_len));
    if gorilla.len() < raw_len {
        out.push(F32_GORILLA);
        out.extend_from_slice(&gorilla);
    } else {
        out.push(F32_RAW);
        for f in &floats {
            out.extend_from_slice(&f.to_le_bytes());
        }
    }
    Ok(out)
}

pub(super) fn decode_f32(body: &[u8], expected: usize) -> ArrayResult<Vec<CellValue>> {
    let Some((&mode, rest)) = body.split_first() else {
        return Err(ArrayError::SegmentCorruption {
            detail: "attr col FLOAT32: missing mode byte".into(),
        });
    };
    let floats = match mode {
        F32_RAW => {
            if rest.len() != expected * 4 {
                return Err(count_mismatch("FLOAT32", rest.len() / 4, expected));
            }
            rest.as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect::<Vec<f32>>()
        }
        F32_GORILLA => nodedb_codec::gorilla_f32::decode_f32(rest).map_err(codec_err)?,
        other => {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("attr col FLOAT32: unknown mode {other}"),
            });
        }
    };
    if floats.len() != expected {
        return Err(count_mismatch("FLOAT32", floats.len(), expected));
    }
    Ok(floats.into_iter().map(CellValue::Float32).collect())
}
