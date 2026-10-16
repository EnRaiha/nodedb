// SPDX-License-Identifier: Apache-2.0

//! Gorilla XOR encoding for `f32` value columns.
//!
//! The 32-bit form of the Gorilla value channel: each value XORs against
//! the previous one, and only the meaningful bits of a non-zero XOR are
//! written. No timestamp channel.
//!
//! Wire format:
//! ```text
//! [4 bytes] value count (LE u32)
//! [N bytes] bitstream: first value raw (32 bits), then per value:
//!           0                          XOR is zero
//!           1 0 <bits>                 XOR fits the previous window
//!           1 1 <5 lead> <5 len-1> <bits>   new window
//! ```

use crate::double_delta::{BitReader, BitWriter};
use crate::error::CodecError;

/// Encode `values` with 32-bit Gorilla XOR compression.
pub fn encode_f32(values: &[f32]) -> Vec<u8> {
    let mut buf = BitWriter::new();
    let mut prev = 0u32;
    let mut prev_leading = u8::MAX;
    let mut prev_trailing = 0u8;
    for (i, v) in values.iter().enumerate() {
        let bits = v.to_bits();
        if i == 0 {
            buf.write_bits(u64::from(bits), 32);
            prev = bits;
            continue;
        }
        let xor = prev ^ bits;
        prev = bits;
        if xor == 0 {
            buf.write_bit(false);
            continue;
        }
        buf.write_bit(true);
        let leading = xor.leading_zeros() as u8;
        let trailing = xor.trailing_zeros() as u8;
        if prev_leading != u8::MAX && leading >= prev_leading && trailing >= prev_trailing {
            buf.write_bit(false);
            let meaningful = 32 - prev_leading - prev_trailing;
            buf.write_bits(u64::from(xor >> prev_trailing), meaningful as usize);
        } else {
            buf.write_bit(true);
            let meaningful = 32 - leading - trailing;
            buf.write_bits(u64::from(leading), 5);
            buf.write_bits(u64::from(meaningful - 1), 5);
            buf.write_bits(u64::from(xor >> trailing), meaningful as usize);
            prev_leading = leading;
            prev_trailing = trailing;
        }
    }
    let bitstream = buf.as_bytes();
    let mut out = Vec::with_capacity(4 + bitstream.len());
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    out.extend_from_slice(bitstream);
    out
}

/// Decode values written by [`encode_f32`].
pub fn decode_f32(data: &[u8]) -> Result<Vec<f32>, CodecError> {
    let Some((header, body)) = data.split_first_chunk::<4>() else {
        return Err(CodecError::Truncated {
            expected: 4,
            actual: data.len(),
        });
    };
    let count = u32::from_le_bytes(*header) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    // The first value takes 32 bits and every later value at least one, so a
    // count past that bound is corrupt and is refused before allocation.
    let max_count = (body.len() * 8).saturating_sub(31);
    if count > max_count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "gorilla_f32 count {count} exceeds the {max_count} values a {}-byte stream holds",
                body.len()
            ),
        });
    }
    let mut reader = BitReader::new(body);
    let mut out = Vec::with_capacity(count);
    let mut prev = reader.read_bits(32)? as u32;
    out.push(f32::from_bits(prev));
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;
    for _ in 1..count {
        if !reader.read_bit()? {
            out.push(f32::from_bits(prev));
            continue;
        }
        let xor = if !reader.read_bit()? {
            let meaningful = 32 - prev_leading - prev_trailing;
            (reader.read_bits(meaningful as usize)? as u32) << prev_trailing
        } else {
            let leading = reader.read_bits(5)? as u8;
            let meaningful = reader.read_bits(5)? as u8 + 1;
            if leading + meaningful > 32 {
                return Err(CodecError::Corrupt {
                    detail: format!(
                        "invalid gorilla_f32 window: {leading} leading + {meaningful} meaningful bits"
                    ),
                });
            }
            let trailing = 32 - leading - meaningful;
            prev_leading = leading;
            prev_trailing = trailing;
            (reader.read_bits(meaningful as usize)? as u32) << trailing
        };
        prev ^= xor;
        out.push(f32::from_bits(prev));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(values: &[f32]) -> Vec<f32> {
        decode_f32(&encode_f32(values)).expect("decode")
    }

    #[test]
    fn empty_round_trips() {
        assert!(round_trip(&[]).is_empty());
    }

    #[test]
    fn values_round_trip_bit_exact() {
        let values = [
            0.1f32,
            0.1,
            -2.5,
            f32::MAX,
            f32::MIN_POSITIVE,
            0.0,
            -0.0,
            f32::INFINITY,
            1.0e-40,
        ];
        let out = round_trip(&values);
        let bits: Vec<u32> = out.iter().map(|v| v.to_bits()).collect();
        let want: Vec<u32> = values.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits, want);
    }

    #[test]
    fn nan_keeps_its_bits() {
        let out = round_trip(&[f32::NAN, 1.0, f32::NAN]);
        assert!(out[0].is_nan() && out[2].is_nan());
        assert_eq!(out[1], 1.0);
    }

    #[test]
    fn smooth_series_beats_raw_width() {
        let values: Vec<f32> = (0..1024).map(|i| 20.0 + (i % 16) as f32 * 0.25).collect();
        let encoded = encode_f32(&values);
        assert!(encoded.len() < values.len() * 4, "{} bytes", encoded.len());
        assert_eq!(round_trip(&values), values);
    }

    #[test]
    fn a_count_past_the_stream_is_refused() {
        let mut data = (1_000_000u32).to_le_bytes().to_vec();
        data.extend_from_slice(&[0u8; 8]);
        assert!(matches!(decode_f32(&data), Err(CodecError::Corrupt { .. })));
    }

    #[test]
    fn a_truncated_header_is_refused() {
        assert!(matches!(
            decode_f32(&[1, 0]),
            Err(CodecError::Truncated { .. })
        ));
    }
}
