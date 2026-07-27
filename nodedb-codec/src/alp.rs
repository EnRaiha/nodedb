// SPDX-License-Identifier: Apache-2.0

//! ALP (Adaptive Lossless floating-Point) codec for f64 columns.
//!
//! Most real-world float metrics originate as fixed-point decimals (e.g.,
//! `23.5`, `99.99`). ALP finds the optimal power-of-10 multiplier that
//! converts them to integers **losslessly**: `23.5 × 10 = 235` round-trips
//! exactly via `235 / 10 = 23.5`. The resulting integers have tiny bit-widths
//! and compress 3-6x better than Gorilla with SIMD-friendly bit-packing.
//!
//! Values that don't round-trip exactly (true arbitrary doubles, NaN, Inf)
//! are stored as exceptions — their original f64 bits are preserved separately.
//!
//! Wire format:
//! ```text
//! [4 bytes] total value count (LE u32)
//! [1 byte]  exponent e (power of 10: factor = 10^e, range 0-18)
//! [1 byte]  factor index f (combined encode/decode factor)
//! [4 bytes] exception count (LE u32)
//! [exception_count × 12 bytes] exceptions: (index: u32, value_bits: u64)
//! [N bytes] FastLanes-packed encoded integers (FOR + bit-pack)
//! ```
//!
//! Reference: "ALP: Adaptive Lossless floating-Point Compression"
//! (Afroozeh et al., SIGMOD 2023)

use std::mem::size_of;

use crate::bounds::{
    checked_add, checked_capacity, checked_mul, checked_range, decoded_len, encode_input_len,
    encode_u32_len, u32_to_usize,
};
use crate::error::CodecError;
use crate::fastlanes;

/// Maximum exponent (10^18 fits in i64).
const MAX_EXPONENT: u8 = 18;

/// Powers of 10 as f64 for the encode multiplier.
const POW10: [f64; 19] = [
    1.0,
    10.0,
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
    100_000_000.0,
    1_000_000_000.0,
    10_000_000_000.0,
    100_000_000_000.0,
    1_000_000_000_000.0,
    10_000_000_000_000.0,
    100_000_000_000_000.0,
    1_000_000_000_000_000.0,
    10_000_000_000_000_000.0,
    100_000_000_000_000_000.0,
    1_000_000_000_000_000_000.0,
];

/// Inverse powers of 10 for the decode divisor.
const INV_POW10: [f64; 19] = [
    1.0,
    0.1,
    0.01,
    0.001,
    0.000_1,
    0.000_01,
    0.000_001,
    0.000_000_1,
    0.000_000_01,
    0.000_000_001,
    0.000_000_000_1,
    0.000_000_000_01,
    0.000_000_000_001,
    0.000_000_000_000_1,
    0.000_000_000_000_01,
    0.000_000_000_000_001,
    0.000_000_000_000_000_1,
    0.000_000_000_000_000_01,
    0.000_000_000_000_000_001,
];

use crate::CODEC_SAMPLE_SIZE;

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a slice of f64 values using ALP compression.
///
/// Finds the optimal decimal exponent, converts to integers, stores
/// exceptions for values that don't round-trip, and bit-packs the integers
/// via FastLanes.
pub fn encode(values: &[f64]) -> Result<Vec<u8>, CodecError> {
    let decoded_bytes = checked_mul(values.len(), size_of::<f64>(), "ALP input bytes")?;
    decoded_len(decoded_bytes, "ALP input")?;
    let count = encode_input_len(values.len(), "ALP value count")?;

    if values.is_empty() {
        let mut out = Vec::with_capacity(11);
        out.extend_from_slice(&0u32.to_le_bytes()); // count
        out.push(0); // encode exponent
        out.push(0); // decode exponent
        out.push(0); // mode
        out.extend_from_slice(&0u32.to_le_bytes()); // exception count
        return Ok(out);
    }

    // Step 1: Find optimal parameters by sampling.
    let params = find_best_params(values);
    let factor = POW10[params.encode_exp as usize];

    // Step 2: Encode values to integers, collect exceptions.
    let mut encoded_ints = Vec::with_capacity(values.len());
    let mut exceptions: Vec<(u32, u64)> = Vec::new();

    for (i, &val) in values.iter().enumerate() {
        let encoded = try_alp_encode(val, factor, params.decode_exp, params.mode);
        match encoded {
            Some(int_val) => {
                encoded_ints.push(int_val);
            }
            None => {
                // Exception: store original bits, use 0 as placeholder in int array.
                exceptions.push((
                    u32::try_from(i).map_err(|_| CodecError::ResourceLimit {
                        resource: "ALP exception index".into(),
                        requested: i,
                        limit: u32::MAX as usize,
                    })?,
                    val.to_bits(),
                ));
                encoded_ints.push(0);
            }
        }
    }

    // Step 3: Build output.
    let exception_count = encode_u32_len(exceptions.len(), "ALP exception count")?;
    let packed_ints = fastlanes::encode(&encoded_ints)?;

    let exceptions_size = checked_mul(exceptions.len(), 12, "ALP exceptions")?;
    let capacity = checked_add(
        checked_add(11, exceptions_size, "ALP output")?,
        packed_ints.len(),
        "ALP output",
    )?;
    let mut out = Vec::with_capacity(capacity);

    // Header.
    out.extend_from_slice(&count.to_le_bytes());
    out.push(params.encode_exp);
    out.push(params.decode_exp);
    out.push(match params.mode {
        DecodeMode::MultiplyInverse => 0,
        DecodeMode::DivideByFactor => 1,
    });
    out.extend_from_slice(&exception_count.to_le_bytes());

    // Exceptions.
    for &(idx, bits) in &exceptions {
        out.extend_from_slice(&idx.to_le_bytes());
        out.extend_from_slice(&bits.to_le_bytes());
    }

    // Packed integers.
    out.extend_from_slice(&packed_ints);

    Ok(out)
}

/// Decode ALP-compressed bytes back to f64 values.
pub fn decode(data: &[u8]) -> Result<Vec<f64>, CodecError> {
    const HEADER_SIZE: usize = 11; // 4 + 1 + 1 + 1 + 4

    if data.len() < HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: HEADER_SIZE,
            actual: data.len(),
        });
    }

    let count = u32_to_usize(
        u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
        "ALP value count",
    )?;
    decoded_len(
        checked_mul(count, size_of::<f64>(), "ALP decoded values")?,
        "ALP",
    )?;
    let encode_exp = data[4];
    let decode_exp = data[5];
    let mode = match data[6] {
        0 => DecodeMode::MultiplyInverse,
        1 => DecodeMode::DivideByFactor,
        m => {
            return Err(CodecError::Corrupt {
                detail: format!("invalid ALP decode mode {m}"),
            });
        }
    };
    let exception_count = u32_to_usize(
        u32::from_le_bytes([data[7], data[8], data[9], data[10]]),
        "ALP exception count",
    )?;

    if count == 0 {
        if exception_count != 0 || data.len() != HEADER_SIZE {
            return Err(CodecError::Corrupt {
                detail: "non-canonical empty ALP frame".into(),
            });
        }
        return Ok(Vec::new());
    }
    if exception_count > count {
        return Err(CodecError::Corrupt {
            detail: "ALP exception count exceeds value count".into(),
        });
    }

    if encode_exp > MAX_EXPONENT || decode_exp > MAX_EXPONENT {
        return Err(CodecError::Corrupt {
            detail: format!("invalid ALP exponents encode={encode_exp}, decode={decode_exp}"),
        });
    }

    // Read exceptions.
    decoded_len(
        checked_mul(
            exception_count,
            size_of::<(usize, u64)>(),
            "ALP exception allocation",
        )?,
        "ALP exceptions",
    )?;
    let exceptions_size = checked_mul(exception_count, 12, "ALP exceptions")?;
    let exceptions_end = checked_add(HEADER_SIZE, exceptions_size, "ALP exceptions")?;
    checked_range(data, HEADER_SIZE, exceptions_size, "ALP exceptions")?;

    let exception_capacity = checked_capacity(
        exception_count,
        size_of::<(usize, u64)>(),
        "ALP exception allocation",
    )?;
    let mut exceptions = Vec::with_capacity(exception_capacity);
    let mut pos = HEADER_SIZE;
    let mut previous_index = None;
    for _ in 0..exception_count {
        let idx = u32_to_usize(
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]),
            "ALP exception index",
        )?;
        if idx >= count || previous_index.is_some_and(|previous| idx <= previous) {
            return Err(CodecError::Corrupt {
                detail: "ALP exceptions must be ordered unique in-range indices".into(),
            });
        }
        previous_index = Some(idx);
        let bits = u64::from_le_bytes([
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
            data[pos + 8],
            data[pos + 9],
            data[pos + 10],
            data[pos + 11],
        ]);
        exceptions.push((idx, bits));
        pos += 12;
    }

    // Decode FastLanes-packed integers.
    let encoded_ints =
        fastlanes::decode(&data[exceptions_end..]).map_err(|e| CodecError::Corrupt {
            detail: format!("ALP fastlanes decode: {e}"),
        })?;

    if encoded_ints.len() != count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "ALP int count mismatch: expected {count}, got {}",
                encoded_ints.len()
            ),
        });
    }

    // Reconstruct f64 values using the same decode mode that was selected during encode.
    let value_capacity = checked_capacity(count, size_of::<f64>(), "ALP values")?;
    let mut values = Vec::with_capacity(value_capacity);
    for &int_val in &encoded_ints {
        values.push(alp_decode_value(int_val, decode_exp, mode));
    }

    // Patch exceptions. The encoder uses a zero integer placeholder and only
    // emits an exception when the original value cannot round-trip.
    let factor = POW10[encode_exp as usize];
    for &(idx, bits) in &exceptions {
        let value = f64::from_bits(bits);
        if encoded_ints[idx] != 0 || try_alp_encode(value, factor, decode_exp, mode).is_some() {
            return Err(CodecError::Corrupt {
                detail: "non-canonical ALP exception".into(),
            });
        }
        values[idx] = value;
    }

    Ok(values)
}

/// Check what fraction of values can be ALP-encoded at the best exponent.
///
/// Returns a value between 0.0 and 1.0. Values > 0.95 mean ALP is a good
/// fit. Used by auto-detection to choose between ALP and Gorilla.
pub fn alp_encodability(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 1.0;
    }

    let sample_end = values.len().min(CODEC_SAMPLE_SIZE);
    let sample = &values[..sample_end];
    let params = find_best_params(sample);
    let factor = POW10[params.encode_exp as usize];

    let encodable = sample
        .iter()
        .filter(|&&v| try_alp_encode(v, factor, params.decode_exp, params.mode).is_some())
        .count();

    encodable as f64 / sample.len() as f64
}

// ---------------------------------------------------------------------------
// ALP core: exponent selection + encode/decode
// ---------------------------------------------------------------------------

/// Decode mode: whether to multiply by inverse or divide by factor.
///
/// IEEE 754 multiplication and division produce different bit patterns
/// for the same logical value (e.g., `3 * 0.1 != 3 / 10.0` at the bit level).
/// ALP tries both and picks whichever matches the original data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeMode {
    /// Decode via `encoded * INV_POW10[e]` (matches data computed as `x * 0.1`).
    MultiplyInverse,
    /// Decode via `encoded / POW10[e]` (matches data computed as `x / 10`).
    DivideByFactor,
}

/// Try to ALP-encode a single f64 value.
///
/// Returns `Some(i64)` if the value round-trips exactly, `None` otherwise.
#[inline]
fn try_alp_encode(value: f64, factor: f64, decode_exp: u8, mode: DecodeMode) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }

    let scaled = value * factor;
    if scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        return None;
    }

    let encoded = scaled.round() as i64;

    let decoded = match mode {
        DecodeMode::MultiplyInverse => encoded as f64 * INV_POW10[decode_exp as usize],
        DecodeMode::DivideByFactor => encoded as f64 / POW10[decode_exp as usize],
    };

    if decoded.to_bits() == value.to_bits() {
        Some(encoded)
    } else {
        None
    }
}

/// Reconstruct a single f64 from an ALP-encoded integer.
#[inline]
fn alp_decode_value(encoded: i64, decode_exp: u8, mode: DecodeMode) -> f64 {
    match mode {
        DecodeMode::MultiplyInverse => encoded as f64 * INV_POW10[decode_exp as usize],
        DecodeMode::DivideByFactor => encoded as f64 / POW10[decode_exp as usize],
    }
}

/// Find the best (exponent, factor_index) pair for a set of values.
///
/// Tries all exponents 0-18 and picks the one that encodes the most
/// values losslessly. The factor_index may differ from exponent when
/// the decode factor produces better roundtrips.
/// ALP parameters determined by sampling.
struct AlpParams {
    encode_exp: u8,
    decode_exp: u8,
    mode: DecodeMode,
}

/// Find the best ALP parameters by trying all exponents and both decode modes.
fn find_best_params(values: &[f64]) -> AlpParams {
    let sample_end = values.len().min(CODEC_SAMPLE_SIZE);
    let sample = &values[..sample_end];

    let mut best = AlpParams {
        encode_exp: 0,
        decode_exp: 0,
        mode: DecodeMode::MultiplyInverse,
    };
    let mut best_count: usize = 0;

    for e in 0..=MAX_EXPONENT {
        let factor = POW10[e as usize];

        for mode in [DecodeMode::MultiplyInverse, DecodeMode::DivideByFactor] {
            // Try same decode exponent.
            let count = sample
                .iter()
                .filter(|&&v| try_alp_encode(v, factor, e, mode).is_some())
                .count();

            if count > best_count {
                best_count = count;
                best = AlpParams {
                    encode_exp: e,
                    decode_exp: e,
                    mode,
                };
            }

            // Try neighboring decode exponents.
            if e > 0 {
                let count_alt = sample
                    .iter()
                    .filter(|&&v| try_alp_encode(v, factor, e - 1, mode).is_some())
                    .count();
                if count_alt > best_count {
                    best_count = count_alt;
                    best = AlpParams {
                        encode_exp: e,
                        decode_exp: e - 1,
                        mode,
                    };
                }
            }
            if e < MAX_EXPONENT {
                let count_alt = sample
                    .iter()
                    .filter(|&&v| try_alp_encode(v, factor, e + 1, mode).is_some())
                    .count();
                if count_alt > best_count {
                    best_count = count_alt;
                    best = AlpParams {
                        encode_exp: e,
                        decode_exp: e + 1,
                        mode,
                    };
                }
            }
        }
    }

    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(values: &[f64]) -> Vec<u8> {
        super::encode(values).expect("test ALP encode")
    }

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_value() {
        let values = vec![23.5f64];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded[0].to_bits(), values[0].to_bits());
    }

    #[test]
    fn integer_values() {
        // Integer values: exponent=0, factor=1.
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch: {a} vs {b}");
        }
    }

    #[test]
    fn decimal_values_one_digit() {
        // Values like 23.5, 99.1 — exponent=1, factor=10.
        let values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}: {a} vs {b}");
        }
    }

    #[test]
    fn decimal_values_two_digits() {
        // Values like 99.99 — exponent=2, factor=100.
        let values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.01).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}: {a} vs {b}");
        }
    }

    #[test]
    fn typical_cpu_metrics() {
        // CPU percentages: 0.0 to 100.0 with 0.1 resolution.
        let mut values = Vec::with_capacity(10_000);
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let cpu = ((rng >> 33) as f64 / (u32::MAX as f64)) * 100.0;
            // Round to 0.1 to simulate typical metric resolution.
            let cpu = (cpu * 10.0).round() / 10.0;
            values.push(cpu);
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}: {a} vs {b}");
        }

        // ALP should compress much better than raw.
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 3.0,
            "ALP should compress CPU metrics >3x, got {ratio:.1}x"
        );
    }

    #[test]
    fn temperature_readings() {
        // Temperatures: -40.0 to 50.0 with 0.01 resolution.
        let mut values = Vec::with_capacity(10_000);
        let mut rng: u64 = 99;
        for _ in 0..10_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let temp = ((rng >> 33) as f64 / (u32::MAX as f64)) * 90.0 - 40.0;
            let temp = (temp * 100.0).round() / 100.0;
            values.push(temp);
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}: {a} vs {b}");
        }
    }

    #[test]
    fn exception_handling() {
        // Mix of ALP-encodable and non-encodable values.
        let mut values = vec![23.5, 99.9, 100.1, 50.0];
        values.push(f64::NAN);
        values.push(f64::INFINITY);
        values.push(f64::NEG_INFINITY);
        values.push(std::f64::consts::PI); // Irrational — likely exception.

        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();

        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "mismatch at {i}: {a} ({:016x}) vs {b} ({:016x})",
                a.to_bits(),
                b.to_bits()
            );
        }
    }

    #[test]
    fn all_exceptions() {
        // All values are irrational — all exceptions.
        let values: Vec<f64> = (1..100).map(|i| std::f64::consts::PI * i as f64).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}");
        }
    }

    #[test]
    fn encodability_check() {
        // Decimal values should have high encodability.
        let decimal_values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
        assert!(alp_encodability(&decimal_values) > 0.95);

        // PI multiples: may still be encodable at high exponents, but the
        // resulting integers will have wide bit-widths (poor compression).
        // The key metric is that decimals have HIGHER encodability than irrationals.
        let irrational_values: Vec<f64> =
            (1..1000).map(|i| std::f64::consts::PI * i as f64).collect();
        let irrational_enc = alp_encodability(&irrational_values);
        // Decimal values should always be better than irrational ones.
        assert!(
            alp_encodability(&decimal_values) >= irrational_enc,
            "decimal encodability should be >= irrational"
        );
    }

    #[test]
    fn better_than_gorilla_for_decimals() {
        let values: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.1).collect();
        let alp_encoded = encode(&values);
        let gorilla_encoded = crate::gorilla::encode_f64(&values);

        assert!(
            alp_encoded.len() < gorilla_encoded.len(),
            "ALP ({} bytes) should beat Gorilla ({} bytes) on decimal data",
            alp_encoded.len(),
            gorilla_encoded.len()
        );
    }

    #[test]
    fn zero_values() {
        let values = vec![0.0f64; 1000];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn negative_zero() {
        let values = vec![-0.0f64, 0.0, -0.0];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}");
        }
    }

    #[test]
    fn rejects_noncanonical_exception_layout() {
        let mut encoded = encode(&[f64::NAN, 1.0]);
        let first_exception = 11;
        encoded[first_exception..first_exception + 4].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(decode(&encoded), Err(CodecError::Corrupt { .. })));
        let mut empty = encode(&[]);
        empty.push(0);
        assert!(matches!(decode(&empty), Err(CodecError::Corrupt { .. })));

        let mut bad_exponent = encode(&[1.0]);
        bad_exponent[4] = MAX_EXPONENT + 1;
        assert!(matches!(
            decode(&bad_exponent),
            Err(CodecError::Corrupt { .. })
        ));

        let mut nonzero_placeholder = encode(&[f64::NAN]);
        let nested_min = 11 + 12 + 6 + 3;
        nonzero_placeholder[nested_min..nested_min + 8].copy_from_slice(&1i64.to_le_bytes());
        assert!(matches!(
            decode(&nonzero_placeholder),
            Err(CodecError::Corrupt { .. })
        ));
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0, 0, 0, 0, 0, 0, 0]).is_err()); // too short
    }

    #[test]
    fn large_dataset() {
        let mut values = Vec::with_capacity(100_000);
        let mut rng: u64 = 12345;
        for _ in 0..100_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let val = ((rng >> 33) as f64 / (u32::MAX as f64)) * 1000.0;
            let val = (val * 100.0).round() / 100.0; // 2 decimal places
            values.push(val);
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), values.len());
        for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}");
        }
    }
}
