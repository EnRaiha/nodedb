// SPDX-License-Identifier: Apache-2.0

//! Byte-level dtype conversion helpers for vector buffers.
//!
//! These scalar primitives cast raw byte slices between F32, F16, and BF16
//! representations. SIMD acceleration (D5-D8) will dispatch through these
//! validated paths; this layer handles correctness only.
//!
//! All multi-byte values are little-endian, matching NodeDB's on-wire and WAL
//! conventions.

use half::{bf16, f16};
use nodedb_types::vector_dtype::VectorStorageDtype;

/// Error returned when byte buffer dimensions do not match the expected dtype
/// layout, or when input is otherwise malformed for the requested dtype cast.
#[derive(thiserror::Error, Debug)]
pub enum DtypeError {
    /// Byte buffer length does not match `dtype.bytes_for_dim(dim)`.
    #[error(
        "dtype byte-length mismatch for {dtype}: expected {expected} bytes for dim {dim}, got {actual}"
    )]
    BadByteLen {
        dtype: VectorStorageDtype,
        dim: usize,
        expected: usize,
        actual: usize,
    },
}

/// Verify that `bytes.len() == dtype.bytes_for_dim(dim)`.
///
/// Returns `Err(DtypeError::BadByteLen)` on mismatch. Public so distance and
/// index code can validate inputs before delegating to the cast functions.
pub fn validate_byte_len(
    bytes: &[u8],
    dtype: VectorStorageDtype,
    dim: usize,
) -> Result<(), DtypeError> {
    let expected = dtype.bytes_for_dim(dim);
    if bytes.len() != expected {
        return Err(DtypeError::BadByteLen {
            dtype,
            dim,
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

/// Cast a typed byte buffer to a freshly-allocated `Vec<f32>` of length `dim`.
///
/// Up-converts F16 / BF16 to F32 element-wise. F32 input is a memcopy (still
/// allocates a new `Vec` to keep the return type uniform; zero-copy paths live
/// in the distance kernels themselves, not here).
///
/// All multi-byte reads are little-endian.
///
/// # Errors
///
/// Returns `DtypeError::BadByteLen` if `src.len() != dtype.bytes_for_dim(dim)`.
pub fn cast_to_f32(
    src: &[u8],
    dtype: VectorStorageDtype,
    dim: usize,
) -> Result<Vec<f32>, DtypeError> {
    validate_byte_len(src, dtype, dim)?;

    match dtype {
        VectorStorageDtype::F32 => {
            // bytemuck::cast_slice requires the source to be aligned to f32's
            // alignment. A raw &[u8] slice has alignment 1, which satisfies
            // bytemuck's contract only when the destination type has alignment 1
            // too. Use explicit chunked reads instead to avoid alignment issues
            // on arbitrary byte slices passed in from WAL / mmap regions.
            let out = src
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect();
            Ok(out)
        }
        VectorStorageDtype::F16 => {
            let out = src
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| f16::from_le_bytes(*c).to_f32())
                .collect();
            Ok(out)
        }
        VectorStorageDtype::BF16 => {
            let out = src
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| bf16::from_le_bytes(*c).to_f32())
                .collect();
            Ok(out)
        }
        // `VectorStorageDtype` is #[non_exhaustive]; this arm is required by
        // the compiler but unreachable with any currently-defined variant.
        _ => unreachable!("unrecognised VectorStorageDtype variant in cast_to_f32"),
    }
}

/// Cast a `&[f32]` slice into a freshly-allocated byte buffer in the target
/// dtype, suitable for storage.
///
/// - `F32` → 4 bytes per element (little-endian IEEE 754 single).
/// - `F16` / `BF16` → 2 bytes per element. Rounding follows
///   `half::f16::from_f32` / `half::bf16::from_f32` (round-to-nearest-even,
///   per IEEE 754 / Brain Float spec).
///
/// Returns an empty `Vec<u8>` for an empty `src` slice.
pub fn cast_from_f32(src: &[f32], dtype: VectorStorageDtype) -> Vec<u8> {
    match dtype {
        VectorStorageDtype::F32 => {
            let mut out = Vec::with_capacity(src.len() * 4);
            for &x in src {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        VectorStorageDtype::F16 => {
            let mut out = Vec::with_capacity(src.len() * 2);
            for &x in src {
                out.extend_from_slice(&f16::from_f32(x).to_le_bytes());
            }
            out
        }
        VectorStorageDtype::BF16 => {
            let mut out = Vec::with_capacity(src.len() * 2);
            for &x in src {
                out.extend_from_slice(&bf16::from_f32(x).to_le_bytes());
            }
            out
        }
        // `VectorStorageDtype` is #[non_exhaustive]; this arm is required by
        // the compiler but unreachable with any currently-defined variant.
        _ => unreachable!("unrecognised VectorStorageDtype variant in cast_from_f32"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── F32 ──────────────────────────────────────────────────────────────────

    #[test]
    fn f32_round_trip_identity() {
        let src = [1.0_f32, 2.0, 3.0];
        let bytes = cast_from_f32(&src, VectorStorageDtype::F32);
        let got = cast_to_f32(&bytes, VectorStorageDtype::F32, 3).unwrap();
        assert_eq!(got, vec![1.0_f32, 2.0, 3.0]);
    }

    #[test]
    fn f32_empty_round_trip() {
        let bytes = cast_from_f32(&[], VectorStorageDtype::F32);
        assert!(bytes.is_empty());
        let got = cast_to_f32(&[], VectorStorageDtype::F32, 0).unwrap();
        assert!(got.is_empty());
    }

    // ── F16 ──────────────────────────────────────────────────────────────────

    #[test]
    fn f16_round_trip_within_tolerance() {
        // Values chosen to round cleanly in F16 (exact or near-exact repr).
        let src = [0.5_f32, 1.0, 2.5, 100.0];
        let bytes = cast_from_f32(&src, VectorStorageDtype::F16);
        let got = cast_to_f32(&bytes, VectorStorageDtype::F16, 4).unwrap();
        for (orig, recovered) in src.iter().zip(got.iter()) {
            assert!(
                (orig - recovered).abs() < 1e-3,
                "F16 round-trip: {orig} → {recovered}, diff too large"
            );
        }
    }

    #[test]
    fn f16_empty_round_trip() {
        let bytes = cast_from_f32(&[], VectorStorageDtype::F16);
        assert!(bytes.is_empty());
        let got = cast_to_f32(&[], VectorStorageDtype::F16, 0).unwrap();
        assert!(got.is_empty());
    }

    // ── BF16 ─────────────────────────────────────────────────────────────────

    #[test]
    fn bf16_round_trip_within_tolerance() {
        // BF16 has ~7-bit mantissa; pick floats representable within 1% error.
        let src = [0.5_f32, 1.0, 2.5, 100.0];
        let bytes = cast_from_f32(&src, VectorStorageDtype::BF16);
        let got = cast_to_f32(&bytes, VectorStorageDtype::BF16, 4).unwrap();
        for (orig, recovered) in src.iter().zip(got.iter()) {
            assert!(
                (orig - recovered).abs() < 1e-2,
                "BF16 round-trip: {orig} → {recovered}, diff too large"
            );
        }
    }

    #[test]
    fn bf16_empty_round_trip() {
        let bytes = cast_from_f32(&[], VectorStorageDtype::BF16);
        assert!(bytes.is_empty());
        let got = cast_to_f32(&[], VectorStorageDtype::BF16, 0).unwrap();
        assert!(got.is_empty());
    }

    // ── Range semantics (BF16 wide range vs F16 overflow) ────────────────────

    #[test]
    fn bf16_can_represent_large_values_f16_cannot() {
        // 1e30 is within BF16's exponent range (matches F32 range) but overflows F16.
        let large = [1.0e30_f32];

        let bf16_bytes = cast_from_f32(&large, VectorStorageDtype::BF16);
        let bf16_back = cast_to_f32(&bf16_bytes, VectorStorageDtype::BF16, 1).unwrap();
        assert!(
            bf16_back[0].is_finite(),
            "BF16 should represent 1e30 as finite"
        );

        let f16_bytes = cast_from_f32(&large, VectorStorageDtype::F16);
        let f16_back = cast_to_f32(&f16_bytes, VectorStorageDtype::F16, 1).unwrap();
        assert!(
            f16_back[0].is_infinite(),
            "F16 should overflow 1e30 to infinity"
        );
    }

    // ── Byte-length validation ────────────────────────────────────────────────

    #[test]
    fn bad_byte_len_f32_mismatch() {
        let err = cast_to_f32(&[0u8; 7], VectorStorageDtype::F32, 2).unwrap_err();
        match err {
            DtypeError::BadByteLen {
                dtype,
                dim,
                expected,
                actual,
            } => {
                assert_eq!(dtype, VectorStorageDtype::F32);
                assert_eq!(dim, 2);
                assert_eq!(expected, 8);
                assert_eq!(actual, 7);
            }
        }
    }

    #[test]
    fn bad_byte_len_f16_odd_byte_count() {
        let err = cast_to_f32(&[0u8; 3], VectorStorageDtype::F16, 2).unwrap_err();
        match err {
            DtypeError::BadByteLen {
                dtype,
                dim,
                expected,
                actual,
            } => {
                assert_eq!(dtype, VectorStorageDtype::F16);
                assert_eq!(dim, 2);
                assert_eq!(expected, 4);
                assert_eq!(actual, 3);
            }
        }
    }

    #[test]
    fn bad_byte_len_bf16_mismatch() {
        let err = cast_to_f32(&[0u8; 5], VectorStorageDtype::BF16, 3).unwrap_err();
        match err {
            DtypeError::BadByteLen {
                dtype,
                dim,
                expected,
                actual,
            } => {
                assert_eq!(dtype, VectorStorageDtype::BF16);
                assert_eq!(dim, 3);
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
        }
    }

    // ── validate_byte_len independently ──────────────────────────────────────

    #[test]
    fn validate_byte_len_correct_passes() {
        let bytes = [0u8; 12]; // 3 × F32
        assert!(validate_byte_len(&bytes, VectorStorageDtype::F32, 3).is_ok());
    }

    #[test]
    fn validate_byte_len_off_by_one_fails() {
        let bytes = [0u8; 11]; // should be 12
        let err = validate_byte_len(&bytes, VectorStorageDtype::F32, 3).unwrap_err();
        match err {
            DtypeError::BadByteLen {
                expected, actual, ..
            } => {
                assert_eq!(expected, 12);
                assert_eq!(actual, 11);
            }
        }
    }
}
