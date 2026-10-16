// SPDX-License-Identifier: Apache-2.0

//! Widening a stored `f32` to the `f64` every output path carries.
//!
//! The `f64` is the nearest double to the shortest decimal that names the
//! `f32`, so `0.1` stored as `FLOAT32` reads as `0.1` over JSON, the native
//! protocol, and pgwire alike. Narrowing that `f64` back gives the stored
//! `f32` bit for bit.

/// The `f64` an `f32` cell reads as.
pub fn widen_f32(v: f32) -> f64 {
    let exact = f64::from(v);
    // The shortest decimal of a finite `f32` round-trips to it. A value that
    // does not, such as a NaN payload, widens exactly.
    v.to_string()
        .parse::<f64>()
        .ok()
        .filter(|d| (*d as f32).to_bits() == v.to_bits())
        .unwrap_or(exact)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_one_widens_to_point_one() {
        assert_eq!(widen_f32(0.1), 0.1_f64);
        assert_ne!(f64::from(0.1_f32), 0.1_f64);
    }

    #[test]
    fn f32_max_widens_to_its_shortest_decimal() {
        assert_eq!(widen_f32(f32::MAX), 3.4028235e38_f64);
        assert_eq!(widen_f32(f32::MAX) as f32, f32::MAX);
    }

    #[test]
    fn every_widened_value_narrows_back_bit_exact() {
        for v in [
            0.0_f32,
            -0.0,
            f32::from_bits(1),
            f32::MIN_POSITIVE,
            0.3,
            -123.456,
            16_777_217.0,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ] {
            assert_eq!((widen_f32(v) as f32).to_bits(), v.to_bits(), "{v}");
        }
        assert!(widen_f32(f32::NAN).is_nan());
    }
}
