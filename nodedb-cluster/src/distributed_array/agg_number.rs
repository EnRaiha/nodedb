// SPDX-License-Identifier: BUSL-1.1

//! The running number a distributed array aggregate partial carries.
//!
//! An integer attr's partial carries an exact `i128`, so a cluster `SUM`
//! equals the single-node `SUM` to the unit. A float attr's partial carries
//! an `f64`. `MIN` / `MAX` sort NaN above every other float, as PostgreSQL
//! does.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// A partial's sum, minimum, or maximum.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ArrayAggNumber {
    /// An exact integer: the high and low 64 bits of an `i128`.
    Int {
        hi: i64,
        lo: u64,
    },
    Float(f64),
}

impl ArrayAggNumber {
    pub fn int(v: i128) -> Self {
        ArrayAggNumber::Int {
            hi: (v >> 64) as i64,
            lo: v as u64,
        }
    }

    /// The exact integer, or `None` for a float.
    pub fn as_i128(self) -> Option<i128> {
        match self {
            ArrayAggNumber::Int { hi, lo } => Some(join(hi, lo)),
            ArrayAggNumber::Float(_) => None,
        }
    }

    pub fn as_f64(self) -> f64 {
        match self {
            ArrayAggNumber::Int { hi, lo } => join(hi, lo) as f64,
            ArrayAggNumber::Float(v) => v,
        }
    }

    /// The sum of two numbers. An integer sum saturates at the `i128`
    /// bounds, far past the `i64` range a final `SUM` must fit.
    pub fn sum_with(self, other: Self) -> Self {
        match (self.as_i128(), other.as_i128()) {
            (Some(a), Some(b)) => Self::int(a.saturating_add(b)),
            _ => ArrayAggNumber::Float(self.as_f64() + other.as_f64()),
        }
    }

    /// The smaller number. NaN loses to every other value.
    pub fn min_with(self, other: Self) -> Self {
        match cmp_nan_high(self, other) {
            Ordering::Greater => other,
            Ordering::Less | Ordering::Equal => self,
        }
    }

    /// The larger number. NaN wins over every other value.
    pub fn max_with(self, other: Self) -> Self {
        match cmp_nan_high(self, other) {
            Ordering::Less => other,
            Ordering::Greater | Ordering::Equal => self,
        }
    }
}

fn join(hi: i64, lo: u64) -> i128 {
    (i128::from(hi) << 64) | i128::from(lo)
}

fn cmp_nan_high(a: ArrayAggNumber, b: ArrayAggNumber) -> Ordering {
    if let (Some(x), Some(y)) = (a.as_i128(), b.as_i128()) {
        return x.cmp(&y);
    }
    let (x, y) = (a.as_f64(), b.as_f64());
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_split_i128_round_trips() {
        for v in [0i128, -1, 1 << 70, -(1 << 70), i128::from(i64::MIN) - 5] {
            assert_eq!(ArrayAggNumber::int(v).as_i128(), Some(v));
        }
    }

    #[test]
    fn integer_sums_stay_exact_past_2_pow_53() {
        let a = ArrayAggNumber::int(1 << 53);
        let b = ArrayAggNumber::int(1);
        assert_eq!(a.sum_with(b).as_i128(), Some((1 << 53) + 1));
    }

    #[test]
    fn nan_sorts_high() {
        let nan = ArrayAggNumber::Float(f64::NAN);
        let one = ArrayAggNumber::Float(1.0);
        assert!(one.max_with(nan).as_f64().is_nan());
        assert_eq!(nan.min_with(one), one);
    }

    #[test]
    fn the_number_survives_msgpack() {
        let n = ArrayAggNumber::int(-(1 << 80));
        let bytes = zerompk::to_msgpack_vec(&n).expect("encode");
        let back: ArrayAggNumber = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, n);
    }
}
