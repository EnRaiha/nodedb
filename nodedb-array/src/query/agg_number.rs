// SPDX-License-Identifier: Apache-2.0

//! Running and final numbers of the array reducers.
//!
//! An integer attr accumulates exactly in `i128`, so `SUM` of `INT64`
//! cells past 2^53 keeps every unit. A float attr accumulates in `f64`.
//! `MIN` / `MAX` follow PostgreSQL: NaN sorts above every other float.

use std::cmp::Ordering;

use crate::error::{ArrayError, ArrayResult};
use crate::types::cell_value::value::CellValue;

/// A running reducer number.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggNumber {
    Int(i128),
    Float(f64),
}

/// A finalized reducer value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggScalar {
    Int(i64),
    Float(f64),
}

impl AggNumber {
    /// The number a numeric cell contributes. `None` for a NULL, String, or
    /// Bytes cell.
    pub fn of_cell(v: &CellValue) -> Option<AggNumber> {
        match v.as_i64() {
            Some(i) => Some(AggNumber::Int(i128::from(i))),
            None => v.as_float().map(AggNumber::Float),
        }
    }

    pub fn to_f64(self) -> f64 {
        match self {
            AggNumber::Int(v) => v as f64,
            AggNumber::Float(v) => v,
        }
    }

    /// The sum of two numbers. An `i128` sum saturates, which is far past
    /// the `i64` range a final `SUM` must fit, so it still refuses.
    pub fn sum_with(self, other: AggNumber) -> AggNumber {
        match (self, other) {
            (AggNumber::Int(a), AggNumber::Int(b)) => AggNumber::Int(a.saturating_add(b)),
            (a, b) => AggNumber::Float(a.to_f64() + b.to_f64()),
        }
    }

    /// The smaller number. NaN loses to every other value.
    pub fn min_with(self, other: AggNumber) -> AggNumber {
        match cmp_nan_high(self, other) {
            Ordering::Greater => other,
            Ordering::Less | Ordering::Equal => self,
        }
    }

    /// The larger number. NaN wins over every other value.
    pub fn max_with(self, other: AggNumber) -> AggNumber {
        match cmp_nan_high(self, other) {
            Ordering::Less => other,
            Ordering::Greater | Ordering::Equal => self,
        }
    }

    /// The value as a final `i64` or `f64`. An integer past the `i64`
    /// range is refused as SQLSTATE `22003`.
    pub fn finish(self) -> ArrayResult<AggScalar> {
        match self {
            AggNumber::Int(v) => {
                i64::try_from(v)
                    .map(AggScalar::Int)
                    .map_err(|_| ArrayError::NumericOverflow {
                        detail: format!("aggregate value {v} is out of range for bigint"),
                    })
            }
            AggNumber::Float(v) => Ok(AggScalar::Float(v)),
        }
    }
}

/// Total order with NaN above every other value, as PostgreSQL sorts floats.
fn cmp_nan_high(a: AggNumber, b: AggNumber) -> Ordering {
    match (a, b) {
        (AggNumber::Int(x), AggNumber::Int(y)) => x.cmp(&y),
        (x, y) => {
            let (x, y) = (x.to_f64(), y.to_f64());
            match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_sum_is_exact_past_2_pow_53() {
        let big = AggNumber::Int(1 << 53);
        let sum = big.sum_with(AggNumber::Int(1));
        assert_eq!(sum.finish().unwrap(), AggScalar::Int((1 << 53) + 1));
    }

    #[test]
    fn integer_sum_past_i64_is_refused() {
        let sum = AggNumber::Int(i128::from(i64::MAX)).sum_with(AggNumber::Int(1));
        assert!(matches!(
            sum.finish(),
            Err(ArrayError::NumericOverflow { .. })
        ));
    }

    #[test]
    fn nan_sorts_above_every_float() {
        let nan = AggNumber::Float(f64::NAN);
        let one = AggNumber::Float(1.0);
        assert!(nan.max_with(one).to_f64().is_nan());
        assert!(one.max_with(nan).to_f64().is_nan());
        assert_eq!(nan.min_with(one), one);
        assert_eq!(one.min_with(nan), one);
    }

    #[test]
    fn narrow_cells_contribute_exact_integers() {
        assert_eq!(
            AggNumber::of_cell(&CellValue::UInt16(65535)),
            Some(AggNumber::Int(65535))
        );
        assert_eq!(
            AggNumber::of_cell(&CellValue::Float32(0.5)),
            Some(AggNumber::Float(0.5))
        );
        assert_eq!(AggNumber::of_cell(&CellValue::Null), None);
    }
}
