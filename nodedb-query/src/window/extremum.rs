// SPDX-License-Identifier: Apache-2.0

//! Extremum selection for MIN / MAX aggregates.
//!
//! MIN / MAX keep the original value and return it unchanged. An integer
//! stays an integer, a float stays a float. A numeric pair compares by its
//! exact numeric reading, so integers above 2^53 and fractional decimals
//! never round through `f64`. NaN sorts above every number, as in
//! PostgreSQL: MAX over a NaN input is NaN, and MIN skips NaN unless every
//! input is NaN. A non-numeric pair uses the coerced value order.

use std::cmp::Ordering;

use nodedb_types::Value;

/// Whether the `Value` `candidate` replaces `current` as the MIN (`want_max`
/// false) or MAX (`want_max` true). An empty `current` is always replaced.
pub fn value_replaces(candidate: &Value, current: Option<&Value>, want_max: bool) -> bool {
    let Some(current) = current else {
        return true;
    };
    let order = crate::value_ops::numeric_order(candidate, current)
        .unwrap_or_else(|| crate::value_ops::compare_values(candidate, current));
    let wanted = if want_max {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    order == wanted
}

/// Direction of an extremum function: `Some(false)` for MIN, `Some(true)`
/// for MAX, `None` for any other function.
pub fn extremum_direction(func_name: &str) -> Option<bool> {
    match func_name {
        "min" => Some(false),
        "max" => Some(true),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_above_2_pow_53_compare_exactly() {
        let a = Value::Integer(9_007_199_254_740_993);
        let b = Value::Integer(9_007_199_254_740_992);
        assert!(value_replaces(&b, Some(&a), false));
        assert!(!value_replaces(&b, Some(&a), true));
        assert!(value_replaces(&a, Some(&b), true));
    }

    /// NaN is above every number: it wins MAX and loses MIN.
    #[test]
    fn value_nan_is_the_largest_extreme() {
        let one = Value::Integer(1);
        let nan = Value::Float(f64::NAN);
        assert!(!value_replaces(&nan, Some(&one), false));
        assert!(value_replaces(&nan, Some(&one), true));
        assert!(value_replaces(&one, Some(&nan), false));
        assert!(!value_replaces(&one, Some(&nan), true));
        assert!(!value_replaces(&nan, Some(&nan), true));
        let inf = Value::Float(f64::INFINITY);
        assert!(value_replaces(&nan, Some(&inf), true));
    }

    /// MAX over decimal text one hundredth apart past `2^53` picks the
    /// larger, whatever the input order.
    #[test]
    fn decimal_text_max_compares_exactly() {
        let low = Value::String("12345678901234567.01".into());
        let high = Value::String("12345678901234567.02".into());
        assert!(value_replaces(&high, Some(&low), true));
        assert!(!value_replaces(&low, Some(&high), true));
        assert!(value_replaces(&low, Some(&high), false));
        let low_dec = Value::Decimal(rust_decimal::Decimal::new(1_234_567_890_123_456_701, 2));
        let high_dec = Value::Decimal(rust_decimal::Decimal::new(1_234_567_890_123_456_702, 2));
        assert!(value_replaces(&high_dec, Some(&low_dec), true));
        assert!(!value_replaces(&low_dec, Some(&high_dec), true));
        assert!(value_replaces(&high, Some(&low_dec), true));
    }

    #[test]
    fn value_mixed_int_float_compare_exactly() {
        let big = Value::Integer(9_007_199_254_740_993);
        let float = Value::Float(9_007_199_254_740_992.0);
        assert!(value_replaces(&big, Some(&float), true));
        assert!(!value_replaces(&big, Some(&float), false));
        assert!(value_replaces(&float, Some(&big), false));
    }
}
