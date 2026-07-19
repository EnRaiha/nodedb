// SPDX-License-Identifier: Apache-2.0

//! Type-coerced equality and ordering for `Value`.
//!
//! Single source of truth for type coercion in filter/sort evaluation.

use super::core::Value;

impl Value {
    /// Coerced equality: `Value` vs `Value` with numeric/string coercion.
    ///
    /// Single source of truth for type coercion in filter evaluation.
    /// Used by `matches_binary` (msgpack path) and `matches_value` (Value path).
    pub fn eq_coerced(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Integer(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Integer(b)) => *a == *b as f64,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => {
                a == b
                    || matches!(
                        (crate::NdbDateTime::parse(a), crate::NdbDateTime::parse(b)),
                        (Some(x), Some(y)) if x.micros == y.micros
                    )
            }
            // Coercion: number vs string
            (Value::Integer(a), Value::String(s)) => {
                s.parse::<i64>().is_ok_and(|n| *a == n)
                    || s.parse::<f64>().is_ok_and(|n| *a as f64 == n)
            }
            (Value::String(s), Value::Integer(b)) => {
                s.parse::<i64>().is_ok_and(|n| n == *b)
                    || s.parse::<f64>().is_ok_and(|n| n == *b as f64)
            }
            (Value::Float(a), Value::String(s)) => s.parse::<f64>().is_ok_and(|n| *a == n),
            (Value::String(s), Value::Float(b)) => s.parse::<f64>().is_ok_and(|n| n == *b),
            // Structural equality on ND cells: same coords and same attrs.
            (Value::ArrayCell(a), Value::ArrayCell(b)) => a == b,
            (a, b) => match (datetime_micros(a), datetime_micros(b)) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            },
        }
    }

    /// Coerced ordering: `Value` vs `Value` with numeric/string coercion.
    ///
    /// Single source of truth for ordering in filter/sort evaluation.
    pub fn cmp_coerced(&self, other: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        // ND cells: lexicographic on coords, then attrs. Matches array
        // engine cell ordering (coordinate-major).
        if let (Value::ArrayCell(a), Value::ArrayCell(b)) = (self, other) {
            for (x, y) in a.coords.iter().zip(b.coords.iter()) {
                match x.cmp_coerced(y) {
                    Ordering::Equal => continue,
                    non_eq => return non_eq,
                }
            }
            match a.coords.len().cmp(&b.coords.len()) {
                Ordering::Equal => {}
                non_eq => return non_eq,
            }
            for (x, y) in a.attrs.iter().zip(b.attrs.iter()) {
                match x.cmp_coerced(y) {
                    Ordering::Equal => continue,
                    non_eq => return non_eq,
                }
            }
            return a.attrs.len().cmp(&b.attrs.len());
        }
        let self_f64 = match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        };
        let other_f64 = match other {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        };
        if let (Some(a), Some(b)) = (self_f64, other_f64) {
            return a.partial_cmp(&b).unwrap_or(Ordering::Equal);
        }
        if let (Some(a), Some(b)) = (datetime_micros(self), datetime_micros(other)) {
            return a.cmp(&b);
        }
        let a_str = match self {
            Value::String(s) => s.as_str(),
            _ => return Ordering::Equal,
        };
        let b_str = match other {
            Value::String(s) => s.as_str(),
            _ => return Ordering::Equal,
        };
        a_str.cmp(b_str)
    }
}

/// Epoch micros for a datetime-typed value, or a string that parses as an
/// ISO-8601 / SQL timestamp. Returns `None` for anything not datetime-like so
/// ordinary strings keep lexicographic ordering.
fn datetime_micros(v: &Value) -> Option<i64> {
    match v {
        Value::NaiveDateTime(dt) | Value::DateTime(dt) => Some(dt.micros),
        Value::String(s) => crate::NdbDateTime::parse(s).map(|dt| dt.micros),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_coerced_same_type() {
        assert!(Value::Null.eq_coerced(&Value::Null));
        assert!(Value::Bool(true).eq_coerced(&Value::Bool(true)));
        assert!(!Value::Bool(true).eq_coerced(&Value::Bool(false)));
        assert!(Value::Integer(42).eq_coerced(&Value::Integer(42)));
        assert!(Value::Float(2.78).eq_coerced(&Value::Float(2.78)));
        assert!(Value::String("hello".into()).eq_coerced(&Value::String("hello".into())));
    }

    #[test]
    fn eq_coerced_int_float() {
        assert!(Value::Integer(5).eq_coerced(&Value::Float(5.0)));
        assert!(Value::Float(5.0).eq_coerced(&Value::Integer(5)));
        assert!(!Value::Integer(5).eq_coerced(&Value::Float(5.1)));
    }

    #[test]
    fn eq_coerced_string_number() {
        assert!(Value::String("5".into()).eq_coerced(&Value::Integer(5)));
        assert!(Value::Integer(5).eq_coerced(&Value::String("5".into())));
        assert!(Value::String("2.78".into()).eq_coerced(&Value::Float(2.78)));
        assert!(Value::Float(2.78).eq_coerced(&Value::String("2.78".into())));
        assert!(!Value::String("abc".into()).eq_coerced(&Value::Integer(5)));
        assert!(!Value::Integer(5).eq_coerced(&Value::String("abc".into())));
    }

    #[test]
    fn eq_coerced_cross_type_false() {
        assert!(!Value::Bool(true).eq_coerced(&Value::Integer(1)));
        assert!(!Value::Null.eq_coerced(&Value::Integer(0)));
        assert!(!Value::Null.eq_coerced(&Value::String("".into())));
    }

    #[test]
    fn cmp_coerced_numeric() {
        use std::cmp::Ordering;
        assert_eq!(
            Value::Integer(5).cmp_coerced(&Value::Integer(10)),
            Ordering::Less
        );
        assert_eq!(
            Value::Integer(10).cmp_coerced(&Value::Float(5.0)),
            Ordering::Greater
        );
        assert_eq!(
            Value::String("90".into()).cmp_coerced(&Value::Integer(80)),
            Ordering::Greater
        );
        assert_eq!(
            Value::Float(2.78).cmp_coerced(&Value::String("2.78".into())),
            Ordering::Equal
        );
    }

    #[test]
    fn cmp_coerced_string_fallback() {
        use std::cmp::Ordering;
        assert_eq!(
            Value::String("abc".into()).cmp_coerced(&Value::String("def".into())),
            Ordering::Less
        );
        assert_eq!(
            Value::String("z".into()).cmp_coerced(&Value::String("a".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn cmp_coerced_timestamp_mismatched_string_formats() {
        use std::cmp::Ordering;
        let stored = Value::String("2026-07-02T13:00:00.000000Z".into());
        assert_eq!(
            stored.cmp_coerced(&Value::String("2026-07-02 14:00:00".into())),
            Ordering::Less
        );
        assert_eq!(
            stored.cmp_coerced(&Value::String("2026-07-02 12:00:00".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn eq_coerced_timestamp_mismatched_string_formats() {
        let stored = Value::String("2026-07-02T13:00:00.000000Z".into());
        assert!(stored.eq_coerced(&Value::String("2026-07-02 13:00:00".into())));
    }

    #[test]
    fn cmp_coerced_ordinary_strings_stay_lexicographic() {
        use std::cmp::Ordering;
        // Non-timestamp strings must NOT be datetime-coerced.
        assert_eq!(
            Value::String("apple".into()).cmp_coerced(&Value::String("banana".into())),
            Ordering::Less
        );
        assert_eq!(
            Value::String("zed".into()).cmp_coerced(&Value::String("abc".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn eq_coerced_symmetry() {
        let cases = [
            (Value::Integer(42), Value::String("42".into())),
            (Value::Float(2.78), Value::String("2.78".into())),
            (Value::Integer(5), Value::Float(5.0)),
        ];
        for (a, b) in &cases {
            assert_eq!(
                a.eq_coerced(b),
                b.eq_coerced(a),
                "symmetry violated for {a:?} vs {b:?}"
            );
        }
    }
}
