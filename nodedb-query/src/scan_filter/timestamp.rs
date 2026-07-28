// SPDX-License-Identifier: Apache-2.0

//! Coercion of a filter's comparison value to epoch milliseconds.
//!
//! A predicate against a timestamp column can arrive carrying any of the
//! shapes SQL accepts for an instant: a datetime literal (`ts < '2020-03-05
//! 10:00:00'`), an epoch-millisecond integer (`ts < 1583402400000`), or an
//! already-typed datetime value. Timestamp columns store epoch milliseconds,
//! so every one of those has to reduce to the same scalar before comparison —
//! otherwise a perfectly ordinary predicate silently matches nothing.

use nodedb_types::Value;

/// Reduce a filter value to epoch milliseconds, or `None` when it cannot
/// denote an instant.
pub fn value_as_timestamp_ms(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(ms) => Some(*ms),
        Value::Float(ms) => Some(*ms as i64),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => Some(dt.unix_millis()),
        Value::String(text) => nodedb_types::datetime::NdbDateTime::parse(text)
            .map(|dt| dt.unix_millis())
            .or_else(|| text.trim().parse::<i64>().ok()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2020-03-05T10:00:00Z in epoch milliseconds.
    const MARCH_5_2020: i64 = 1_583_402_400_000;

    #[test]
    fn epoch_millis_pass_through() {
        assert_eq!(
            value_as_timestamp_ms(&Value::Integer(MARCH_5_2020)),
            Some(MARCH_5_2020)
        );
    }

    #[test]
    fn datetime_literals_are_parsed() {
        assert_eq!(
            value_as_timestamp_ms(&Value::String("2020-03-05 10:00:00".into())),
            Some(MARCH_5_2020)
        );
        assert_eq!(
            value_as_timestamp_ms(&Value::String("2020-03-05T10:00:00Z".into())),
            Some(MARCH_5_2020)
        );
    }

    #[test]
    fn numeric_strings_are_read_as_epoch_millis() {
        assert_eq!(
            value_as_timestamp_ms(&Value::String(MARCH_5_2020.to_string())),
            Some(MARCH_5_2020)
        );
    }

    #[test]
    fn typed_datetimes_are_accepted() {
        let dt = nodedb_types::NdbDateTime::from_millis(MARCH_5_2020).unwrap();
        assert_eq!(
            value_as_timestamp_ms(&Value::NaiveDateTime(dt)),
            Some(MARCH_5_2020)
        );
        assert_eq!(
            value_as_timestamp_ms(&Value::DateTime(dt)),
            Some(MARCH_5_2020)
        );
    }

    #[test]
    fn non_instants_are_rejected() {
        assert_eq!(value_as_timestamp_ms(&Value::Null), None);
        assert_eq!(value_as_timestamp_ms(&Value::Bool(true)), None);
        assert_eq!(
            value_as_timestamp_ms(&Value::String("not a date".into())),
            None
        );
    }
}
