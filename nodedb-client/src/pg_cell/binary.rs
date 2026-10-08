// SPDX-License-Identifier: Apache-2.0

//! Binary-format decoders for the scalar types the server sends in binary.
//!
//! `tokio_postgres` requests the binary result format for every column. The
//! server honours it for `bool`, `int2`, `int4`, `int8`, `float4`, `float8`,
//! `timestamp` and `timestamptz`, so these decoders read the PostgreSQL
//! binary wire form of exactly those types.

use nodedb_types::{NdbDateTime, Value};

use super::error::DecodeReason;

/// Microseconds from the Unix epoch to the PostgreSQL epoch
/// (2000-01-01 00:00:00 UTC), which binary timestamps count from.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// The `N` bytes of a fixed-width binary scalar.
fn fixed<const N: usize>(raw: &[u8]) -> Result<[u8; N], DecodeReason> {
    raw.try_into().map_err(|_| DecodeReason::Width {
        expected: N,
        found: raw.len(),
    })
}

/// Binary `bool`: one byte, 0 or 1.
pub(super) fn boolean(raw: &[u8]) -> Result<Value, DecodeReason> {
    match fixed::<1>(raw)? {
        [0] => Ok(Value::Bool(false)),
        [1] => Ok(Value::Bool(true)),
        _ => Err(DecodeReason::BoolByte),
    }
}

/// Binary `int2`: a big-endian `i16`.
pub(super) fn int2(raw: &[u8]) -> Result<Value, DecodeReason> {
    Ok(Value::Integer(i64::from(i16::from_be_bytes(fixed(raw)?))))
}

/// Binary `int4`: a big-endian `i32`.
pub(super) fn int4(raw: &[u8]) -> Result<Value, DecodeReason> {
    Ok(Value::Integer(i64::from(i32::from_be_bytes(fixed(raw)?))))
}

/// Binary `int8`: a big-endian `i64`.
pub(super) fn int8(raw: &[u8]) -> Result<Value, DecodeReason> {
    Ok(Value::Integer(i64::from_be_bytes(fixed(raw)?)))
}

/// Binary `float4`: a big-endian IEEE-754 `f32`, widened exactly to `f64`.
/// `NaN` and the infinities keep their value.
pub(super) fn float4(raw: &[u8]) -> Result<Value, DecodeReason> {
    Ok(Value::Float(f64::from(f32::from_be_bytes(fixed(raw)?))))
}

/// Binary `float8`: a big-endian IEEE-754 `f64`.
pub(super) fn float8(raw: &[u8]) -> Result<Value, DecodeReason> {
    Ok(Value::Float(f64::from_be_bytes(fixed(raw)?)))
}

/// The instant a binary `timestamp`/`timestamptz` holds: an `i64` of
/// microseconds since the PostgreSQL epoch.
fn instant(raw: &[u8]) -> Result<NdbDateTime, DecodeReason> {
    let pg_micros = i64::from_be_bytes(fixed(raw)?);
    pg_micros
        .checked_add(PG_EPOCH_OFFSET_MICROS)
        .map(NdbDateTime::from_micros)
        .ok_or(DecodeReason::OutOfRange { what: "timestamp" })
}

/// Binary `timestamp`: a naive instant.
pub(super) fn timestamp(raw: &[u8]) -> Result<Value, DecodeReason> {
    instant(raw).map(Value::NaiveDateTime)
}

/// Binary `timestamptz`: a UTC instant.
pub(super) fn timestamptz(raw: &[u8]) -> Result<Value, DecodeReason> {
    instant(raw).map(Value::DateTime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_bool() {
        assert_eq!(boolean(&[1]), Ok(Value::Bool(true)));
        assert_eq!(boolean(&[0]), Ok(Value::Bool(false)));
        assert_eq!(boolean(&[2]), Err(DecodeReason::BoolByte));
        assert_eq!(
            boolean(b"t"),
            Err(DecodeReason::BoolByte),
            "text `t` under a binary bool is refused"
        );
        assert_eq!(
            boolean(&[]),
            Err(DecodeReason::Width {
                expected: 1,
                found: 0
            })
        );
    }

    #[test]
    fn decodes_integers() {
        assert_eq!(int2(&(-7i16).to_be_bytes()), Ok(Value::Integer(-7)));
        assert_eq!(
            int4(&i32::MAX.to_be_bytes()),
            Ok(Value::Integer(i64::from(i32::MAX)))
        );
        assert_eq!(int8(&i64::MIN.to_be_bytes()), Ok(Value::Integer(i64::MIN)));
        assert_eq!(
            int4(b"42"),
            Err(DecodeReason::Width {
                expected: 4,
                found: 2
            })
        );
        assert_eq!(
            int8(&[0; 4]),
            Err(DecodeReason::Width {
                expected: 8,
                found: 4
            })
        );
    }

    #[test]
    fn decodes_floats_including_non_finite() {
        assert_eq!(float4(&1.5f32.to_be_bytes()), Ok(Value::Float(1.5)));
        assert_eq!(float8(&(-2.25f64).to_be_bytes()), Ok(Value::Float(-2.25)));
        assert_eq!(
            float8(&f64::INFINITY.to_be_bytes()),
            Ok(Value::Float(f64::INFINITY))
        );
        assert_eq!(
            float4(&f32::NEG_INFINITY.to_be_bytes()),
            Ok(Value::Float(f64::NEG_INFINITY))
        );
        let Ok(Value::Float(nan)) = float8(&f64::NAN.to_be_bytes()) else {
            panic!("binary NaN decodes as a float");
        };
        assert!(nan.is_nan());
        assert_eq!(
            float4(&[0; 8]),
            Err(DecodeReason::Width {
                expected: 4,
                found: 8
            })
        );
    }

    #[test]
    fn decodes_timestamps_from_the_postgres_epoch() {
        // 2020-03-05T10:00:00Z.
        let unix_micros = 1_583_402_400_000_000i64;
        let raw = (unix_micros - PG_EPOCH_OFFSET_MICROS).to_be_bytes();
        let at = NdbDateTime::from_micros(unix_micros);
        assert_eq!(timestamp(&raw), Ok(Value::NaiveDateTime(at)));
        assert_eq!(timestamptz(&raw), Ok(Value::DateTime(at)));
        assert_eq!(
            timestamptz(&i64::MAX.to_be_bytes()),
            Err(DecodeReason::OutOfRange { what: "timestamp" })
        );
        assert_eq!(
            timestamp(b"2020"),
            Err(DecodeReason::Width {
                expected: 8,
                found: 4
            })
        );
    }
}
