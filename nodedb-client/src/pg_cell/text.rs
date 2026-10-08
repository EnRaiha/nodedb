// SPDX-License-Identifier: Apache-2.0

//! Text-format decoders: the PostgreSQL text form of each scalar type.
//!
//! The server sends `bytea`, `json`, `jsonb` and the array types in text
//! format. It sends every type outside the binary scalar set (`numeric`,
//! `uuid`, `interval`, `text`, `varchar`, `name`) as its text bytes. Array
//! elements are text too, so every scalar type has a text decoder here.

use nodedb_types::{NdbDateTime, NdbDuration, Value};
use rust_decimal::Decimal;
use tokio_postgres::types::Type;

use super::error::DecodeReason;

/// Decode `text` as the text form of the scalar PG type `ty`.
pub(super) fn scalar(ty: &Type, text: &str) -> Result<Value, DecodeReason> {
    match *ty {
        Type::BOOL => boolean(text),
        Type::INT2 => integer::<i16>(text, "int2"),
        Type::INT4 => integer::<i32>(text, "int4"),
        Type::INT8 => integer::<i64>(text, "int8"),
        Type::FLOAT4 => float4(text).map(|f| Value::Float(f64::from(f))),
        Type::FLOAT8 => float8(text).map(Value::Float),
        Type::NUMERIC => numeric(text),
        Type::TEXT | Type::VARCHAR | Type::NAME | Type::BPCHAR => {
            Ok(Value::String(text.to_owned()))
        }
        Type::BYTEA => bytea(text),
        Type::UUID => uuid(text),
        Type::JSON | Type::JSONB => json(text),
        Type::TIMESTAMP => instant(text).map(Value::NaiveDateTime),
        Type::TIMESTAMPTZ => instant(text).map(Value::DateTime),
        Type::INTERVAL => {
            NdbDuration::parse(text)
                .map(Value::Duration)
                .ok_or(DecodeReason::Invalid {
                    expected: "interval",
                })
        }
        _ => Err(DecodeReason::Unsupported),
    }
}

/// `t` or `f`, as PostgreSQL renders a `bool`.
fn boolean(text: &str) -> Result<Value, DecodeReason> {
    match text {
        "t" => Ok(Value::Bool(true)),
        "f" => Ok(Value::Bool(false)),
        _ => Err(DecodeReason::Invalid { expected: "bool" }),
    }
}

/// A decimal integer that fits `T`.
fn integer<T>(text: &str, expected: &'static str) -> Result<Value, DecodeReason>
where
    T: std::str::FromStr + Into<i64>,
{
    text.parse::<T>()
        .map(|n| Value::Integer(n.into()))
        .map_err(|_| DecodeReason::Invalid { expected })
}

/// A `float8` in PostgreSQL text: a finite number, `NaN`, `Infinity` or
/// `-Infinity`. A finite text that overflows `f64` is refused.
fn float8(text: &str) -> Result<f64, DecodeReason> {
    match text {
        "NaN" => Ok(f64::NAN),
        "Infinity" => Ok(f64::INFINITY),
        "-Infinity" => Ok(f64::NEG_INFINITY),
        _ => text
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or(DecodeReason::Invalid { expected: "float8" }),
    }
}

/// A `float4` in PostgreSQL text. It parses as `f32`, so the value is the
/// one the server held.
fn float4(text: &str) -> Result<f32, DecodeReason> {
    match text {
        "NaN" => Ok(f32::NAN),
        "Infinity" => Ok(f32::INFINITY),
        "-Infinity" => Ok(f32::NEG_INFINITY),
        _ => text
            .parse::<f32>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or(DecodeReason::Invalid { expected: "float4" }),
    }
}

/// A `numeric`. A finite number is an exact `Value::Decimal`: digits past
/// the `Decimal` precision are refused, never rounded. An unsigned integer
/// above `i64::MAX` goes through [`Value::from_u64`], the decimal a native
/// `uint64` decodes to. `Decimal` holds no `NaN` or infinity, so those
/// become the `Value::Float` of the same value.
fn numeric(text: &str) -> Result<Value, DecodeReason> {
    match text {
        "NaN" => return Ok(Value::Float(f64::NAN)),
        "Infinity" => return Ok(Value::Float(f64::INFINITY)),
        "-Infinity" => return Ok(Value::Float(f64::NEG_INFINITY)),
        _ => {}
    }
    if let Ok(unsigned) = text.parse::<u64>()
        && i64::try_from(unsigned).is_err()
    {
        return Ok(Value::from_u64(unsigned));
    }
    let parsed = if text.contains(['e', 'E']) {
        Decimal::from_scientific(text)
    } else {
        Decimal::from_str_exact(text)
    };
    parsed
        .map(Value::Decimal)
        .map_err(|_| DecodeReason::Invalid {
            expected: "numeric",
        })
}

/// A `bytea` in PostgreSQL hex form: `\x` then two hex digits per byte.
fn bytea(text: &str) -> Result<Value, DecodeReason> {
    let invalid = DecodeReason::Invalid {
        expected: "bytea in \\x hex form",
    };
    let Some(hex) = text.strip_prefix("\\x") else {
        return Err(invalid);
    };
    if hex.len() % 2 != 0 {
        return Err(invalid);
    }
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|&[high, low]| match (hex_digit(high), hex_digit(low)) {
            (Some(high), Some(low)) => Ok((high << 4) | low),
            _ => Err(invalid.clone()),
        })
        .collect::<Result<Vec<u8>, _>>()
        .map(Value::Bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    char::from(byte)
        .to_digit(16)
        .and_then(|d| u8::try_from(d).ok())
}

/// A `uuid` column cell. The server sends a UUID as 36-character hyphenated
/// hex and a ULID, which shares the `uuid` OID, as 26 Crockford base32
/// characters.
fn uuid(text: &str) -> Result<Value, DecodeReason> {
    if is_hyphenated_uuid(text) {
        Ok(Value::Uuid(text.to_ascii_lowercase()))
    } else if is_ulid(text) {
        Ok(Value::Ulid(text.to_ascii_uppercase()))
    } else {
        Err(DecodeReason::Invalid {
            expected: "uuid or ulid",
        })
    }
}

fn is_hyphenated_uuid(text: &str) -> bool {
    text.len() == 36
        && text.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

/// 26 Crockford base32 characters whose first is at most `7`, so the
/// 128-bit value does not overflow.
fn is_ulid(text: &str) -> bool {
    text.len() == 26
        && text
            .bytes()
            .next()
            .is_some_and(|b| (b'0'..=b'7').contains(&b))
        && text.bytes().all(|b| {
            let upper = b.to_ascii_uppercase();
            upper.is_ascii_digit()
                || (upper.is_ascii_uppercase() && !matches!(upper, b'I' | b'L' | b'O' | b'U'))
        })
}

/// A `json` or `jsonb` document, through `json_to_value`.
fn json(text: &str) -> Result<Value, DecodeReason> {
    let invalid = DecodeReason::Invalid { expected: "json" };
    let parsed: serde_json::Value = sonic_rs::from_str(text).map_err(|_| invalid.clone())?;
    crate::remote_parse::json_to_value(&parsed).map_err(|_| invalid)
}

/// An ISO-8601 instant, as the server renders a timestamp in text.
fn instant(text: &str) -> Result<NdbDateTime, DecodeReason> {
    NdbDateTime::parse(text).ok_or(DecodeReason::Invalid {
        expected: "timestamp",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(ty: &Type, text: &str) -> Value {
        scalar(ty, text).unwrap_or_else(|e| panic!("{text:?} as {ty}: {e}"))
    }

    fn refused(ty: &Type, text: &str) -> DecodeReason {
        match scalar(ty, text) {
            Ok(v) => panic!("{text:?} as {ty} must be refused, got {v:?}"),
            Err(e) => e,
        }
    }

    #[test]
    fn decodes_bool_and_integers() {
        assert_eq!(ok(&Type::BOOL, "t"), Value::Bool(true));
        assert_eq!(ok(&Type::BOOL, "f"), Value::Bool(false));
        refused(&Type::BOOL, "true");
        assert_eq!(ok(&Type::INT2, "-32768"), Value::Integer(-32768));
        assert_eq!(ok(&Type::INT4, "7"), Value::Integer(7));
        assert_eq!(
            ok(&Type::INT8, "-9223372036854775808"),
            Value::Integer(i64::MIN)
        );
        refused(&Type::INT2, "32768");
        refused(&Type::INT8, "1.5");
    }

    #[test]
    fn decodes_float_text_including_non_finite() {
        assert_eq!(ok(&Type::FLOAT8, "2.5"), Value::Float(2.5));
        assert_eq!(ok(&Type::FLOAT8, "1e+20"), Value::Float(1e20));
        assert_eq!(ok(&Type::FLOAT8, "Infinity"), Value::Float(f64::INFINITY));
        assert_eq!(
            ok(&Type::FLOAT8, "-Infinity"),
            Value::Float(f64::NEG_INFINITY)
        );
        assert!(matches!(ok(&Type::FLOAT8, "NaN"), Value::Float(f) if f.is_nan()));
        assert_eq!(ok(&Type::FLOAT4, "0.1"), Value::Float(f64::from(0.1f32)));
        assert_eq!(
            ok(&Type::FLOAT4, "-Infinity"),
            Value::Float(f64::NEG_INFINITY)
        );
        assert!(matches!(ok(&Type::FLOAT4, "NaN"), Value::Float(f) if f.is_nan()));
        for text in ["inf", "nan", "1e400", "abc", ""] {
            refused(&Type::FLOAT8, text);
        }
        refused(&Type::FLOAT4, "1e39");
    }

    #[test]
    fn decodes_numeric_exactly() {
        assert_eq!(
            ok(&Type::NUMERIC, "12.50"),
            Value::Decimal(Decimal::new(1250, 2))
        );
        assert_eq!(ok(&Type::NUMERIC, "-3"), Value::Decimal(Decimal::from(-3)));
        assert_eq!(
            ok(&Type::NUMERIC, "18446744073709551615"),
            Value::from_u64(u64::MAX)
        );
        assert_eq!(
            ok(&Type::NUMERIC, "9223372036854775807"),
            Value::Decimal(Decimal::from(i64::MAX))
        );
        assert_eq!(
            ok(&Type::NUMERIC, "1e3"),
            Value::Decimal(Decimal::from(1000))
        );
        assert_eq!(ok(&Type::NUMERIC, "Infinity"), Value::Float(f64::INFINITY));
        assert!(matches!(ok(&Type::NUMERIC, "NaN"), Value::Float(f) if f.is_nan()));
        // More digits than `Decimal` holds: refused, never rounded.
        refused(&Type::NUMERIC, "0.12345678901234567890123456789012");
        refused(&Type::NUMERIC, "12,5");
    }

    #[test]
    fn decodes_strings() {
        for ty in [Type::TEXT, Type::VARCHAR, Type::NAME, Type::BPCHAR] {
            assert_eq!(ok(&ty, "héllo"), Value::String("héllo".into()));
        }
    }

    #[test]
    fn decodes_bytea_hex() {
        assert_eq!(
            ok(&Type::BYTEA, "\\x00ff7A"),
            Value::Bytes(vec![0, 255, 0x7a])
        );
        assert_eq!(ok(&Type::BYTEA, "\\x"), Value::Bytes(Vec::new()));
        for text in ["00ff", "\\x0", "\\xzz", "AP8"] {
            refused(&Type::BYTEA, text);
        }
    }

    #[test]
    fn decodes_uuid_and_ulid() {
        assert_eq!(
            ok(&Type::UUID, "550E8400-E29B-41D4-A716-446655440000"),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into())
        );
        assert_eq!(
            ok(&Type::UUID, "01arz3ndektsv4rrffq69g5fav"),
            Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into())
        );
        for text in [
            "550e8400e29b41d4a716446655440000",
            "550e8400-e29b-41d4-a716-44665544000g",
            "81ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01ARZ3NDEKTSV4RRFFQ69G5FAI",
        ] {
            refused(&Type::UUID, text);
        }
    }

    #[test]
    fn decodes_json_through_json_to_value() {
        let Value::Object(map) = ok(
            &Type::JSONB,
            r#"{"a":[1,2.5,null],"big":18446744073709551615}"#,
        ) else {
            panic!("a JSON object decodes as an Object");
        };
        assert_eq!(
            map.get("a"),
            Some(&Value::Array(vec![
                Value::Integer(1),
                Value::Float(2.5),
                Value::Null
            ]))
        );
        assert_eq!(map.get("big"), Some(&Value::from_u64(u64::MAX)));
        assert_eq!(ok(&Type::JSON, "\"x\""), Value::String("x".into()));
        refused(&Type::JSON, "{bad");
    }

    #[test]
    fn decodes_timestamps_and_interval() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        assert_eq!(
            ok(&Type::TIMESTAMP, "2020-03-05T10:00:00.000000Z"),
            Value::NaiveDateTime(at)
        );
        assert_eq!(
            ok(&Type::TIMESTAMPTZ, "2020-03-05 10:00:00"),
            Value::DateTime(at)
        );
        refused(&Type::TIMESTAMPTZ, "yesterday");
        assert_eq!(
            ok(&Type::INTERVAL, "1h30m"),
            Value::Duration(NdbDuration::from_micros(5_400_000_000))
        );
        refused(&Type::INTERVAL, "01:30:00");
    }

    #[test]
    fn refuses_a_type_without_a_decoder() {
        assert_eq!(refused(&Type::POINT, "(1,2)"), DecodeReason::Unsupported);
    }
}
