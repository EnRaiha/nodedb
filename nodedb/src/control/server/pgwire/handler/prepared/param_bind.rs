// SPDX-License-Identifier: BUSL-1.1

//! Convert pgwire extended-query portal parameters (text or binary wire
//! format) into typed `nodedb_sql::ParamValue` for AST/DSL binding.

use bytes::Bytes;
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use postgres_types::FromSql;

/// Convert pgwire portal parameters to typed `ParamValue` for AST-level binding.
///
/// Uses per-parameter format codes from the pgwire 0.38 `Format` API to determine
/// whether each parameter was sent in text or binary format.
///
/// Binary-format `BOOL`/`INT2`/`INT4`/`INT8`/`FLOAT4`/`FLOAT8` are decoded directly
/// via their well-specified `postgres_types::FromSql` binary encodings.
/// Binary-format `TEXT`/`VARCHAR`/`BPCHAR`/`UNKNOWN` fall through to the text path
/// below — the binary wire encoding of these types *is* the text bytes, so
/// reusing `pgwire_text_to_param` is correct, not a workaround.
///
/// Binary-format `NUMERIC`, `TIMESTAMP`, and `TIMESTAMPTZ` are explicitly
/// rejected with SQLSTATE 0A000: their binary encodings are well-specified
/// (NUMERIC is a variable-length base-10000 digit encoding; TIMESTAMP/
/// TIMESTAMPTZ are an 8-byte big-endian microsecond offset from 2000-01-01)
/// but not yet decoded here, so we refuse rather than guess. Clients must use
/// text format for these types.
///
/// Every other binary-format type (UUID, BYTEA, DATE, TIME, JSON/JSONB, array
/// types, INTERVAL, user-defined types, ...) is also rejected with SQLSTATE
/// 0A000 rather than silently mis-decoded as UTF-8 text — its bytes may
/// happen to be valid UTF-8 without being a valid text representation of
/// the type.
pub(super) fn convert_portal_params(
    params: &[Option<Bytes>],
    param_types: &[Option<Type>],
    param_format: &pgwire::api::portal::Format,
) -> PgWireResult<Vec<nodedb_sql::ParamValue>> {
    let mut result = Vec::with_capacity(params.len());
    for (i, param) in params.iter().enumerate() {
        let pg_type = param_types
            .get(i)
            .and_then(|t| t.as_ref())
            .unwrap_or(&Type::UNKNOWN);

        let pv = match param {
            None => nodedb_sql::ParamValue::Null,
            Some(bytes) => {
                if param_format.is_binary(i) {
                    convert_binary_param(bytes, pg_type, i)?
                } else {
                    let text = decode_utf8_param(bytes, i)?;
                    pgwire_text_to_param(text, pg_type)
                }
            }
        };
        result.push(pv);
    }
    Ok(result)
}

/// Decode a single binary-format parameter.
fn convert_binary_param(
    bytes: &Bytes,
    pg_type: &Type,
    index: usize,
) -> PgWireResult<nodedb_sql::ParamValue> {
    match *pg_type {
        Type::BOOL => {
            decode_binary::<bool>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Bool)
        }
        Type::INT2 => decode_binary::<i16>(bytes, pg_type, index)
            .map(|v| nodedb_sql::ParamValue::Int64(v as i64)),
        Type::INT4 => decode_binary::<i32>(bytes, pg_type, index)
            .map(|v| nodedb_sql::ParamValue::Int64(v as i64)),
        Type::INT8 => {
            decode_binary::<i64>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Int64)
        }
        Type::FLOAT4 => decode_binary::<f32>(bytes, pg_type, index)
            .map(|v| nodedb_sql::ParamValue::Float64(v as f64)),
        Type::FLOAT8 => {
            decode_binary::<f64>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Float64)
        }
        // Binary wire bytes for these types are already the text
        // representation — reuse the text path rather than duplicate it.
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UNKNOWN => {
            let text = decode_utf8_param(bytes, index)?;
            Ok(pgwire_text_to_param(text, pg_type))
        }
        // NUMERIC / TIMESTAMP / TIMESTAMPTZ binary encodings are
        // well-specified but not decoded here — refuse rather than guess.
        Type::NUMERIC => Err(binary_unsupported_error("NUMERIC", index)),
        Type::TIMESTAMP => Err(binary_unsupported_error("TIMESTAMP", index)),
        Type::TIMESTAMPTZ => Err(binary_unsupported_error("TIMESTAMPTZ", index)),
        // Every other binary type: refuse rather than silently mis-decode
        // as UTF-8 text (its bytes may happen to be valid UTF-8 without
        // being a valid text representation of the type).
        _ => Err(binary_unsupported_error(pg_type.name(), index)),
    }
}

/// Decode a parameter's raw bytes as UTF-8 text, mapping a decode failure to
/// a typed pgwire error (SQLSTATE 22021 - character_not_in_repertoire).
/// Shared by the text-format path and the binary TEXT/VARCHAR/BPCHAR/UNKNOWN
/// arm, whose wire bytes are already the text representation.
fn decode_utf8_param(bytes: &[u8], index: usize) -> PgWireResult<&str> {
    std::str::from_utf8(bytes).map_err(|_| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22021".to_owned(),
            format!("invalid UTF-8 in parameter ${}", index + 1),
        )))
    })
}

fn binary_unsupported_error(type_name: &str, index: usize) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "binary {type_name} parameter format is not supported for parameter ${n}; \
             use text format",
            n = index + 1
        ),
    )))
}

/// Decode a binary parameter payload via `postgres_types::FromSql`, mapping
/// a decode failure to a typed pgwire error (SQLSTATE 22P02 -
/// invalid_binary_representation), never a panic.
fn decode_binary<'a, T: FromSql<'a>>(
    bytes: &'a [u8],
    pg_type: &Type,
    index: usize,
) -> PgWireResult<T> {
    T::from_sql(pg_type, bytes).map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P02".to_owned(),
            format!(
                "invalid binary representation for parameter ${}: {e}",
                index + 1
            ),
        )))
    })
}

/// Convert a pgwire text parameter + declared type to a typed
/// `ParamValue` for AST/DSL binding.
///
/// # Type coverage
///
/// Natively decoded: `BOOL`, `INT2`/`INT4`/`INT8`, `FLOAT4`/`FLOAT8`/
/// `NUMERIC`, `TIMESTAMP`, `TIMESTAMPTZ`, `TEXT`/`VARCHAR` (implicit via
/// fall-through), and `UNKNOWN` (the untyped-driver path).
///
/// # TIMESTAMP / TIMESTAMPTZ
///
/// Text-format TIMESTAMP and TIMESTAMPTZ parameters are parsed directly to
/// `ParamValue::Timestamp` / `ParamValue::Timestamptz`. This produces the
/// correct typed `SqlValue` variant (Timestamp vs Timestamptz) through the
/// resolver, ensuring the planner and engine see the right column type rather
/// than a generic string that must be coerced.
///
/// If parsing fails the text is passed through as `ParamValue::Text` so the
/// engine's string-coercion path can attempt a best-effort conversion — the
/// same as all other text-passthrough types.
///
/// # Fallback policy (catch-all arm)
///
/// Types the bind layer does not decode natively — `DATE`, `TIME`, `BYTEA`,
/// `UUID`, `JSON`, `JSONB`, `INTERVAL`, array types, and user-defined types —
/// fall through to `ParamValue::Text(text)`. The pgwire text representation of
/// these types is well-defined and the AST bind emits it as a
/// `SingleQuotedString`. Downstream, the planner/engine type-coerces the text
/// via the same path used for literal strings in simple-query SQL.
///
/// Binary-format parameters are handled at a layer above this function
/// (see `convert_portal_params`); only binary TEXT/VARCHAR/BPCHAR/UNKNOWN
/// reach this function with binary-sourced bytes (which are already text).
///
/// # Why not error on unknown types
///
/// Postgres itself accepts text representations of every built-in type through
/// the extended-query protocol; refusing here would break drivers that
/// legitimately send dates/UUIDs/etc. as text.
pub(super) fn pgwire_text_to_param(text: &str, pg_type: &Type) -> nodedb_sql::ParamValue {
    match *pg_type {
        Type::BOOL => {
            let lower = text.to_lowercase();
            if lower == "t" || lower == "true" || lower == "1" {
                return nodedb_sql::ParamValue::Bool(true);
            }
            if lower == "f" || lower == "false" || lower == "0" {
                return nodedb_sql::ParamValue::Bool(false);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::INT2 | Type::INT4 | Type::INT8 => {
            if let Ok(n) = text.parse::<i64>() {
                return nodedb_sql::ParamValue::Int64(n);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::FLOAT4 | Type::FLOAT8 => {
            if let Ok(f) = text.parse::<f64>() {
                return nodedb_sql::ParamValue::Float64(f);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::NUMERIC => {
            // Parse NUMERIC as exact Decimal, not lossy f64.
            if let Ok(d) = rust_decimal::Decimal::from_str_exact(text) {
                return nodedb_sql::ParamValue::Decimal(d);
            }
            // If parsing fails, return typed error — do not fall back to Float
            // since that would silently lose precision.
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::TIMESTAMP => {
            // Parse ISO 8601 / PostgreSQL timestamp text to a typed NaiveDateTime.
            if let Some(dt) = nodedb_types::datetime::NdbDateTime::parse(text) {
                return nodedb_sql::ParamValue::Timestamp(dt);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::TIMESTAMPTZ => {
            // Parse ISO 8601 / PostgreSQL timestamptz text to a typed DateTime (UTC).
            if let Some(dt) = nodedb_types::datetime::NdbDateTime::parse(text) {
                return nodedb_sql::ParamValue::Timestamptz(dt);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        // Text-passthrough types: wire-format text is already the
        // canonical representation. Engine performs type coercion.
        _ => nodedb_sql::ParamValue::Text(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use pgwire::api::portal::Format;

    use super::*;

    fn text_format() -> Format {
        Format::UnifiedText
    }

    fn binary_format() -> Format {
        Format::UnifiedBinary
    }

    #[test]
    fn convert_null_param() {
        let params = vec![None];
        let types = vec![Some(Type::INT8)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], nodedb_sql::ParamValue::Null));
    }

    #[test]
    fn convert_typed_params() {
        let params = vec![
            Some(Bytes::from_static(b"42")),
            Some(Bytes::from_static(b"hello")),
            Some(Bytes::from_static(b"true")),
        ];
        let types = vec![Some(Type::INT8), Some(Type::TEXT), Some(Type::BOOL)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int64(42)));
        assert!(matches!(&result[1], nodedb_sql::ParamValue::Text(s) if s == "hello"));
        assert!(matches!(result[2], nodedb_sql::ParamValue::Bool(true)));
    }

    #[test]
    fn convert_float_param() {
        let params = vec![Some(Bytes::from_static(b"2.78"))];
        let types = vec![Some(Type::FLOAT8)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.78).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_numeric_text_to_decimal() {
        let params = vec![Some(Bytes::from_static(b"123.45"))];
        let types = vec![Some(Type::NUMERIC)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    fn assert_binary_type_rejected(ty: Type, bytes: &'static [u8], name: &str) {
        let params = vec![Some(Bytes::from_static(bytes))];
        let types = vec![Some(ty)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        let message = error.to_string();
        assert!(message.contains(name) || message.contains("0A000"));
    }

    #[test]
    fn convert_numeric_binary_returns_error() {
        assert_binary_type_rejected(Type::NUMERIC, &[0x00, 0x03, 0x00, 0x02], "NUMERIC");
    }

    #[test]
    fn convert_timestamp_binary_returns_error() {
        assert_binary_type_rejected(Type::TIMESTAMP, &[0; 8], "TIMESTAMP");
    }

    #[test]
    fn convert_timestamptz_binary_returns_error() {
        assert_binary_type_rejected(Type::TIMESTAMPTZ, &[0; 8], "TIMESTAMPTZ");
    }

    #[test]
    fn convert_uuid_binary_returns_error() {
        // Pins the silent-misdecode fix: an unmodelled binary type must be
        // refused with 0A000, never guessed at as UTF-8 text.
        assert_binary_type_rejected(Type::UUID, &[0u8; 16], "0A000");
    }

    #[test]
    fn convert_bool_binary() {
        let params = vec![Some(Bytes::from_static(&[1u8]))];
        let types = vec![Some(Type::BOOL)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(true)));
    }

    #[test]
    fn convert_int2_binary_widens_to_int64() {
        let params = vec![Some(Bytes::from((-1i16).to_be_bytes().to_vec()))];
        let types = vec![Some(Type::INT2)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int64(-1)));
    }

    #[test]
    fn convert_int4_binary() {
        let params = vec![Some(Bytes::from(42i32.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::INT4)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int64(42)));
    }

    #[test]
    fn convert_int8_binary() {
        let params = vec![Some(Bytes::from(9_999_999_999i64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::INT8)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(
            result[0],
            nodedb_sql::ParamValue::Int64(9_999_999_999)
        ));
    }

    #[test]
    fn convert_float4_binary_widens_to_float64() {
        let params = vec![Some(Bytes::from(2.5f32.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::FLOAT4)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.5).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_float8_binary() {
        let params = vec![Some(Bytes::from(2.78f64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::FLOAT8)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.78).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_int4_binary_wrong_length_returns_22p02() {
        let params = vec![Some(Bytes::from_static(&[0u8, 1, 2]))];
        let types = vec![Some(Type::INT4)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    fn assert_text_param(
        input: &'static [u8],
        ty: Type,
        expected: fn(&nodedb_sql::ParamValue) -> bool,
    ) {
        let params = vec![Some(Bytes::from_static(input))];
        let types = vec![Some(ty)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(expected(&result[0]));
    }

    #[test]
    fn convert_timestamp_text_to_typed() {
        assert_text_param(b"2024-01-01 00:00:00", Type::TIMESTAMP, |value| {
            matches!(value, nodedb_sql::ParamValue::Timestamp(_))
        });
    }

    #[test]
    fn convert_timestamptz_text_to_typed() {
        assert_text_param(b"2024-01-01 00:00:00+00", Type::TIMESTAMPTZ, |value| {
            matches!(value, nodedb_sql::ParamValue::Timestamptz(_))
        });
    }

    #[test]
    fn convert_bool_variants() {
        for (input, expected) in [("t", true), ("f", false), ("1", true), ("0", false)] {
            let params = vec![Some(Bytes::from(input))];
            let types = vec![Some(Type::BOOL)];
            let result = convert_portal_params(&params, &types, &text_format()).unwrap();
            assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(value) if value == expected));
        }
    }

    #[test]
    fn passthrough_date_text() {
        let value = pgwire_text_to_param("2026-04-19", &Type::DATE);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "2026-04-19"));
    }

    #[test]
    fn timestamp_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00", &Type::TIMESTAMP);
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamp(_)));
    }

    #[test]
    fn timestamptz_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00+00", &Type::TIMESTAMPTZ);
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamptz(_)));
    }

    #[test]
    fn passthrough_uuid_text() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let value = pgwire_text_to_param(uuid, &Type::UUID);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == uuid));
    }

    #[test]
    fn passthrough_jsonb_text() {
        let json = r#"{"a":1}"#;
        let value = pgwire_text_to_param(json, &Type::JSONB);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == json));
    }

    #[test]
    fn passthrough_bytea_hex_text() {
        let value = pgwire_text_to_param("\\xDEADBEEF", &Type::BYTEA);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "\\xDEADBEEF"));
    }

    #[test]
    fn int_parse_failure_falls_back_to_text() {
        let value = pgwire_text_to_param("abc", &Type::INT8);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "abc"));
    }

    #[test]
    fn unknown_type_routes_to_text() {
        let value = pgwire_text_to_param("42", &Type::UNKNOWN);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "42"));
    }
}
