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
/// Binary-format `NUMERIC` is decoded via `rust_decimal`'s `FromSql` impl
/// (the variable-length base-10000 digit encoding), producing the same
/// `ParamValue::Decimal` as the text path. Overflow, NaN, and +/-Infinity
/// payloads come back as typed errors mapped to SQLSTATE 22P02.
///
/// Binary-format `TIMESTAMP` and `TIMESTAMPTZ` are decoded directly: an
/// 8-byte big-endian microsecond offset from 2000-01-01T00:00:00Z, converted
/// to `NdbDateTime` (which stores microseconds since the Unix epoch).
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
                    pgwire_text_to_param(text, pg_type, i)?
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
            pgwire_text_to_param(text, pg_type, index)
        }
        Type::NUMERIC => decode_binary::<rust_decimal::Decimal>(bytes, pg_type, index)
            .map(nodedb_sql::ParamValue::Decimal),
        Type::TIMESTAMP => {
            decode_binary_timestamp(bytes, index).map(nodedb_sql::ParamValue::Timestamp)
        }
        Type::TIMESTAMPTZ => {
            decode_binary_timestamp(bytes, index).map(nodedb_sql::ParamValue::Timestamptz)
        }
        // Every other binary type: refuse rather than silently mis-decode
        // as UTF-8 text (its bytes may happen to be valid UTF-8 without
        // being a valid text representation of the type).
        _ => Err(binary_unsupported_error(pg_type.name(), index)),
    }
}

/// Build a typed pgwire `ErrorInfo::new("ERROR", sqlstate, message)` error.
/// Shared boilerplate for every SQLSTATE-tagged error this module raises;
/// callers own the SQLSTATE and message, which intentionally differ per
/// call site.
fn pg_error(sqlstate: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        sqlstate.to_owned(),
        message,
    )))
}

/// Decode a parameter's raw bytes as UTF-8 text, mapping a decode failure to
/// a typed pgwire error (SQLSTATE 22021 - character_not_in_repertoire).
/// Shared by the text-format path and the binary TEXT/VARCHAR/BPCHAR/UNKNOWN
/// arm, whose wire bytes are already the text representation.
fn decode_utf8_param(bytes: &[u8], index: usize) -> PgWireResult<&str> {
    std::str::from_utf8(bytes).map_err(|_| {
        pg_error(
            "22021",
            format!("invalid UTF-8 in parameter ${}", index + 1),
        )
    })
}

/// Build a typed pgwire error (SQLSTATE 22P02 - invalid_text_representation)
/// for a text-format NUMERIC parameter that cannot be parsed as an exact
/// `rust_decimal::Decimal` (parse failure or magnitude beyond the 96-bit
/// mantissa). Mirrors `decode_binary`'s 22P02 wording but describes a text,
/// not binary, format failure — the two paths must not share prose.
fn numeric_text_invalid_error(index: usize) -> PgWireError {
    pg_error(
        "22P02",
        format!(
            "invalid NUMERIC text representation for parameter ${}",
            index + 1
        ),
    )
}

fn binary_unsupported_error(type_name: &str, index: usize) -> PgWireError {
    pg_error(
        "0A000",
        format!(
            "binary {type_name} parameter format is not supported for parameter ${n}; \
             use text format",
            n = index + 1
        ),
    )
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
        pg_error(
            "22P02",
            format!(
                "invalid binary representation for parameter ${}: {e}",
                index + 1
            ),
        )
    })
}

/// Microseconds between the Unix epoch and the PostgreSQL binary timestamp
/// epoch (2000-01-01T00:00:00Z): 10_957 days x 86_400 s x 1e6. PostgreSQL
/// transmits binary TIMESTAMP/TIMESTAMPTZ as microseconds from that epoch.
const POSTGRES_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Decode a binary TIMESTAMP/TIMESTAMPTZ payload: an 8-byte big-endian
/// microsecond offset from the PostgreSQL epoch (2000-01-01T00:00:00Z),
/// converted to `NdbDateTime` (microseconds since the Unix epoch).
///
/// Rejects payloads that are not exactly 8 bytes (SQLSTATE 22P02).
///
/// Also rejects PostgreSQL's +/-infinity sentinels, which are transmitted
/// as the raw `i64::MAX` / `i64::MIN` values. `i64::MIN` rebases to a valid
/// (if absurdly distant) Unix-epoch offset under plain addition — it does
/// not overflow `checked_add` — so it is checked explicitly rather than
/// relying on overflow detection alone. `i64::MAX` does overflow once
/// rebased and is caught by `checked_add`. `NdbDateTime` has no infinity
/// concept, so both sentinels must error rather than decode to a finite
/// (and wrong) timestamp.
fn decode_binary_timestamp(
    bytes: &[u8],
    index: usize,
) -> PgWireResult<nodedb_types::datetime::NdbDateTime> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
        pg_error(
            "22P02",
            format!(
                "invalid binary representation for parameter ${}: expected 8 bytes, got {}",
                index + 1,
                bytes.len()
            ),
        )
    })?;
    let pg_micros = i64::from_be_bytes(raw);
    let out_of_range = || {
        pg_error(
            "22P02",
            format!(
                "invalid binary representation for parameter ${}: timestamp out of range",
                index + 1
            ),
        )
    };
    if pg_micros == i64::MAX || pg_micros == i64::MIN {
        return Err(out_of_range());
    }
    let unix_micros = pg_micros
        .checked_add(POSTGRES_EPOCH_OFFSET_MICROS)
        .ok_or_else(out_of_range)?;
    Ok(nodedb_types::datetime::NdbDateTime::from_micros(
        unix_micros,
    ))
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
/// # NUMERIC
///
/// Text-format NUMERIC is parsed as an exact `rust_decimal::Decimal`, never a
/// lossy `f64`. Unlike every other native-type arm, a parse failure here does
/// NOT fall through to `ParamValue::Text` — `SqlValue::Decimal` is itself
/// `rust_decimal::Decimal`, so a value this parser cannot represent (parse
/// failure, or magnitude beyond the 96-bit mantissa) cannot be represented
/// anywhere downstream either, and silently passing it through as text would
/// be exactly the silent precision loss this path exists to avoid. It errors
/// with SQLSTATE 22P02, matching the binary NUMERIC path's error code (see
/// `convert_binary_param`).
///
/// # Fallback policy (catch-all arm)
///
/// Types the bind layer does not decode natively — `DATE`, `TIME`, `BYTEA`,
/// `UUID`, `JSON`, `JSONB`, `INTERVAL`, array types, and user-defined types —
/// fall through to `ParamValue::Text(text)`. The pgwire text representation of
/// these types is well-defined and the AST bind emits it as a
/// `SingleQuotedString`. Downstream, the planner/engine type-coerces the text
/// via the same path used for literal strings in simple-query SQL. NUMERIC is
/// the one exception to this policy: see above.
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
pub(super) fn pgwire_text_to_param(
    text: &str,
    pg_type: &Type,
    index: usize,
) -> PgWireResult<nodedb_sql::ParamValue> {
    Ok(match *pg_type {
        Type::BOOL => {
            let lower = text.to_lowercase();
            if lower == "t" || lower == "true" || lower == "1" {
                nodedb_sql::ParamValue::Bool(true)
            } else if lower == "f" || lower == "false" || lower == "0" {
                nodedb_sql::ParamValue::Bool(false)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        Type::INT2 | Type::INT4 | Type::INT8 => {
            if let Ok(n) = text.parse::<i64>() {
                nodedb_sql::ParamValue::Int64(n)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        Type::FLOAT4 | Type::FLOAT8 => {
            if let Ok(f) = text.parse::<f64>() {
                nodedb_sql::ParamValue::Float64(f)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        Type::NUMERIC => {
            // Parse NUMERIC as exact Decimal, not lossy f64. Unlike every
            // other arm here, a parse failure errors rather than falling
            // back to ParamValue::Text — Decimal is the terminal
            // representation, so anything it can't parse can't be
            // represented downstream either.
            let d = rust_decimal::Decimal::from_str_exact(text)
                .map_err(|_| numeric_text_invalid_error(index))?;
            nodedb_sql::ParamValue::Decimal(d)
        }
        Type::TIMESTAMP => {
            // Parse ISO 8601 / PostgreSQL timestamp text to a typed NaiveDateTime.
            if let Some(dt) = nodedb_types::datetime::NdbDateTime::parse(text) {
                nodedb_sql::ParamValue::Timestamp(dt)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        Type::TIMESTAMPTZ => {
            // Parse ISO 8601 / PostgreSQL timestamptz text to a typed DateTime (UTC).
            if let Some(dt) = nodedb_types::datetime::NdbDateTime::parse(text) {
                nodedb_sql::ParamValue::Timestamptz(dt)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        // Text-passthrough types: wire-format text is already the
        // canonical representation. Engine performs type coercion.
        _ => nodedb_sql::ParamValue::Text(text.to_string()),
    })
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
    fn convert_uuid_binary_returns_error() {
        // Pins the silent-misdecode fix: an unmodelled binary type must be
        // refused with 0A000, never guessed at as UTF-8 text.
        assert_binary_type_rejected(Type::UUID, &[0u8; 16], "0A000");
    }

    /// Build a PostgreSQL binary NUMERIC payload: ndigits/weight/sign/dscale
    /// header followed by base-10000 digit groups, all big-endian i16.
    fn numeric_binary(digits: &[i16], weight: i16, sign: u16, dscale: u16) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + digits.len() * 2);
        bytes.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        bytes.extend_from_slice(&weight.to_be_bytes());
        bytes.extend_from_slice(&sign.to_be_bytes());
        bytes.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            bytes.extend_from_slice(&d.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn convert_numeric_binary_decodes_to_decimal() {
        // 123.45: digit groups [0123, 4500], weight 0, dscale 2, positive.
        let bytes = numeric_binary(&[123, 4500], 0, 0x0000, 2);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn convert_numeric_binary_nan_returns_22p02() {
        let bytes = numeric_binary(&[], 0, 0xC000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_numeric_binary_positive_infinity_returns_22p02() {
        let bytes = numeric_binary(&[], 0, 0xD000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_numeric_binary_negative_infinity_returns_22p02() {
        let bytes = numeric_binary(&[], 0, 0xF000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_numeric_binary_overflow_returns_22p02_not_panic() {
        // 10 digit groups of 9999 at weight 9 vastly exceeds Decimal's
        // 96-bit mantissa (~29 decimal digits).
        let digits = [9999i16; 10];
        let bytes = numeric_binary(&digits, 9, 0x0000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_timestamp_binary_decodes_epoch() {
        // Raw 0 is the PostgreSQL binary epoch (2000-01-01T00:00:00Z), which
        // is 946_684_800_000_000 microseconds after the Unix epoch.
        let params = vec![Some(Bytes::from(0i64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Timestamp(dt) => {
                assert_eq!(dt.micros, 946_684_800_000_000);
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_timestamp_binary_decodes_unix_epoch() {
        // Pre-2000 value: -946_684_800_000_000 microseconds from the
        // PostgreSQL epoch lands exactly on the Unix epoch (micros = 0).
        let params = vec![Some(Bytes::from(
            (-946_684_800_000_000i64).to_be_bytes().to_vec(),
        ))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Timestamp(dt) => assert_eq!(dt.micros, 0),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_timestamp_binary_decodes_pre_1970_without_panic() {
        // Well before the Unix epoch: must decode to negative micros, not
        // panic or clamp.
        let params = vec![Some(Bytes::from(
            (-2_000_000_000_000_000i64).to_be_bytes().to_vec(),
        ))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Timestamp(dt) => assert!(dt.micros < 0),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_timestamptz_binary_produces_timestamptz_variant() {
        // Pins that TIMESTAMPTZ is not conflated with TIMESTAMP.
        let params = vec![Some(Bytes::from(0i64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMPTZ)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Timestamptz(_)));
    }

    #[test]
    fn convert_timestamp_binary_wrong_length_returns_22p02() {
        for bytes in [vec![0u8; 4], vec![0u8; 9]] {
            let params = vec![Some(Bytes::from(bytes))];
            let types = vec![Some(Type::TIMESTAMP)];
            let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
            assert!(error.to_string().contains("22P02"));
        }
    }

    #[test]
    fn convert_timestamp_binary_i64_max_returns_22p02() {
        // PostgreSQL's +infinity sentinel: rebasing to the Unix epoch would
        // overflow i64, so this must error rather than wrap or panic.
        let params = vec![Some(Bytes::from(i64::MAX.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMP)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_timestamp_binary_i64_min_returns_22p02() {
        // PostgreSQL's -infinity sentinel: same overflow-must-error rule.
        let params = vec![Some(Bytes::from(i64::MIN.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMP)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
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
        let value = pgwire_text_to_param("2026-04-19", &Type::DATE, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "2026-04-19"));
    }

    #[test]
    fn timestamp_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00", &Type::TIMESTAMP, 0).unwrap();
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamp(_)));
    }

    #[test]
    fn timestamptz_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00+00", &Type::TIMESTAMPTZ, 0).unwrap();
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamptz(_)));
    }

    #[test]
    fn passthrough_uuid_text() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let value = pgwire_text_to_param(uuid, &Type::UUID, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == uuid));
    }

    #[test]
    fn passthrough_jsonb_text() {
        let json = r#"{"a":1}"#;
        let value = pgwire_text_to_param(json, &Type::JSONB, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == json));
    }

    #[test]
    fn passthrough_bytea_hex_text() {
        let value = pgwire_text_to_param("\\xDEADBEEF", &Type::BYTEA, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "\\xDEADBEEF"));
    }

    #[test]
    fn int_parse_failure_falls_back_to_text() {
        let value = pgwire_text_to_param("abc", &Type::INT8, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "abc"));
    }

    #[test]
    fn unknown_type_routes_to_text() {
        let value = pgwire_text_to_param("42", &Type::UNKNOWN, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "42"));
    }

    #[test]
    fn numeric_text_parses_to_decimal() {
        let value = pgwire_text_to_param("123.45", &Type::NUMERIC, 0).unwrap();
        match value {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn numeric_text_non_numeric_returns_22p02_not_text_fallback() {
        // Pins the fix: NUMERIC no longer falls back to ParamValue::Text on
        // parse failure — that would silently and permanently lose the
        // value, since SqlValue::Decimal is the only downstream
        // representation for a NUMERIC parameter.
        let error = pgwire_text_to_param("abc", &Type::NUMERIC, 0).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn numeric_text_out_of_range_returns_22p02() {
        // A magnitude beyond Decimal's 96-bit mantissa (~29 decimal digits)
        // cannot be parsed exactly and must error, not fall back to text.
        let huge = "9".repeat(50);
        let error = pgwire_text_to_param(&huge, &Type::NUMERIC, 0).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn numeric_text_and_binary_agree_on_out_of_range_rejection() {
        // Pins that the text and binary NUMERIC paths agree: the same
        // unrepresentable value is rejected by both with 22P02, not silently
        // accepted as text on one side and rejected on the other.
        let huge = "9".repeat(50);
        let text_error = pgwire_text_to_param(&huge, &Type::NUMERIC, 0).unwrap_err();
        assert!(text_error.to_string().contains("22P02"));

        // Binary side: 10 digit groups of 9999 at weight 9 vastly exceeds
        // Decimal's mantissa, mirroring convert_numeric_binary_overflow_returns_22p02_not_panic.
        let digits = [9999i16; 10];
        let bytes = numeric_binary(&digits, 9, 0x0000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let binary_error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(binary_error.to_string().contains("22P02"));
    }
}
