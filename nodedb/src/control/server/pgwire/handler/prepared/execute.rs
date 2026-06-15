// SPDX-License-Identifier: BUSL-1.1

//! Execute a prepared statement from an extended query portal.
//!
//! Binds parameter values from the portal into the SQL, then executes
//! through the same `execute_sql` path as SimpleQuery — preserving
//! all DDL dispatch, transaction handling, and permission checks.

use std::fmt::Debug;

use bytes::Bytes;
use futures::sink::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::results::{FieldInfo, Response};
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use super::super::core::NodeDbPgHandler;
use super::statement::ParsedStatement;

impl NodeDbPgHandler {
    /// Execute a prepared statement from a portal.
    ///
    /// Called by the `ExtendedQueryHandler::do_query` implementation.
    /// Binds parameters at the AST level (not SQL text substitution), then
    /// plans and dispatches through the standard pipeline.
    pub(crate) async fn execute_prepared<C>(
        &self,
        client: &mut C,
        portal: &Portal<ParsedStatement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        let identity = self.resolve_identity(client, &addr)?;
        self.enforce_database_access(&identity, &addr)?;
        let stmt = &portal.statement.statement;
        let tenant_id = identity.tenant_id;

        // J.4: mirror `do_query`'s audit scope. The extended-query
        // path also triggers DDL (a prepared `CREATE COLLECTION`
        // binds parameters then dispatches), so audit context must
        // be installed here too or followers receive a plain
        // `CatalogDdl` with no SQL trail.
        let _audit_scope = crate::control::server::pgwire::session::audit_context::AuditScope::new(
            crate::control::server::pgwire::session::audit_context::AuditCtx {
                auth_user_id: identity.user_id.to_string(),
                auth_user_name: identity.username.clone(),
                sql_text: stmt.sql.clone(),
            },
        );

        // Wire-streaming COPY shapes for backup/restore. Recognised before
        // sqlparser-based execution because the shapes aren't standard COPY
        // grammar. See `control::backup::detect`.
        if let Some(intent) = crate::control::backup::detect(&stmt.sql) {
            return self.intent_to_response(&identity, addr, intent).await;
        }

        // Convert pgwire binary parameters to typed ParamValues for AST/DSL
        // binding. Done once, used by both the DSL path and the planned-SQL
        // path below.
        let params = convert_portal_params(
            &portal.parameters,
            &stmt.param_types,
            &portal.parameter_format,
        )?;

        // DSL passthroughs (SEARCH, GRAPH, MATCH, UPSERT INTO, etc.) cannot be
        // handled by the planned-SQL path because sqlparser doesn't parse the
        // DSL grammar. Before dispatching, substitute `$N` placeholders in the
        // SQL text via sqlparser's tokenizer (string/identifier/comment-aware).
        // `BoundDslSql` is a newtype — the compiler refuses to pass a raw
        // `&str` to a DSL execution path, so forgetting binding on a future
        // DSL is a compile error, not a runtime silent-drop.
        if stmt.is_dsl {
            let bound = nodedb_sql::dsl_bind::bind_dsl(&stmt.sql, &params).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".into(),
                    "42601".into(),
                    format!("DSL parameter bind: {e}"),
                )))
            })?;
            let mut results = self.execute_sql(&identity, &addr, bound.as_str()).await?;
            return Ok(results.pop().unwrap_or(Response::EmptyQuery));
        }

        // Execute through the planned SQL path with AST-level parameter binding.
        let mut results = self
            .execute_planned_sql_with_params(&identity, &stmt.sql, tenant_id, &addr, &params)
            .await?;
        let result = results.pop().unwrap_or(Response::EmptyQuery);

        // When the statement declared typed result columns via Describe, the
        // client expects DataRow messages with one field per declared column.
        //
        // The generic `payload_to_response` path produces a single-column
        // QueryResponse with the full JSON as one text field. In the extended-
        // query protocol the RowDescription was already sent by Describe, so
        // pgwire sends only the DataRow messages on Execute — the client maps
        // them against the previously-described schema. A 1-field row against
        // an N-column schema causes null values for columns 2..N.
        //
        // Fix: when result_fields is non-empty, consume the single-field stream,
        // parse each JSON object, and re-encode with one pgwire field per
        // declared column.
        //
        // Exception: DML RETURNING responses are already shaped as multi-column
        // RowsPayload by `payload_to_response(PlanKind::ReturningRows)`. Applying
        // `reproject_response` on top would re-read the first column of each row
        // as JSON (which is a plain field value, not a JSON object) and produce
        // empty rows. Detect this by checking whether the response is already a
        // multi-column QueryResponse whose column count matches the declared schema.
        if !stmt.result_fields.is_empty() && !is_already_shaped(&result) {
            reproject_response(result, &stmt.result_fields).await
        } else {
            Ok(result)
        }
    }
}

/// Return true when `response` is already a multi-column QueryResponse.
///
/// Used to skip `reproject_response` for DML RETURNING payloads that
/// `payload_to_response(PlanKind::ReturningRows)` already shaped as one
/// field per RETURNING column. Re-projecting them would treat each row's
/// first column value as a JSON object and produce empty rows.
///
/// Single-column envelope responses (produced by the regular scan/document
/// path) always have exactly one column named "document" or "result"; any
/// response with two or more columns is already correctly shaped.
fn is_already_shaped(response: &Response) -> bool {
    match response {
        Response::Query(qr) => qr.row_schema.len() >= 2,
        _ => false,
    }
}

/// Re-encode a simple-query envelope response to match the column schema
/// declared by Describe. Delegates to the shared projection module.
///
/// Prepared statements that reach this path are scalar / single-table SELECTs
/// where the lookup key matches the display name; we pass the field names as
/// the lookup keys.
async fn reproject_response(
    response: Response,
    result_fields: &[FieldInfo],
) -> PgWireResult<Response> {
    let lookup_keys: Vec<String> = result_fields.iter().map(|f| f.name().to_string()).collect();
    super::super::projection::reproject_response(response, result_fields, &lookup_keys).await
}

/// Convert pgwire portal parameters to typed `ParamValue` for AST-level binding.
///
/// Uses per-parameter format codes from the pgwire 0.38 `Format` API to determine
/// whether each parameter was sent in text or binary format.
///
/// Binary-format NUMERIC, TIMESTAMP, and TIMESTAMPTZ parameters are explicitly
/// rejected with SQLSTATE 0A000 — their binary encodings are client-library-specific
/// structs that would produce corrupt values if decoded naively. Clients must use
/// text format for these types.
fn convert_portal_params(
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
                // Reject binary format for types whose binary encoding is
                // client-library-specific and cannot be decoded portably.
                if param_format.is_binary(i) {
                    let type_name = if *pg_type == Type::NUMERIC {
                        Some("NUMERIC")
                    } else if *pg_type == Type::TIMESTAMP {
                        Some("TIMESTAMP")
                    } else if *pg_type == Type::TIMESTAMPTZ {
                        Some("TIMESTAMPTZ")
                    } else {
                        None
                    };
                    if let Some(name) = type_name {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "0A000".to_owned(),
                            format!(
                                "binary {name} parameter format is not supported for \
                                 parameter ${n}; use text format",
                                n = i + 1
                            ),
                        ))));
                    }
                }

                let text = std::str::from_utf8(bytes).map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22021".to_owned(),
                        format!("invalid UTF-8 in parameter ${}", i + 1),
                    )))
                })?;

                pgwire_text_to_param(text, pg_type)
            }
        };
        result.push(pv);
    }
    Ok(result)
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
/// (see `convert_portal_params`); they never reach this function.
///
/// # Why not error on unknown types
///
/// Postgres itself accepts text representations of every built-in type through
/// the extended-query protocol; refusing here would break drivers that
/// legitimately send dates/UUIDs/etc. as text.
fn pgwire_text_to_param(text: &str, pg_type: &Type) -> nodedb_sql::ParamValue {
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
            nodedb_sql::ParamValue::Decimal(d) => {
                assert_eq!(d.to_string(), "123.45");
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn convert_numeric_binary_returns_error() {
        // Binary format code + NUMERIC type → explicit rejection.
        let params = vec![Some(Bytes::from_static(&[0x00, 0x03, 0x00, 0x02]))];
        let types = vec![Some(Type::NUMERIC)];
        let err = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("binary NUMERIC") || msg.contains("0A000"),
            "expected binary-format error, got: {msg}"
        );
    }

    #[test]
    fn convert_timestamp_binary_returns_error() {
        let params = vec![Some(Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]))];
        let types = vec![Some(Type::TIMESTAMP)];
        let err = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("binary TIMESTAMP") || msg.contains("0A000"),
            "expected binary-format error, got: {msg}"
        );
    }

    #[test]
    fn convert_timestamptz_binary_returns_error() {
        let params = vec![Some(Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]))];
        let types = vec![Some(Type::TIMESTAMPTZ)];
        let err = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("binary TIMESTAMPTZ") || msg.contains("0A000"),
            "expected binary-format error, got: {msg}"
        );
    }

    #[test]
    fn convert_timestamp_text_to_typed() {
        let params = vec![Some(Bytes::from_static(b"2024-01-01 00:00:00"))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Timestamp(_)),
            "expected Timestamp, got {:?}",
            result[0]
        );
    }

    #[test]
    fn convert_timestamptz_text_to_typed() {
        let params = vec![Some(Bytes::from_static(b"2024-01-01 00:00:00+00"))];
        let types = vec![Some(Type::TIMESTAMPTZ)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Timestamptz(_)),
            "expected Timestamptz, got {:?}",
            result[0]
        );
    }

    #[test]
    fn convert_bool_variants() {
        for (input, expected) in [("t", true), ("f", false), ("1", true), ("0", false)] {
            let params = vec![Some(Bytes::from(input))];
            let types = vec![Some(Type::BOOL)];
            let result = convert_portal_params(&params, &types, &text_format()).unwrap();
            assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(v) if v == expected));
        }
    }

    /// DATE params arrive as text per pgwire spec. The bind layer
    /// preserves the text so the engine's literal-coercion path can
    /// convert it to the target column type.
    #[test]
    fn passthrough_date_text() {
        let out = pgwire_text_to_param("2026-04-19", &Type::DATE);
        assert!(matches!(&out, nodedb_sql::ParamValue::Text(s) if s == "2026-04-19"));
    }

    #[test]
    fn timestamp_text_parses_to_typed() {
        let out = pgwire_text_to_param("2026-04-19 12:00:00", &Type::TIMESTAMP);
        assert!(
            matches!(out, nodedb_sql::ParamValue::Timestamp(_)),
            "expected Timestamp variant, got {out:?}"
        );
    }

    #[test]
    fn timestamptz_text_parses_to_typed() {
        let out = pgwire_text_to_param("2026-04-19 12:00:00+00", &Type::TIMESTAMPTZ);
        assert!(
            matches!(out, nodedb_sql::ParamValue::Timestamptz(_)),
            "expected Timestamptz variant, got {out:?}"
        );
    }

    #[test]
    fn passthrough_uuid_text() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let out = pgwire_text_to_param(uuid, &Type::UUID);
        assert!(matches!(&out, nodedb_sql::ParamValue::Text(s) if s == uuid));
    }

    #[test]
    fn passthrough_jsonb_text() {
        let json = r#"{"a":1}"#;
        let out = pgwire_text_to_param(json, &Type::JSONB);
        assert!(matches!(&out, nodedb_sql::ParamValue::Text(s) if s == json));
    }

    /// BYTEA text form per pgwire is `\x<hex>` — passed through as-is
    /// so the engine's BYTEA parser (which already handles both escape
    /// and hex forms) converts it.
    #[test]
    fn passthrough_bytea_hex_text() {
        let out = pgwire_text_to_param("\\xDEADBEEF", &Type::BYTEA);
        assert!(matches!(&out, nodedb_sql::ParamValue::Text(s) if s == "\\xDEADBEEF"));
    }

    #[test]
    fn int_parse_failure_falls_back_to_text() {
        // `abc` isn't a valid INT8 text representation. The function
        // preserves the text rather than dropping the binding.
        let out = pgwire_text_to_param("abc", &Type::INT8);
        assert!(matches!(&out, nodedb_sql::ParamValue::Text(s) if s == "abc"));
    }

    #[test]
    fn unknown_type_routes_to_text() {
        // `Type::UNKNOWN` — the postgres-js fetch_types:false path.
        // Text is the correct output: the planner's use-site coercion
        // (`coerce::as_usize_literal`, etc.) handles numeric contexts.
        let out = pgwire_text_to_param("42", &Type::UNKNOWN);
        assert!(matches!(&out, nodedb_sql::ParamValue::Text(s) if s == "42"));
    }

    #[test]
    fn decode_first_field_text_normal() {
        use crate::control::server::pgwire::handler::projection::decode_first_field_text;
        // Wire format: 4-byte length (big-endian) + UTF-8 bytes.
        let text = b"hello";
        let mut data = bytes::BytesMut::new();
        data.extend_from_slice(&(text.len() as i32).to_be_bytes());
        data.extend_from_slice(text);
        assert_eq!(decode_first_field_text(&data), Some("hello"));
    }

    #[test]
    fn decode_first_field_text_null() {
        use crate::control::server::pgwire::handler::projection::decode_first_field_text;
        // -1 length means SQL NULL.
        let mut data = bytes::BytesMut::new();
        data.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(decode_first_field_text(&data), None);
    }

    #[test]
    fn decode_first_field_text_empty() {
        use crate::control::server::pgwire::handler::projection::decode_first_field_text;
        assert_eq!(decode_first_field_text(&bytes::BytesMut::new()), None);
    }
}
