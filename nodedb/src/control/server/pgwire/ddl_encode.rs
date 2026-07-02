// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`DdlResult`] / [`DdlError`] into pgwire
//! `Response` values.
//!
//! This is the pgwire entrypoint's consumer of the shared, protocol-neutral
//! DDL dispatch result — the mirror of the native and http encoders. It
//! reproduces the exact wire shape (RowDescription type OIDs, DataRow text
//! bytes, CommandComplete tag) the pgwire DDL router produced directly,
//! because the neutral result captured each column's original OID (as a
//! [`DdlColType`]) and each cell's already-text-rendered value. Values are
//! re-emitted as captured text — never re-typed or re-parsed — so the field
//! bytes are byte-identical.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use serde_json::Value as JsonValue;

use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};

use super::types::{
    bool_field, bytea_field, float4_array_field, float4_field, float8_array_field, float8_field,
    int2_field, int4_field, int8_field, json_field, jsonb_field, sqlstate_error, text_field,
    timestamp_field, timestamptz_field, varchar_field,
};

/// Encode a protocol-neutral DDL dispatch result into pgwire responses.
///
/// An `Err(DdlError)` maps to a pgwire `UserError` carrying the SQLSTATE +
/// message; each `DdlResult` maps to exactly one `Response`.
pub fn ddl_results_to_pgwire(
    result: Result<Vec<DdlResult>, DdlError>,
) -> PgWireResult<Vec<Response>> {
    let results = match result {
        Ok(results) => results,
        Err(DdlError { sqlstate, message }) => return Err(sqlstate_error(&sqlstate, &message)),
    };

    let mut responses = Vec::with_capacity(results.len());
    for ddl in results {
        responses.push(ddl_result_to_response(ddl)?);
    }
    Ok(responses)
}

/// Map a single [`DdlResult`] to a pgwire [`Response`].
fn ddl_result_to_response(ddl: DdlResult) -> PgWireResult<Response> {
    match ddl {
        DdlResult::Status {
            command,
            rows_affected,
        } => {
            let tag = match rows_affected {
                Some(n) => Tag::new(&command).with_rows(n as usize),
                None => Tag::new(&command),
            };
            Ok(Response::Execution(tag))
        }
        DdlResult::Empty => Ok(Response::EmptyQuery),
        DdlResult::Rows(shaped) => rows_to_response(shaped),
    }
}

/// Build a `Response::Query` from a protocol-neutral shaped row set.
///
/// The `notice` field is intentionally ignored: the pgwire DDL router never
/// attached a NOTICE to a `Response::Query` (notices are a separate protocol
/// message), so honouring it here would diverge from the captured wire shape.
fn rows_to_response(shaped: ShapedRows) -> PgWireResult<Response> {
    let ShapedRows {
        columns,
        column_types,
        rows,
        ..
    } = shaped;

    // Build the RowDescription. Each column's OID is reproduced from its
    // captured `DdlColType`; a missing/short `column_types` defaults to text.
    let fields: Vec<FieldInfo> = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let ct = column_types.get(i).copied().unwrap_or(DdlColType::Text);
            col_type_to_field(name, ct)
        })
        .collect();
    let schema = Arc::new(fields);

    let mut encoded_rows: Vec<PgWireResult<pgwire::messages::data::DataRow>> =
        Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for name in &columns {
            match row.get(name) {
                // Captured text: re-emit verbatim so the DataRow bytes match.
                Some(JsonValue::String(s)) => encoder.encode_field(&s)?,
                // Explicit NULL (or absent key) → -1 length field.
                Some(JsonValue::Null) | None => encoder.encode_field(&None::<&str>)?,
                // Defensive: the transitional wrapper emits only String/Null,
                // but any other scalar is rendered to its text form.
                Some(other) => encoder.encode_field(&other.to_string())?,
            }
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(encoded_rows),
    )))
}

/// Map a protocol-neutral [`DdlColType`] to the pgwire `FieldInfo` builder
/// that produces the matching type OID (all with `FieldFormat::Text`). This
/// is the inverse of the OID→`DdlColType` mapping the neutral dispatch used
/// when capturing the schema, so the RowDescription round-trips losslessly.
fn col_type_to_field(name: &str, ct: DdlColType) -> FieldInfo {
    match ct {
        DdlColType::Text => text_field(name),
        DdlColType::Int8 => int8_field(name),
        DdlColType::Int4 => int4_field(name),
        DdlColType::Int2 => int2_field(name),
        DdlColType::Float8 => float8_field(name),
        DdlColType::Float4 => float4_field(name),
        DdlColType::Bool => bool_field(name),
        DdlColType::Bytea => bytea_field(name),
        DdlColType::Json => json_field(name),
        DdlColType::Jsonb => jsonb_field(name),
        DdlColType::Timestamp => timestamp_field(name),
        DdlColType::Timestamptz => timestamptz_field(name),
        DdlColType::Varchar => varchar_field(name),
        DdlColType::Float4Array => float4_array_field(name),
        DdlColType::Float8Array => float8_array_field(name),
    }
}
