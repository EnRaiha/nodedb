// SPDX-License-Identifier: BUSL-1.1

//! Column projection for pgwire SELECT responses.
//!
//! Both the simple-query path and the extended-query path share this logic:
//! the Data Plane returns a JSON payload (an array of row objects, or a
//! single row object) wrapped in a single-column envelope by
//! `payload_to_response`. Clients expect one pgwire field per projected
//! column, not a JSON blob in one field.
//!
//! Shaping itself (scan-envelope unwrap, SELECT-list column selection,
//! id-first column-union derivation) lives in the canonical, protocol-neutral
//! `response_shape::compose::shape_decoded_rows`. This module holds only the
//! pgwire-side adapter: decode the envelope text, hand the parsed JSON to the
//! neutral shaping core, and encode the resulting `ShapedRows` back into
//! pgwire `DataRow`s.
//!
//! The entry point is `reproject_if_select`: parse the SQL's SELECT list,
//! determine the projected column names, and re-encode the response rows
//! with one pgwire field per column.

use std::sync::Arc;

use futures::StreamExt;
use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::super::types::text_field;
use super::shape_encode::{self, encode_shaped_row};
use crate::control::server::response_shape::compose::shape_decoded_rows;

pub(super) use crate::control::server::response_shape::project::{
    ProjectionItem, lookup_keys_for_projection, needs_projection, parse_select_projection,
};

/// Build `FieldInfo`s from a projection list, all as TEXT.
///
/// The `FieldInfo` name is the `display_name` (bare column name for the
/// client). The `lookup_key` is carried separately and used in
/// `reproject_response` to locate the value in the flat row object.
pub(super) fn fields_for_projection(items: &[ProjectionItem]) -> Vec<FieldInfo> {
    items
        .iter()
        .filter_map(|item| match item {
            ProjectionItem::Named { display_name, .. } => Some(FieldInfo::new(
                display_name.clone(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )),
            ProjectionItem::Star => None,
        })
        .collect()
}

/// Re-encode a single-column envelope response into one pgwire field per
/// declared column.
///
/// The envelope produced by `payload_to_response` has one text field per row
/// containing the row's JSON. This function:
/// 1. Streams envelope rows lazily (no collect) and decodes the JSON text.
/// 2. Flattens each JSON value into flat row objects (unwrapping the
///    `{id, data: {...}}` scan wrapper when present).
/// 3. Re-encodes each flat row object as one pgwire field per `result_fields`
///    column; missing columns become SQL NULL.
///
/// `lookup_keys` must be the same length as `result_fields`. For each
/// position `i`, `lookup_keys[i]` is the key used to look up the value in
/// the flat row object, while `result_fields[i].name()` is the column label
/// sent to the client. For plain (unqualified) columns the two are identical;
/// for qualified references like `table.column` the lookup key is the full
/// dot-joined form (`"table.column"`) matching the join-executor's prefixed
/// key, and the display name is just the bare column name.
///
/// Non-query responses (execution tags, empty query) pass through unchanged.
///
/// The returned `QueryResponse` streams rows lazily — upstream chunks from
/// `streaming_multirow_response` flow to the client without first being
/// collected into a `Vec`.
pub(super) fn reproject_response(
    response: Response,
    result_fields: &[FieldInfo],
    lookup_keys: &[String],
) -> PgWireResult<Response> {
    let qr = match response {
        Response::Query(qr) => qr,
        other => return Ok(other),
    };

    let schema = Arc::new(result_fields.to_vec());

    // Build the neutral `ProjectionItem` list from the already-resolved
    // lookup keys / display names, and clone the small, bounded per-query
    // metadata so we can move it into the stream closure without borrowing
    // across the async boundary.
    let items: Vec<ProjectionItem> = lookup_keys
        .iter()
        .zip(result_fields.iter())
        .map(|(lookup_key, field)| ProjectionItem::Named {
            lookup_key: lookup_key.clone(),
            display_name: field.name().to_owned(),
        })
        .collect();
    let display_columns: Vec<String> = result_fields.iter().map(|f| f.name().to_owned()).collect();
    let stream_schema = schema.clone();

    // Move the upstream row stream into the closure. `data_rows` is
    // `Pin<Box<dyn Stream + Send>>` which is `Unpin` (Box makes it so),
    // so we can move it by value and call `StreamExt::next` via a plain
    // `&mut` reference inside the generator.
    let mut upstream = qr.data_rows;

    let row_stream = async_stream::try_stream! {
        while let Some(row_result) = futures::StreamExt::next(&mut upstream).await {
            let row = row_result?;
            let Some(text) = decode_first_field_text(&row.data) else {
                continue;
            };
            let value = decode_envelope_value(text)?;

            let shaped = shape_decoded_rows(&value, Some(&items));
            for row in shaped.rows {
                yield encode_shaped_row(&stream_schema, &display_columns, &row)?;
            }
        }
    };

    Ok(Response::Query(QueryResponse::new(schema, row_stream)))
}

/// Parse a single envelope row's text payload into a `serde_json::Value`,
/// wrapping malformed JSON in a client-facing `PgWireError`.
///
/// Shared by `reproject_response`'s streaming decode and
/// `collect_decoded_values`'s eager decode — same envelope format, same
/// error shape either way.
fn decode_envelope_value(text: &str) -> PgWireResult<serde_json::Value> {
    sonic_rs::from_str::<serde_json::Value>(text).map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("malformed Data-Plane response envelope: {e}"),
        )))
    })
}

/// Consume an envelope `QueryResponse` and return each row's decoded JSON
/// value (before scan-envelope unwrap or projection — that is applied
/// uniformly afterward by `shape_decoded_rows`).
async fn collect_decoded_values(mut qr: QueryResponse) -> PgWireResult<Vec<serde_json::Value>> {
    let mut values = Vec::new();
    while let Some(row_result) = qr.data_rows.next().await {
        let row = row_result?;
        let Some(text) = decode_first_field_text(&row.data) else {
            continue;
        };
        values.push(decode_envelope_value(text)?);
    }
    Ok(values)
}

/// Re-encode a `SELECT *` envelope response as one pgwire field per key found
/// in the row objects.
///
/// The column order is derived from the union of keys across all rows, with
/// `id` placed first when present. Non-query responses pass through unchanged.
pub(super) async fn reproject_star_response(response: Response) -> PgWireResult<Response> {
    let qr = match response {
        Response::Query(qr) => qr,
        other => return Ok(other),
    };

    let values = collect_decoded_values(qr).await?;
    if values.is_empty() {
        let schema = Arc::new(vec![text_field("result")]);
        return Ok(Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(Vec::<PgWireResult<_>>::new()),
        )));
    }

    let shaped = shape_decoded_rows(&serde_json::Value::Array(values), None);
    let (response, _notice) = shape_encode::shaped_query_response(shaped);
    Ok(response)
}

/// Decode the text bytes of the first field from a pgwire `DataRow` wire buffer.
///
/// Wire format: 4-byte big-endian length followed by bytes.
/// Returns `None` for NULL fields or invalid encodings.
pub(super) fn decode_first_field_text(data: &bytes::BytesMut) -> Option<&str> {
    if data.len() < 4 {
        return None;
    }
    let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if len < 0 {
        return None;
    }
    let len = len as usize;
    if data.len() < 4 + len {
        return None;
    }
    std::str::from_utf8(&data[4..4 + len]).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use pgwire::api::Type;
    use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
    use pgwire::error::PgWireResult;

    use super::super::super::types::text_field;
    use super::{decode_first_field_text, reproject_response};

    /// Build a single-column `DataRow` whose text field contains `json_str`.
    /// The wire format is a 4-byte big-endian length prefix followed by the
    /// UTF-8 bytes, which is what `decode_first_field_text` expects.
    fn envelope_row(json_str: &str) -> pgwire::messages::data::DataRow {
        let schema = Arc::new(vec![text_field("result")]);
        let mut enc = DataRowEncoder::new(schema);
        enc.encode_field(&json_str).unwrap();
        enc.take_row()
    }

    /// Drain a `QueryResponse` stream into a `Vec` of `DataRow`s.
    async fn drain(mut qr: QueryResponse) -> Vec<pgwire::messages::data::DataRow> {
        let mut rows = Vec::new();
        while let Some(r) = qr.data_rows.next().await {
            rows.push(r.unwrap());
        }
        rows
    }

    /// Helper: read the text value of field `idx` from a `DataRow`'s raw wire buffer.
    ///
    /// Each field in the buffer is: 4-byte big-endian length + bytes.
    fn field_text(row: &pgwire::messages::data::DataRow, idx: usize) -> Option<String> {
        let data = &row.data;
        let mut offset = 0usize;
        for field_i in 0..=idx {
            if offset + 4 > data.len() {
                return None;
            }
            let len = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;
            if len < 0 {
                // NULL field
                if field_i == idx {
                    return None;
                }
                continue;
            }
            let len = len as usize;
            if offset + len > data.len() {
                return None;
            }
            if field_i == idx {
                return Some(
                    std::str::from_utf8(&data[offset..offset + len])
                        .unwrap()
                        .to_owned(),
                );
            }
            offset += len;
        }
        None
    }

    /// `reproject_response` maps named columns lazily from single-row JSON envelopes.
    #[tokio::test]
    async fn reproject_named_columns_streams_lazily() {
        // Two envelope DataRows, each carrying a single JSON object.
        let row1_json = r#"{"a":1,"b":"hello"}"#;
        let row2_json = r#"{"a":2,"b":"world"}"#;

        let schema = Arc::new(vec![text_field("result")]);
        let rows: Vec<PgWireResult<_>> =
            vec![Ok(envelope_row(row1_json)), Ok(envelope_row(row2_json))];
        let upstream = QueryResponse::new(schema, futures::stream::iter(rows));
        let response = Response::Query(upstream);

        let result_fields = vec![
            FieldInfo::new("a".to_owned(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("b".to_owned(), None, None, Type::TEXT, FieldFormat::Text),
        ];
        let lookup_keys = vec!["a".to_owned(), "b".to_owned()];

        let projected = reproject_response(response, &result_fields, &lookup_keys).unwrap();
        let Response::Query(qr) = projected else {
            panic!("expected Query response");
        };

        let out_rows = drain(qr).await;
        assert_eq!(out_rows.len(), 2, "expected 2 output rows");

        assert_eq!(field_text(&out_rows[0], 0).as_deref(), Some("1"));
        assert_eq!(field_text(&out_rows[0], 1).as_deref(), Some("hello"));
        assert_eq!(field_text(&out_rows[1], 0).as_deref(), Some("2"));
        assert_eq!(field_text(&out_rows[1], 1).as_deref(), Some("world"));
    }

    /// An envelope DataRow whose JSON value is an ARRAY of row objects must be
    /// flat-mapped: one array element → multiple output DataRows.
    #[tokio::test]
    async fn reproject_array_envelope_flatmaps_to_multiple_output_rows() {
        // One envelope DataRow carrying a JSON array of 3 row objects.
        let array_json = r#"[{"a":10,"b":"x"},{"a":20,"b":"y"},{"a":30,"b":"z"}]"#;

        let schema = Arc::new(vec![text_field("result")]);
        let rows: Vec<PgWireResult<_>> = vec![Ok(envelope_row(array_json))];
        let upstream = QueryResponse::new(schema, futures::stream::iter(rows));
        let response = Response::Query(upstream);

        let result_fields = vec![
            FieldInfo::new("a".to_owned(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("b".to_owned(), None, None, Type::TEXT, FieldFormat::Text),
        ];
        let lookup_keys = vec!["a".to_owned(), "b".to_owned()];

        let projected = reproject_response(response, &result_fields, &lookup_keys).unwrap();
        let Response::Query(qr) = projected else {
            panic!("expected Query response");
        };

        let out_rows = drain(qr).await;
        assert_eq!(out_rows.len(), 3, "array envelope must flat-map to 3 rows");
        assert_eq!(field_text(&out_rows[0], 0).as_deref(), Some("10"));
        assert_eq!(field_text(&out_rows[0], 1).as_deref(), Some("x"));
        assert_eq!(field_text(&out_rows[1], 0).as_deref(), Some("20"));
        assert_eq!(field_text(&out_rows[2], 0).as_deref(), Some("30"));
        assert_eq!(field_text(&out_rows[2], 1).as_deref(), Some("z"));
    }

    /// A mix of single-object and array envelopes in the same upstream stream
    /// must be handled correctly end-to-end.
    #[tokio::test]
    async fn reproject_mixed_single_and_array_envelopes() {
        let single_json = r#"{"a":1,"b":"single"}"#;
        let array_json = r#"[{"a":2,"b":"arr1"},{"a":3,"b":"arr2"}]"#;

        let schema = Arc::new(vec![text_field("result")]);
        let rows: Vec<PgWireResult<_>> =
            vec![Ok(envelope_row(single_json)), Ok(envelope_row(array_json))];
        let upstream = QueryResponse::new(schema, futures::stream::iter(rows));
        let response = Response::Query(upstream);

        let result_fields = vec![
            FieldInfo::new("a".to_owned(), None, None, Type::TEXT, FieldFormat::Text),
            FieldInfo::new("b".to_owned(), None, None, Type::TEXT, FieldFormat::Text),
        ];
        let lookup_keys = vec!["a".to_owned(), "b".to_owned()];

        let projected = reproject_response(response, &result_fields, &lookup_keys).unwrap();
        let Response::Query(qr) = projected else {
            panic!("expected Query response");
        };

        let out_rows = drain(qr).await;
        assert_eq!(out_rows.len(), 3, "1 single + 2 array = 3 output rows");
        assert_eq!(field_text(&out_rows[0], 1).as_deref(), Some("single"));
        assert_eq!(field_text(&out_rows[1], 1).as_deref(), Some("arr1"));
        assert_eq!(field_text(&out_rows[2], 1).as_deref(), Some("arr2"));
    }

    /// `decode_first_field_text` round-trips what `DataRowEncoder` writes.
    #[test]
    fn decode_first_field_text_reads_encoder_output() {
        let row = envelope_row(r#"{"k":1}"#);
        let text = decode_first_field_text(&row.data).unwrap();
        assert_eq!(text, r#"{"k":1}"#);
    }
}
