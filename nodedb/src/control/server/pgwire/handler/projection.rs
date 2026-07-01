// SPDX-License-Identifier: BUSL-1.1

//! Column projection for pgwire SELECT responses.
//!
//! Both the simple-query path and the extended-query path share this logic:
//! the Data Plane returns a JSON payload (an array of row objects, or a
//! single row object) wrapped in a single-column envelope by
//! `payload_to_response`. Clients expect one pgwire field per projected
//! column, not a JSON blob in one field.
//!
//! The entry point is `reproject_if_select`: parse the SQL's SELECT list,
//! determine the projected column names, and re-encode the response rows
//! with one pgwire field per column.

use std::sync::Arc;

use futures::StreamExt;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

use super::super::types::text_field;

/// Projection item from a parsed SELECT list.
pub(super) enum ProjectionItem {
    /// SELECT *
    Star,
    /// SELECT col  /  SELECT tbl.col  /  SELECT expr AS alias
    ///
    /// `lookup_key` is the key used to look up the value in the flat row
    /// object emitted by the Data Plane. For qualified references like
    /// `table.column` the Data Plane emits `"table.column"` as the key
    /// (prefix-merged by the join executor), so `lookup_key` preserves the
    /// full dot-joined form. `display_name` is the column label sent to the
    /// client (the last identifier segment, matching PostgreSQL behaviour).
    Named {
        lookup_key: String,
        display_name: String,
    },
}

/// Parse the SELECT projection list from `sql`. Returns `None` if the SQL is
/// not a simple SELECT or parsing fails; returns `Some([Star])` for `SELECT *`.
pub(super) fn parse_select_projection(sql: &str) -> Option<Vec<ProjectionItem>> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // NodeDB temporal clauses (`AS OF SYSTEM TIME`, `FOR SYSTEM_TIME`,
    // `AS OF VALID TIME`, ...) are extensions sqlparser cannot parse. Strip
    // them first — reusing the same preprocessing the planner uses — so the
    // SELECT list still reprojects into flat columns. Without this, a temporal
    // SELECT skips column projection and leaks the raw `{id,data}` envelope.
    let stripped = match nodedb_sql::parser::preprocess::temporal::extract(sql) {
        Ok(Some(extracted)) => extracted.sql,
        _ => sql.to_string(),
    };
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &stripped).ok()?;
    let stmt = stmts.into_iter().next()?;
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    let mut out = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                out.push(ProjectionItem::Star);
            }
            SelectItem::UnnamedExpr(expr) => {
                let (lookup_key, display_name) = expr_column_names(expr);
                out.push(ProjectionItem::Named {
                    lookup_key,
                    display_name,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                // The alias is the display name. The lookup key is the
                // underlying expression's full qualified name.
                let (lookup_key, _) = expr_column_names(expr);
                out.push(ProjectionItem::Named {
                    lookup_key,
                    display_name: alias.value.clone(),
                });
            }
        }
    }
    Some(out)
}

/// Returns `(lookup_key, display_name)` for an expression in the SELECT list.
///
/// For a plain `Identifier` both are the same bare column name.
/// For a `CompoundIdentifier` (e.g. `table.column`):
/// - `lookup_key` is the full dot-joined form (`"table.column"`) because the
///   join executor prefixes every key with its source collection name.
/// - `display_name` is the last segment (`"column"`) to match PostgreSQL
///   client expectations.
fn expr_column_names(expr: &sqlparser::ast::Expr) -> (String, String) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => {
            let name = id.value.clone();
            (name.clone(), name)
        }
        Expr::CompoundIdentifier(parts) => {
            let lookup_key = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            let display_name = parts
                .last()
                .map(|p| p.value.clone())
                .unwrap_or_else(|| lookup_key.clone());
            (lookup_key, display_name)
        }
        other => {
            // Normalize to lowercase so that aggregate functions like COUNT(*)
            // produce lookup keys ("count(*)") that match the canonical aggregate
            // key format used by the Data Plane response ("count(*)").
            let s = other.to_string().to_lowercase();
            (s.clone(), s)
        }
    }
}

/// Returns true when the projection list contains at least one non-Star named
/// column (i.e. we need to apply projection rather than pass through).
pub(super) fn needs_projection(items: &[ProjectionItem]) -> bool {
    items
        .iter()
        .any(|i| matches!(i, ProjectionItem::Named { .. }))
}

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

/// Build the ordered list of lookup keys that correspond to `fields_for_projection`.
///
/// Callers pass this alongside `result_fields` to `reproject_response` so
/// that qualified column references (`table.column`) are resolved against the
/// join-prefixed keys the Data Plane emits.
pub(super) fn lookup_keys_for_projection(items: &[ProjectionItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            ProjectionItem::Named { lookup_key, .. } => Some(lookup_key.clone()),
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

    // Clone the small, bounded per-query metadata so we can move it into the
    // stream closure without borrowing across the async boundary.
    let stream_schema = schema.clone();
    let stream_lookup_keys: Vec<String> = lookup_keys.to_vec();
    let stream_display_names: Vec<String> =
        result_fields.iter().map(|f| f.name().to_owned()).collect();

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
            let value = sonic_rs::from_str::<serde_json::Value>(text).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("malformed Data-Plane response envelope: {e}"),
                )))
            })?;

            let mut flat = Vec::new();
            push_flat_rows(value, &mut flat);

            for obj in flat {
                let data_row = encode_projected_row(
                    &obj,
                    &stream_schema,
                    &stream_lookup_keys,
                    &stream_display_names,
                )?;
                yield data_row;
            }
        }
    };

    Ok(Response::Query(QueryResponse::new(schema, row_stream)))
}

/// Encode a single flat row object into a pgwire `DataRow` using the projected
/// column schema.
///
/// For each column position `i`:
/// - `lookup_keys[i]` is tried first (handles qualified `table.col` references
///   against join-prefixed row objects).
/// - Falls back to the bare column name (last dot-segment) for plain
///   single-table queries.
/// - Falls back to `display_names[i]` (the SELECT alias) for aliased
///   function calls like `rrf_score(...) AS score`.
///
/// Missing or `null` values are encoded as SQL `NULL`.
fn encode_projected_row(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &Arc<Vec<FieldInfo>>,
    lookup_keys: &[String],
    display_names: &[String],
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (i, lookup_key) in lookup_keys.iter().enumerate() {
        let bare = lookup_key
            .rfind('.')
            .map(|dot_pos| &lookup_key[dot_pos + 1..])
            .unwrap_or(lookup_key.as_str());
        let display_name: Option<&str> = display_names.get(i).map(String::as_str);
        let value = obj
            .get(lookup_key.as_str())
            .or_else(|| {
                if bare != lookup_key {
                    obj.get(bare)
                } else {
                    None
                }
            })
            .or_else(|| {
                display_name.and_then(|n| {
                    if n != lookup_key.as_str() && Some(n) != Some(bare) {
                        obj.get(n)
                    } else {
                        None
                    }
                })
            });
        match value {
            None | Some(serde_json::Value::Null) => {
                encoder.encode_field(&Option::<String>::None).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("failed to encode NULL field: {e}"),
                    )))
                })?;
            }
            Some(v) => {
                let text = json_value_to_text(v);
                encoder.encode_field(&text).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("failed to encode field: {e}"),
                    )))
                })?;
            }
        }
    }
    Ok(encoder.take_row())
}

/// Convert a JSON scalar value to its PostgreSQL text-format string.
///
/// - `String` values are returned as-is (no extra quoting).
/// - `Bool` uses PostgreSQL text format: `t` for true, `f` for false.
/// - All other scalars (`Number`, `Array`, `Object`) use their JSON
///   `Display` representation; arrays/objects should not normally appear
///   as individual cell values but are rendered faithfully.
fn json_value_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        // PostgreSQL text format for boolean is `t`/`f`.
        serde_json::Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        other => other.to_string(),
    }
}

/// Consume an envelope `QueryResponse` and return flat row objects.
pub(super) async fn collect_flat_rows(
    mut qr: QueryResponse,
) -> PgWireResult<Vec<serde_json::Map<String, serde_json::Value>>> {
    let mut rows = Vec::new();
    while let Some(row_result) = qr.data_rows.next().await {
        let row = row_result?;
        let Some(text) = decode_first_field_text(&row.data) else {
            continue;
        };
        let value = sonic_rs::from_str::<serde_json::Value>(text).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("malformed Data-Plane response envelope: {e}"),
            )))
        })?;
        push_flat_rows(value, &mut rows);
    }
    Ok(rows)
}

/// Flatten a parsed JSON value into row objects.
pub(super) fn push_flat_rows(
    value: serde_json::Value,
    out: &mut Vec<serde_json::Map<String, serde_json::Value>>,
) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                push_flat_rows(item, out);
            }
        }
        serde_json::Value::Object(mut map) => {
            if is_scan_wrapper(&map)
                && let Some(serde_json::Value::Object(inner)) = map.remove("data")
            {
                out.push(inner);
                return;
            }
            out.push(map);
        }
        _ => {}
    }
}

/// The Data Plane's raw document-scan codec emits objects with exactly
/// the keys `id` (string) and `data` (object). This is the wire shape
/// we unwrap before column projection.
pub(super) fn is_scan_wrapper(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.len() == 2
        && matches!(map.get("id"), Some(serde_json::Value::String(_)))
        && matches!(map.get("data"), Some(serde_json::Value::Object(_)))
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

    let flat_rows = collect_flat_rows(qr).await?;
    if flat_rows.is_empty() {
        let schema = Arc::new(vec![text_field("result")]);
        return Ok(Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(Vec::<PgWireResult<_>>::new()),
        )));
    }

    // Derive column order: id first (if present), then remaining keys
    // in stable insertion order from the first row.
    let mut cols: Vec<String> = Vec::new();
    let first = &flat_rows[0];
    if first.contains_key("id") {
        cols.push("id".to_string());
    }
    for key in first.keys() {
        if key != "id" {
            cols.push(key.clone());
        }
    }
    // Ensure any keys from later rows that were absent in the first row
    // are also included (union over all rows).
    for row in flat_rows.iter().skip(1) {
        for key in row.keys() {
            if !cols.contains(key) {
                cols.push(key.clone());
            }
        }
    }

    let schema: Arc<Vec<_>> = Arc::new(cols.iter().map(|c| text_field(c)).collect());
    let row_schema = schema.clone();
    let pgwire_rows: Vec<PgWireResult<_>> = flat_rows
        .iter()
        .map(|obj| {
            let mut encoder = DataRowEncoder::new(row_schema.clone());
            for col in &cols {
                match obj.get(col.as_str()) {
                    None | Some(serde_json::Value::Null) => {
                        let _ = encoder.encode_field(&Option::<String>::None);
                    }
                    Some(v) => {
                        let text = json_value_to_text(v);
                        let _ = encoder.encode_field(&text);
                    }
                }
            }
            Ok(encoder.take_row())
        })
        .collect();

    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(pgwire_rows),
    )))
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
