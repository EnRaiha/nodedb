// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`ShapedRows`] (from
//! `response_shape::types`/`response_shape::project`) into a pgwire
//! `Response::Query`.
//!
//! This is the pgwire entrypoint's encoder for the canonical neutral shaping
//! core: the SELECT-read path builds a `ShapedRows` once and every protocol
//! entrypoint (pgwire, native, http) renders it in its own wire format. Here,
//! each cell renders in its column's PostgreSQL text form, driven by the
//! per-column `DdlColType` the shaper threaded through `ShapedRows`:
//! `Float8`/`Float4` go through pgwire's native float encoder (so `0.0` stays
//! `"0.0"`, not `"0"`), `Timestamp`/`Timestamptz` epoch-microsecond cells
//! render as ISO-8601 text, `Bytea` renders as `\x<hex>`, and everything else
//! (`Text`, integers, `Bool`) falls back to `json_value_to_text` — notably
//! `Bool` as `t`/`f`, not `true`/`false`.

use std::fmt::Write as _;
use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use nodedb_types::NdbDateTime;

use crate::control::server::response_shape::project::json_value_to_text;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};

use super::super::ddl_encode::col_type_to_field;

/// Encode one flat row object into a pgwire `DataRow`, using `columns` (in
/// order) to look up cells in `row` and `column_types` (parallel to `columns`)
/// to pick each cell's text rendering.
///
/// Missing keys and explicit JSON `null` both encode as SQL NULL. Every other
/// cell renders per its column type via [`encode_typed_cell`]; a
/// missing/short `column_types` entry defaults to `Text`.
pub(in crate::control::server::pgwire) fn encode_shaped_row(
    schema: &Arc<Vec<FieldInfo>>,
    columns: &[String],
    column_types: &[DdlColType],
    row: &serde_json::Map<String, serde_json::Value>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (idx, name) in columns.iter().enumerate() {
        let ct = column_types.get(idx).copied().unwrap_or(DdlColType::Text);
        match row.get(name) {
            None | Some(serde_json::Value::Null) => {
                encoder.encode_field(&None::<&str>)?;
            }
            Some(v) => encode_typed_cell(&mut encoder, ct, v)?,
        }
    }
    Ok(encoder.take_row())
}

/// Encode one non-NULL JSON cell into `encoder` per its column type `ct`.
///
/// `Float8`/`Float4` numeric cells go through pgwire's native float encoder
/// (ryu + `extra_float_digits`) so their text bytes match PostgreSQL exactly;
/// `Timestamp`/`Timestamptz` epoch-microsecond numbers render as ISO-8601
/// text; `Bytea` base64 cells render as PostgreSQL's `\x<hex>` form. Any cell
/// whose JSON shape doesn't match the typed arm (e.g. an already-formatted
/// timestamp string) falls back to `json_value_to_text`, as does every other
/// type — `Text`, integers, and `Bool` (`t`/`f`).
fn encode_typed_cell(
    encoder: &mut DataRowEncoder,
    ct: DdlColType,
    v: &serde_json::Value,
) -> PgWireResult<()> {
    use serde_json::Value;
    match ct {
        DdlColType::Float8 => match v {
            Value::Number(n) => match n.as_f64() {
                Some(f) => encoder.encode_field(&f),
                None => encoder.encode_field(&None::<f64>),
            },
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        DdlColType::Float4 => match v {
            Value::Number(n) => match n.as_f64() {
                Some(f) => encoder.encode_field(&(f as f32)),
                None => encoder.encode_field(&None::<f32>),
            },
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        DdlColType::Timestamp | DdlColType::Timestamptz => match v {
            Value::Number(n) => match n.as_i64() {
                Some(micros) => {
                    encoder.encode_field(&NdbDateTime::from_micros(micros).to_iso8601())
                }
                None => encoder.encode_field(&json_value_to_text(v)),
            },
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        DdlColType::Bytea => match v {
            Value::String(s) => encoder.encode_field(&bytea_hex_text(s)),
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        _ => encoder.encode_field(&json_value_to_text(v)),
    }
}

/// Render a base64-encoded byte string as PostgreSQL's `bytea` hex text output
/// (`\x` followed by lowercase hex). The shaper transcodes msgpack `bin`
/// payloads to base64 JSON strings; a value that fails to base64-decode is
/// hexed from its raw UTF-8 bytes rather than erroring.
fn bytea_hex_text(base64_str: &str) -> String {
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, base64_str)
        .unwrap_or_else(|_| base64_str.as_bytes().to_vec());
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for b in &bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Build a `Response::Query` from a protocol-neutral [`ShapedRows`], plus its
/// carried client-facing notice.
///
/// Unlike `ddl_encode::rows_to_response` (which intentionally drops the
/// notice — the pgwire DDL router never attached one to a `Response::Query`),
/// this path preserves `notice`: the caller is expected to surface it via
/// `sessions.push_notice`.
pub(in crate::control::server::pgwire) fn shaped_query_response(
    shaped: ShapedRows,
) -> (Response, Option<String>) {
    let ShapedRows {
        columns,
        column_types,
        rows,
        notice,
    } = shaped;

    let fields: Vec<FieldInfo> = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let ct = column_types.get(i).copied().unwrap_or(DdlColType::Text);
            col_type_to_field(name, ct)
        })
        .collect();
    let schema = Arc::new(fields);

    let encoded_rows: Vec<PgWireResult<DataRow>> = rows
        .iter()
        .map(|row| encode_shaped_row(&schema, &columns, &column_types, row))
        .collect();

    let response = Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(encoded_rows),
    ));
    (response, notice)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use pgwire::api::results::{QueryResponse, Response};
    use serde_json::json;

    use super::shaped_query_response;
    use crate::control::server::response_shape::types::ShapedRows;

    /// Drain a `QueryResponse` stream into a `Vec` of `DataRow`s.
    async fn drain(mut qr: QueryResponse) -> Vec<pgwire::messages::data::DataRow> {
        let mut rows = Vec::new();
        while let Some(r) = qr.data_rows.next().await {
            rows.push(r.unwrap());
        }
        rows
    }

    /// Read the text value of field `idx` from a `DataRow`'s raw wire buffer.
    ///
    /// Wire format: 4-byte big-endian length + bytes per field; a negative
    /// length denotes SQL NULL.
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

    fn make_shaped(
        columns: &[&str],
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
    ) -> ShapedRows {
        let columns: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let column_types = ShapedRows::text_types(columns.len());
        ShapedRows {
            columns,
            column_types,
            rows,
            notice: None,
        }
    }

    fn obj(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[tokio::test]
    async fn string_cell_renders_verbatim() {
        let shaped = make_shaped(&["a"], vec![obj(&[("a", json!("hello"))])]);
        let (response, notice) = shaped_query_response(shaped);
        assert!(notice.is_none());
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn bool_cells_render_as_t_f_not_true_false() {
        let shaped = make_shaped(
            &["a"],
            vec![obj(&[("a", json!(true))]), obj(&[("a", json!(false))])],
        );
        let (response, _notice) = shaped_query_response(shaped);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("t"));
        assert_eq!(field_text(&rows[1], 0).as_deref(), Some("f"));
    }

    #[tokio::test]
    async fn number_cells_render_via_to_string() {
        let shaped = make_shaped(
            &["a"],
            vec![obj(&[("a", json!(42))]), obj(&[("a", json!(0.0))])],
        );
        let (response, _notice) = shaped_query_response(shaped);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("42"));
        assert_eq!(field_text(&rows[1], 0).as_deref(), Some("0.0"));
    }

    #[tokio::test]
    async fn null_and_missing_column_both_encode_as_sql_null() {
        let shaped = make_shaped(&["a", "b"], vec![obj(&[("a", serde_json::Value::Null)])]);
        let (response, _notice) = shaped_query_response(shaped);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        // "a" was explicit JSON null.
        assert_eq!(field_text(&rows[0], 0), None);
        // "b" was entirely absent from the row object.
        assert_eq!(field_text(&rows[0], 1), None);
    }

    #[tokio::test]
    async fn column_order_is_preserved() {
        let shaped = make_shaped(
            &["b", "a"],
            vec![obj(&[("a", json!("first")), ("b", json!("second"))])],
        );
        let (response, _notice) = shaped_query_response(shaped);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("second"));
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("first"));
    }

    /// A user `SELECT` of typed columns on the simple-query path must report
    /// the correct RowDescription type OID AND render each cell in that type's
    /// PostgreSQL text form — the two halves that must land together.
    #[tokio::test]
    async fn typed_columns_report_correct_oid_and_text() {
        use pgwire::api::Type;

        use crate::control::server::response_shape::types::DdlColType;

        // A base64 string is how the shaper transcodes a `bytea` msgpack `bin`.
        let raw = [0xDE_u8, 0xAD];
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw);

        let columns: Vec<String> = ["i", "f", "b", "ts", "by"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let column_types = vec![
            DdlColType::Int8,
            DdlColType::Float8,
            DdlColType::Bool,
            DdlColType::Timestamp,
            DdlColType::Bytea,
        ];
        let row = obj(&[
            ("i", json!(42)),
            // Integral float renders Postgres-style "0" (shortest form) via the
            // native float encoder, not serde's "0.0".
            ("f", json!(0.0)),
            ("b", json!(true)),
            // Epoch microseconds → ISO-8601 text (0 == Unix epoch).
            ("ts", json!(0)),
            ("by", json!(b64)),
        ]);
        let shaped = ShapedRows {
            columns,
            column_types,
            rows: vec![row],
            notice: None,
        };

        let (response, _notice) = shaped_query_response(shaped);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        // RowDescription OIDs are the typed ones, not TEXT.
        let schema = qr.row_schema.clone();
        assert_eq!(schema[0].datatype(), &Type::INT8);
        assert_eq!(schema[1].datatype(), &Type::FLOAT8);
        assert_eq!(schema[2].datatype(), &Type::BOOL);
        assert_eq!(schema[3].datatype(), &Type::TIMESTAMP);
        assert_eq!(schema[4].datatype(), &Type::BYTEA);

        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("42"));
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("0"));
        assert_eq!(field_text(&rows[0], 2).as_deref(), Some("t"));
        assert_eq!(
            field_text(&rows[0], 3).as_deref(),
            Some("1970-01-01T00:00:00.000000Z")
        );
        assert_eq!(field_text(&rows[0], 4).as_deref(), Some("\\xdead"));
    }

    #[tokio::test]
    async fn notice_is_preserved_not_dropped() {
        let mut shaped = make_shaped(&["a"], vec![obj(&[("a", json!("x"))])]);
        shaped.notice = Some("heads up".to_owned());
        let (_response, notice) = shaped_query_response(shaped);
        assert_eq!(notice.as_deref(), Some("heads up"));
    }
}
