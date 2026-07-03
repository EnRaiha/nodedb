// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`ShapedRows`] (from
//! `response_shape::types`/`response_shape::project`) into a pgwire
//! `Response::Query`.
//!
//! This is the pgwire entrypoint's encoder for the canonical neutral shaping
//! core: the SELECT-read path builds a `ShapedRows` once and every protocol
//! entrypoint (pgwire, native, http) renders it in its own wire format. Here,
//! each cell renders through `json_value_to_text` to match PostgreSQL's text
//! format exactly — notably `Bool` renders as `t`/`f`, not `true`/`false`.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use crate::control::server::response_shape::project::json_value_to_text;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};

use super::super::ddl_encode::col_type_to_field;

/// Encode one flat row object into a pgwire `DataRow`, using `columns` (in
/// order) to look up cells in `row`.
///
/// Missing keys and explicit JSON `null` both encode as SQL NULL. All other
/// values render via `json_value_to_text` — this path is all-TEXT columns
/// with raw JSON cells, so it must not use the typed per-`DdlColType` cell
/// logic in `ddl_encode.rs` (that renders `Bool` as `true`/`false`, which
/// would diverge from PostgreSQL's `t`/`f` text format).
pub(in crate::control::server::pgwire) fn encode_shaped_row(
    schema: &Arc<Vec<FieldInfo>>,
    columns: &[String],
    row: &serde_json::Map<String, serde_json::Value>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for name in columns {
        match row.get(name) {
            None | Some(serde_json::Value::Null) => {
                encoder.encode_field(&None::<&str>)?;
            }
            Some(v) => {
                let text = json_value_to_text(v);
                encoder.encode_field(&text)?;
            }
        }
    }
    Ok(encoder.take_row())
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
        .map(|row| encode_shaped_row(&schema, &columns, row))
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

    #[tokio::test]
    async fn notice_is_preserved_not_dropped() {
        let mut shaped = make_shaped(&["a"], vec![obj(&[("a", json!("x"))])]);
        shaped.notice = Some("heads up".to_owned());
        let (_response, notice) = shaped_query_response(shaped);
        assert_eq!(notice.as_deref(), Some("heads up"));
    }
}
