// SPDX-License-Identifier: BUSL-1.1

//! Helper for projecting post-write documents into a `RowsPayload`.
//!
//! Called by every RETURNING-producing handler when the plan carries a
//! `ReturningSpec`. Produces a `RowsPayload` msgpack blob that the Control
//! Plane decodes into multi-column pgwire rows.
//!
//! This is the single choke point where the row-level-security READ policy is
//! applied to DML output: every caller passes the full pre-projection
//! documents, so the policy can be evaluated against columns the `RETURNING`
//! list never mentions.

use super::{returning_doc, rls_eval};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::response_codec::RowsPayload;
use crate::data::executor::scan_normalize::{kv_row_to_doc, sparse_row_to_doc};
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
use nodedb_types::columnar::StrictSchema;

/// Rows a write path hands back to a `RETURNING` projection: the user-facing
/// document id paired with the exact bytes stored for it.
pub(in crate::data::executor) type StoredRow<'a> = (&'a str, &'a [u8]);

impl CoreLoop {
    /// Build this task's `RETURNING` response from the rows it just stored.
    ///
    /// The single exit every insert-family handler uses, so the read gate and
    /// the storage-mode-aware decode can never be applied on one write path and
    /// skipped on another. An empty `rows` slice is the correct answer for a
    /// write that stored nothing (an `ON CONFLICT DO NOTHING` that hit a
    /// conflict): the statement still returns a row set, it is just empty.
    pub(in crate::data::executor) fn stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        strict_schema: Option<&StrictSchema>,
        rows: &[StoredRow<'_>],
    ) -> Response {
        match build_stored_rows_payload(spec, rls_filters, strict_schema, rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

/// A KV row a write path hands back to a `RETURNING` projection: the raw key
/// bytes paired with the exact value bytes stored under them.
pub(in crate::data::executor) type KvStoredRow<'a> = (&'a [u8], &'a [u8]);

impl CoreLoop {
    /// Build a KV write's `RETURNING` response from the rows it just stored.
    ///
    /// The row shape comes from [`kv_row_to_doc`], the same helper the KV scan
    /// paths use, so a row a write hands back is byte-identical to the row a
    /// `SELECT` on that key would produce — including the `key` field, and
    /// including the `{key, value}` wrapper an opaque single-scalar value gets
    /// (such a value has no field map to inject into, and inventing a second
    /// shape here would make `RETURNING *` disagree with `SELECT *`).
    ///
    /// A body that fails to decode fails the statement. `kv_row_to_doc` wraps
    /// every shape a KV value can take, so a wrapped row that will not decode
    /// means the stored value is not what the KV engine says it is — and a bare
    /// `{key}` substituted for it would report a row whose value the client
    /// never wrote, in the very response it uses to learn what landed.
    pub(in crate::data::executor) fn kv_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[KvStoredRow<'_>],
    ) -> Response {
        let docs: Vec<serde_json::Value> = match rows
            .iter()
            .map(|(key, value)| {
                let (_key_str, body) = kv_row_to_doc(key, value);
                doc_format::decode_document(&body)
            })
            .collect::<crate::Result<Vec<_>>>()
        {
            Ok(docs) => docs,
            Err(e) => return self.response_error(task, e),
        };
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

impl CoreLoop {
    /// Build a columnar write's `RETURNING` response from the rows it stored.
    ///
    /// A columnar row is `Vec<Value>` in schema order, not a stored msgpack
    /// body, so the row image is assembled by `row_values_to_object` — the same
    /// builder the columnar WHERE evaluator and the row-level-security write
    /// gate use. Assembling it a second way here is how `RETURNING` output would
    /// drift from what a `SELECT` on the same key reports.
    ///
    /// `rows` must already exclude anything the engine skipped (an
    /// `ON CONFLICT DO NOTHING` that hit a conflict), so an empty slice is the
    /// correct answer for a write that stored nothing.
    pub(in crate::data::executor) fn columnar_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        schema: &nodedb_types::columnar::ColumnarSchema,
        rows: &[Vec<nodedb_types::Value>],
    ) -> Response {
        let docs: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::Value::from(super::columnar_write::row_values_to_object(schema, row))
            })
            .collect();
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

impl CoreLoop {
    /// Build a timeseries ingest's `RETURNING` response from the points it stored.
    ///
    /// `rows` are already the output of the ordinary memtable scan projection —
    /// see `raw_scan::emit_memtable_rows_at` — so a stored point renders here
    /// exactly as `SELECT` renders it, `NaN`-as-NULL included. This function
    /// only re-keys that row into the shape `build_rows_payload` reads; it makes
    /// no per-cell decisions of its own, which is the entire reason the read-back
    /// happens upstream rather than here.
    pub(in crate::data::executor) fn timeseries_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[rmpv::Value],
    ) -> Response {
        let docs: Vec<serde_json::Value> = rows.iter().map(rmpv_row_to_json).collect();
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

impl CoreLoop {
    /// Build a vector-primary upsert's `RETURNING` response from the sidecar it
    /// just stored.
    ///
    /// The row image comes from [`sparse_row_to_doc`] against
    /// [`SparseBodyFormatRef::VectorSidecar`] — the SAME converter the `SELECT`
    /// scan resolves for this collection — so the returned row is byte-for-byte
    /// what a later read renders. The sidecar is `zerompk` TAGGED bytes, which
    /// an ordinary document decode ACCEPTS and misreads: a stored `"alice"`
    /// comes back as the tag array `[4,"alice"]`. Deciding the encoding here a
    /// second time is exactly how that defect would return.
    ///
    /// The format is passed as the literal `VectorSidecar` rather than resolved
    /// via `sparse_body_format` because this op only ever runs against a
    /// vector-primary collection — the same reasoning the vector-primary body
    /// fetch already uses.
    ///
    /// `id` is injected only when the body carries none — a vector-primary
    /// sidecar stores the user's declared primary key, and the sparse key is
    /// the internal surrogate hex, which must not displace it.
    pub(in crate::data::executor) fn vector_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        row_key: &str,
        sidecar: &[u8],
    ) -> Response {
        let (_id, mp) = sparse_row_to_doc(row_key, sidecar, SparseBodyFormatRef::VectorSidecar);
        // An empty row set here would report "the write affected nothing" for a
        // write that did land, so an unreadable sidecar fails the statement.
        let docs: Vec<serde_json::Value> = match doc_format::decode_document(&mp) {
            Ok(doc) => vec![doc],
            Err(e) => return self.response_error(task, e),
        };
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

/// Re-key one scan-projected timeseries row into JSON for `build_rows_payload`.
///
/// A straight transcode: msgpack nil stays SQL NULL, and no value is
/// reinterpreted. Anything cleverer here would be a second shaper.
fn rmpv_row_to_json(row: &rmpv::Value) -> serde_json::Value {
    let rmpv::Value::Map(fields) = row else {
        return serde_json::Value::Null;
    };
    let mut obj = serde_json::Map::with_capacity(fields.len());
    for (key, value) in fields {
        let Some(name) = key.as_str() else { continue };
        let cell = match value {
            rmpv::Value::Nil => serde_json::Value::Null,
            rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
            rmpv::Value::Integer(n) => n
                .as_i64()
                .map(|i| serde_json::Value::Number(i.into()))
                .unwrap_or(serde_json::Value::Null),
            rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            rmpv::Value::F32(f) => serde_json::Number::from_f64(f64::from(*f))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            rmpv::Value::String(s) => s
                .as_str()
                .map(|text| serde_json::Value::String(text.to_string()))
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        };
        obj.insert(name.to_string(), cell);
    }
    serde_json::Value::Object(obj)
}

/// Project the STORED post-images of freshly written rows into a `RowsPayload`.
///
/// The write paths (point insert / put / batch insert / upsert) hold the exact
/// bytes they handed to storage, so `RETURNING` reports what landed — generated
/// columns evaluated, `_rowid` injected, an `ON CONFLICT` merge applied — rather
/// than echoing the caller's submitted body, which would report the request
/// instead of the row and would bypass the read gate entirely.
///
/// `strict_schema` must be `Some` exactly when the collection stores Binary
/// Tuples; a body that fails to decode fails the statement rather than
/// degrading to a bare `{id}` row, which would report a row the client never
/// wrote and hide that the stored body is unreadable.
fn build_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    strict_schema: Option<&StrictSchema>,
    rows: &[StoredRow<'_>],
) -> crate::Result<Vec<u8>> {
    let docs: Vec<serde_json::Value> = rows
        .iter()
        .map(|(doc_id, body)| returning_doc::from_stored(body, doc_id, strict_schema))
        .collect::<crate::Result<Vec<_>>>()?;
    build_rows_payload(spec, rls_filters, &docs)
}

/// Project a slice of documents per `spec` and encode as a `RowsPayload` msgpack blob.
///
/// For `ReturningColumns::Star`, all fields in each document are emitted in
/// insertion order (JSON object key order). For `ReturningColumns::Named`,
/// only the named fields are emitted in spec order. Missing fields and JSON
/// nulls are encoded as `None` so the Control Plane can emit a real SQL NULL.
///
/// `rls_filters` carries the collection's compiled read policy. Rows failing it
/// are dropped BEFORE projection — the predicate routinely references a column
/// the `RETURNING` list omits, so a post-projection test would have nothing to
/// evaluate against and would leak the row. The write itself already happened
/// and the affected count already counted it; only the visible row set shrinks,
/// matching how PostgreSQL applies the SELECT policy to `RETURNING` output.
/// Empty `rls_filters` (no policy / superuser) admits every row.
pub(super) fn build_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    docs: &[serde_json::Value],
) -> crate::Result<Vec<u8>> {
    let visible: Vec<&serde_json::Value> = docs
        .iter()
        .filter(|doc| rls_eval::rls_check_document(rls_filters, doc))
        .collect();

    let (columns, source_names) = match &spec.columns {
        ReturningColumns::Star => {
            if visible.is_empty() {
                return encode_empty(Vec::new());
            }
            // Derive column names from the first doc's keys; both output and
            // source names are identical for `RETURNING *`.
            let cols: Vec<String> = visible
                .first()
                .and_then(|d| d.as_object())
                .map(|obj| obj.keys().cloned().collect())
                .unwrap_or_default();
            (cols.clone(), cols)
        }
        ReturningColumns::Named(items) => {
            let output_names: Vec<String> = items
                .iter()
                .map(|item| item.alias.clone().unwrap_or_else(|| item.name.clone()))
                .collect();
            let source_names: Vec<String> = items.iter().map(|item| item.name.clone()).collect();
            (output_names, source_names)
        }
    };

    let rows: Vec<Vec<Option<String>>> = visible
        .iter()
        .map(|doc| project_row(doc, &source_names))
        .collect();

    let payload = RowsPayload { columns, rows };
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode: {e}"),
    })
}

fn encode_empty(columns: Vec<String>) -> crate::Result<Vec<u8>> {
    let payload = RowsPayload {
        columns,
        rows: Vec::new(),
    };
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode empty: {e}"),
    })
}

/// Project a single document into one cell per source name.
///
/// Returns `None` for missing fields or JSON null, `Some(text)` otherwise.
fn project_row(doc: &serde_json::Value, source_names: &[String]) -> Vec<Option<String>> {
    let obj = doc.as_object();
    source_names
        .iter()
        .map(|name| obj.and_then(|o| o.get(name)).and_then(value_to_text))
        .collect()
}

/// Convert a JSON value to its TEXT representation for pgwire.
///
/// Returns `None` for JSON null so the Control Plane emits a real SQL NULL
/// rather than the string "null". Strings are returned as-is (no extra
/// quotes); numbers, booleans, arrays, and objects use JSON text.
fn value_to_text(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::ScanFilter;
    use nodedb_physical::physical_plan::ReturningItem;
    use serde_json::json;

    /// `owner = <value>`, the shape a compiled read policy lands in.
    fn owner_policy(value: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String(value.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    fn named(cols: &[&str]) -> ReturningSpec {
        ReturningSpec {
            columns: ReturningColumns::Named(
                cols.iter()
                    .map(|c| ReturningItem {
                        name: (*c).to_string(),
                        alias: None,
                    })
                    .collect(),
            ),
        }
    }

    fn decode(payload: &[u8]) -> RowsPayload {
        zerompk::from_msgpack(payload).expect("decode RowsPayload")
    }

    fn docs() -> Vec<serde_json::Value> {
        vec![
            json!({"id": "r1", "owner": "alice", "note": "hidden"}),
            json!({"id": "r2", "owner": "bob", "note": "shown"}),
        ]
    }

    #[test]
    fn empty_filters_return_every_row() {
        let payload = build_rows_payload(&named(&["id"]), &[], &docs()).expect("build");
        let rows = decode(&payload).rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn excluded_rows_are_dropped() {
        let payload =
            build_rows_payload(&named(&["id"]), &owner_policy("bob"), &docs()).expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.rows, vec![vec![Some("r2".to_string())]]);
    }

    /// The predicate names a column the projection omits — the filter must
    /// still see it, which it only can before projection.
    #[test]
    fn a_policy_column_outside_the_projection_still_filters() {
        let payload =
            build_rows_payload(&named(&["note"]), &owner_policy("bob"), &docs()).expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.columns, vec!["note".to_string()]);
        assert_eq!(decoded.rows, vec![vec![Some("shown".to_string())]]);
    }

    /// `RETURNING *` derives its column list from the first VISIBLE row, so a
    /// hidden leading row cannot dictate the shape of the result.
    #[test]
    fn star_columns_come_from_the_first_visible_row() {
        let docs = vec![
            json!({"id": "r1", "owner": "alice", "secret": "hidden"}),
            json!({"id": "r2", "owner": "bob"}),
        ];
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        let payload = build_rows_payload(&spec, &owner_policy("bob"), &docs).expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.columns, vec!["id".to_string(), "owner".to_string()]);
        assert_eq!(decoded.rows.len(), 1);
    }

    /// A malformed policy denies rather than passing rows through.
    #[test]
    fn a_corrupt_policy_returns_no_rows() {
        let payload = build_rows_payload(&named(&["id"]), &[0xFF, 0xFE], &docs()).expect("build");
        assert!(decode(&payload).rows.is_empty());
    }
}
