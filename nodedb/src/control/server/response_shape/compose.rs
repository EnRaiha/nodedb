// SPDX-License-Identifier: BUSL-1.1

//! Composed, protocol-neutral materialized response shaping.
//!
//! `shape_response_materialized` and `shape_decoded_rows` are the canonical
//! SELECT-read shaping used by every protocol entrypoint. `shape_response_materialized`
//! performs the full per-payload shaping order (`apply_kv_wrap` ->
//! `translate_search_response` -> decode -> scan-envelope unwrap -> optional
//! SELECT-list projection) as a single call, producing an already-shaped,
//! already-projected [`ShapeOutcome`]. Every SELECT-read producer — pgwire's
//! non-streaming dispatch, native's dispatch loop — calls this directly and
//! hands the resulting `ShapedRows` to its own protocol encoder; each
//! protocol then encodes those rows in its own wire format (pgwire's
//! RowDescription/DataRow, native's MessagePack, http's JSON).
//!
//! Producers with no `PhysicalPlan` in scope (ClusterArray, set-op merges,
//! gateway forwarding, clone merges) call [`shape_payload_no_plan`], which
//! skips the plan-dependent `apply_kv_wrap` / `translate_search_response` transforms
//! those callers never ran. The pure kernel [`shape_decoded_rows`] is shared
//! with per-batch lazy streaming callers, which have an already-decoded batch
//! and only need the envelope-unwrap + projection logic.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::response_translate::dispatch::translate_search_response;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{
    ArraySliceResponse, RowsPayload, decode_payload_to_json,
};
use nodedb_types::columnar::schema::is_reserved_bitemporal_column;
use nodedb_types::{DatabaseId, NodeDbError, TenantId};

use super::kv::apply_kv_wrap;
use super::project::push_flat_rows;
use super::schema::OutputSchema;
use super::types::{DdlColType, PlanKind, ShapedRows};

/// NOTICE text for an `AS OF SYSTEM TIME` cutoff older than the oldest
/// retained tile version. This is the canonical definition, surfaced to
/// every protocol via [`ShapedRows::notice`].
const TRUNCATED_BEFORE_HORIZON_NOTICE: &str = "AS OF SYSTEM TIME cutoff is older than the oldest retained tile version; \
     results may be incomplete";

/// Outcome of materialized response shaping.
///
/// Row-producing plan kinds (`SingleDocument`, `MultiRow`, `ReturningRows`,
/// `ArraySlice`) yield `Rows`. Tag/execution kinds (`Execution`,
/// `DmlResult`) yield `Passthrough` — a `ShapedRows` cannot represent a bare
/// `CommandComplete` tag or affected-row count, so callers keep their
/// existing tag / `rows_affected` handling for those.
pub enum ShapeOutcome {
    Rows(ShapedRows),
    Passthrough,
}

/// Shape a single Data-Plane payload into protocol-neutral rows, applying
/// the canonical shaping order: KV point-get wrap, vector surrogate->PK
/// translation, payload decode, scan-envelope unwrap, and (when
/// `projection` names columns) SELECT-list column selection.
pub fn shape_response_materialized(
    payload: &[u8],
    plan: &PhysicalPlan,
    plan_kind: PlanKind,
    projection: Option<&OutputSchema>,
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<ShapeOutcome, NodeDbError> {
    match plan_kind {
        PlanKind::Execution | PlanKind::DmlResult(_) => return Ok(ShapeOutcome::Passthrough),
        PlanKind::ArraySlice
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => {}
    }

    // Seam-1 order, exactly as pgwire's `dispatch_task_loop` applies it
    // (apply_kv_wrap -> translate_search_response) before any decode/shape step.
    let wrapped = apply_kv_wrap(plan, payload);
    let translated = translate_search_response(&wrapped, plan, state, database_id, tenant_id);

    let shaped = match plan_kind {
        PlanKind::ArraySlice => shape_array_slice(&translated),
        PlanKind::ReturningRows => shape_returning_rows(&translated),
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            shape_generic_rows(&translated, projection)
        }
        // Handled by the early return above; kept exhaustive (no catch-all,
        // no panic) so a future PlanKind desync degrades to passthrough
        // rather than crashing the connection.
        PlanKind::Execution | PlanKind::DmlResult(_) => return Ok(ShapeOutcome::Passthrough),
    };
    Ok(ShapeOutcome::Rows(shaped))
}

/// Shape a Data-Plane payload with no `PhysicalPlan` in scope.
///
/// Producers that never had a plan to KV-wrap or vector-translate
/// (ClusterArray, set-op merges, gateway forwarding, clone merges) call this
/// instead of [`shape_response_materialized`]: it applies only the decode +
/// scan-envelope unwrap + optional SELECT-list projection steps, skipping the
/// plan-dependent `apply_kv_wrap` / `translate_search_response` transforms those
/// callers never ran.
pub fn shape_payload_no_plan(
    payload: &[u8],
    plan_kind: PlanKind,
    projection: Option<&OutputSchema>,
) -> ShapeOutcome {
    match plan_kind {
        PlanKind::Execution | PlanKind::DmlResult(_) => ShapeOutcome::Passthrough,
        PlanKind::ArraySlice => ShapeOutcome::Rows(shape_array_slice(payload)),
        PlanKind::ReturningRows => ShapeOutcome::Rows(shape_returning_rows(payload)),
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            ShapeOutcome::Rows(shape_generic_rows(payload, projection))
        }
    }
}

/// Shape an `ArrayOp::Slice` response: decode the `ArraySliceResponse`
/// envelope (falling back to a plain payload decode for legacy shapes),
/// unwrap the row envelope, and surface `truncated_before_horizon` as a
/// notice.
///
/// Array slices never carry a SELECT-list projection today (matching the
/// pre-extraction behavior), so `shape_decoded_rows` is always called with
/// `None` here.
fn shape_array_slice(payload: &[u8]) -> ShapedRows {
    if payload.is_empty() {
        return empty_shaped();
    }
    let (rows_json, truncated) =
        if let Ok(resp) = zerompk::from_msgpack::<ArraySliceResponse>(payload) {
            (
                decode_payload_to_json(&resp.rows_msgpack),
                resp.truncated_before_horizon,
            )
        } else {
            (decode_payload_to_json(payload), false)
        };
    let notice = truncated.then(|| TRUNCATED_BEFORE_HORIZON_NOTICE.to_string());

    let mut shaped = match sonic_rs::from_str::<JsonValue>(&rows_json) {
        Ok(value) => shape_decoded_rows(&value, None),
        Err(_) => empty_shaped(),
    };
    shaped.notice = notice;
    shaped
}

/// Shape a DML-with-`RETURNING` response: decode the `RowsPayload` envelope
/// (already TEXT-formatted cells), falling back to a single "result" column
/// on a malformed payload.
fn shape_returning_rows(payload: &[u8]) -> ShapedRows {
    if payload.is_empty() {
        return single_result_column_empty();
    }
    match zerompk::from_msgpack::<RowsPayload>(payload) {
        Ok(rp) => {
            if rp.rows.is_empty() {
                let columns = if rp.columns.is_empty() {
                    vec!["result".to_string()]
                } else {
                    rp.columns
                };
                let column_types = ShapedRows::text_types(columns.len());
                return ShapedRows {
                    columns,
                    column_types,
                    rows: Vec::new(),
                    notice: None,
                };
            }
            let rows = rp
                .rows
                .iter()
                .map(|row_vals| {
                    let mut map = Map::new();
                    for (col, cell) in rp.columns.iter().zip(row_vals.iter()) {
                        let v = match cell {
                            Some(s) => JsonValue::String(s.clone()),
                            None => JsonValue::Null,
                        };
                        map.insert(col.clone(), v);
                    }
                    map
                })
                .collect();
            let column_types = ShapedRows::text_types(rp.columns.len());
            ShapedRows {
                columns: rp.columns,
                column_types,
                rows,
                notice: None,
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                payload_len = payload.len(),
                "ReturningRows msgpack decode failed; falling back to single-column JSON"
            );
            let text = decode_payload_to_json(payload);
            single_result_row(text)
        }
    }
}

/// Shape a `SingleDocument` / `MultiRow` response: decode to JSON, then
/// hand the parsed value to the pure [`shape_decoded_rows`] core.
///
/// Non-JSON scalar payloads (undecodable envelope) fall back to a single
/// "result" column, matching pgwire's single-row fallback.
fn shape_generic_rows(payload: &[u8], projection: Option<&OutputSchema>) -> ShapedRows {
    if payload.is_empty() {
        return empty_shaped();
    }
    let text = decode_payload_to_json(payload);
    match sonic_rs::from_str::<JsonValue>(&text) {
        Ok(value) => shape_decoded_rows(&value, projection),
        Err(_) => single_result_row(text),
    }
}

/// Pure shaping core: given an already-decoded Data-Plane JSON payload,
/// unwrap the `{id, data}` scan envelope via `push_flat_rows`, then either
/// select the named SELECT-list columns when a projection is given, or
/// derive the id-first column union across all rows when no named
/// projection applies.
///
/// Callers needing the composed materialized-shaping order (KV wrap, vector
/// translation, payload decode) should use [`shape_response_materialized`];
/// this function does none of that — it is the shared core both the
/// materialized path and a per-batch lazy streaming caller (native's
/// `emit_sql_stream`) call directly, since a streamed scan batch has no plan
/// to KV-wrap or vector-translate but still needs the same envelope-unwrap +
/// projection logic applied per batch.
pub fn shape_decoded_rows(decoded: &JsonValue, projection: Option<&OutputSchema>) -> ShapedRows {
    let mut rows = Vec::new();
    push_flat_rows(decoded.clone(), &mut rows);

    match projection {
        Some(s) if !s.is_star && !s.columns.is_empty() => {
            let lookup_keys: Vec<String> = s.columns.iter().map(|c| c.lookup_key.clone()).collect();
            let display_names: Vec<String> =
                s.columns.iter().map(|c| c.display_name.clone()).collect();
            let projected_rows = rows
                .iter()
                .map(|row| project_row(row, &lookup_keys, &display_names))
                .collect();
            // Carry each projected column's real catalog type, aligned in
            // order with `display_names`. Only the pgwire encoder consumes
            // these — mapping them to typed RowDescription OIDs and rendering
            // each cell in that type's PostgreSQL text form; native/http
            // ignore column types entirely.
            let column_types: Vec<DdlColType> = s.columns.iter().map(|c| c.ty).collect();
            ShapedRows {
                columns: display_names,
                column_types,
                rows: projected_rows,
                notice: None,
            }
        }
        _ => {
            // Star / derived columns come from JSON rows with no catalog type,
            // so they stay TEXT — typing them would regress `SELECT *` on
            // schemaless collections.
            let columns = derive_columns(&rows);
            let column_types = ShapedRows::text_types(columns.len());
            ShapedRows {
                columns,
                column_types,
                rows,
                notice: None,
            }
        }
    }
}

/// Select and rename one flat row's fields per the projection lists, trying
/// each candidate key in order: the full lookup key, then the bare
/// (post-dot) column name, then the SELECT alias.
fn project_row(
    row: &Map<String, JsonValue>,
    lookup_keys: &[String],
    display_names: &[String],
) -> Map<String, JsonValue> {
    let mut out = Map::new();
    for (i, lookup_key) in lookup_keys.iter().enumerate() {
        let bare = lookup_key
            .rfind('.')
            .map(|dot_pos| &lookup_key[dot_pos + 1..])
            .unwrap_or(lookup_key.as_str());
        let display_name = display_names
            .get(i)
            .map(String::as_str)
            .unwrap_or(lookup_key.as_str());
        let value = row
            .get(lookup_key.as_str())
            .or_else(|| {
                if bare != lookup_key {
                    row.get(bare)
                } else {
                    None
                }
            })
            .or_else(|| {
                if display_name != lookup_key.as_str() && display_name != bare {
                    row.get(display_name)
                } else {
                    None
                }
            })
            .cloned()
            .unwrap_or(JsonValue::Null);
        out.insert(display_name.to_string(), value);
    }
    out
}

/// Derive the id-first column union across all rows: `id` first (if
/// present), then each row's remaining keys in first-seen order.
fn derive_columns(rows: &[Map<String, JsonValue>]) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    if let Some(first) = rows.first() {
        if first.contains_key("id") {
            cols.push("id".to_string());
        }
        for key in first.keys() {
            if key != "id" && !is_reserved_bitemporal_column(key) {
                cols.push(key.clone());
            }
        }
    }
    for row in rows.iter().skip(1) {
        for key in row.keys() {
            if !is_reserved_bitemporal_column(key) && !cols.contains(key) {
                cols.push(key.clone());
            }
        }
    }
    cols
}

fn empty_shaped() -> ShapedRows {
    ShapedRows {
        columns: Vec::new(),
        column_types: Vec::new(),
        rows: Vec::new(),
        notice: None,
    }
}

/// Single "result" column with zero rows, matching `payload_to_response`'s
/// `ReturningRows` arm when the payload itself is empty.
fn single_result_column_empty() -> ShapedRows {
    ShapedRows {
        columns: vec!["result".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: Vec::new(),
        notice: None,
    }
}

fn single_result_row(text: String) -> ShapedRows {
    let mut map = Map::new();
    map.insert("result".to_string(), JsonValue::String(text));
    ShapedRows {
        columns: vec!["result".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![map],
        notice: None,
    }
}
