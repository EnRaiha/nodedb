// SPDX-License-Identifier: BUSL-1.1

//! Composed, protocol-neutral materialized response shaping.
//!
//! `shape_response_materialized` reproduces pgwire's per-payload shaping
//! order (`apply_kv_wrap` -> `translate_if_vector` -> decode -> scan-envelope
//! unwrap -> optional SELECT-list projection), exactly as traced in
//! `pgwire::handler::routing::execute::dispatch_task_loop` and
//! `pgwire::handler::projection`, as a single call. It exists for callers —
//! today, only the native protocol dispatch loop — that need one materialized
//! shot of shaping rather than pgwire's own two-seam pipeline.
//!
//! pgwire itself is NOT routed through this function: its lazy streaming
//! projection path (`reproject_response`) and its `SELECT *` path
//! (`reproject_star_response`) stay exactly as they are, both for byte-for-
//! byte wire compatibility and because the lazy path must not be
//! materialized. This module only shares the per-step LOGIC (via
//! `apply_kv_wrap`, `translate_if_vector`, `push_flat_rows`,
//! `parse_select_projection` and friends) with pgwire — it does not touch
//! pgwire's call sites.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::response_translate::vector::translate_if_vector;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{
    ArraySliceResponse, RowsPayload, decode_payload_to_json,
};
use nodedb_types::{DatabaseId, NodeDbError, TenantId};

use super::kv::apply_kv_wrap;
use super::project::{
    ProjectionItem, lookup_keys_for_projection, needs_projection, push_flat_rows,
};
use super::types::{PlanKind, ShapedRows};

/// NOTICE text for an `AS OF SYSTEM TIME` cutoff older than the oldest
/// retained tile version. Must stay byte-identical to pgwire's private
/// `TRUNCATED_BEFORE_HORIZON_NOTICE` in
/// `pgwire::handler::plan` — duplicated here rather than shared because
/// pgwire's copy is intentionally left untouched.
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
/// exactly the transforms pgwire's materialized path applies, in the same
/// order: KV point-get wrap, vector surrogate->PK translation, payload
/// decode, scan-envelope unwrap, and (when `projection` names columns)
/// SELECT-list column selection.
pub fn shape_response_materialized(
    payload: &[u8],
    plan: &PhysicalPlan,
    plan_kind: PlanKind,
    projection: Option<&[ProjectionItem]>,
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
    // (apply_kv_wrap -> translate_if_vector) before any decode/shape step.
    let wrapped = apply_kv_wrap(plan, payload);
    let translated = translate_if_vector(&wrapped, plan, state, database_id, tenant_id);

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

/// Shape an `ArrayOp::Slice` response: decode the `ArraySliceResponse`
/// envelope (falling back to a plain payload decode for legacy shapes,
/// mirroring `payload_to_response`'s `ArraySlice` arm), unwrap the row
/// envelope, and surface `truncated_before_horizon` as a notice.
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
/// (already TEXT-formatted cells), mirroring `payload_to_response`'s
/// `ReturningRows` arm including its malformed-payload fallback.
fn shape_returning_rows(payload: &[u8]) -> ShapedRows {
    if payload.is_empty() {
        return empty_shaped();
    }
    match zerompk::from_msgpack::<RowsPayload>(payload) {
        Ok(rp) => {
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
            ShapedRows {
                columns: rp.columns,
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
fn shape_generic_rows(payload: &[u8], projection: Option<&[ProjectionItem]>) -> ShapedRows {
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
/// select the named SELECT-list columns (mirroring
/// `pgwire::handler::projection::reproject_response`'s column-selection
/// logic) or derive the id-first column union (mirroring
/// `reproject_star_response`) when no named projection applies.
///
/// Callers needing the composed materialized-shaping order (KV wrap, vector
/// translation, payload decode) should use [`shape_response_materialized`];
/// this function does none of that — it is the shared core both the
/// materialized path and a per-batch lazy streaming caller (native's
/// `emit_sql_stream`) call directly, since a streamed scan batch has no plan
/// to KV-wrap or vector-translate but still needs the same envelope-unwrap +
/// projection logic applied per batch.
pub fn shape_decoded_rows(
    decoded: &JsonValue,
    projection: Option<&[ProjectionItem]>,
) -> ShapedRows {
    let mut rows = Vec::new();
    push_flat_rows(decoded.clone(), &mut rows);

    match projection {
        Some(items) if needs_projection(items) => {
            let lookup_keys = lookup_keys_for_projection(items);
            let display_names: Vec<String> = items
                .iter()
                .filter_map(|i| match i {
                    ProjectionItem::Named { display_name, .. } => Some(display_name.clone()),
                    ProjectionItem::Star => None,
                })
                .collect();
            let projected_rows = rows
                .iter()
                .map(|row| project_row(row, &lookup_keys, &display_names))
                .collect();
            ShapedRows {
                columns: display_names,
                rows: projected_rows,
                notice: None,
            }
        }
        _ => {
            let columns = derive_columns(&rows);
            ShapedRows {
                columns,
                rows,
                notice: None,
            }
        }
    }
}

/// Select and rename one flat row's fields per the projection lists, using
/// the same fallback order as
/// `pgwire::handler::projection::encode_projected_row`: full lookup key,
/// then the bare (post-dot) column name, then the SELECT alias.
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

/// Derive the id-first column union across all rows, matching
/// `reproject_star_response`'s column-ordering rule exactly.
fn derive_columns(rows: &[Map<String, JsonValue>]) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    if let Some(first) = rows.first() {
        if first.contains_key("id") {
            cols.push("id".to_string());
        }
        for key in first.keys() {
            if key != "id" {
                cols.push(key.clone());
            }
        }
    }
    for row in rows.iter().skip(1) {
        for key in row.keys() {
            if !cols.contains(key) {
                cols.push(key.clone());
            }
        }
    }
    cols
}

fn empty_shaped() -> ShapedRows {
    ShapedRows {
        columns: Vec::new(),
        rows: Vec::new(),
        notice: None,
    }
}

fn single_result_row(text: String) -> ShapedRows {
    let mut map = Map::new();
    map.insert("result".to_string(), JsonValue::String(text));
    ShapedRows {
        columns: vec!["result".to_string()],
        rows: vec![map],
        notice: None,
    }
}
