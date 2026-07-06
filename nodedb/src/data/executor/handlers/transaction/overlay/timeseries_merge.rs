// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base timeseries RAW scan
//! result, so an in-transaction raw timeseries `SELECT` observes the
//! transaction's own uncommitted `TimeseriesOp::Ingest` rows
//! (read-your-own-writes).
//!
//! Additive-only merge — this is the deliberate divergence from
//! [`super::columnar_merge`]. A columnar base row carries a cross-engine
//! surrogate, so the columnar merge can supersede / tombstone an existing base
//! row by surrogate. A timeseries base row has NO surrogate identity in the
//! scan (it is keyed internally by `series_id`, a measurement + tag hash), so
//! there is no base row to supersede or tombstone: RYOW for timeseries is
//! INSERT-only, and the merge simply APPENDS each staged `Put` row that
//! satisfies the scan's time-range and WHERE predicate. A staged `Tombstone`
//! (only reachable via a future in-transaction timeseries DELETE, not staged
//! today) has no base row to remove and is skipped.
//!
//! Only invoked on the RAW-scan branch (`execute_ts_raw_scan`); the
//! aggregate / time-bucket branch (`execute_ts_aggregate`) is committed-only
//! and never merges the overlay (continuous-aggregate correctness).

use nodedb_types::value::Value;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Inputs for [`CoreLoop::merge_overlay_into_timeseries_scan`].
pub(in crate::data::executor) struct TimeseriesOverlayMergeParams<'a> {
    pub txn_id: TxnId,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    /// `(min_ts_ms, max_ts_ms)` — same inclusive range the base memtable /
    /// partition scan applied. `(0, i64::MAX)` = no time filter.
    pub time_range: (i64, i64),
    pub filter_predicates: &'a [ScanFilter],
    pub has_filters: bool,
    /// Row ceiling for the whole scan (base + staged). Staged rows are only
    /// appended while the result is below this bound, so the merge never
    /// exceeds the SQL `LIMIT`.
    pub limit: usize,
}

/// Decode a staged timeseries row body (a `Value::Object` field map encoded by
/// `stage_timeseries_insert` via `nodedb_types::value_to_msgpack`). Returns
/// `None` for a body that fails to decode or is not an object — defensively
/// treated as "does not match" rather than surfacing a panic.
fn decode_staged_row(body: &[u8]) -> Option<Value> {
    match nodedb_types::value_from_msgpack(body) {
        Ok(v @ Value::Object(_)) => Some(v),
        _ => None,
    }
}

/// Extract the timestamp value (ms) from a staged row map, matching the time
/// column under any alias the planner's time-range extractor recognizes
/// (`ts` / `timestamp` / `time`; see the Control Plane `extract_time_range`).
/// Rows staged verbatim keep the INSERT's own column name, so the lookup must
/// try every alias. Rows without a numeric timestamp fall outside every
/// bounded range and are treated as non-matching by the caller.
fn row_timestamp_ms(row: &Value) -> Option<i64> {
    let Value::Object(map) = row else {
        return None;
    };
    for key in ["timestamp", "ts", "time"] {
        match map.get(key) {
            Some(Value::Integer(ms)) => return Some(*ms),
            Some(Value::Float(ms)) => return Some(*ms as i64),
            _ => {}
        }
    }
    None
}

/// Convert a decoded staged row (`Value::Object`) into the `rmpv::Value::Map`
/// row shape the base raw scan emits, so a merged staged row is
/// indistinguishable from a base row downstream (computed columns, encoding).
fn staged_row_to_rmpv(row: &Value) -> rmpv::Value {
    let Value::Object(map) = row else {
        return rmpv::Value::Nil;
    };
    let fields: Vec<(rmpv::Value, rmpv::Value)> = map
        .iter()
        .map(|(k, v)| (rmpv::Value::String(k.as_str().into()), scalar_to_rmpv(v)))
        .collect();
    rmpv::Value::Map(fields)
}

/// Scalar `nodedb_types::Value` → `rmpv::Value`, matching the raw scan's own
/// value emission (`row_emit::nodedb_value_to_rmpv`).
fn scalar_to_rmpv(v: &Value) -> rmpv::Value {
    match v {
        Value::Integer(n) => rmpv::Value::Integer((*n).into()),
        Value::Float(f) => rmpv::Value::F64(*f),
        Value::String(s) => rmpv::Value::String(s.as_str().into()),
        Value::Bool(b) => rmpv::Value::Boolean(*b),
        _ => rmpv::Value::Nil,
    }
}

impl CoreLoop {
    /// Append this transaction's staged `TimeseriesOp::Ingest` rows to
    /// `results` (base raw-scan rows), each subject to the scan's time-range,
    /// WHERE predicate, and row limit. No-op when the transaction has no
    /// overlay entries for this collection.
    pub(in crate::data::executor) fn merge_overlay_into_timeseries_scan(
        &self,
        params: TimeseriesOverlayMergeParams<'_>,
        results: &mut Vec<rmpv::Value>,
    ) {
        let TimeseriesOverlayMergeParams {
            txn_id,
            coll_key,
            time_range,
            filter_predicates,
            has_filters,
            limit,
        } = params;

        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        for (_surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if results.len() >= limit {
                break;
            }
            // A base timeseries row carries no surrogate, so a staged
            // `Tombstone` has nothing to remove here — skip it. Only staged
            // inserts (`Put`) contribute new rows.
            let Staged::Put(body) = staged else {
                continue;
            };
            let Some(row) = decode_staged_row(body) else {
                continue;
            };

            // Time-range prune, mirroring the base memtable scan's
            // `timestamp_range_filter` (inclusive bounds).
            match row_timestamp_ms(&row) {
                Some(ts) if ts >= time_range.0 && ts <= time_range.1 => {}
                _ => continue,
            }

            // Re-apply the scan's WHERE predicate on the staged body (already
            // msgpack), exactly like the raw scan's `need_json_filter` path
            // does per base row.
            if has_filters && !filter_predicates.iter().all(|f| f.matches_binary(body)) {
                continue;
            }

            results.push(staged_row_to_rmpv(&row));
        }
    }
}
