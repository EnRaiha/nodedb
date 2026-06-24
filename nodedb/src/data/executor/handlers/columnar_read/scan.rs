// SPDX-License-Identifier: BUSL-1.1

//! Scan-params struct and the base scan entry point.

use nodedb_types::surrogate_bitmap::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::expr_eval::ComputedColumn;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::scan_normalize::decoded_col_to_value;
use crate::data::executor::task::ExecutionTask;

use super::bitemporal::bitemporal_row_visible;
use super::convert::value_to_json;
use super::filter::row_matches_filters;
use super::sort::sort_rows_by_keys;

/// Parameters for a columnar base scan. Bundled as a struct because the
/// raw parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct ColumnarScanParams<'a> {
    pub collection: &'a str,
    pub projection: &'a [String],
    pub limit: usize,
    pub filters: &'a [u8],
    /// RLS filter bytes — wiring is the responsibility of a separate
    /// enforcement pass; the base scan handler itself does not consume
    /// them (hence the `_` destructure).
    #[allow(dead_code)]
    pub rls_filters: &'a [u8],
    pub sort_keys: &'a [(String, bool)],
    /// Bitemporal system-time selection. `Current` is a current-state read;
    /// `AsOf(ms)` drops rows with `_ts_system > ms`; `AllVersions` emits every
    /// `_ts_system` row ordered ascending (audit log), with the system-time
    /// column projected.
    pub system_time: nodedb_types::SystemTimeScope,
    /// Bitemporal valid-time point: drop rows whose
    /// `[_ts_valid_from, _ts_valid_until)` interval does not contain this
    /// point. `None` skips valid-time filtering entirely.
    pub valid_at_ms: Option<i64>,
    /// Optional cross-engine surrogate prefilter. When `Some`, the scan
    /// skips whole memtable blocks whose surrogate range does not intersect
    /// the bitmap (block boundary) and skips individual rows whose surrogate
    /// is absent from the bitmap (row boundary). `None` = no prefilter.
    pub prefilter: Option<&'a SurrogateBitmap>,
    /// MessagePack-serialized `Vec<ComputedColumn>` for scalar projection
    /// expressions such as JSON arrow operators. Empty slice means no
    /// computed columns are requested.
    pub computed_columns: &'a [u8],
}

impl CoreLoop {
    /// Execute a base columnar scan: flushed segments first, then the live
    /// memtable. Flushed rows are delete-bitmap filtered. Surrogates are not
    /// stored in segment bytes; when a prefilter is active, flushed segments
    /// are skipped entirely. See `scan_normalize::scan_columnar` for the
    /// parallel read path — keep both in sync on segment-iteration changes.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        params: ColumnarScanParams<'_>,
    ) -> Response {
        let ColumnarScanParams {
            collection,
            projection,
            limit,
            filters,
            rls_filters: _,
            sort_keys,
            system_time,
            valid_at_ms,
            prefilter,
            computed_columns,
        } = params;

        use nodedb_types::SystemTimeScope;
        let all_versions = system_time.is_all_versions();
        // AS OF SYSTEM TIME NULL must surface every version: do not apply a
        // system-time cutoff. `AsOf(ms)` applies the ceiling; `Current` is
        // unconstrained.
        let system_as_of_ms = match system_time {
            SystemTimeScope::Current | SystemTimeScope::AllVersions => None,
            SystemTimeScope::AsOf(ms) => Some(ms),
        };

        let computed_cols: Vec<ComputedColumn> = if !computed_columns.is_empty() {
            match zerompk::from_msgpack(computed_columns) {
                Ok(cols) => cols,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("computed_columns decode: {e}"),
                        },
                    );
                }
            }
        } else {
            Vec::new()
        };
        // A no-LIMIT SQL `SELECT * FROM <columnar>` arrives as
        // `limit == usize::MAX`. Capture that before the `limit == 0` rewrite
        // so the budget bound applies only to the unbounded path. Spatial
        // scans arrive with a finite `10000` and are therefore unaffected.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let unbounded = limit == usize::MAX;
        let limit = if limit == 0 {
            1000
        } else if unbounded {
            // Bound the materialized row count to a ceiling derived from the
            // memory budget (+1 row to detect "more exist") so the scan does
            // not pull the whole memtable into the `matched` Vec.
            super::super::scan_budget::fetch_limit_for(limit, 0, scan_budget_bytes)
        } else {
            limit
        };

        // Scan-quiesce gate.
        let _scan_guard =
            match self.acquire_scan_guard(task, task.request.tenant_id.as_u64(), collection) {
                Ok(g) => g,
                Err(resp) => return resp,
            };

        let engine_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );

        let engine = match self.columnar_engines.get(&engine_key) {
            Some(e) => e,
            None => {
                // Empty result for missing collection.
                return match response_codec::encode_json_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        let schema = engine.schema();

        let filter_predicates: Vec<ScanFilter> = if !filters.is_empty() {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Collect matched rows as (row_values, json_object) pairs. We keep
        // the raw `Vec<Value>` for sort-key comparison — the JSON form is
        // emitted only after ORDER BY + limit are applied. When no sort
        // is requested we short-circuit the limit enforcement inside the
        // loop to avoid materialising the entire memtable.
        let mut matched: Vec<(Vec<nodedb_types::value::Value>, serde_json::Value)> = Vec::new();
        let scan_budget = if sort_keys.is_empty() {
            limit.saturating_mul(10).max(limit)
        } else {
            usize::MAX
        };
        // Resolve hidden bitemporal column positions once; `None` means
        // the collection is not bitemporal, so the per-row filter is a
        // no-op regardless of `system_as_of_ms` / `valid_at_ms` values.
        let ts_system_idx = schema.columns.iter().position(|c| c.name == "_ts_system");
        let ts_valid_from_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "_ts_valid_from");
        let ts_valid_until_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "_ts_valid_until");

        // Block-boundary prefilter: if a prefilter is present and none of
        // the memtable's surrogates fall within the bitmap's [min, max]
        // range, the entire memtable block can be skipped before any row
        // decoding takes place.
        let block_skipped = if let Some(bitmap) = prefilter {
            if bitmap.is_empty() {
                true
            } else {
                let surrogates = engine.memtable_surrogates();
                // Compute the surrogate range of non-None entries in the memtable.
                let (mt_min, mt_max) = surrogates
                    .iter()
                    .flatten()
                    .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
                        (lo.min(s.0), hi.max(s.0))
                    });
                // If no surrogate was found (mt_min > mt_max) or the bitmap's
                // range lies entirely outside the memtable range, skip.
                if mt_min > mt_max {
                    // No surrogates in memtable — cannot apply block skip.
                    false
                } else {
                    let bm_min = bitmap.0.min().unwrap_or(0);
                    let bm_max = bitmap.0.max().unwrap_or(0);
                    // Disjoint ranges: bitmap entirely before or after memtable.
                    bm_max < mt_min || bm_min > mt_max
                }
            }
        } else {
            false
        };

        // ── Phase 1: flushed segments ───────────────────────────────────────
        // Read rows that were drained from the memtable during prior flushes.
        // These rows are older than anything in the current memtable.
        //
        // Surrogate note: surrogates are not serialised into segment bytes (they
        // are cleared from `memtable_surrogates` during flush). When a surrogate
        // prefilter is active we therefore skip flushed segments entirely —
        // we cannot verify per-row membership without a stored surrogate.
        // See the method-level doc comment for the full rationale.
        if prefilter.is_none()
            && let Some(segments) = self.columnar_flushed_segments.get(&engine_key)
        {
            for (seg_idx, seg_bytes) in segments.iter().enumerate() {
                if sort_keys.is_empty() && matched.len() >= limit {
                    break;
                }
                // Segment ids are 1-based (segment_id 0 is reserved for the
                // active memtable virtual segment). Mirror: materialize_scan.rs.
                let seg_id = seg_idx as u64 + 1;

                let reader = if let Some(ref reg) = self.quarantine_registry {
                    match crate::storage::quarantine::engines::open_segment_with_quarantine(
                        reg,
                        seg_bytes,
                        collection,
                        &seg_id.to_string(),
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                collection,
                                seg_id,
                                error = %e,
                                "execute_columnar_scan: failed to open flushed segment (quarantine); skipping"
                            );
                            continue;
                        }
                    }
                } else {
                    match nodedb_columnar::SegmentReader::open(seg_bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                collection,
                                seg_id,
                                error = %e,
                                "execute_columnar_scan: failed to open flushed segment; skipping"
                            );
                            continue;
                        }
                    }
                };

                let row_count = reader.row_count() as usize;
                let col_count = schema.columns.len();

                // Decode all columns for this segment up front.
                let mut decoded_cols = Vec::with_capacity(col_count);
                let mut decode_ok = true;
                for col_idx in 0..col_count {
                    match reader.read_column(col_idx) {
                        Ok(dc) => decoded_cols.push(dc),
                        Err(e) => {
                            tracing::warn!(
                                collection,
                                seg_id,
                                col_idx,
                                error = %e,
                                "execute_columnar_scan: column decode failed; skipping segment"
                            );
                            decode_ok = false;
                            break;
                        }
                    }
                }
                if !decode_ok {
                    continue;
                }

                // Fetch the delete bitmap for this segment once per segment.
                let delete_bm = engine.delete_bitmap(seg_id);

                for row_idx in 0..row_count {
                    // Skip tombstoned rows.
                    if delete_bm.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                        continue;
                    }

                    // Build the row as Vec<Value> using the shared decoder.
                    let row: Vec<nodedb_types::value::Value> = decoded_cols
                        .iter()
                        .map(|dc| decoded_col_to_value(dc, row_idx))
                        .collect();

                    if !bitemporal_row_visible(
                        &row,
                        ts_system_idx,
                        ts_valid_from_idx,
                        ts_valid_until_idx,
                        system_as_of_ms,
                        valid_at_ms,
                    ) {
                        continue;
                    }
                    if !filter_predicates.is_empty()
                        && !row_matches_filters(&row, schema, &filter_predicates)
                    {
                        continue;
                    }

                    let mut obj = serde_json::Map::new();
                    for (i, col_def) in schema.columns.iter().enumerate() {
                        let force_system_col = all_versions && col_def.name == "_ts_system";
                        if !projection.is_empty()
                            && !force_system_col
                            && !projection.iter().any(|p| p == &col_def.name)
                            && !computed_cols.iter().any(|cc| cc.alias == col_def.name)
                        {
                            continue;
                        }
                        if i < row.len() {
                            obj.insert(col_def.name.clone(), value_to_json(&row[i]));
                        }
                    }
                    if !computed_cols.is_empty() {
                        let doc_val =
                            nodedb_types::Value::from(serde_json::Value::Object(obj.clone()));
                        for cc in &computed_cols {
                            let existing = obj.get(&cc.alias);
                            if matches!(existing, Some(v) if !v.is_null()) {
                                continue;
                            }
                            obj.insert(
                                cc.alias.clone(),
                                serde_json::Value::from(cc.expr.eval(&doc_val)),
                            );
                        }
                        if !projection.is_empty() {
                            obj.retain(|k, _| {
                                projection.iter().any(|p| p == k)
                                    || computed_cols.iter().any(|cc| &cc.alias == k)
                                    || (all_versions && k == "_ts_system")
                            });
                        }
                    }
                    matched.push((row, serde_json::Value::Object(obj)));
                    if sort_keys.is_empty() && matched.len() >= limit {
                        break;
                    }
                }
            }
        }

        // ── Phase 2: live memtable ──────────────────────────────────────────
        // Rows still in the active memtable (not yet flushed).
        // Reduce the over-fetch budget by however many rows flushed segments
        // already contributed so we do not materialise more than needed.
        let memtable_budget = scan_budget.saturating_sub(matched.len());
        if !block_skipped && memtable_budget > 0 {
            for (row_surrogate, row) in engine
                .scan_memtable_rows_with_surrogates()
                .take(memtable_budget)
            {
                // Row-boundary prefilter: skip this row when its surrogate is
                // absent from the bitmap. Rows without a recorded surrogate
                // (legacy / test paths) are always included when no prefilter
                // is active; when a prefilter is active they are excluded
                // because the surrogate identity is unknown.
                if let Some(bitmap) = prefilter {
                    match row_surrogate {
                        Some(s) if bitmap.contains(s) => {}
                        _ => continue,
                    }
                }

                if !bitemporal_row_visible(
                    &row,
                    ts_system_idx,
                    ts_valid_from_idx,
                    ts_valid_until_idx,
                    system_as_of_ms,
                    valid_at_ms,
                ) {
                    continue;
                }
                if !filter_predicates.is_empty()
                    && !row_matches_filters(&row, schema, &filter_predicates)
                {
                    continue;
                }
                let mut obj = serde_json::Map::new();
                for (i, col_def) in schema.columns.iter().enumerate() {
                    // Under all-versions (audit log) the system-time column is
                    // always projected so callers can order/inspect history.
                    let force_system_col = all_versions && col_def.name == "_ts_system";
                    if !projection.is_empty()
                        && !force_system_col
                        && !projection.iter().any(|p| p == &col_def.name)
                        && !computed_cols.iter().any(|cc| cc.alias == col_def.name)
                    {
                        continue;
                    }
                    if i < row.len() {
                        obj.insert(col_def.name.clone(), value_to_json(&row[i]));
                    }
                }
                if !computed_cols.is_empty() {
                    let doc_val = nodedb_types::Value::from(serde_json::Value::Object(obj.clone()));
                    for cc in &computed_cols {
                        let existing = obj.get(&cc.alias);
                        if matches!(existing, Some(v) if !v.is_null()) {
                            continue;
                        }
                        obj.insert(
                            cc.alias.clone(),
                            serde_json::Value::from(cc.expr.eval(&doc_val)),
                        );
                    }
                    // Remove base columns that were only fetched to serve as
                    // expression inputs but are not in the requested projection.
                    if !projection.is_empty() {
                        obj.retain(|k, _| {
                            projection.iter().any(|p| p == k)
                                || computed_cols.iter().any(|cc| &cc.alias == k)
                                || (all_versions && k == "_ts_system")
                        });
                    }
                }
                matched.push((row, serde_json::Value::Object(obj)));
                if sort_keys.is_empty() && matched.len() >= limit {
                    break;
                }
            }
        }

        if !sort_keys.is_empty() {
            matched.sort_by(|(a, _), (b, _)| sort_rows_by_keys(a, b, schema, sort_keys));
        } else if all_versions {
            // Audit-log order: ascending by system time. The hidden
            // `_ts_system` column index was resolved above.
            matched.sort_by(|(a, _), (b, _)| {
                super::bitemporal::row_system_time(a, ts_system_idx)
                    .cmp(&super::bitemporal::row_system_time(b, ts_system_idx))
            });
        }

        let results: Vec<serde_json::Value> =
            matched.into_iter().take(limit).map(|(_, j)| j).collect();

        let payload = match response_codec::encode_json_vec(&results) {
            Ok(payload) => payload,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Bound an unbounded (no-LIMIT) scan by the memory budget. The encoded
        // msgpack payload is the authoritative size of the materialized result;
        // surface a deterministic error if it exceeds the budget rather than
        // silently truncating. Spatial scans are bounded (finite limit) and so
        // skip this check.
        if unbounded && super::super::scan_budget::budget_exceeded(payload.len(), scan_budget_bytes)
        {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        self.response_with_payload(task, payload)
    }
}
