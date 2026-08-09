// SPDX-License-Identifier: BUSL-1.1

//! Scan-params struct and the base scan entry point.

use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::surrogate_bitmap::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::expr_eval::ComputedColumn;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

use super::bitemporal::bitemporal_row_visible;
use super::filter::row_matches_filters;
use super::sort::{compare_sort_values, eval_row_sort_values};

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
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
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
    /// The in-transaction identity of the caller, when the scan is issued
    /// inside `BEGIN..COMMIT`. `Some` gates a post-scan overlay merge
    /// (`merge_overlay_into_columnar_scan`) so the scan observes the
    /// transaction's own staged, not-yet-durable `ColumnarOp::Insert` rows
    /// (read-your-own-writes). `None` for autocommit reads.
    pub txn_id: Option<crate::types::TxnId>,
}

impl CoreLoop {
    /// Execute a base columnar scan: flushed segments first, then the live
    /// memtable. Flushed rows are delete-bitmap filtered. Surrogates are not
    /// stored in segment bytes but are retained in the in-memory
    /// `columnar_flushed_surrogates` sidecar (lockstep with the segment-bytes
    /// map); when a prefilter is active, flushed rows are filtered per-row by
    /// surrogate membership exactly like the live-memtable phase. See
    /// `scan_normalize::scan_columnar` for the parallel read path — keep both
    /// in sync on segment-iteration changes.
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
            txn_id,
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
                return match response_codec::encode_json_vec_as_msgpack(&[]) {
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
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed scan filters: {e}"),
                        },
                    );
                }
            }
        } else {
            Vec::new()
        };

        // Collect matched rows as (row_values, json_object) pairs. We keep
        // the raw `Vec<Value>` for sort-key comparison — the JSON form is
        // emitted only after ORDER BY + limit are applied. When no sort
        // is requested we short-circuit the limit enforcement inside the
        // loop to avoid materialising the entire memtable.
        let mut matched: Vec<(
            Option<nodedb_types::Surrogate>,
            Vec<nodedb_types::value::Value>,
            serde_json::Value,
        )> = Vec::new();
        let scan_budget = if sort_keys.is_empty() {
            limit.saturating_mul(10).max(limit)
        } else {
            usize::MAX
        };
        // Resolve hidden bitemporal column positions once; `None` means
        // the collection is not bitemporal, so the per-row filter is a
        // no-op regardless of `system_as_of_ms` / `valid_at_ms` values.
        let ts_system_idx = schema.columns.iter().position(|c| c.name == TS_SYSTEM);
        let ts_valid_from_idx = schema.columns.iter().position(|c| c.name == TS_VALID_FROM);
        let ts_valid_until_idx = schema.columns.iter().position(|c| c.name == TS_VALID_UNTIL);

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
                    // The bitmap is non-empty (guarded above); min/max are
                    // always `Some`. Pattern-match to avoid `unwrap` in
                    // non-test code while keeping the compiler honest.
                    match (bitmap.0.min(), bitmap.0.max()) {
                        (Some(bm_min), Some(bm_max)) => {
                            // Disjoint ranges: bitmap entirely before or after memtable.
                            bm_max < mt_min || bm_min > mt_max
                        }
                        // Unreachable: `is_empty()` was already checked above.
                        _ => false,
                    }
                }
            }
        } else {
            false
        };

        // ── Phase 1: flushed segments ───────────────────────────────────────
        // Read rows that were drained from the memtable during prior flushes.
        // These rows are older than anything in the current memtable.
        //
        // Surrogate note: surrogates are not serialised into segment bytes, but
        // they ARE retained in the in-memory `columnar_flushed_surrogates`
        // sidecar (kept in lockstep with `columnar_flushed_segments`). When a
        // surrogate prefilter is active we therefore scan flushed segments and
        // apply a per-row membership test below, mirroring the live-memtable
        // phase. See the method-level doc comment for the full rationale.
        if let Err(e) = self.scan_flushed_columnar_segments(
            super::scan_flushed::FlushedScanCtx {
                collection,
                engine_key: &engine_key,
                schema,
                projection,
                limit,
                sort_keys,
                filter_predicates: &filter_predicates,
                prefilter,
                computed_cols: &computed_cols,
                all_versions,
                system_as_of_ms,
                valid_at_ms,
                ts_system_idx,
                ts_valid_from_idx,
                ts_valid_until_idx,
            },
            &mut matched,
        ) {
            // `scan_flushed_columnar_segments` returns `crate::Result<()>`,
            // so `e` already carries the real typed error (e.g. a
            // division/modulo-by-zero in `filter_predicates`) — propagate
            // it as-is via `From<crate::Error> for ErrorCode` instead of
            // collapsing it to `Internal`/`XX000`, mirroring
            // `document/read/scan.rs`'s `Err(e) => self.response_error(task, e)`.
            return self.response_error(task, e);
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
                if !filter_predicates.is_empty() {
                    match row_matches_filters(&row, schema, &filter_predicates) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        // `row_matches_filters` returns `EvalError`, which
                        // has exactly one variant and no direct
                        // `Into<ErrorCode>` — mirrors
                        // `stage_columnar_dml.rs`'s identical call site,
                        // which hardcodes the same typed code rather than
                        // collapsing to `Internal`/`XX000`.
                        Err(_e) => {
                            return self.response_error(task, ErrorCode::DivisionByZero);
                        }
                    }
                }
                let obj = match super::convert::row_to_projected_json(
                    &row,
                    schema,
                    projection,
                    &computed_cols,
                    all_versions,
                ) {
                    Ok(v) => v,
                    // `row_to_projected_json` returns `crate::Result<_>`
                    // (unlike `row_matches_filters` above) — its only
                    // fallible step is a computed-column expression eval,
                    // and `computed_cols` here is real, not `&[]` like the
                    // DML staging path, so propagate the actual typed error
                    // rather than assuming which one it is.
                    Err(e) => {
                        return self.response_error(task, e);
                    }
                };
                matched.push((row_surrogate, row, obj));
                if sort_keys.is_empty() && matched.len() >= limit {
                    break;
                }
            }
        }

        // In-transaction read-your-own-writes: fold this transaction's staged
        // `ColumnarOp::Insert` rows into the base result. Gated on `txn_id`
        // (autocommit reads never carry one) and skipped for temporal reads —
        // staged bodies represent the current version only, matching the
        // Document engine's overlay-merge convention.
        if let Some(txn_id) = txn_id
            && !all_versions
            && system_as_of_ms.is_none()
        {
            let coll_key = (
                task.request.database_id,
                task.request.tenant_id,
                collection.to_string(),
            );
            if let Err(e) = self.merge_overlay_into_columnar_scan(
                crate::data::executor::handlers::transaction::overlay::ColumnarOverlayMergeParams {
                    txn_id,
                    coll_key: &coll_key,
                    schema,
                    projection,
                    filter_predicates: &filter_predicates,
                    computed_cols: &computed_cols,
                    all_versions,
                },
                &mut matched,
            ) {
                // `merge_overlay_into_columnar_scan` returns
                // `crate::Result<()>` (the overlay's own residual-predicate
                // re-check can divide/modulo by zero) — propagate the typed
                // error directly, matching both `document/read/scan.rs`'s
                // generic `self.response_error(task, e)` pattern and
                // `stage_columnar_dml.rs`'s identical call to this same
                // function (`Err(e) => return Err(self.response_error(task, e))`).
                return self.response_error(task, e);
            }
        }

        if !sort_keys.is_empty() {
            // Sort keys are expressions, so evaluating them can fail. Evaluate
            // every row's keys first — `sort_by` has no way to report an error
            // — then order by the results.
            let mut keyed = Vec::with_capacity(matched.len());
            for (_, row, _) in &matched {
                match eval_row_sort_values(row, schema, sort_keys) {
                    Ok(values) => keyed.push(values),
                    Err(e) => return self.response_error(task, e),
                }
            }
            let mut order: Vec<usize> = (0..matched.len()).collect();
            order.sort_by(|&a, &b| compare_sort_values(&keyed[a], &keyed[b], sort_keys));
            let mut reordered: Vec<_> = order
                .into_iter()
                .map(|i| matched[i].clone())
                .collect::<Vec<_>>();
            std::mem::swap(&mut matched, &mut reordered);
        } else if all_versions {
            // Audit-log order: ascending by system time. The hidden
            // `_ts_system` column index was resolved above.
            matched.sort_by(|(_, a, _), (_, b, _)| {
                super::bitemporal::row_system_time(a, ts_system_idx)
                    .cmp(&super::bitemporal::row_system_time(b, ts_system_idx))
            });
        }

        let results: Vec<serde_json::Value> =
            matched.into_iter().take(limit).map(|(_, _, j)| j).collect();

        let payload = match response_codec::encode_json_vec_as_msgpack(&results) {
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

#[cfg(test)]
mod tests {
    //! Cross-engine prefilter coverage for FLUSHED plain-columnar segments.
    //!
    //! These tests prove the silent-wrong-results fix: rows that live only in a
    //! flushed segment (drained out of the live memtable) are now visible to a
    //! prefiltered scan and are filtered per-row by their cross-engine
    //! surrogate, instead of being skipped wholesale. Each test forces a real
    //! flush — drain the memtable, encode a `SegmentWriter` segment, push the
    //! bytes + the captured surrogate sidecar in lockstep, then `on_memtable_flushed`
    //! clears the memtable — so the rows truly exist only in the flushed segment
    //! before the scan runs. This mirrors the production flush block in
    //! `handlers/columnar_write/insert.rs`.

    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_columnar::MutationEngine;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    use super::ColumnarScanParams;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .expect("valid schema")
    }

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            // Plan is irrelevant: `execute_columnar_scan` is called directly
            // with `ColumnarScanParams` and only reads `database_id`/`tenant_id`.
            plan: PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Insert `(id, name, surrogate)` rows into a fresh engine, then run the
    /// EXACT production flush sequence so the rows end up only in a flushed
    /// segment. Registers the engine + populates both lockstep maps on `core`.
    /// Returns the engine key.
    fn insert_and_flush(
        core: &mut CoreLoop,
        collection: &str,
        rows: &[(i64, &str, Surrogate)],
    ) -> (DatabaseId, TenantId, String) {
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            collection.to_string(),
        );
        let mut engine = MutationEngine::new(collection.to_string(), schema());
        for (id, name, surr) in rows {
            engine
                .insert_with_surrogate(&[Value::Integer(*id), Value::String((*name).into())], *surr)
                .expect("insert_with_surrogate");
        }

        // ── Mirror handlers/columnar_write/insert.rs flush block ────────────
        let new_segment_id = engine.next_segment_id();
        let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
        // Capture surrogates BEFORE `on_memtable_flushed` clears them.
        let flushed_surrogates: Vec<Option<Surrogate>> = engine.memtable_surrogates().to_vec();
        assert_eq!(
            row_count,
            flushed_surrogates.len(),
            "drained row_count must equal captured surrogate count"
        );
        let bytes = nodedb_columnar::SegmentWriter::plain()
            .write_segment(&seg_schema, &columns, row_count, None)
            .expect("write_segment");
        // Lockstep push: both maps, same key, same order.
        core.columnar_flushed_segments
            .entry(key.clone())
            .or_default()
            .push(bytes);
        core.columnar_flushed_surrogates
            .entry(key.clone())
            .or_default()
            .push(flushed_surrogates);
        engine
            .on_memtable_flushed(new_segment_id)
            .expect("on_memtable_flushed");

        // After flush the memtable is empty: rows live ONLY in the segment.
        assert_eq!(
            engine.memtable_surrogates().len(),
            0,
            "memtable surrogates cleared after flush"
        );
        core.columnar_engines.insert(key.clone(), engine);
        key
    }

    /// Run a prefiltered scan and return the decoded result rows.
    fn scan_with_prefilter(
        core: &mut CoreLoop,
        collection: &str,
        prefilter: Option<&SurrogateBitmap>,
    ) -> Vec<serde_json::Value> {
        let task = make_task();
        let params = ColumnarScanParams {
            collection,
            projection: &[],
            limit: 0,
            filters: &[],
            rls_filters: &[],
            sort_keys: &[],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter,
            computed_columns: &[],
            txn_id: None,
        };
        let resp = core.execute_columnar_scan(&task, params);
        let decoded: Vec<nodedb_types::JsonValue> =
            zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode scan payload");
        decoded.into_iter().map(|j| j.0).collect()
    }

    fn ids(rows: &[serde_json::Value]) -> Vec<i64> {
        let mut v: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.get("id").and_then(|x| x.as_i64()))
            .collect();
        v.sort_unstable();
        v
    }

    /// A prefilter that includes SOME flushed-row surrogates must return
    /// exactly those rows — proving flushed rows are now prefiltered, not
    /// skipped wholesale.
    #[test]
    fn flushed_rows_are_prefiltered_not_skipped() {
        let (mut core, _dir) = make_core();
        let coll = "cf_some";
        insert_and_flush(
            &mut core,
            coll,
            &[
                (1, "a", Surrogate(101)),
                (2, "b", Surrogate(102)),
                (3, "c", Surrogate(103)),
            ],
        );

        let mut bitmap = SurrogateBitmap::new();
        bitmap.insert(Surrogate(101));
        bitmap.insert(Surrogate(103));

        let rows = scan_with_prefilter(&mut core, coll, Some(&bitmap));
        assert_eq!(
            ids(&rows),
            vec![1, 3],
            "only rows whose surrogate is in the bitmap come back"
        );
    }

    /// A prefilter that includes NONE of the surrogates must return zero rows.
    #[test]
    fn flushed_rows_empty_prefilter_returns_nothing() {
        let (mut core, _dir) = make_core();
        let coll = "cf_none";
        insert_and_flush(
            &mut core,
            coll,
            &[(1, "a", Surrogate(201)), (2, "b", Surrogate(202))],
        );

        let mut bitmap = SurrogateBitmap::new();
        bitmap.insert(Surrogate(999)); // no overlap

        let rows = scan_with_prefilter(&mut core, coll, Some(&bitmap));
        assert!(
            rows.is_empty(),
            "no surrogate matches => zero rows, got {}",
            rows.len()
        );
    }

    /// Sanity: with NO prefilter all flushed rows are returned (unchanged
    /// behaviour — the gate only applies when a prefilter is present).
    #[test]
    fn flushed_rows_no_prefilter_returns_all() {
        let (mut core, _dir) = make_core();
        let coll = "cf_all";
        insert_and_flush(
            &mut core,
            coll,
            &[
                (1, "a", Surrogate(301)),
                (2, "b", Surrogate(302)),
                (3, "c", Surrogate(303)),
            ],
        );

        let rows = scan_with_prefilter(&mut core, coll, None);
        assert_eq!(ids(&rows), vec![1, 2, 3]);
    }

    /// Lockstep invariant: after a flush the two maps are equal-length per key
    /// and the inner surrogate Vec length equals the segment row count.
    #[test]
    fn lockstep_lengths_match_after_flush() {
        let (mut core, _dir) = make_core();
        let coll = "cf_lockstep";
        let rows = [
            (1, "a", Surrogate(401)),
            (2, "b", Surrogate(402)),
            (3, "c", Surrogate(403)),
            (4, "d", Surrogate(404)),
        ];
        let key = insert_and_flush(&mut core, coll, &rows);

        let segs = core
            .columnar_flushed_segments
            .get(&key)
            .expect("segments present");
        let surrs = core
            .columnar_flushed_surrogates
            .get(&key)
            .expect("surrogate sidecar present");
        assert_eq!(
            segs.len(),
            surrs.len(),
            "outer Vec lengths must be equal (lockstep)"
        );
        assert_eq!(segs.len(), 1, "exactly one flushed segment");
        assert_eq!(
            surrs[0].len(),
            rows.len(),
            "inner per-row surrogate Vec length must equal segment row count"
        );
    }
}
