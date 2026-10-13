// SPDX-License-Identifier: BUSL-1.1

//! KV Scan handler and filter extraction.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{FilterOp, ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::scan_budget;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvScanParams;
use crate::engine::kv::current_ms;

/// Parameters for the KV SCAN handler.
pub(in crate::data::executor) struct KvScanHandlerParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub cursor: &'a [u8],
    pub count: usize,
    pub match_pattern: Option<&'a str>,
    pub filters: &'a [u8],
    pub projection: &'a [String],
    pub computed_columns_bytes: &'a [u8],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub surrogate_ceiling: Option<u32>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_scan(
        &self,
        task: &ExecutionTask,
        params: KvScanHandlerParams<'_>,
    ) -> Response {
        let KvScanHandlerParams {
            did,
            tid,
            collection,
            cursor,
            count,
            match_pattern,
            filters,
            projection,
            computed_columns_bytes,
            sort_keys,
            surrogate_ceiling,
        } = params;

        debug!(core = self.core_id, %collection, count, "kv scan");

        // Scan-quiesce gate: refuse new scans against a draining
        // collection so the purge handler can unlink on-disk files
        // without racing an in-flight reader.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let now_ms = current_ms();

        // A no-LIMIT SQL `SELECT * FROM <kv>` arrives as `count == usize::MAX`.
        // Bound the engine fetch to a row ceiling derived from the per-query
        // memory budget (+1 row to detect "more exist") so the materialized
        // `Vec` cannot grow to the whole collection. The RESP cursor
        // pagination path always carries a finite `count`, so it is unaffected.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let unbounded = count == usize::MAX;
        let fetch_count = if unbounded {
            // KV scans have no row offset (pagination is cursor-based).
            scan_budget::fetch_limit_for(count, 0, scan_budget_bytes)
        } else {
            count
        };

        // A malformed filter payload is an error. Reading it as "no filter"
        // would return every row of the collection.
        let filter_predicates = match decode_scan_filters(filters, "kv scan filter") {
            Ok(predicates) => predicates,
            Err(e) => return self.response_error(task, e),
        };

        // Index pushdown for one equality conjunct. The post-scan filter
        // below still checks every predicate on every row.
        let probe = eq_index_probe(&filter_predicates);
        let (mut entries, _next_cursor) = self.kv_engine.scan(KvScanParams {
            database_id: did,
            tenant_id: tid,
            collection,
            cursor,
            count: fetch_count,
            now_ms,
            match_pattern,
            filter_field: probe.map(|(field, _)| field),
            filter_value: probe.map(|(_, value)| value),
            surrogate_ceiling,
        });

        // Read-your-own-writes: fold this transaction's staged KV writes
        // into the base scan result before the shared filter/sort/encode
        // pipeline below, which then treats merged and base rows alike.
        // `matches` always accepts here -- the per-entry filter loop further
        // down re-applies `filter_predicates` uniformly to every row
        // (base or merged), so there is no need to duplicate that check
        // in the merge itself.
        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                crate::types::DatabaseId::new(did),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_kv_overlay_into_scan(
                txn_id,
                &coll_key,
                &mut entries,
                &|_key: &[u8], _value: &[u8]| true,
            );
        }

        // Bound an unbounded (no-LIMIT) scan by the memory budget. Sum the raw
        // key+value bytes and surface a deterministic error if the result would
        // exceed the budget rather than silently truncating it.
        if unbounded {
            let total = entries.iter().fold(0usize, |acc, (k, v)| {
                acc.saturating_add(k.len()).saturating_add(v.len())
            });
            if scan_budget::budget_exceeded(total, scan_budget_bytes) {
                return self.response_error(task, ErrorCode::ResourcesExhausted);
            }
        }

        // Build results as raw msgpack — no serde_json::Value intermediary.
        let mut result_entries: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        for (k, v) in &entries {
            // The two storage shapes (msgpack map vs raw bytes) are resolved by
            // the one shared shaper, so this scan, the streaming/materializing
            // scans, and a write's `RETURNING` projection cannot disagree about
            // what a KV row looks like. A private copy of that logic in this
            // loop risks diverging from the others on the raw-bytes case.
            let (_key_str, entry_mp) = crate::data::executor::scan_normalize::kv_row_to_doc(k, v);

            // Apply filter predicates post-scan (already works on raw msgpack).
            if !filter_predicates.is_empty() {
                match crate::bridge::scan_filter::ScanFilter::all_match_binary(
                    &filter_predicates,
                    &entry_mp,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        return self.response_error(task, ErrorCode::from(e));
                    }
                }
            }

            result_entries.push(entry_mp);
        }

        result_entries = match self.apply_kv_projection_and_computed(
            task,
            result_entries,
            projection,
            computed_columns_bytes,
        ) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        if !sort_keys.is_empty()
            && let Err(e) =
                super::super::sort_utils::sort_msgpack_rows(&mut result_entries, sort_keys)
        {
            return self.response_error(task, e);
        }

        // Build response as flat msgpack array — same format as document/columnar scan.
        // RESP SCAN handles cursor pagination at its own handler layer.
        let mut payload =
            Vec::with_capacity(result_entries.iter().map(|e| e.len()).sum::<usize>() + 64);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, result_entries.len());
        for entry in &result_entries {
            payload.extend_from_slice(entry);
        }
        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }

    /// Apply projection and computed columns to a kv scan's raw msgpack rows.
    ///
    /// Runs before sort so `ORDER BY` can name a computed alias. An empty
    /// projection with non-empty computed columns keeps the full row (kv has
    /// no fixed schema to project against) and appends the computed aliases;
    /// a non-empty projection keeps only the named columns plus the computed
    /// aliases. Returns `Err(Response)` on malformed computed-column bytes
    /// or a per-row evaluation error, ready to return directly to the caller.
    fn apply_kv_projection_and_computed(
        &self,
        task: &ExecutionTask,
        result_entries: Vec<Vec<u8>>,
        projection: &[String],
        computed_columns_bytes: &[u8],
    ) -> Result<Vec<Vec<u8>>, Response> {
        if projection.is_empty() && computed_columns_bytes.is_empty() {
            return Ok(result_entries);
        }

        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                match zerompk::from_msgpack(computed_columns_bytes) {
                    Ok(cols) => cols,
                    Err(e) => {
                        return Err(self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("kv scan: malformed computed bytes: {e}"),
                            },
                        ));
                    }
                }
            };

        if projection.is_empty() {
            crate::data::executor::handlers::provider_scan_compute::apply_windows_and_computed(
                result_entries,
                &[],
                computed_columns_bytes,
            )
            .map_err(|e| self.response_error(task, e))
        } else {
            let mut projected_entries = Vec::with_capacity(result_entries.len());
            for entry in &result_entries {
                let row = crate::data::executor::handlers::document::read::projection::apply_projection_msgpack(
                    entry,
                    &computed_cols,
                    projection,
                )
                .map_err(|e| self.response_error(task, e))?;
                projected_entries.push(row);
            }
            Ok(projected_entries)
        }
    }
}

/// The secondary-index probe for a decoded filter set: the field and value
/// bytes of the first top-level `field = '<text>'` conjunct.
///
/// The probe narrows the scan to the index entries holding those bytes, so
/// it must find every row the post-scan filter accepts. A KV index stores a
/// string as its UTF-8 bytes and a number in the sorted number encoding. A
/// literal that coerced equality can match against other stored bytes, such
/// as `'5'` against a stored `5`, a timestamp string, or a non-string
/// literal, gives no probe, and the scan reads every row.
pub(in crate::data::executor) fn eq_index_probe(filters: &[ScanFilter]) -> Option<(&str, &[u8])> {
    filters.iter().find_map(|f| {
        let plain_eq = matches!(f.op, FilterOp::Eq)
            && !f.field.is_empty()
            && f.clauses.is_empty()
            && f.expr.is_none();
        if !plain_eq {
            return None;
        }
        f.value
            .eq_coerced_exact_text()
            .map(|text| (f.field.as_str(), text.as_bytes()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    fn eq(field: &str, value: Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op: FilterOp::Eq,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    #[test]
    fn probe_uses_a_plain_text_equality() {
        let filters = [
            eq("age", Value::Integer(5)),
            eq("region", Value::String("us-east".into())),
        ];
        assert_eq!(
            eq_index_probe(&filters),
            Some(("region", b"us-east".as_slice()))
        );
    }

    #[test]
    fn literal_with_a_coerced_reading_gives_no_probe() {
        // `'5'` equals a stored integer 5, whose index bytes differ.
        assert_eq!(eq_index_probe(&[eq("n", Value::String("5".into()))]), None);
        assert_eq!(
            eq_index_probe(&[eq("t", Value::String("2024-01-02".into()))]),
            None
        );
        assert_eq!(eq_index_probe(&[eq("n", Value::Integer(5))]), None);
    }

    #[test]
    fn non_equality_or_grouped_filter_gives_no_probe() {
        let mut ne = eq("region", Value::String("us".into()));
        ne.op = FilterOp::Ne;
        let mut or = eq("region", Value::String("us".into()));
        or.clauses = vec![vec![eq("region", Value::String("eu".into()))]];
        assert_eq!(eq_index_probe(&[ne, or]), None);
    }
}
