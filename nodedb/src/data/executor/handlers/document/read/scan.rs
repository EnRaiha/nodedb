// SPDX-License-Identifier: BUSL-1.1

//! Document collection scan handler.

use tracing::{debug, warn};

use super::decode::{decode_scanned_document, decode_scanned_document_msgpack};
use super::projection::{apply_projection, apply_projection_msgpack};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::document::sort;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::strict_format;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_document_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        limit: usize,
        offset: usize,
        sort_keys: &[(String, bool)],
        filters: &[u8],
        distinct: bool,
        projection: &[String],
        computed_columns_bytes: &[u8],
        window_functions_bytes: &[u8],
        prefilter: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            sort_fields = sort_keys.len(),
            "document scan"
        );

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let window_specs: Vec<crate::bridge::window_func::WindowFuncSpec> =
            if window_functions_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(window_functions_bytes).unwrap_or_default()
            };

        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(computed_columns_bytes).unwrap_or_default()
            };

        // For an explicit LIMIT this preserves the over-fetch heuristic; for a
        // no-LIMIT scan (`limit == usize::MAX`) it bounds the storage fetch to
        // a row ceiling derived from the byte budget (+1 row to detect "more
        // exist") so the data-plane Vec cannot grow to the whole collection.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let fetch_limit = super::scan_budget::fetch_limit_for(limit, offset, scan_budget_bytes);

        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "failed to parse scan filters");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed scan filters: {e}"),
                        },
                    );
                }
            }
        };

        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Scan strategy:
        // 1. Try sparse engine first (with optimized push-down filters when present).
        // 2. If sparse returns empty, fall back to scan_collection which routes
        //    to the correct engine (KV → columnar → sparse). This makes
        //    DocumentOp::Scan the universal scan for ALL collection types.
        //
        // Strict (Binary Tuple) collections need JSON-level filter evaluation
        // because `matches_binary` operates on MessagePack, not Binary Tuples.
        let scan_result = if filter_predicates.is_empty() {
            let sparse_result = self.sparse.scan_documents(tid, collection, fetch_limit);
            match &sparse_result {
                Ok(docs) if docs.is_empty() => {
                    let fallback = self.scan_collection(tid, collection, fetch_limit);
                    if let Ok(ref docs) = fallback
                        && !docs.is_empty()
                    {
                        warn!(
                            core = self.core_id,
                            %collection,
                            count = docs.len(),
                            "document scan fallback to scan_collection"
                        );
                    }
                    fallback
                }
                _ => sparse_result,
            }
        } else if let Some(ref schema) = strict_schema {
            self.sparse
                .scan_documents_filtered(tid, collection, fetch_limit, &|value: &[u8]| {
                    match strict_format::binary_tuple_to_msgpack(value, schema) {
                        Some(mp) => filter_predicates.iter().all(|f| f.matches_binary(&mp)),
                        None => false,
                    }
                })
        } else {
            let sparse_result = self.sparse.scan_documents_filtered(
                tid,
                collection,
                fetch_limit,
                &|value: &[u8]| filter_predicates.iter().all(|f| f.matches_binary(value)),
            );
            match &sparse_result {
                Ok(docs) if docs.is_empty() => self
                    .scan_collection(tid, collection, fetch_limit)
                    .map(|docs| {
                        docs.into_iter()
                            .filter(|(_, data)| {
                                filter_predicates.iter().all(|f| f.matches_binary(data))
                            })
                            .collect()
                    }),
                _ => sparse_result,
            }
        };

        match scan_result {
            Ok(mut filtered) => {
                if let Some(ref m) = self.metrics {
                    m.record_document_read();
                }

                // Bound an unbounded (no-LIMIT) scan by the memory budget. If
                // the materialized result exceeds `max_scan_result_bytes`,
                // surface a deterministic error instead of silently dropping
                // rows. Only enforced for unbounded scans — an explicit
                // `LIMIT n` is already row-bounded by the planner.
                if limit == usize::MAX
                    && super::scan_budget::scan_bytes_exceeded(&filtered, scan_budget_bytes)
                {
                    return self.response_error(task, ErrorCode::ResourcesExhausted);
                }

                if let Some(pf) = prefilter {
                    filtered.retain(|(doc_id, _)| {
                        if let Ok(n) = u32::from_str_radix(doc_id, 16) {
                            pf.contains(nodedb_types::Surrogate::new(n))
                        } else {
                            false
                        }
                    });
                }

                // Strict collections may store binary tuples. Sort and projection
                // operate on msgpack, so normalize binary tuples here.
                let filtered = if !sort_keys.is_empty() || !projection.is_empty() {
                    if let Some(ref schema) = strict_schema {
                        filtered
                            .into_iter()
                            .map(|(id, bytes)| {
                                match strict_format::binary_tuple_to_msgpack(&bytes, schema) {
                                    Some(mp) => (id, mp),
                                    None => (id, bytes),
                                }
                            })
                            .collect()
                    } else {
                        filtered
                    }
                } else {
                    filtered
                };

                let sorted = if sort_keys.is_empty() {
                    filtered
                } else if filtered.len() <= self.query_tuning.sort_run_size {
                    let mut v = filtered;
                    sort::sort_rows(&mut v, sort_keys);
                    v
                } else {
                    match self.external_sort(filtered, sort_keys, limit.saturating_add(offset)) {
                        Ok(merged) => merged,
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "external sort failed");
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("external sort failed: {e}"),
                                },
                            );
                        }
                    }
                };

                let stream_chunk_size = self.query_tuning.stream_chunk_size;

                if let Some(ref schema) = strict_schema
                    && window_specs.is_empty()
                {
                    // SQL DISTINCT semantics require deduplication on the
                    // *projected* row, not the raw document bytes — two rows
                    // with the same `category` but different ids/payload are
                    // distinct as documents but the same under
                    // `SELECT DISTINCT category`. Project first, then dedupe.
                    let projected_rows: Vec<_> = sorted
                        .into_iter()
                        .map(|(doc_id, val)| {
                            let mp = decode_scanned_document_msgpack(&val, Some(schema));
                            let projected =
                                apply_projection_msgpack(&mp, &computed_cols, projection);
                            (doc_id, projected)
                        })
                        .collect();
                    let deduped = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        projected_rows
                            .into_iter()
                            .filter(|(_, value)| seen.insert(value.clone()))
                            .collect::<Vec<_>>()
                    } else {
                        projected_rows
                    };
                    let result: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                    return self.send_document_rows_raw(task, &result, stream_chunk_size);
                }

                if !window_specs.is_empty() {
                    let mut decoded_rows: Vec<(String, serde_json::Value)> = sorted
                        .into_iter()
                        .map(|(id, val)| {
                            let doc = decode_scanned_document(&val, strict_schema.as_ref());
                            (id, doc)
                        })
                        .collect();
                    crate::bridge::window_func::evaluate_window_functions(
                        &mut decoded_rows,
                        &window_specs,
                    );

                    // Project first, then dedupe on the projected JSON value
                    // so `SELECT DISTINCT col` honours SQL semantics.
                    let projected_rows: Vec<_> = decoded_rows
                        .into_iter()
                        .map(|(doc_id, data)| {
                            let projected = apply_projection(data, &computed_cols, projection);
                            DocumentRow {
                                id: doc_id,
                                data: projected,
                            }
                        })
                        .collect();

                    let deduped: Vec<_> = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        projected_rows
                            .into_iter()
                            .filter(|row| seen.insert(row.data.to_string()))
                            .collect()
                    } else {
                        projected_rows
                    };

                    let result: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                    self.send_document_rows_transformed(task, &result, stream_chunk_size)
                } else {
                    let needs_transform = !computed_cols.is_empty() || !projection.is_empty();

                    if needs_transform {
                        // Project first so DISTINCT acts on the projected
                        // row, not the raw document.
                        let projected_rows: Vec<_> = sorted
                            .into_iter()
                            .map(|(doc_id, value)| {
                                let mp = doc_format::json_to_msgpack(&value);
                                let projected =
                                    apply_projection_msgpack(&mp, &computed_cols, projection);
                                (doc_id, projected)
                            })
                            .collect();
                        let deduped = if distinct {
                            let mut seen = std::collections::HashSet::new();
                            projected_rows
                                .into_iter()
                                .filter(|(_, value)| seen.insert(value.clone()))
                                .collect()
                        } else {
                            projected_rows
                        };
                        let result: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                        self.send_document_rows_raw(task, &result, stream_chunk_size)
                    } else {
                        // No projection — `SELECT DISTINCT *` semantics dedupe
                        // on the entire raw value, which is what the
                        // pre-existing path does.
                        let deduped = if distinct {
                            let mut seen = std::collections::HashSet::new();
                            sorted
                                .into_iter()
                                .filter(|(_, value)| seen.insert(value.clone()))
                                .collect()
                        } else {
                            sorted
                        };
                        let rows: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                        self.send_document_rows_raw(task, &rows, stream_chunk_size)
                    }
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
