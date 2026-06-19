// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal `AS OF` scan handler. Reads from the versioned document table at
//! the requested system-time cutoff, applies an optional valid-time predicate
//! per version, and emits rows in the same wire format as the regular scan.

use tracing::debug;

use super::projection::apply_projection_msgpack;
use super::scan_params::VersionedScanParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_scan_as_of(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: VersionedScanParams<'_>,
        system_as_of_ms: Option<i64>,
    ) -> Response {
        let VersionedScanParams {
            collection,
            limit,
            offset,
            filters,
            projection,
            valid_at_ms,
        } = params;

        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            ?system_as_of_ms,
            ?valid_at_ms,
            "document scan (bitemporal)"
        );

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
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
        };

        // Push the scan filters down into the engine so the `limit` early-stop
        // counts only matching documents. Filtering the truncated result here
        // (as the old fetch-then-filter heuristic did) silently under-returned
        // when the filter was selective.
        let predicate = |body: &[u8]| filter_predicates.iter().all(|f| f.matches_binary(body));
        let scan_limit = offset.saturating_add(limit);
        let rows = match self.sparse.versioned_scan_as_of(
            task.request.database_id.as_u64(),
            tid,
            collection,
            system_as_of_ms,
            valid_at_ms,
            scan_limit,
            &predicate,
        ) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // A no-LIMIT bitemporal scan shares the `usize::MAX` limit; bound its
        // materialized result by the scan memory budget and surface a
        // deterministic error rather than silently truncating.
        if limit == usize::MAX
            && crate::data::executor::handlers::scan_budget::scan_bytes_exceeded(
                &rows,
                self.query_tuning.max_scan_result_bytes,
            )
        {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let sliced: Vec<(String, Vec<u8>)> = rows.into_iter().skip(offset).take(limit).collect();

        if projection.is_empty() {
            return self.send_document_rows_raw(task, &sliced, 1024);
        }

        let transformed: Vec<_> = sliced
            .into_iter()
            .map(|(doc_id, body)| {
                let projected = apply_projection_msgpack(&body, &[], projection);
                (doc_id, projected)
            })
            .collect();
        self.send_document_rows_raw(task, &transformed, 1024)
    }
}
