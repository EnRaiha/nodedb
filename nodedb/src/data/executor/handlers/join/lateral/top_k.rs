// SPDX-License-Identifier: BUSL-1.1

//! `LateralTopK` handler: equi-correlated LATERAL with ORDER BY + LIMIT optimization.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::JoinProjection;

use super::shared::{
    MAX_RESULT_ROWS, build_row, build_scan_plan, extract_outer_field, flatten_outer_row,
    strip_filter_qualifiers, unwrap_data_field,
};

/// Parameters for `LateralTopK` execution.
pub struct LateralTopKParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub outer_plan: &'a PhysicalPlan,
    pub outer_alias: &'a str,
    pub inner_collection: &'a str,
    pub inner_filters: &'a [u8],
    pub inner_order_by: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub inner_limit: usize,
    pub correlation_keys: &'a [(String, String)],
    pub lateral_alias: &'a str,
    pub projection: &'a [JoinProjection],
    pub left_join: bool,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_lateral_top_k(
        &mut self,
        p: LateralTopKParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            inner = %p.inner_collection,
            limit = p.inner_limit,
            "lateral top-k"
        );

        let outer_resp = self.execute_plan(p.task, p.outer_plan);
        let outer_docs = match response_codec::decode_response_to_docs(&outer_resp) {
            Some(docs) => docs,
            None => return outer_resp,
        };

        let base_inner_filters: Vec<ScanFilter> = {
            let raw: Vec<ScanFilter> = if p.inner_filters.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(p.inner_filters).unwrap_or_default()
            };
            // Strip table-alias qualifiers from field names so the inner scan
            // can resolve them against unqualified document fields.
            strip_filter_qualifiers(raw)
        };

        let mut result: Vec<Vec<u8>> = Vec::new();

        for (_outer_id, outer_bytes) in &outer_docs {
            // Produce a flat outer map that includes wrapper-level fields (e.g.
            // the primary-key `id`) alongside the document payload fields so
            // that the merge and projection steps can reference any column.
            let flat_outer = flatten_outer_row(outer_bytes);

            let mut corr_filters = base_inner_filters.clone();
            for (outer_col, inner_col) in p.correlation_keys {
                if let Some(val) = extract_outer_field(outer_bytes, outer_col) {
                    corr_filters.push(ScanFilter {
                        field: inner_col.clone(),
                        op: FilterOp::Eq,
                        value: val,
                        clauses: Vec::new(),
                        expr: None,
                    });
                }
            }

            let filter_bytes = match zerompk::to_msgpack_vec(&corr_filters) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        p.task,
                        ErrorCode::Internal {
                            detail: format!("lateral filter serialization: {e}"),
                        },
                    );
                }
            };

            let inner_plan = build_scan_plan(
                p.inner_collection,
                filter_bytes,
                p.inner_order_by,
                p.inner_limit,
            );

            let inner_resp = self.execute_plan(p.task, &inner_plan);
            let inner_docs = response_codec::decode_response_to_docs(&inner_resp);

            match inner_docs {
                None if p.left_join => {
                    let row = build_row(
                        &flat_outer,
                        None,
                        p.outer_alias,
                        p.lateral_alias,
                        p.projection,
                    );
                    result.push(row);
                }
                None => {}
                Some(docs) if docs.is_empty() && p.left_join => {
                    let row = build_row(
                        &flat_outer,
                        None,
                        p.outer_alias,
                        p.lateral_alias,
                        p.projection,
                    );
                    result.push(row);
                }
                Some(docs) => {
                    for (_inner_id, inner_bytes) in &docs {
                        let inner_data = unwrap_data_field(inner_bytes);
                        let row = build_row(
                            &flat_outer,
                            Some(inner_data),
                            p.outer_alias,
                            p.lateral_alias,
                            p.projection,
                        );
                        result.push(row);
                        if result.len() >= MAX_RESULT_ROWS {
                            break;
                        }
                    }
                }
            }
            if result.len() >= MAX_RESULT_ROWS {
                break;
            }
        }

        let payload = response_codec::encode_binary_rows(&result);
        self.response_with_payload(p.task, payload)
    }
}
