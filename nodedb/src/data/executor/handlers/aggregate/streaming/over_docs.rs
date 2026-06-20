// SPDX-License-Identifier: BUSL-1.1

//! `aggregate_over_docs`: orchestrate accumulate + finalize over an
//! already-materialized doc set, layering the per-shard result cache on top.

use super::super::cache_key::aggregate_cache_key;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::AggregateSpec;

impl CoreLoop {
    /// Streaming aggregation over an already-materialized set of `(doc_id,
    /// msgpack_bytes)` rows.
    ///
    /// Shared by the per-shard scan path (`docs` from `scan_collection`) and
    /// the input-sourced catalog path (`docs` decoded from a sub-plan
    /// Response). Documents are processed one at a time; per-group
    /// accumulators hold only the derived scalar / approximate state needed
    /// for the final result — no raw document bytes are retained. Memory is
    /// O(num_groups × num_aggregates) instead of O(all_docs).
    ///
    /// WHERE filters, GROUP BY, sub-groups, HAVING, ORDER BY, and LIMIT are
    /// applied identically regardless of the row source.
    ///
    /// `cache_tid` controls the aggregate result cache: `Some(tid)` writes the
    /// result keyed on `(tid, collection, ...)` (the per-shard scan path);
    /// `None` skips caching (the input-sourced catalog path — catalog rows are
    /// identity-scoped, so caching them across identities would be incorrect).
    ///
    /// The accumulate and finalize phases are factored into `accumulate_groups`
    /// and `finalize_groups` respectively, so the distributed-shuffle producer
    /// and consumer can reuse each half without duplicating logic.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn aggregate_over_docs(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        cache_tid: Option<u64>,
        docs: Vec<(String, Vec<u8>)>,
        group_by: &[String],
        aggregates: &[AggregateSpec],
        filters: &[u8],
        having: &[u8],
        limit: usize,
        sub_group_by: &[String],
        sub_aggregates: &[AggregateSpec],
        sort_keys: &[(String, bool)],
    ) -> Response {
        let (groups, sub_groups) = match self.accumulate_groups(
            &docs,
            group_by,
            aggregates,
            filters,
            sub_group_by,
            sub_aggregates,
        ) {
            Ok(g) => g,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        match self.finalize_groups(
            groups,
            sub_groups,
            group_by,
            aggregates,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            sort_keys,
        ) {
            Ok(payload) => {
                if let Some(tid) = cache_tid
                    && filters.is_empty()
                    && having.is_empty()
                {
                    let cache_key = aggregate_cache_key(
                        tid,
                        collection,
                        group_by,
                        aggregates,
                        sub_group_by,
                        sub_aggregates,
                    );
                    if self.aggregate_cache.len() < 256 {
                        self.aggregate_cache.insert(cache_key, payload.clone());
                    }
                }
                self.response_with_payload(task, payload)
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
