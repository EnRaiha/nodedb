// SPDX-License-Identifier: BUSL-1.1

//! Distributed GROUP BY shuffle PRODUCER (`QueryOp::PartialAggregateState`).
//!
//! Accumulates the named collection's documents into per-group `GroupState`
//! accumulators exactly like the partial-aggregate path, but instead of
//! finalizing it emits ONE flat row PER GROUP:
//!
//! ```text
//! { <group_by[0]>: value_0, ..., "__agg_state": <bytes> }
//! ```
//!
//! where `__agg_state` carries the serialized partial `GroupState`. A
//! downstream [`crate::data::executor::handlers::aggregate::shuffle_merge`]
//! consumer merges these partial states from every producer and finalizes them.
//!
//! Rows are flat maps (NO `{id, data}` storage wrapper) so the consume side can
//! re-derive the group key from the row's own GROUP BY columns
//! byte-identically.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::accum::GroupState;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_types::Value;

/// The field name carrying a group's serialized partial `GroupState` in a
/// producer-emitted row. The consume side looks this exact key up.
pub(in crate::data::executor) const AGG_STATE_FIELD: &str = "__agg_state";

/// Borrowed inputs to [`CoreLoop::execute_partial_aggregate_state`]: the
/// source-doc selector (sub-plan or named collection) plus the GROUP BY /
/// aggregate / filter specs the producer accumulates over.
pub(in crate::data::executor) struct PartialAggregateStateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub input: Option<&'a nodedb_physical::physical_plan::PhysicalPlan>,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
}

impl CoreLoop {
    /// Execute a `PartialAggregateState` producer: acquire the source documents
    /// (the `input` sub-plan's rows when present, else a per-shard scan of the
    /// named `collection`), accumulate them, then emit one serialized
    /// partial-state row per group.
    pub(in crate::data::executor) fn execute_partial_aggregate_state(
        &mut self,
        params: PartialAggregateStateParams<'_>,
    ) -> Response {
        let PartialAggregateStateParams {
            task,
            tid,
            collection,
            input,
            group_by,
            aggregates,
            filters,
        } = params;
        // Input-sourced producer (catalog): the rows come from executing the
        // sub-plan (a coordinator-materialized `ProviderScan`), not from a
        // per-shard collection scan. Decode the sub-plan rows the SAME way
        // `execute_aggregate` does — an empty / undecodable payload aggregates
        // over zero rows, matching a per-shard scan that matched nothing.
        let docs = if let Some(sub_plan) = input {
            let sub_response = self.execute_plan(task, sub_plan);
            crate::data::executor::response_codec::decode_response_to_docs(&sub_response)
                .unwrap_or_default()
        } else {
            // Same per-shard scan cap the streaming aggregate path uses.
            let scan_limit = self.query_tuning.aggregate_scan_cap;
            match self.scan_collection(
                task.request.database_id.as_u64(),
                tid,
                collection,
                scan_limit,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };

        // Plain GROUP BY: no sub-groups for the shuffle producer.
        let (groups, _sub) =
            match self.accumulate_groups(super::streaming::accumulate::AccumulateGroupsParams {
                docs: &docs,
                group_by,
                aggregates,
                filters,
                sub_group_by: &[],
                sub_aggregates: &[],
            }) {
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

        let rows = match Self::partial_state_rows(groups, group_by) {
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

        match crate::data::executor::response_codec::encode_value_vec(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Build the flat per-group rows `{<gb cols>, "__agg_state": <bytes>}` from
    /// the consolidated group map.
    ///
    /// The GROUP BY column values are recovered from the JSON-array group key
    /// (the same encoding `build_group_key` produces) so that the consume side,
    /// which re-derives the key from these same row fields via `build_group_key`,
    /// reconstructs a byte-identical key.
    fn partial_state_rows(
        groups: std::collections::HashMap<String, GroupState>,
        group_by: &[GroupKeySpec],
    ) -> crate::Result<Vec<Value>> {
        let mut rows: Vec<Value> = Vec::with_capacity(groups.len());
        for (group_key, state) in groups {
            let mut map: std::collections::HashMap<String, Value> =
                std::collections::HashMap::with_capacity(group_by.len() + 1);

            if !group_by.is_empty() {
                let parts: Vec<serde_json::Value> =
                    sonic_rs::from_str(&group_key).map_err(|e| crate::Error::Codec {
                        detail: format!("partial-state group key decode: {e}"),
                    })?;
                // One positional slot per key that contributed to the key bytes
                // (a bare `field` or a computed `expr`); re-emit each under its
                // `output_name` so the consume side can rebuild a byte-identical
                // key from these flat row fields.
                let mut part_idx = 0usize;
                for spec in group_by {
                    if spec.field.is_none() && spec.expr.is_none() {
                        continue;
                    }
                    let jv = parts
                        .get(part_idx)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    map.insert(spec.output_name.clone(), Value::from(jv));
                    part_idx += 1;
                }
            }

            // GroupState serializes via serde (sonic_rs JSON) — the same
            // canonical encoding `GroupBySpiller` already uses to persist it.
            let state_bytes = sonic_rs::to_vec(&state).map_err(|e| crate::Error::Codec {
                detail: format!("partial-state serialize: {e}"),
            })?;
            map.insert(AGG_STATE_FIELD.to_string(), Value::Bytes(state_bytes));

            rows.push(Value::Object(map));
        }
        Ok(rows)
    }
}
