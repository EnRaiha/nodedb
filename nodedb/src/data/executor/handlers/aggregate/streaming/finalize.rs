// SPDX-License-Identifier: BUSL-1.1

//! Finalize phase: turn consolidated per-group `GroupState`s into the encoded
//! Response payload.
//!
//! Split out of `aggregate_over_docs` so the distributed-shuffle consumer
//! (`ShuffleAggregateConsume`) can reuse the identical finalize tail —
//! row build, HAVING, user-alias renaming, ORDER BY, LIMIT, encode — after it
//! has merged partial states from every producer.

use std::collections::HashMap;

use super::super::rows::{apply_user_aliases_to_rows, sort_aggregated_rows};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::accum::GroupState;
use nodedb_physical::physical_plan::AggregateSpec;

impl CoreLoop {
    /// Finalize consolidated per-group states into an encoded response payload.
    ///
    /// Mirrors the tail of `aggregate_over_docs`: each `GroupState` is finalized
    /// into a row (the GROUP BY columns parsed back from the JSON-array group
    /// key plus the aggregate outputs), HAVING is applied, user aliases are
    /// renamed, the rows are sorted by `sort_keys`, truncated to `limit`, and
    /// encoded as a MessagePack array.
    ///
    /// `sub_groups` carries the optional sub-aggregation map (empty for plain
    /// GROUP BY, including the distributed-shuffle path).
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn finalize_groups(
        &self,
        groups: HashMap<String, GroupState>,
        mut sub_groups: HashMap<String, HashMap<String, GroupState>>,
        group_by: &[String],
        aggregates: &[AggregateSpec],
        having: &[u8],
        limit: usize,
        sub_group_by: &[String],
        sub_aggregates: &[AggregateSpec],
        sort_keys: &[(String, bool)],
    ) -> crate::Result<Vec<u8>> {
        let need_sub = !sub_group_by.is_empty() && !sub_aggregates.is_empty();
        let mut results: Vec<serde_json::Value> = Vec::new();

        for (group_key, state) in groups {
            let mut row = serde_json::Map::new();

            if !group_by.is_empty()
                && let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(&group_key)
            {
                for (i, field) in group_by.iter().enumerate() {
                    let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                    row.insert(field.clone(), val);
                }
            }

            for (alias, val) in state.finalize(aggregates) {
                let json_val: serde_json::Value = val.into();
                row.insert(alias, json_val);
            }

            if need_sub {
                let sub_map = sub_groups.remove(&group_key).unwrap_or_default();
                let mut sub_results: Vec<serde_json::Value> = Vec::new();
                for (sub_key, sub_state) in sub_map {
                    let mut sub_row = serde_json::Map::new();
                    if let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(&sub_key) {
                        for (i, field) in sub_group_by.iter().enumerate() {
                            let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                            sub_row.insert(field.clone(), val);
                        }
                    }
                    for (alias, val) in sub_state.finalize(sub_aggregates) {
                        let json_val: serde_json::Value = val.into();
                        sub_row.insert(alias, json_val);
                    }
                    let mut sub_value = serde_json::Value::Object(sub_row);
                    apply_user_aliases_to_rows(
                        std::slice::from_mut(&mut sub_value),
                        sub_aggregates,
                    );
                    sub_results.push(sub_value);
                }
                row.insert(
                    "sub_groups".to_string(),
                    serde_json::Value::Array(sub_results),
                );
            }

            results.push(serde_json::Value::Object(row));
        }

        if !having.is_empty() {
            let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        error = %e,
                        "HAVING predicate deserialization failed (schemaless)"
                    );
                    Vec::new()
                }
            };
            if !having_predicates.is_empty() {
                results.retain(|row| {
                    let mp = nodedb_types::json_to_msgpack_or_empty(row);
                    having_predicates.iter().all(|f| f.matches_binary(&mp))
                });
            }
        }

        apply_user_aliases_to_rows(&mut results, aggregates);
        // Post-aggregate ORDER BY: sort group rows before
        // truncating so LIMIT picks the requested top-N.
        sort_aggregated_rows(&mut results, sort_keys);
        results.truncate(limit);

        crate::data::executor::response_codec::encode_json_vec(&results)
    }
}
