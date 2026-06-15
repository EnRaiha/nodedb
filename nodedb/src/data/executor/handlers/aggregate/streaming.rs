// SPDX-License-Identifier: BUSL-1.1

//! Streaming, spill-backed GROUP BY accumulation shared by the per-shard scan
//! path and the input-sourced (catalog) path.

use std::collections::HashMap;

use super::super::accum::GroupState;
use super::super::spill::groupby::GroupBySpiller;
use super::cache_key::aggregate_cache_key;
use super::rows::{apply_user_aliases_to_rows, sort_aggregated_rows};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_query::msgpack_scan;

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
    #[allow(clippy::too_many_arguments)]
    pub(super) fn aggregate_over_docs(
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
        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "filter predicate deserialization failed");
                    Vec::new()
                }
            }
        };

        let use_field_index = filter_predicates.len() + group_by.len() >= 2;
        let need_sub = !sub_group_by.is_empty() && !sub_aggregates.is_empty();

        // Spill-to-disk GROUP BY accumulator.
        //
        // Sub-groups are flattened into the same spiller using composite keys:
        //   outer_key + '\x1F' + sub_key
        // U+001F (ASCII Unit Separator) cannot appear in JSON-encoded string
        // values, so the composite key is unambiguous.  At finalize time, keys
        // containing '\x1F' are split to reconstruct outer/sub structure.
        let spill_dir = self
            .data_dir
            .join("groupby-spill")
            .join(format!("core-{}", self.core_id));
        let cap = self.query_tuning.groupby_max_groups_in_mem;

        let mut spiller = match GroupBySpiller::new(spill_dir, cap, self.governor.clone()) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Accumulate matching documents. Spill errors are fatal and surfaced
        // once accumulation stops — the first error breaks out of both loops.
        let mut spill_err: Option<crate::Error> = None;
        let chunk_size = 10_000;

        for chunk in docs.chunks(chunk_size) {
            if spill_err.is_some() {
                break;
            }
            for (_, value) in chunk {
                let outer_key = if use_field_index {
                    let idx = msgpack_scan::FieldIndex::build(value, 0)
                        .unwrap_or_else(msgpack_scan::FieldIndex::empty);
                    if !filter_predicates
                        .iter()
                        .all(|f| f.matches_binary_indexed(value, &idx))
                    {
                        continue;
                    }
                    msgpack_scan::group_key::build_group_key_indexed(value, group_by, &idx)
                } else {
                    if !filter_predicates.iter().all(|f| f.matches_binary(value)) {
                        continue;
                    }
                    msgpack_scan::build_group_key(value, group_by)
                };

                if let Err(e) = spiller.feed(outer_key.clone(), aggregates, value) {
                    spill_err = Some(e);
                    break;
                }

                if need_sub {
                    let sub_key = msgpack_scan::build_group_key(value, sub_group_by);
                    // Composite key: outer + U+001F + sub.
                    let composite = format!("{outer_key}\x1F{sub_key}");
                    if let Err(e) = spiller.feed(composite, sub_aggregates, value) {
                        spill_err = Some(e);
                        break;
                    }
                }
            }
        }

        // Surface spill-level errors before proceeding.
        if let Some(e) = spill_err {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }

        // Merge all spill runs into the consolidated map.
        let consolidated = match spiller.finalize() {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Separate outer groups from sub-group composite entries.
        let mut groups: HashMap<String, GroupState> = HashMap::new();
        // outer_key → sub_key → GroupState
        let mut sub_groups: HashMap<String, HashMap<String, GroupState>> = HashMap::new();

        for (key, state) in consolidated {
            if let Some(sep_pos) = key.find('\x1F') {
                // Sub-group composite key.
                let outer = key[..sep_pos].to_string();
                let sub = key[sep_pos + 1..].to_string();
                sub_groups.entry(outer).or_default().insert(sub, state);
            } else {
                groups.insert(key, state);
            }
        }

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

        match crate::data::executor::response_codec::encode_json_vec(&results) {
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
