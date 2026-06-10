// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::Aggregate` handler.
//!
//! Cross-tile reduction with optional group-by-dim. The tile-local
//! reducers in `nodedb-array::query::aggregate` produce
//! `AggregateResult` partials that merge exactly across tiles (Mean
//! carries `(sum, count)`); we fold them here and finalize once.

use std::collections::{BTreeMap, HashMap};

use nodedb_array::query::aggregate::{GroupAggregate, aggregate_attr, group_by_dim};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::TilePayload;
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_types::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ArrayReducer;

use super::aggregate_helpers::{
    AggCell, agg_result_to_partial, apply_surrogate_filter, coord_to_agg_cell, coord_to_group_key,
    encode_agg_rows, encode_bitemporal_agg_partial, float_or_null, map_reducer, unwrap_sparse,
};

/// Aggregate query parameters bundled to avoid exceeding the 7-argument limit.
pub(in crate::data::executor) struct AggParams<'a> {
    pub array_id: &'a ArrayId,
    pub attr_idx: u32,
    pub reducer: ArrayReducer,
    pub group_by_dim_idx: i32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    pub return_partial: bool,
    /// Optional Hilbert-prefix range `[lo, hi]` for shard-level partitioning.
    pub hilbert_range: Option<(u64, u64)>,
    /// Bitemporal system-time cutoff. `None` = live read.
    pub system_as_of: Option<i64>,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

/// Bundled inputs for [`CoreLoop::reduce_and_encode_agg`] — keeps the kernel
/// to a single argument and avoids the 7-argument clippy lint.
struct AggEmit<'a> {
    task: &'a ExecutionTask,
    schema: &'a ArraySchema,
    all_tiles: Vec<TilePayload>,
    attr_idx: u32,
    reducer: ArrayReducer,
    group_by_dim_idx: i32,
    cell_filter: Option<&'a SurrogateBitmap>,
    return_partial: bool,
    /// Below-horizon signal computed by the tile scan (always `false` for
    /// non-temporal current-state reads).
    truncated_before_horizon: bool,
    /// When `true`, surface `truncated_before_horizon` as a trailing
    /// `{"truncated_before_horizon": bool}` summary row. Only set for temporal
    /// queries — the signal is meaningless for current-state reads, and
    /// emitting it there would change the long-standing non-temporal row shape.
    emit_horizon: bool,
}

fn hilbert_prefix_in_range(hp: u64, range: Option<(u64, u64)>) -> bool {
    match range {
        Some((lo, hi)) => hp >= lo && hp <= hi,
        None => true,
    }
}

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_aggregate(
        &mut self,
        task: &ExecutionTask,
        p: AggParams<'_>,
    ) -> Response {
        let AggParams {
            array_id,
            attr_idx,
            reducer,
            group_by_dim_idx,
            cell_filter,
            return_partial,
            hilbert_range,
            system_as_of,
            valid_at_ms,
        } = p;
        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }

        let schema = match self.array_engine.store(array_id) {
            Ok(store) => store.schema().clone(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array '{}' not open: {e}", array_id.name),
                    },
                );
            }
        };

        // Resolve the tile set + below-horizon signal uniformly across temporal
        // and current-state reads. The two differ only in how tiles are sourced;
        // the reduce/encode kernel below is identical for both.
        let temporal = system_as_of.is_some() || valid_at_ms.is_some();
        let (all_tiles, truncated_before_horizon) =
            match self.collect_agg_tiles(array_id, hilbert_range, system_as_of, valid_at_ms) {
                Ok(v) => v,
                Err(detail) => {
                    return self.response_error(task, ErrorCode::Internal { detail });
                }
            };

        self.reduce_and_encode_agg(AggEmit {
            task,
            schema: &schema,
            all_tiles,
            attr_idx,
            reducer,
            group_by_dim_idx,
            cell_filter,
            return_partial,
            truncated_before_horizon,
            emit_horizon: temporal,
        })
    }

    /// Gather the tiles an aggregate must reduce over, plus the below-horizon
    /// flag, via the cell-ceiling resolver `scan_tiles_at`.
    ///
    /// Live reads (no `AS OF`) resolve at the open horizon `i64::MAX`, so each
    /// cell contributes exactly once at its latest version — identical to the
    /// slice path's `Current` handling. Scanning *raw* tile versions here would
    /// double-count any cell overwritten across segments in a bitemporal array
    /// (e.g. v1 in one sealed segment, v2 in another). `system_as_of`/
    /// `valid_at_ms` narrow the cutoff for point-in-time queries; the same
    /// `hilbert_range` shard filter applies in all cases.
    ///
    /// Returns an error *detail* string (wrapped into `ErrorCode::Internal` by
    /// the caller) rather than a `Response`, so it can borrow `&self` cleanly.
    fn collect_agg_tiles(
        &self,
        array_id: &ArrayId,
        hilbert_range: Option<(u64, u64)>,
        system_as_of: Option<i64>,
        valid_at_ms: Option<i64>,
    ) -> Result<(Vec<TilePayload>, bool), String> {
        let cutoff = system_as_of.unwrap_or(i64::MAX);
        let store = self
            .array_engine
            .store(array_id)
            .map_err(|e| format!("array '{}' not open: {e}", array_id.name))?;
        let (resolved_tiles, truncated_before_horizon) =
            store
                .scan_tiles_at(cutoff, valid_at_ms)
                .map_err(|e| format!("array aggregate scan: {e}"))?;
        let tiles = resolved_tiles
            .into_iter()
            .filter(|(hp, _)| hilbert_prefix_in_range(*hp, hilbert_range))
            .map(|(_, tile)| TilePayload::Sparse(tile))
            .collect();
        Ok((tiles, truncated_before_horizon))
    }

    /// Reduce the resolved tiles into a scalar or grouped aggregate and encode
    /// the response. Shared by current-state and temporal queries so the wire
    /// shape can never diverge between them.
    ///
    /// - `return_partial` (distributed shards): emits the
    ///   `(Vec<ArrayAggPartial>, truncated_before_horizon)` tuple via
    ///   `encode_bitemporal_agg_partial` — the single partial wire shape the
    ///   cluster `exec_agg` decodes.
    /// - otherwise: emits finalized `{"result"}` / `{"group","result"}` rows,
    ///   plus a trailing `{"truncated_before_horizon": bool}` summary row when
    ///   `emit_horizon` is set (temporal queries only). The cluster
    ///   `finalize_agg_partials` produces this exact same shape.
    fn reduce_and_encode_agg(&self, e: AggEmit<'_>) -> Response {
        let AggEmit {
            task,
            schema,
            all_tiles,
            attr_idx,
            reducer,
            group_by_dim_idx,
            cell_filter,
            return_partial,
            truncated_before_horizon,
            emit_horizon,
        } = e;

        let r = map_reducer(reducer);
        let attr = attr_idx as usize;

        if group_by_dim_idx < 0 {
            let mut acc = None;
            for tile in all_tiles {
                let sparse = match unwrap_sparse(tile) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let sparse = match apply_surrogate_filter(schema, sparse, cell_filter) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let part = aggregate_attr(&sparse, attr, r);
                acc = Some(match acc {
                    Some(prev) => {
                        nodedb_array::query::aggregate::AggregateResult::merge(prev, part)
                    }
                    None => part,
                });
            }
            if return_partial {
                let partial =
                    acc.map(|a| agg_result_to_partial(0, a))
                        .unwrap_or_else(|| ArrayAggPartial {
                            group_key: 0,
                            count: 0,
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                            welford_mean: 0.0,
                            welford_m2: 0.0,
                        });
                return encode_bitemporal_agg_partial(
                    self,
                    task,
                    &[partial],
                    truncated_before_horizon,
                );
            }
            let final_val = acc.and_then(|a| a.finalize());
            let mut rows: Vec<BTreeMap<&'static str, AggCell>> = Vec::new();
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("result", float_or_null(final_val));
            rows.push(row);
            push_horizon_summary(&mut rows, emit_horizon, truncated_before_horizon);
            return encode_agg_rows(self, task, &rows);
        }

        let dim = group_by_dim_idx as usize;
        let mut order: Vec<CoordValue> = Vec::new();
        let mut by_key: HashMap<CoordValue, nodedb_array::query::aggregate::AggregateResult> =
            HashMap::new();
        for tile in all_tiles {
            let sparse = match unwrap_sparse(tile) {
                Ok(s) => s,
                Err(code) => return self.response_error(task, code),
            };
            let sparse = match apply_surrogate_filter(schema, sparse, cell_filter) {
                Ok(s) => s,
                Err(code) => return self.response_error(task, code),
            };
            let groups: Vec<GroupAggregate> = group_by_dim(&sparse, dim, attr, r);
            for g in groups {
                match by_key.get_mut(&g.key) {
                    Some(prev) => *prev = prev.merge(g.result),
                    None => {
                        order.push(g.key.clone());
                        by_key.insert(g.key, g.result);
                    }
                }
            }
        }

        if return_partial {
            let partials: Vec<ArrayAggPartial> = order
                .iter()
                .filter_map(|key| {
                    by_key
                        .remove(key)
                        .map(|agg| agg_result_to_partial(coord_to_group_key(key), agg))
                })
                .collect();
            return encode_bitemporal_agg_partial(self, task, &partials, truncated_before_horizon);
        }

        let mut rows: Vec<BTreeMap<&'static str, AggCell>> = Vec::with_capacity(order.len() + 1);
        for key in order {
            let result_val = by_key.remove(&key).and_then(|r| r.finalize());
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("group", coord_to_agg_cell(&key));
            row.insert("result", float_or_null(result_val));
            rows.push(row);
        }
        push_horizon_summary(&mut rows, emit_horizon, truncated_before_horizon);
        encode_agg_rows(self, task, &rows)
    }
}

/// Append the trailing `{"truncated_before_horizon": bool}` summary row when
/// `emit_horizon` is set. No-op otherwise so non-temporal aggregates keep their
/// long-standing row shape.
fn push_horizon_summary(
    rows: &mut Vec<BTreeMap<&'static str, AggCell>>,
    emit_horizon: bool,
    truncated_before_horizon: bool,
) {
    if !emit_horizon {
        return;
    }
    let mut summary: BTreeMap<&'static str, AggCell> = BTreeMap::new();
    summary.insert(
        "truncated_before_horizon",
        AggCell::Bool(truncated_before_horizon),
    );
    rows.push(summary);
}
