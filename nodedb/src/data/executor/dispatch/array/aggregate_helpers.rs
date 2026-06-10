// SPDX-License-Identifier: BUSL-1.1

use std::collections::BTreeMap;

use nodedb_array::query::aggregate::{AggregateResult, Reducer};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::TilePayload;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::coord::value::CoordValue;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_types::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ArrayReducer;

/// Standard-msgpack-friendly cell value for aggregate rows.
pub(super) enum AggCell {
    Float(f64),
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl zerompk::ToMessagePack for AggCell {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            AggCell::Float(f) => writer.write_f64(*f),
            AggCell::Int(i) => writer.write_i64(*i),
            AggCell::Str(s) => writer.write_string(s),
            AggCell::Bool(b) => writer.write_boolean(*b),
            AggCell::Null => writer.write_nil(),
        }
    }
}

pub(super) fn coord_to_agg_cell(c: &CoordValue) -> AggCell {
    match c {
        CoordValue::Int64(v) | CoordValue::TimestampMs(v) => AggCell::Int(*v),
        CoordValue::Float64(v) => AggCell::Float(*v),
        CoordValue::String(v) => AggCell::Str(v.clone()),
    }
}

pub(super) fn float_or_null(v: Option<f64>) -> AggCell {
    match v {
        Some(f) => AggCell::Float(f),
        None => AggCell::Null,
    }
}

pub(super) fn map_reducer(r: ArrayReducer) -> Reducer {
    match r {
        ArrayReducer::Sum => Reducer::Sum,
        ArrayReducer::Count => Reducer::Count,
        ArrayReducer::Min => Reducer::Min,
        ArrayReducer::Max => Reducer::Max,
        ArrayReducer::Mean => Reducer::Mean,
    }
}

pub(super) fn unwrap_sparse(t: TilePayload) -> Result<SparseTile, ErrorCode> {
    match t {
        TilePayload::Sparse(s) => Ok(s),
        TilePayload::Dense(_) => Err(ErrorCode::Unsupported {
            detail: "dense tile payload in aggregate".to_string(),
        }),
    }
}

pub(super) fn apply_surrogate_filter(
    schema: &ArraySchema,
    tile: SparseTile,
    filter: Option<&SurrogateBitmap>,
) -> Result<SparseTile, ErrorCode> {
    let f = match filter {
        None => return Ok(tile),
        Some(f) => f,
    };
    let n = tile.row_count();
    let mut live_idx = 0usize;
    let mut b = SparseTileBuilder::new(schema);
    for row in 0..n {
        let kind = tile.row_kind(row).map_err(|e| ErrorCode::Internal {
            detail: format!("array surrogate filter row_kind: {e}"),
        })?;
        if kind != RowKind::Live {
            continue;
        }
        let attr_row = live_idx;
        live_idx += 1;
        let sur = tile
            .surrogates
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::Surrogate::ZERO);
        if !f.contains(sur) {
            continue;
        }
        let coord: Vec<_> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        let attrs: Vec<_> = tile.attr_cols.iter().map(|c| c[attr_row].clone()).collect();
        let valid_from_ms = tile.valid_from_ms.get(row).copied().unwrap_or(0);
        let valid_until_ms = tile
            .valid_until_ms
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::OPEN_UPPER);
        b.push_row(SparseRow {
            coord: &coord,
            attrs: &attrs,
            surrogate: sur,
            valid_from_ms,
            valid_until_ms,
            kind: RowKind::Live,
        })
        .map_err(|e| ErrorCode::Internal {
            detail: format!("array surrogate filter: {e}"),
        })?;
    }
    Ok(b.build())
}

pub(super) fn agg_result_to_partial(group_key: i64, result: AggregateResult) -> ArrayAggPartial {
    match result {
        AggregateResult::Sum { value, count } => ArrayAggPartial {
            group_key,
            count,
            sum: value,
            min: value,
            max: value,
            welford_mean: if count > 0 { value / count as f64 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Count { count } => ArrayAggPartial {
            group_key,
            count,
            sum: count as f64,
            min: count as f64,
            max: count as f64,
            welford_mean: if count > 0 { 1.0 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Min { value, count } => ArrayAggPartial {
            group_key,
            count,
            sum: value,
            min: value,
            max: value,
            welford_mean: if count > 0 { value / count as f64 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Max { value, count } => ArrayAggPartial {
            group_key,
            count,
            sum: value,
            min: value,
            max: value,
            welford_mean: if count > 0 { value / count as f64 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Mean { sum, count } => {
            let mean = if count > 0 { sum / count as f64 } else { 0.0 };
            ArrayAggPartial {
                group_key,
                count,
                sum,
                min: mean,
                max: mean,
                welford_mean: mean,
                welford_m2: 0.0,
            }
        }
        AggregateResult::Empty(_) => ArrayAggPartial {
            group_key,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            welford_mean: 0.0,
            welford_m2: 0.0,
        },
    }
}

pub(super) fn coord_to_group_key(c: &CoordValue) -> i64 {
    match c {
        CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v,
        CoordValue::Float64(v) => v.to_bits() as i64,
        CoordValue::String(s) => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            s.hash(&mut h);
            h.finish() as i64
        }
    }
}

pub(super) fn encode_agg_rows(
    core: &CoreLoop,
    task: &ExecutionTask,
    rows: &[BTreeMap<&'static str, AggCell>],
) -> Response {
    let owned: Vec<&BTreeMap<&'static str, AggCell>> = rows.iter().collect();
    match zerompk::to_msgpack_vec(&owned) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("array aggregate encode: {e}"),
            },
        ),
    }
}

pub(super) fn encode_bitemporal_agg_partial(
    core: &CoreLoop,
    task: &ExecutionTask,
    partials: &[ArrayAggPartial],
    truncated_before_horizon: bool,
) -> Response {
    let owned: Vec<&ArrayAggPartial> = partials.iter().collect();
    match zerompk::to_msgpack_vec(&(&owned, truncated_before_horizon)) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("bitemporal aggregate encode: {e}"),
            },
        ),
    }
}
