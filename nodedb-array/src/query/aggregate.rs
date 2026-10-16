// SPDX-License-Identifier: Apache-2.0

//! Reductions over a sparse tile.
//!
//! Five reducers (Sum / Count / Min / Max / Mean) operate on a single
//! attribute column. The shape is partial-friendly: each tile produces
//! an [`AggregateResult`] which the executor merges across tiles via
//! [`AggregateResult::merge`]. Mean carries (sum, count) so merges are
//! exact rather than averaging averages.
//!
//! Group-by ([`group_by_dim`]) buckets cells by one dim's values and
//! returns a `Vec<(CoordValue, AggregateResult)>` ordered by first
//! appearance. Sort/order is the caller's job — keeping insertion
//! order avoids forcing `Ord` on `CoordValue`.

use std::collections::HashMap;

use crate::error::{ArrayError, ArrayResult};
use crate::query::agg_number::{AggNumber, AggScalar};
use crate::tile::sparse_tile::{RowKind, SparseTile};
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reducer {
    Sum,
    Count,
    Min,
    Max,
    Mean,
}

/// Partial result for one reducer over one (group of) cell(s).
/// `Mean` retains `(sum, count)` so partials merge exactly. An integer attr
/// accumulates exactly; see [`AggNumber`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateResult {
    Sum {
        value: AggNumber,
        count: u64,
    },
    Count {
        count: u64,
    },
    Min {
        value: AggNumber,
        count: u64,
    },
    Max {
        value: AggNumber,
        count: u64,
    },
    Mean {
        sum: AggNumber,
        count: u64,
    },
    /// No non-null cells observed yet for this reducer.
    Empty(Reducer),
}

impl AggregateResult {
    /// Merge two partials of the same reducer kind. Mismatched
    /// reducers produce a left-biased result (the executor only ever
    /// merges same-kind partials, so this is a programming-error path).
    pub fn merge(self, other: AggregateResult) -> AggregateResult {
        use AggregateResult::*;
        match (self, other) {
            (Empty(_), x) | (x, Empty(_)) => x,
            (
                Sum {
                    value: a,
                    count: ca,
                },
                Sum {
                    value: b,
                    count: cb,
                },
            ) => Sum {
                value: a.sum_with(b),
                count: ca + cb,
            },
            (Count { count: a }, Count { count: b }) => Count { count: a + b },
            (
                Min {
                    value: a,
                    count: ca,
                },
                Min {
                    value: b,
                    count: cb,
                },
            ) => Min {
                value: a.min_with(b),
                count: ca + cb,
            },
            (
                Max {
                    value: a,
                    count: ca,
                },
                Max {
                    value: b,
                    count: cb,
                },
            ) => Max {
                value: a.max_with(b),
                count: ca + cb,
            },
            (Mean { sum: a, count: ca }, Mean { sum: b, count: cb }) => Mean {
                sum: a.sum_with(b),
                count: ca + cb,
            },
            (lhs, _) => lhs,
        }
    }

    /// Final value, or `None` when no cell contributed. `SUM` of an integer
    /// attr is an exact integer and refuses past `i64` (SQLSTATE `22003`).
    /// `MIN` / `MAX` keep the attr's kind. `COUNT` is an integer. `MEAN` is
    /// always a float.
    pub fn finalize(self) -> ArrayResult<Option<AggScalar>> {
        match self {
            AggregateResult::Sum { value, .. }
            | AggregateResult::Min { value, .. }
            | AggregateResult::Max { value, .. } => value.finish().map(Some),
            AggregateResult::Count { count } => Ok(Some(AggScalar::Int(
                i64::try_from(count).map_err(|_| ArrayError::NumericOverflow {
                    detail: format!("count {count} is out of range for bigint"),
                })?,
            ))),
            AggregateResult::Mean { sum, count } if count > 0 => {
                Ok(Some(AggScalar::Float(sum.to_f64() / count as f64)))
            }
            AggregateResult::Mean { .. } | AggregateResult::Empty(_) => Ok(None),
        }
    }
}

fn empty(r: Reducer) -> AggregateResult {
    AggregateResult::Empty(r)
}

/// Reduce one attr column over all rows of a tile. `attr_idx` must be
/// in range; non-numeric / null cells are skipped (Count includes
/// non-null cells regardless of dtype, since "count of cells with this
/// attribute populated" is a sensible fold across all attr types).
pub fn aggregate_attr(tile: &SparseTile, attr_idx: usize, reducer: Reducer) -> AggregateResult {
    let Some(col) = tile.attr_cols.get(attr_idx) else {
        return empty(reducer);
    };
    let mut acc = empty(reducer);
    for v in col {
        let one = single_cell(v, reducer);
        acc = acc.merge(one);
    }
    acc
}

fn single_cell(v: &CellValue, reducer: Reducer) -> AggregateResult {
    use AggregateResult::*;
    let n = AggNumber::of_cell(v);
    match (reducer, n) {
        (Reducer::Count, _) if !v.is_null() => Count { count: 1 },
        (Reducer::Count, _) => Empty(Reducer::Count),
        (Reducer::Sum, Some(x)) => Sum { value: x, count: 1 },
        (Reducer::Min, Some(x)) => Min { value: x, count: 1 },
        (Reducer::Max, Some(x)) => Max { value: x, count: 1 },
        (Reducer::Mean, Some(x)) => Mean { sum: x, count: 1 },
        (r, None) => Empty(r),
    }
}

/// One bucket of a group-by aggregate. The key is the dim value the
/// rows share; `result` is the reducer's running partial over that
/// bucket.
#[derive(Debug, Clone)]
pub struct GroupAggregate {
    pub key: CoordValue,
    pub result: AggregateResult,
}

/// Group rows by one dim's values and reduce `attr_idx` per group.
/// Returns groups in first-seen order.
pub fn group_by_dim(
    tile: &SparseTile,
    dim_idx: usize,
    attr_idx: usize,
    reducer: Reducer,
) -> Vec<GroupAggregate> {
    let Some(dict) = tile.dim_dicts.get(dim_idx) else {
        return Vec::new();
    };
    let Some(col) = tile.attr_cols.get(attr_idx) else {
        return Vec::new();
    };
    let mut order: Vec<CoordValue> = Vec::new();
    let mut by_key: HashMap<CoordValue, AggregateResult> = HashMap::new();
    // Iterate physical rows; track live_idx separately because attr_cols only
    // has entries for Live rows — sentinel rows carry no attr payload.
    let mut live_idx = 0usize;
    for row in 0..tile.row_count() {
        let kind = match tile.row_kind(row) {
            Ok(k) => k,
            Err(_) => break,
        };
        if kind != RowKind::Live {
            continue;
        }
        let cell = match col.get(live_idx) {
            Some(c) => c,
            None => break,
        };
        live_idx += 1;
        let key = dict.values[dict.indices[row] as usize].clone();
        let one = single_cell(cell, reducer);
        match by_key.get_mut(&key) {
            Some(slot) => *slot = slot.merge(one),
            None => {
                order.push(key.clone());
                by_key.insert(key, empty(reducer).merge(one));
            }
        }
    }
    order
        .into_iter()
        .map(|k| GroupAggregate {
            result: by_key.remove(&k).unwrap_or(empty(reducer)),
            key: k,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchema;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![16])
            .build()
            .unwrap()
    }

    fn tile(rows: &[(i64, Option<f64>)]) -> SparseTile {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for (k, v) in rows {
            let cv = v.map(CellValue::Float64).unwrap_or(CellValue::Null);
            b.push(&[CoordValue::Int64(*k)], &[cv]).unwrap();
        }
        b.build()
    }

    fn fin(r: AggregateResult) -> Option<AggScalar> {
        r.finalize().expect("finalize")
    }

    fn float(v: f64) -> Option<AggScalar> {
        Some(AggScalar::Float(v))
    }

    fn int_tile(dtype: AttrType, values: &[CellValue]) -> SparseTile {
        let s = ArraySchemaBuilder::new("n")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", dtype, true))
            .tile_extents(vec![16])
            .build()
            .unwrap();
        let mut b = SparseTileBuilder::new(&s);
        for (k, v) in values.iter().enumerate() {
            b.push(&[CoordValue::Int64(k as i64)], std::slice::from_ref(v))
                .unwrap();
        }
        b.build()
    }

    #[test]
    fn sum_skips_nulls() {
        let t = tile(&[(0, Some(1.0)), (1, None), (2, Some(3.0))]);
        let r = aggregate_attr(&t, 0, Reducer::Sum);
        assert_eq!(fin(r), float(4.0));
    }

    #[test]
    fn count_includes_nonnull_only() {
        let t = tile(&[(0, Some(1.0)), (1, None), (2, Some(3.0))]);
        let r = aggregate_attr(&t, 0, Reducer::Count);
        assert_eq!(fin(r), Some(AggScalar::Int(2)));
    }

    #[test]
    fn min_max_mean() {
        let t = tile(&[(0, Some(1.0)), (1, Some(5.0)), (2, Some(3.0))]);
        assert_eq!(fin(aggregate_attr(&t, 0, Reducer::Min)), float(1.0));
        assert_eq!(fin(aggregate_attr(&t, 0, Reducer::Max)), float(5.0));
        assert_eq!(fin(aggregate_attr(&t, 0, Reducer::Mean)), float(3.0));
    }

    #[test]
    fn merge_combines_partials_exactly() {
        let a = AggregateResult::Mean {
            sum: AggNumber::Float(10.0),
            count: 4,
        };
        let b = AggregateResult::Mean {
            sum: AggNumber::Float(6.0),
            count: 2,
        };
        // (10 + 6) / (4 + 2) = 16/6
        assert_eq!(fin(a.merge(b)), float(16.0 / 6.0));
    }

    #[test]
    fn empty_reducer_finalizes_to_none() {
        let t = tile(&[(0, None)]);
        let r = aggregate_attr(&t, 0, Reducer::Sum);
        assert_eq!(fin(r), None);
    }

    /// An `INT64` SUM keeps every unit past 2^53, and MIN / MAX stay exact
    /// integers. MEAN is a float.
    #[test]
    fn integer_reducers_are_exact() {
        let big = (1i64 << 53) + 1;
        let t = int_tile(
            AttrType::Int64,
            &[CellValue::Int64(big), CellValue::Null, CellValue::Int64(0)],
        );
        assert_eq!(
            fin(aggregate_attr(&t, 0, Reducer::Sum)),
            Some(AggScalar::Int(big))
        );
        assert_eq!(
            fin(aggregate_attr(&t, 0, Reducer::Max)),
            Some(AggScalar::Int(big))
        );
        assert_eq!(
            fin(aggregate_attr(&t, 0, Reducer::Min)),
            Some(AggScalar::Int(0))
        );
        assert!(matches!(
            fin(aggregate_attr(&t, 0, Reducer::Mean)),
            Some(AggScalar::Float(_))
        ));
    }

    #[test]
    fn integer_sum_past_bigint_is_refused() {
        let t = int_tile(
            AttrType::Int64,
            &[CellValue::Int64(i64::MAX), CellValue::Int64(1)],
        );
        assert!(matches!(
            aggregate_attr(&t, 0, Reducer::Sum).finalize(),
            Err(ArrayError::NumericOverflow { .. })
        ));
    }

    #[test]
    fn narrow_attrs_reduce() {
        let t = int_tile(
            AttrType::UInt16,
            &[CellValue::UInt16(65535), CellValue::UInt16(65535)],
        );
        assert_eq!(
            fin(aggregate_attr(&t, 0, Reducer::Sum)),
            Some(AggScalar::Int(131_070))
        );
        let f = int_tile(
            AttrType::Float32,
            &[CellValue::Float32(0.5), CellValue::Float32(f32::NAN)],
        );
        assert!(matches!(
            fin(aggregate_attr(&f, 0, Reducer::Max)),
            Some(AggScalar::Float(v)) if v.is_nan()
        ));
        assert_eq!(fin(aggregate_attr(&f, 0, Reducer::Min)), float(0.5));
    }

    #[test]
    fn group_by_buckets_by_dim_value() {
        let t = tile(&[
            (0, Some(1.0)),
            (1, Some(2.0)),
            (0, Some(3.0)),
            (1, Some(4.0)),
        ]);
        let g = group_by_dim(&t, 0, 0, Reducer::Sum);
        assert_eq!(g.len(), 2);
        // first-seen order: 0, then 1
        assert_eq!(g[0].key, CoordValue::Int64(0));
        assert_eq!(fin(g[0].result), float(4.0));
        assert_eq!(g[1].key, CoordValue::Int64(1));
        assert_eq!(fin(g[1].result), float(6.0));
    }
}
