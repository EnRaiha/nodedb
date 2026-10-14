// SPDX-License-Identifier: Apache-2.0

//! Which aggregate shapes the grouped timeseries scan computes natively.
//!
//! The grouped scan groups by plain columns and one optional `time_bucket`
//! key, and accumulates `(function, column)` pairs over typed column
//! vectors. Every other shape runs as the generic aggregate over the raw
//! timeseries row source.

use crate::engine_rules::AggregateParams;
use crate::types::{AggregateExpr, ColumnInfo, SqlDataType, SqlExpr};

/// Aggregate functions the grouped timeseries scan accumulates.
const NATIVE_FUNCTIONS: &[&str] = &["count", "sum", "avg", "min", "max", "ts_stddev"];

/// The grouped-scan form of an aggregate.
pub(super) struct NativeShape {
    /// Plain GROUP BY columns, without the `time_bucket` key.
    pub group_columns: Vec<String>,
    /// `time_bucket` width in milliseconds. 0 = no bucketing.
    pub bucket_interval_ms: i64,
}

/// `Some` when the grouped timeseries scan computes `p` exactly. `None`
/// when any part of it needs the generic aggregate.
pub(super) fn native_shape(p: &AggregateParams) -> Option<NativeShape> {
    // Grouping sets, input joins, and aggregates only HAVING names have no
    // grouped-scan form: its output carries every aggregate it computes.
    if p.grouping_sets.is_some() || p.input_joins || p.hidden_aggregates {
        return None;
    }
    let mut group_columns = Vec::with_capacity(p.group_by.len());
    let mut bucket_interval_ms = 0;
    for (index, key) in p.group_by.iter().enumerate() {
        match key {
            SqlExpr::Column { name, .. } => group_columns.push(name.clone()),
            SqlExpr::Function { .. }
            | SqlExpr::BinaryOp { .. }
            | SqlExpr::UnaryOp { .. }
            | SqlExpr::Cast { .. }
            | SqlExpr::IsNull { .. }
            | SqlExpr::Case { .. }
            | SqlExpr::InList { .. }
            | SqlExpr::Between { .. }
            | SqlExpr::Like { .. }
            | SqlExpr::ArrayLiteral(_)
            | SqlExpr::Literal(_)
            | SqlExpr::Subquery(_)
            | SqlExpr::Wildcard => match p.time_bucket {
                Some(tb) if tb.group_index == index && tb.interval_ms > 0 => {
                    bucket_interval_ms = tb.interval_ms;
                }
                _ => return None,
            },
        }
    }
    // A HAVING term over a `time_bucket` key binds to an expression name the
    // grouped scan's `bucket` column does not carry.
    if !p.having.is_empty() && bucket_interval_ms > 0 {
        return None;
    }
    p.aggregates
        .iter()
        .all(|a| native_aggregate(a, &p.columns))
        .then_some(NativeShape {
            group_columns,
            bucket_interval_ms,
        })
}

/// Whether the grouped scan accumulates `a` exactly.
fn native_aggregate(a: &AggregateExpr, columns: &[ColumnInfo]) -> bool {
    if a.distinct
        || a.grouping_col_index.is_some()
        || a.filter_text.is_some()
        || !NATIVE_FUNCTIONS.contains(&a.function.as_str())
    {
        return false;
    }
    match a.args.as_slice() {
        [] | [SqlExpr::Wildcard] => a.function == "count",
        [SqlExpr::Column { name, .. }] => {
            columns.iter().find(|c| &c.name == name).is_some_and(|c| {
                a.function == "count"
                    || matches!(
                        c.data_type,
                        SqlDataType::Int64
                            | SqlDataType::Float64
                            | SqlDataType::Timestamp
                            | SqlDataType::Timestamptz
                    )
            })
        }
        _ => false,
    }
}
