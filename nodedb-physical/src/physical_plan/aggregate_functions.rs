// SPDX-License-Identifier: Apache-2.0

//! The aggregate functions the generic aggregate accumulates.
//!
//! An [`AggregateSpec`](super::AggregateSpec) sent to the generic aggregate
//! names one of these functions, or the `grouping` pseudo-aggregate. The
//! planner refuses any other function before it builds the spec.

/// Function names with a generic accumulator.
pub const GENERIC_AGGREGATE_FUNCTIONS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "sum_distinct",
    "avg_distinct",
    "min",
    "max",
    "count_distinct",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "ts_stddev",
    "approx_count_distinct",
    "approx_percentile",
    "approx_topk",
    "array_agg",
    "array_agg_distinct",
    "percentile_cont",
    "string_agg",
    "group_concat",
];

/// Generic aggregates that take a constant parameter beside their input:
/// the `k` of `approx_topk`, the percentile of `approx_percentile` and
/// `percentile_cont`. Each spec carries it in
/// [`AggregateSpec::param`](super::AggregateSpec::param), and a spec
/// without it is refused.
pub const PARAMETERIZED_AGGREGATE_FUNCTIONS: &[&str] =
    &["approx_topk", "approx_percentile", "percentile_cont"];

/// The constant parameter of a parameterized aggregate.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AggregateParam {
    /// The fraction of `approx_percentile` / `percentile_cont`, in `[0, 1]`.
    Percentile { fraction: f64 },
    /// The `k` of `approx_topk`, at least 1.
    TopK { k: u32 },
}
