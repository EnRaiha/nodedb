// SPDX-License-Identifier: Apache-2.0

//! Exchange is the single coordinator-mediated data-movement operator in the
//! physical plan tree. It is resolved by the Control-Plane coordinator and
//! NEVER executed on a Data-Plane core.
//!
//! - `Gather` fans the child plan out to all cores and merges their results
//!   back on the coordinator. When `as_aggregate` is true the merge is an
//!   aggregate reduction; otherwise it is a plain concatenation.
//! - `Broadcast` gathers the child plan to the coordinator so that its result
//!   can be embedded as an inline input into a sibling operator (e.g. the
//!   build side of a `HashJoin`).
//! - `Shuffle` wraps a complete `HashJoin` at the plan root and drives a
//!   cross-node hash-repartition grace join: the coordinator resolver fans
//!   per-side scan producers to each collection's owner, repartitions rows on
//!   the join keys to part-owners, runs the node-local grace join on each part,
//!   and merges the results. It is coordinator-resolved and NEVER reaches a
//!   core.

/// Data-movement node; coordinator-resolved, never reaches a core.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ExchangeOp {
    /// Child plan that produces the data to be moved.
    pub child: Box<crate::physical_plan::PhysicalPlan>,
    /// How the child's data is moved.
    pub mode: ExchangeMode,
}

/// Movement strategy for an [`ExchangeOp`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ExchangeMode {
    /// Fan the child plan to all Data-Plane cores and merge results on the
    /// coordinator. When `as_aggregate` is true the merge is an aggregate
    /// reduction (partial-aggregate results combined); when false it is a
    /// plain concatenation.
    Gather { as_aggregate: bool },
    /// Gather the child plan to the coordinator so its result can be embedded
    /// as an inline input into a sibling operator (e.g. the build side of a
    /// `HashJoin`).
    Broadcast,
    /// Cross-node distributed hash-repartition grace join, wrapping a complete
    /// `HashJoin` at the plan root.
    ///
    /// `keys` are the `(left_field, right_field)` equi-join pairs: the left
    /// column partitions the probe side and the right column the build side so
    /// matching rows co-locate on the same part. `num_parts` is the target
    /// partition count (`0` = let the coordinator default to the cluster
    /// data-node count). Resolved on the coordinator by `super::shuffle`.
    Shuffle {
        keys: Vec<(String, String)>,
        num_parts: usize,
    },
    /// Cross-node distributed hash-repartition shuffle-AGGREGATE, wrapping a
    /// complete `QueryOp::Aggregate` at the plan root. `keys` are the GROUP BY
    /// column names (single-side, unlike `Shuffle`'s join-key pairs). `num_parts`
    /// (0 = default to cluster data-node count). Resolved on the coordinator by
    /// `super::shuffle_aggregate`.
    ShuffleAggregate { keys: Vec<String>, num_parts: usize },
}
