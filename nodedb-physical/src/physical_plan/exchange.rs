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
//! - `Shuffle` is a reserved seam for a future distributed hash-repartition
//!   stage. The coordinator resolver returns an error if it encounters this
//!   variant — real distributed shuffle (partition function, spill, memory
//!   budget, cross-node transport) is a dedicated follow-on effort and is not
//!   implemented here.

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
    /// Reserved seam for a future distributed hash-repartition stage.
    ///
    /// `keys` are `(collection_field, partition_key_alias)` pairs that define
    /// the hash partitioning. `num_parts` is the target partition count.
    /// The coordinator resolver returns an error if this variant is
    /// encountered — real distributed shuffle is not yet implemented.
    Shuffle {
        keys: Vec<(String, String)>,
        num_parts: usize,
    },
}
