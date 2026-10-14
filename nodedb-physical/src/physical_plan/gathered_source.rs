// SPDX-License-Identifier: Apache-2.0

//! Where the rows of a coordinator-gathered
//! [`crate::physical_plan::QueryOp::ProviderScan`] were read from.

/// One resource the plan behind a gathered `ProviderScan` read.
///
/// The coordinator replaces a gathered child plan (a join input, a
/// post-processed body, an aggregate input, a set-operation branch) with a
/// `ProviderScan` of its rows. The task is authorized again after that
/// replacement. Each source keeps the requirement the child had, so that
/// check demands what the check of the child demanded.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GatheredSource {
    /// The child read this collection. It requires Read on it.
    Collection(String),
    /// The child read no collection at this point, such as constant rows.
    /// It requires tenant-wide Read.
    Tenant,
}
