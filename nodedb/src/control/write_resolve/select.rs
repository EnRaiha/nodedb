// SPDX-License-Identifier: BUSL-1.1

//! Pick the [`EngineWriteResolver`] for an intercepted plan.

use crate::bridge::envelope::PhysicalPlan;

use super::columnar::resolver_for_columnar_op;
use super::resolver::EngineWriteResolver;

/// The resolver for `plan`, or `None` when it carries no live predicate to
/// resolve before proposing.
///
/// Exhaustive over `PhysicalPlan` and over each engine's write class: a new
/// state-dependent op with no resolver fails to compile here rather than
/// silently reaching Raft as a bare predicate.
pub fn resolver_for_plan(plan: &PhysicalPlan) -> Option<Box<dyn EngineWriteResolver>> {
    match plan {
        PhysicalPlan::Columnar(op) => resolver_for_columnar_op(op),
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}
