// SPDX-License-Identifier: BUSL-1.1

//! The plans a plan reads its rows from, one level down.

use nodedb_physical::physical_plan::PhysicalPlan;

use super::{document, query, vector};

/// The plans whose rows `plan` reads. Exhaustive over [`PhysicalPlan`].
pub fn children(plan: &PhysicalPlan) -> Vec<&PhysicalPlan> {
    match plan {
        PhysicalPlan::Query(op) => query::children(op),
        PhysicalPlan::Document(op) => document::child(op).into_iter().collect(),
        PhysicalPlan::Vector(op) => vector::vector_child(op).into_iter().collect(),
        PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Vec::new(),
    }
}
