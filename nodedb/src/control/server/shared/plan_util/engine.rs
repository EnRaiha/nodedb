// SPDX-License-Identifier: BUSL-1.1

//! Which peer engine a physical plan targets.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::session::read_set::EngineTag;

/// Classify which peer engine a plan targets. Total over the top-level
/// [`PhysicalPlan`] variants (one-to-one with [`EngineTag`]) so a new engine
/// forces an explicit decision rather than a silent default.
pub(crate) fn plan_engine(plan: &PhysicalPlan) -> EngineTag {
    match plan {
        PhysicalPlan::Vector(_) => EngineTag::Vector,
        PhysicalPlan::Graph(_) => EngineTag::Graph,
        PhysicalPlan::Document(_) => EngineTag::Document,
        PhysicalPlan::Kv(_) => EngineTag::Kv,
        PhysicalPlan::Text(_) => EngineTag::Text,
        PhysicalPlan::Columnar(_) => EngineTag::Columnar,
        PhysicalPlan::Timeseries(_) => EngineTag::Timeseries,
        PhysicalPlan::Spatial(_) => EngineTag::Spatial,
        PhysicalPlan::Crdt(_) => EngineTag::Crdt,
        PhysicalPlan::Query(_) => EngineTag::Query,
        PhysicalPlan::Meta(_) => EngineTag::Meta,
        PhysicalPlan::Array(_) => EngineTag::Array,
        PhysicalPlan::ClusterArray(_) => EngineTag::ClusterArray,
        PhysicalPlan::ClusterEvent(_) => EngineTag::Meta,
    }
}
