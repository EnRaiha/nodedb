// SPDX-License-Identifier: Apache-2.0

//! Executor parity contract for `PhysicalPlan`.
//!
//! Every NodeDB deployment that executes physical plans implements
//! [`PhysicalTaskVisitor`] — Origin's Data Plane handlers and Lite's
//! embedded executor both. The trait has no default methods, so adding
//! a new [`PhysicalPlan`] variant becomes a hard compile error on every
//! implementation until it is handled.
//!
//! Method-level granularity is one method per top-level `PhysicalPlan`
//! variant (engine family). Each method receives the variant's inner op
//! enum; the implementer pattern-matches as it sees fit.

use crate::physical_plan::{
    ArrayOp, ClusterArrayOp, ClusterEventOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp,
    PhysicalPlan, QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Per-deployment executor for `PhysicalPlan`.
///
/// Implementations decide their own `Output` and `Error`. Origin's Data
/// Plane handlers may return a row stream; Lite's executor may return a
/// boxed future to a `QueryResult`. The trait stays sync — async backends
/// box their futures and resolve at the call site, mirroring the pattern
/// used by `LiteVisitor` for `PlanVisitor`.
pub trait PhysicalTaskVisitor {
    type Output;
    type Error;

    fn vector(&mut self, op: &VectorOp) -> Result<Self::Output, Self::Error>;
    fn graph(&mut self, op: &GraphOp) -> Result<Self::Output, Self::Error>;
    fn document(&mut self, op: &DocumentOp) -> Result<Self::Output, Self::Error>;
    fn kv(&mut self, op: &KvOp) -> Result<Self::Output, Self::Error>;
    fn text(&mut self, op: &TextOp) -> Result<Self::Output, Self::Error>;
    fn columnar(&mut self, op: &ColumnarOp) -> Result<Self::Output, Self::Error>;
    fn timeseries(&mut self, op: &TimeseriesOp) -> Result<Self::Output, Self::Error>;
    fn spatial(&mut self, op: &SpatialOp) -> Result<Self::Output, Self::Error>;
    fn crdt(&mut self, op: &CrdtOp) -> Result<Self::Output, Self::Error>;
    fn query(&mut self, op: &QueryOp) -> Result<Self::Output, Self::Error>;
    fn meta(&mut self, op: &MetaOp) -> Result<Self::Output, Self::Error>;
    fn array(&mut self, op: &ArrayOp) -> Result<Self::Output, Self::Error>;
    fn cluster_array(&mut self, op: &ClusterArrayOp) -> Result<Self::Output, Self::Error>;
    fn cluster_event(&mut self, op: &ClusterEventOp) -> Result<Self::Output, Self::Error>;
}

/// Dispatch `plan` to the matching method on `visitor`.
/// Adding a [`PhysicalPlan`] variant without a corresponding arm is a
/// compile error.
pub fn dispatch<V: PhysicalTaskVisitor>(
    visitor: &mut V,
    plan: &PhysicalPlan,
) -> Result<V::Output, V::Error> {
    match plan {
        PhysicalPlan::Vector(op) => visitor.vector(op),
        PhysicalPlan::Graph(op) => visitor.graph(op),
        PhysicalPlan::Document(op) => visitor.document(op),
        PhysicalPlan::Kv(op) => visitor.kv(op),
        PhysicalPlan::Text(op) => visitor.text(op),
        PhysicalPlan::Columnar(op) => visitor.columnar(op),
        PhysicalPlan::Timeseries(op) => visitor.timeseries(op),
        PhysicalPlan::Spatial(op) => visitor.spatial(op),
        PhysicalPlan::Crdt(op) => visitor.crdt(op),
        PhysicalPlan::Query(op) => visitor.query(op),
        PhysicalPlan::Meta(op) => visitor.meta(op),
        PhysicalPlan::Array(op) => visitor.array(op),
        PhysicalPlan::ClusterArray(op) => visitor.cluster_array(op),
        PhysicalPlan::ClusterEvent(op) => visitor.cluster_event(op),
    }
}
