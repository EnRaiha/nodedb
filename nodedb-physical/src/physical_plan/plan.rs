// SPDX-License-Identifier: Apache-2.0

//! The top-level physical plan enum — the wire shape and nothing else.

use super::{
    ArrayOp, ClusterArrayOp, ClusterEventOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp,
    QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Physical plan dispatched to the Data Plane.
///
/// Each variant wraps a per-engine operation enum. The Data Plane dispatcher
/// matches on the top-level variant, then delegates to engine-specific handlers.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum PhysicalPlan {
    /// Vector engine: HNSW search, insert, delete, params.
    Vector(VectorOp),
    /// Graph engine: edges, traversal, algorithms, pattern matching.
    Graph(GraphOp),
    /// Document engine: point CRUD, scans, indexes, bulk DML.
    Document(DocumentOp),
    /// KV engine: hash-indexed point ops, TTL, batch ops.
    Kv(KvOp),
    /// Full-text search: BM25, hybrid vector+text.
    Text(TextOp),
    /// Columnar engine (base): scan + insert for plain columnar collections.
    Columnar(ColumnarOp),
    /// Timeseries profile: extends columnar with time-range + bucketing.
    Timeseries(TimeseriesOp),
    /// Spatial profile: extends columnar with R-tree + OGC predicates.
    Spatial(SpatialOp),
    /// CRDT engine: read, apply delta, set policy.
    Crdt(CrdtOp),
    /// Query operations: joins, aggregates, and the coordinator-resolved
    /// `Exchange` data-movement node (see `QueryOp::Exchange`).
    Query(QueryOp),
    /// Meta / maintenance: WAL, cancel, snapshot, compact, checkpoint.
    Meta(MetaOp),
    /// Array engine: ND-array query operators + put/delete/flush/compact.
    Array(ArrayOp),
    /// Cluster-mode array operations executed by the coordinator on the
    /// Control Plane. Never sent to the Data Plane.
    ClusterArray(ClusterArrayOp),
    /// Event-Plane operations executed by a receiving Control Plane.
    /// Never sent to the Data Plane.
    ClusterEvent(ClusterEventOp),
}
