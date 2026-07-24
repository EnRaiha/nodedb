// SPDX-License-Identifier: BUSL-1.1

//! Temporal `AS OF` extractors for the clone CoW read-path.

/// Extract the `system_as_of_ms` value from a physical plan, if present.
///
/// Used to derive the clone predation query LSN when the SQL query carries
/// `FOR SYSTEM_TIME AS OF <ms>`.  Returns `None` for plan types that do not
/// carry a temporal qualifier (KV, DDL, writes, etc.).
pub(super) fn extract_system_as_of_ms(
    plan: Option<&nodedb_physical::physical_plan::PhysicalPlan>,
) -> Option<i64> {
    use nodedb_physical::physical_plan::PhysicalPlan;
    // Exhaustive match — adding a new top-level engine MUST require an
    // explicit decision here about how `FOR SYSTEM_TIME AS OF` is plumbed
    // (or that it is intentionally unsupported on that engine). A
    // catch-all `_ =>` would silently let new temporal-capable plan
    // variants be ignored by the clone predation check.
    match plan? {
        PhysicalPlan::Document(op) => extract_doc_as_of(op),
        PhysicalPlan::Columnar(op) => extract_columnar_as_of(op),
        PhysicalPlan::Timeseries(op) => extract_timeseries_as_of(op),
        // Index-only / overlay engines (Vector, Text, Spatial, Graph) and
        // engines that do not currently carry a `system_as_of_ms` qualifier
        // on their plan variants (Kv, Crdt, Query, Meta, Array,
        // ClusterArray) are explicitly None. Bitemporal queries against
        // these go through composition with a data-bearing collection;
        // when that changes, add a branch here rather than relaxing this
        // match.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

fn extract_doc_as_of(op: &nodedb_physical::physical_plan::DocumentOp) -> Option<i64> {
    use nodedb_physical::physical_plan::DocumentOp;
    match op {
        DocumentOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}

fn extract_columnar_as_of(op: &nodedb_physical::physical_plan::ColumnarOp) -> Option<i64> {
    use nodedb_physical::physical_plan::ColumnarOp;
    match op {
        ColumnarOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}

fn extract_timeseries_as_of(op: &nodedb_physical::physical_plan::TimeseriesOp) -> Option<i64> {
    use nodedb_physical::physical_plan::TimeseriesOp;
    match op {
        TimeseriesOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}
