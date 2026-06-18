// SPDX-License-Identifier: BUSL-1.1

//! Read handlers: GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::GraphDirection;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH;
use crate::types::TraceId;
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::DatabaseId;

use super::response::payload_to_query_response;

fn to_engine_direction(d: GraphDirection) -> Direction {
    match d {
        GraphDirection::In => Direction::In,
        GraphDirection::Out => Direction::Out,
        GraphDirection::Both => Direction::Both,
    }
}

fn clamp_depth(value: usize, field: &'static str) -> PgWireResult<usize> {
    if value > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(sqlstate_error(
            "22023",
            &format!("{field} {value} exceeds maximum allowed value {MAX_GRAPH_TRAVERSAL_DEPTH}"),
        ));
    }
    Ok(value)
}

/// Check a requested traversal depth against a tenant depth limit.
///
/// `limit = 0` means unlimited — the same convention as `max_connections`.
/// Returns a pgwire error if the depth exceeds a finite limit.
pub(crate) fn check_graph_depth_against_limit(
    depth: usize,
    limit: u32,
    field: &'static str,
) -> PgWireResult<()> {
    if limit > 0 && depth as u32 > limit {
        return Err(sqlstate_error(
            "42P17",
            &format!("{field} {depth} exceeds tenant quota max_graph_depth={limit}"),
        ));
    }
    Ok(())
}

/// Look up the tenant's `max_graph_depth` quota and check against it.
fn check_tenant_graph_depth(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    depth: usize,
    field: &'static str,
) -> PgWireResult<()> {
    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };
    let limit = tenants.quota(tenant_id).max_graph_depth;
    check_graph_depth_against_limit(depth, limit, field)
}

/// `GRAPH TRAVERSE FROM '<node_id>' [DEPTH <n>] [LABEL '<label>'] [DIRECTION in|out|both]`
pub async fn traverse(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    start: String,
    depth: usize,
    edge_label: Option<String>,
    direction: GraphDirection,
) -> PgWireResult<Vec<Response>> {
    if start.is_empty() {
        return Err(sqlstate_error("42601", "missing FROM '<node_id>'"));
    }
    let depth = clamp_depth(depth, "DEPTH")?;
    let tenant_id = identity.tenant_id;
    check_tenant_graph_depth(state, tenant_id, depth, "DEPTH")?;
    let dir = to_engine_direction(direction);

    // Subgraph-shaped dispatcher: emits `{nodes,edges}` JSON matching
    // the remote client's `parse_graph_traverse_json` decoder. Tree
    // DDL aggregates that only need a flat reachable set still call
    // `cross_core_bfs_with_options` directly.
    match crate::control::server::graph_dispatch::cross_core_traverse_subgraph(
        state,
        tenant_id,
        start,
        edge_label,
        dir,
        depth,
        &GraphTraversalOptions::default(),
    )
    .await
    {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// `GRAPH NEIGHBORS OF '<node_id>' [LABEL '<label>'] [DIRECTION in|out|both]`
pub async fn neighbors(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    node: String,
    edge_label: Option<String>,
    direction: GraphDirection,
) -> PgWireResult<Vec<Response>> {
    if node.is_empty() {
        return Err(sqlstate_error("42601", "missing OF '<node_id>'"));
    }
    let dir = to_engine_direction(direction);
    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
        node_id: node,
        edge_label,
        direction: dir,
        rls_filters: Vec::new(),
    });

    match crate::control::server::broadcast::broadcast_to_all_cores(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// `GRAPH PATH FROM '<src>' TO '<dst>' [MAX_DEPTH <n>] [LABEL '<label>']`
///
/// Returns the actual shortest path `[src, hop_1, ..., dst]`. An
/// unreachable destination yields an empty array. Orchestrated by
/// `cross_core_shortest_path`, which records parent pointers per
/// hop so the path can be reconstructed across every topology —
/// single core, single-node multi-core, and clustered.
pub async fn shortest_path(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    src: String,
    dst: String,
    max_depth: usize,
    edge_label: Option<String>,
) -> PgWireResult<Vec<Response>> {
    if src.is_empty() || dst.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH PATH requires FROM '<src>' TO '<dst>'",
        ));
    }
    let max_depth = clamp_depth(max_depth, "MAX_DEPTH")?;
    let tenant_id = identity.tenant_id;
    check_tenant_graph_depth(state, tenant_id, max_depth, "MAX_DEPTH")?;
    match crate::control::server::graph_dispatch::cross_core_shortest_path(
        state, tenant_id, src, dst, edge_label, max_depth,
    )
    .await
    {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::check_graph_depth_against_limit;

    #[test]
    fn tenant_graph_depth_under_bound_succeeds() {
        assert!(check_graph_depth_against_limit(5, 10, "DEPTH").is_ok());
        assert!(check_graph_depth_against_limit(10, 10, "DEPTH").is_ok());
    }

    #[test]
    fn tenant_graph_depth_exceeded_rejected() {
        let err = check_graph_depth_against_limit(11, 10, "DEPTH")
            .expect_err("depth > limit must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("11"), "error must include the requested depth");
        assert!(msg.contains("10"), "error must include the limit");
    }

    #[test]
    fn tenant_graph_depth_zero_means_unlimited() {
        assert!(check_graph_depth_against_limit(usize::MAX, 0, "DEPTH").is_ok());
        assert!(check_graph_depth_against_limit(99999, 0, "MAX_DEPTH").is_ok());
    }
}
