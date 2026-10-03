// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral peer health DDL command: SHOW PEER HEALTH.
//!
//! The topology / circuit-breaker reads run here. The result is the
//! protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{cluster_not_started, ddl_err, node_state_str};

/// SHOW PEER HEALTH — circuit breaker state for all known peers.
///
/// Superuser only.
pub fn show_peer_health(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view peer health",
        ));
    }

    let transport = match &state.cluster_transport {
        Some(t) => t,
        None => return Err(cluster_not_started("cluster transport")),
    };

    let topo = match &state.cluster_topology {
        Some(t) => t,
        None => return Err(cluster_not_started("cluster topology")),
    };

    let topo = topo.read().unwrap_or_else(|p| p.into_inner());
    let cb = transport.circuit_breaker();

    let columns = vec![
        "node_id".to_string(),
        "address".to_string(),
        "node_state".to_string(),
        "circuit".to_string(),
        "failures".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
    ];

    let mut nodes: Vec<_> = topo
        .all_nodes()
        .filter(|n| n.node_id != state.node_id)
        .collect();
    nodes.sort_by_key(|n| n.node_id);

    let mut rows = Vec::new();
    for node in nodes {
        let state_str = node_state_str(node.state);
        let circuit = cb.state(node.node_id);
        let circuit_str = match circuit {
            nodedb_cluster::circuit_breaker::CircuitState::Closed => "closed",
            nodedb_cluster::circuit_breaker::CircuitState::Open => "OPEN",
            nodedb_cluster::circuit_breaker::CircuitState::HalfOpen => "half-open",
        };
        let failures = cb.failure_count(node.node_id) as i64;

        let mut row = Map::new();
        row.insert(
            "node_id".to_string(),
            JsonValue::String((node.node_id as i64).to_string()),
        );
        row.insert("address".to_string(), JsonValue::String(node.addr.clone()));
        row.insert(
            "node_state".to_string(),
            JsonValue::String(state_str.to_string()),
        );
        row.insert(
            "circuit".to_string(),
            JsonValue::String(circuit_str.to_string()),
        );
        row.insert(
            "failures".to_string(),
            JsonValue::String(failures.to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
