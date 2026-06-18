// SPDX-License-Identifier: BUSL-1.1

//! Cluster topology DDL commands: SHOW NODES, SHOW NODE, REMOVE NODE, SHOW CLUSTER.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

pub(super) fn node_state_str(state: nodedb_cluster::NodeState) -> &'static str {
    match state {
        nodedb_cluster::NodeState::Joining => "joining",
        nodedb_cluster::NodeState::Active => "active",
        nodedb_cluster::NodeState::Draining => "draining",
        nodedb_cluster::NodeState::Learner => "learner",
        nodedb_cluster::NodeState::Decommissioned => "decommissioned",
    }
}

/// SHOW NODES — list all cluster members with state.
///
/// Superuser only.
pub fn show_nodes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can list nodes",
        ));
    }

    let schema = Arc::new(vec![
        int8_field("node_id"),
        text_field("address"),
        text_field("state"),
        text_field("raft_groups"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    match &state.cluster_topology {
        Some(t) => {
            let topo = t.read().unwrap_or_else(|p| p.into_inner());
            let mut nodes: Vec<_> = topo.all_nodes().collect();
            nodes.sort_by_key(|n| n.node_id);

            for node in nodes {
                encoder.encode_field(&(node.node_id as i64))?;
                encoder.encode_field(&node.addr)?;
                let state_str = node_state_str(node.state);
                encoder.encode_field(&state_str)?;
                let groups_str: String = node
                    .raft_groups
                    .iter()
                    .map(|g| g.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                encoder.encode_field(&groups_str)?;
                rows.push(Ok(encoder.take_row()));
            }
        }
        None => {
            // Single-node mode: show this node as the only member.
            encoder.encode_field(&(state.node_id as i64))?;
            encoder.encode_field(&"local")?;
            encoder.encode_field(&"active")?;
            encoder.encode_field(&"")?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW NODE <node_id> — detailed info for a specific node.
///
/// Superuser only.
pub fn show_node(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can inspect nodes",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: SHOW NODE <node_id>"));
    }

    let node_id: u64 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", &format!("invalid node_id: '{}'", parts[2])))?;

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let props = match &state.cluster_topology {
        Some(t) => {
            let topo = t.read().unwrap_or_else(|p| p.into_inner());
            let node = match topo.get_node(node_id) {
                Some(n) => n,
                None => {
                    return Err(sqlstate_error(
                        "42704",
                        &format!("node {node_id} not found in cluster topology"),
                    ));
                }
            };
            vec![
                ("node_id".to_string(), node.node_id.to_string()),
                ("address".to_string(), node.addr.clone()),
                ("state".to_string(), format!("{:?}", node.state)),
                (
                    "raft_groups".to_string(),
                    node.raft_groups
                        .iter()
                        .map(|g| g.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            ]
        }
        None => {
            // Single-node mode: show self info if node_id matches.
            if node_id != state.node_id {
                return Err(sqlstate_error(
                    "42704",
                    &format!(
                        "node {node_id} not found (single-node instance, this node is {})",
                        state.node_id
                    ),
                ));
            }
            let wal_lsn = state.wal.next_lsn().as_u64().saturating_sub(1);
            vec![
                ("node_id".to_string(), state.node_id.to_string()),
                ("address".to_string(), "local".to_string()),
                ("state".to_string(), "active".to_string()),
                ("mode".to_string(), "single-node".to_string()),
                ("wal_lsn".to_string(), wal_lsn.to_string()),
            ]
        }
    };

    for (key, value) in &props {
        encoder.encode_field(key)?;
        encoder.encode_field(value)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// REMOVE NODE <node_id> — mark a node as decommissioned.
///
/// Superuser only.
pub fn remove_node(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can remove nodes",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: REMOVE NODE <node_id>"));
    }

    let node_id: u64 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", &format!("invalid node_id: '{}'", parts[2])))?;

    let topo = match &state.cluster_topology {
        Some(t) => t,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let mut topo = topo.write().unwrap_or_else(|p| p.into_inner());

    if !topo.contains(node_id) {
        return Err(sqlstate_error(
            "42704",
            &format!("node {node_id} not found in cluster topology"),
        ));
    }

    topo.set_state(node_id, nodedb_cluster::NodeState::Decommissioned);

    Ok(vec![Response::Execution(Tag::new("REMOVE NODE"))])
}

/// SHOW CLUSTER — cluster overview.
///
/// Superuser only.
pub fn show_cluster(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view cluster status",
        ));
    }

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let mut props = vec![("node_id", state.node_id.to_string())];

    if let Some(topo) = &state.cluster_topology {
        let topo = topo.read().unwrap_or_else(|p| p.into_inner());
        props.push(("nodes_total", topo.node_count().to_string()));
        props.push(("nodes_active", topo.active_nodes().len().to_string()));
        props.push(("topology_version", topo.version().to_string()));
    } else {
        props.push(("mode", "single-node".to_string()));
    }

    if let Some(routing) = &state.cluster_routing {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        props.push(("raft_groups", routing.num_groups().to_string()));
        props.push(("vshards", "1024".to_string()));
    }

    if let Some(status_fn) = state.raft_status_fn.get() {
        let statuses = status_fn();
        let leaders = statuses.iter().filter(|s| s.role == "Leader").count();
        props.push(("groups_leading", leaders.to_string()));
        props.push(("groups_following", (statuses.len() - leaders).to_string()));
    }

    for (key, value) in &props {
        encoder.encode_field(key)?;
        encoder.encode_field(value)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
