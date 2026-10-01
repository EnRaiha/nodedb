// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor + node-state formatter for the protocol-neutral
//! cluster handlers.

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// The SQLSTATE and message reach the client unchanged.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// The error for a cluster handle a handler reads before `start_raft`
/// installed it. Every node runs a cluster, a one-node cluster included, and
/// boot runs `start_raft` before any listener opens.
pub(super) fn cluster_not_started(what: &str) -> DdlError {
    DdlError::new(
        "XX000",
        format!("the {what} is not installed: start_raft has not run on this node"),
    )
}

/// Render a [`nodedb_cluster::NodeState`] as its lowercase name.
pub(super) fn node_state_str(state: nodedb_cluster::NodeState) -> &'static str {
    match state {
        nodedb_cluster::NodeState::Joining => "joining",
        nodedb_cluster::NodeState::Active => "active",
        nodedb_cluster::NodeState::Draining => "draining",
        nodedb_cluster::NodeState::Learner => "learner",
        nodedb_cluster::NodeState::Decommissioned => "decommissioned",
    }
}
