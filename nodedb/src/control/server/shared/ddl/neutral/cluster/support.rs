// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor + node-state formatter for the protocol-neutral
//! cluster handlers.

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// Preserves the exact SQLSTATE / message the pgwire cluster handlers
/// produced (via `sqlstate_error`), so error parity stays byte-identical
/// after the migration off the pgwire router.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Render a [`nodedb_cluster::NodeState`] the same way the pgwire
/// `topology.rs` handler did — moved verbatim off `pub(super)` in that file.
pub(super) fn node_state_str(state: nodedb_cluster::NodeState) -> &'static str {
    match state {
        nodedb_cluster::NodeState::Joining => "joining",
        nodedb_cluster::NodeState::Active => "active",
        nodedb_cluster::NodeState::Draining => "draining",
        nodedb_cluster::NodeState::Learner => "learner",
        nodedb_cluster::NodeState::Decommissioned => "decommissioned",
    }
}
