// SPDX-License-Identifier: BUSL-1.1

//! Per-opcode dispatch handlers for the native protocol.

mod auth;
mod conversion;
mod direct_ops;
mod edge_recon_gate;
mod limits;
mod pgwire_bridge;
mod plan_builder;
mod session_ops;
mod sql;
mod sql_gateway;
mod streaming;
mod transaction;

pub(crate) use auth::{handle_auth, handle_ping};
pub(crate) use conversion::{error_to_native, parse_json_to_columns_rows};
pub(crate) use direct_ops::{handle_direct_op, handle_graph_match};
pub(crate) use session_ops::{handle_reset, handle_set, handle_show};
pub(crate) use sql::{handle_sql, handle_sql_streaming};
pub(crate) use streaming::{SqlOutcome, SqlStream};
pub(crate) use transaction::{handle_begin, handle_commit, handle_rollback};

use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::session::SessionStore;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

/// Dispatch context: holds references needed by all handlers.
pub(crate) struct DispatchCtx<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub auth_context: &'a crate::control::security::auth_context::AuthContext,
    pub query_ctx: &'a QueryContext,
    pub sessions: &'a SessionStore,
    pub peer_addr: &'a std::net::SocketAddr,
}

impl DispatchCtx<'_> {
    pub(super) fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id
    }

    /// Database scope for this connection: the session's current database,
    /// falling back to the default database when none is selected.
    pub(super) fn database_id(&self) -> crate::types::DatabaseId {
        self.sessions
            .get_current_database(self.peer_addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT)
    }

    pub(super) fn vshard_for_key(&self, key: &str) -> VShardId {
        VShardId::from_key(key.as_bytes())
    }
}
