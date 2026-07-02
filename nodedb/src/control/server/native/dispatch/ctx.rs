// SPDX-License-Identifier: BUSL-1.1

//! Dispatch context: holds references needed by all per-opcode handlers.
//! Split out of `mod.rs` to keep that file declarations/re-exports only.

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
