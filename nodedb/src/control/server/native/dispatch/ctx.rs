// SPDX-License-Identifier: BUSL-1.1

//! Dispatch context: holds references needed by all per-opcode handlers.
//! Split out of `mod.rs` to keep that file declarations/re-exports only.

use std::sync::Arc;

use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

/// Dispatch context: holds references needed by all handlers.
///
/// `scope` is the single, request-scoped auth contract: it is built once per
/// request (in `session::request::handle_request`) and carries both the
/// resolved `database_id` and the scope-enriched `AuthContext` in lockstep.
/// There is deliberately no separate `auth_context` or `database_id` field
/// here — every handler reads both through `scope` so the two values can
/// never drift apart across native call sites (SQL, direct ops, MATCH,
/// SQL-admin) the way they did before `RequestAuthScope` existed.
pub(crate) struct DispatchCtx<'a> {
    pub state: &'a Arc<SharedState>,
    pub identity: &'a AuthenticatedIdentity,
    pub scope: RequestAuthScope<'a>,
    pub query_ctx: &'a QueryContext,
    pub sessions: &'a SessionStore,
    pub peer_addr: &'a std::net::SocketAddr,
}

impl DispatchCtx<'_> {
    pub(super) fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id
    }

    /// Database scope for this request, as resolved once by `scope` at
    /// request setup. Delegating here (rather than re-querying
    /// `self.sessions` independently) is what keeps this value in lockstep
    /// with `scope.auth().database_id` — see the struct docs.
    pub(super) fn database_id(&self) -> crate::types::DatabaseId {
        self.scope.database_id()
    }

    /// The resolved, scope-enriched `AuthContext` for `$auth.*` RLS
    /// substitution. Every native call site that used to read
    /// `ctx.auth_context` reads this instead, so RLS enforcement (including
    /// `$auth.scope_status(...)`) is identical regardless of which opcode
    /// dispatched the request.
    pub(crate) fn auth_context(&self) -> &crate::control::security::auth_context::AuthContext {
        self.scope.auth()
    }

    pub(super) fn vshard_for_key(&self, key: &str) -> VShardId {
        VShardId::from_key(key.as_bytes())
    }
}
