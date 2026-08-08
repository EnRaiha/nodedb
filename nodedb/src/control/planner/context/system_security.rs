// SPDX-License-Identifier: BUSL-1.1

//! The security context server-owned SQL planning runs under.
//!
//! Every path that plans SQL goes through
//! [`QueryContext::plan_sql_with_rls_and_versions`](super::QueryContext::plan_sql_with_rls_and_versions),
//! which always runs the injection pass — there is no un-injected planning
//! entry point to reach for. Server-owned work that has no external requester
//! (an AFTER-trigger action, a stored-procedure body, a cross-shard traversal
//! fan-out) still has to pass a context, and this is it: an internal-service
//! identity carrying catalog superuser authority and NO roles.
//!
//! Both halves matter, and neither is incidental:
//!
//! - **Superuser authority** is what the read and write predicate compilers
//!   short-circuit on, so no row policy is applied to work the server issues to
//!   itself.
//! - **An empty role set** is what the redaction-refusal pass short-circuits on:
//!   a redaction rule is keyed on a role, and an identity holding none can match
//!   no rule.
//!
//! Reaching for this is a deliberate statement that the statement has no
//! requester to govern. Anything a client typed DOES have one, and must plan
//! under that requester's own scope instead — planning it as the system is
//! exactly how a transport ends up bypassing row-level security.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, DatabaseSet};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::security::PlanSecurityContext;

/// Owns the identity and auth context a server-owned plan borrows.
///
/// [`PlanSecurityContext`] borrows both, so they need a home that outlives the
/// planning call; constructing one of these is that home.
pub struct SystemPlanSecurity {
    identity: AuthenticatedIdentity,
    auth: AuthContext,
}

impl SystemPlanSecurity {
    /// Build the context `actor`'s server-owned work plans under.
    ///
    /// `actor` names the subsystem and becomes the identity's username, so an
    /// audit row or a trace attributes the statement to the path that issued
    /// it rather than to an anonymous system principal.
    pub fn new(tenant_id: TenantId, actor: &'static str) -> Self {
        let identity = AuthenticatedIdentity::new_internal_service(
            0,
            actor,
            tenant_id,
            Vec::new(),
            true,
            None,
            DatabaseSet::All,
        );
        let auth = AuthContext::from_identity(&identity, format!("s_{actor}"));
        Self { identity, auth }
    }

    /// Borrow it as a planning security context over `state`'s policy stores.
    ///
    /// `permission_cache` is `None`: hierarchical ACL filtering narrows a
    /// requester's view of a collection, and this context has no requester
    /// whose view could be narrowed.
    pub fn context<'a>(&'a self, state: &'a SharedState) -> PlanSecurityContext<'a> {
        PlanSecurityContext {
            identity: &self.identity,
            auth: &self.auth,
            rls_store: &state.rls,
            redaction_store: &state.redaction,
            permissions: &state.permissions,
            roles: &state.roles,
            permission_cache: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two properties every server-owned plan depends on: superuser
    /// authority (so no row policy applies) and no roles (so no redaction rule
    /// can match). A regression in either silently changes what internal work
    /// is allowed to read.
    #[test]
    fn the_system_context_is_superuser_and_roleless() {
        let security = SystemPlanSecurity::new(TenantId::new(7), "_system_test");
        assert!(security.identity.is_superuser());
        assert!(security.auth.is_superuser());
        assert!(
            security.auth.roles.is_empty(),
            "a role would let a redaction rule match server-owned work"
        );
        assert_eq!(security.identity.tenant_id, TenantId::new(7));
    }
}
