// SPDX-License-Identifier: BUSL-1.1

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;

/// Security context for query planning — bundles identity + permission stores.
///
/// Used by `plan_sql_with_rls` to check EXECUTE permissions on user UDFs
/// and inject RLS predicates.
pub struct PlanSecurityContext<'a> {
    pub identity: &'a AuthenticatedIdentity,
    pub auth: &'a crate::control::security::auth_context::AuthContext,
    pub rls_store: &'a crate::control::security::rls::RlsPolicyStore,
    /// Redaction policy registry, consulted to refuse plans whose results the
    /// result-path masking hook cannot rewrite (aggregates over a redacted
    /// column, graph traversals).
    pub redaction_store: &'a crate::control::security::redaction::RedactionStore,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    /// Permission tree cache for hierarchical ACL injection.
    /// `None` = skip permission tree filtering (e.g., internal queries).
    pub permission_cache: Option<&'a crate::control::security::permission_tree::PermissionCache>,
}
