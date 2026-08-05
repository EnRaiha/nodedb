// SPDX-License-Identifier: BUSL-1.1

//! [`AuthStores`] — the auth-adjacent stores every [`RequestAuthScope`]
//! construction needs to enrich the resulting `AuthContext`.
//!
//! [`RequestAuthScope`]: super::RequestAuthScope

use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::scope::grant::ScopeGrantStore;

/// Bundles the stores [`RequestAuthScope::builder`](super::RequestAuthScope::builder)
/// and [`RequestAuthScope::for_database`](super::RequestAuthScope::for_database)
/// need, grouped into one struct instead of widening those constructors'
/// argument list per store.
///
/// `scope_grants` was already a required constructor argument (not an
/// optional builder method) so scope-status enrichment could never be
/// silently skipped by a transport that forgot to opt in — see
/// [`RequestAuthScopeBuilder`](super::RequestAuthScopeBuilder)'s doc comment.
/// `quota_manager` needs exactly the same treatment for
/// `quota_remaining`/`quota_pct`: without it, `$auth.quota_remaining(...)`
/// resolves to `None` in RLS predicates, which is indistinguishable from
/// "this user has no such quota" and fails closed. Bundling both into one
/// required struct keeps that guarantee for both stores at once, and gives
/// future auth-adjacent stores a home that doesn't grow the constructor
/// signature further.
#[derive(Clone, Copy)]
pub struct AuthStores<'a> {
    pub scope_grants: &'a ScopeGrantStore,
    pub quota_manager: &'a QuotaManager,
}

impl<'a> AuthStores<'a> {
    pub fn new(scope_grants: &'a ScopeGrantStore, quota_manager: &'a QuotaManager) -> Self {
        Self {
            scope_grants,
            quota_manager,
        }
    }
}
