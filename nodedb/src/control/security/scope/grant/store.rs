// SPDX-License-Identifier: BUSL-1.1

//! `ScopeGrantStore` — the in-memory read path for scope grants.
//!
//! Effective scopes for a user = user scopes UNION team scopes UNION org
//! scopes.
//!
//! The store is deliberately read-only with respect to durable state: it is
//! seeded once from the catalog by [`ScopeGrantStore::open`] and thereafter
//! mutated only by the replicated installers in [`super::replication`]. A
//! scope grant that reached redb without passing through raft would authorize
//! on one node and be invisible on every other, so this store offers no way to
//! write one.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use tracing::{info, warn};

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::time::now_secs;

use super::types::{ScopeGrant, ScopeStatus, grant_key};

/// Thread-safe scope grant store.
pub struct ScopeGrantStore {
    /// Key: `"{scope}:{type}:{id}"` → grant.
    pub(super) grants: RwLock<HashMap<String, ScopeGrant>>,
}

impl ScopeGrantStore {
    pub fn new() -> Self {
        Self {
            grants: RwLock::new(HashMap::new()),
        }
    }

    /// Seed the in-memory map from the catalog at startup.
    pub fn open(catalog: &SystemCatalog) -> crate::Result<Self> {
        let stored = catalog.load_all_scope_grants()?;
        let mut grants = HashMap::with_capacity(stored.len());
        for s in &stored {
            // A grant whose stored conditions cannot be decoded is dropped,
            // not loaded unconditionally: an unreadable restriction has to
            // deny, never widen.
            match ScopeGrant::from_stored(s) {
                Ok(grant) => {
                    let key = grant_key(&s.scope_name, &s.grantee_type, &s.grantee_id);
                    grants.insert(key, grant);
                }
                Err(e) => warn!(
                    scope = %s.scope_name,
                    grantee_type = %s.grantee_type,
                    grantee_id = %s.grantee_id,
                    error = %e,
                    "scope grant dropped at load: conditions could not be decoded"
                ),
            }
        }
        if !grants.is_empty() {
            info!(count = grants.len(), "scope grants loaded from catalog");
        }
        Ok(Self {
            grants: RwLock::new(grants),
        })
    }

    /// Get all effective scope names granted to a specific grantee.
    /// Filters out expired grants.
    pub fn scopes_for(&self, grantee_type: &str, grantee_id: &str) -> Vec<String> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| {
                g.grantee_type == grantee_type && g.grantee_id == grantee_id && g.is_effective()
            })
            .map(|g| g.scope_name.clone())
            .collect()
    }

    /// Get the status of a specific scope grant.
    pub fn scope_status(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
    ) -> ScopeStatus {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .get(&key)
            .map(|g| g.status())
            .unwrap_or(ScopeStatus::None)
    }

    /// Get the expiry timestamp of a scope grant. Returns 0 if permanent or not found.
    pub fn scope_expires_at(&self, scope_name: &str, grantee_type: &str, grantee_id: &str) -> u64 {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants.get(&key).map(|g| g.expires_at).unwrap_or(0)
    }

    /// List grants expiring within the given window (seconds from now).
    pub fn expiring_within(&self, window_secs: u64) -> Vec<ScopeGrant> {
        let now = now_secs();
        let deadline = now + window_secs;
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| g.expires_at > 0 && g.expires_at <= deadline && g.is_effective())
            .cloned()
            .collect()
    }

    /// Every unexpired grant a user holds, directly or through an org
    /// membership.
    ///
    /// Grant *conditions* are not applied here — evaluating them needs the
    /// request's `AuthContext` and client address, which this store does not
    /// have. `enrich_auth_context_with_scopes` is the one place that pairs
    /// these grants with a request and drops the ones whose conditions fail.
    pub fn effective_grants(&self, user_id: &str, org_ids: &[String]) -> Vec<ScopeGrant> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| {
                g.is_effective()
                    && ((g.grantee_type == "user" && g.grantee_id == user_id)
                        || (g.grantee_type == "org" && org_ids.contains(&g.grantee_id)))
            })
            .cloned()
            .collect()
    }

    /// Resolve effective scope *names* for a user.
    ///
    /// Collects: user's direct scopes + org scopes for each org membership.
    /// Filters out expired grants, but — like [`Self::effective_grants`], on
    /// which it is built — not conditional ones: this answers "what has been
    /// granted", which is what introspection (`SHOW MY SCOPES`, security
    /// explain) and usage metering ask. "What applies to *this* request" is
    /// answered per request by `enrich_auth_context_with_scopes`.
    pub fn effective_scopes(&self, user_id: &str, org_ids: &[String]) -> HashSet<String> {
        self.effective_grants(user_id, org_ids)
            .into_iter()
            .map(|g| g.scope_name)
            .collect()
    }

    /// Check if a user (directly or via orgs) has a specific scope.
    pub fn has_scope(&self, user_id: &str, org_ids: &[String], scope_name: &str) -> bool {
        self.effective_scopes(user_id, org_ids).contains(scope_name)
    }

    /// List all grants, optionally filtered by scope name.
    pub fn list(&self, scope_filter: Option<&str>) -> Vec<ScopeGrant> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| scope_filter.is_none_or(|s| g.scope_name == s))
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.grants.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

impl Default for ScopeGrantStore {
    fn default() -> Self {
        Self::new()
    }
}
