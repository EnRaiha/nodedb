// SPDX-License-Identifier: BUSL-1.1

//! Applier-side helpers for replicated scope grants.
//!
//! The `prepare_*` half is pure: it builds the `StoredScopeGrant` a
//! `CatalogEntry::PutScopeGrant` proposal carries, without touching the
//! catalog or the in-memory map. The `install_replicated_*` half mutates only
//! the in-memory map, and runs on every node from the post-apply hook. Durable
//! state is written exclusively by the applier, so the grant a node authorizes
//! with is always one the cluster agreed on.
//!
//! `propose_grant` / `propose_revoke` are the single entry points that turn a
//! prepared record into that replicated write. Every producer of a scope-grant
//! mutation — the DDL handlers and the expiry sweep alike — goes through them,
//! so there is exactly one place that knows how a grant reaches durable state.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::StoredScopeGrant;
use crate::control::security::time::now_secs;
use crate::control::state::SharedState;

use super::store::ScopeGrantStore;
use super::types::{ScopeGrant, ScopeGrantParams, grant_key};

/// Replicate a scope-grant upsert (`GRANT SCOPE`, and `RENEW SCOPE`, which is
/// the same upsert with a later expiry, and the automatic downgrade the expiry
/// sweep issues).
///
/// A grant that only reached the node that decided on it would authorize there
/// and nowhere else, so the catalog write is the applier's job on every node.
/// The `log_index == 0` branch is the standalone-origin path, where there is
/// no raft group to apply the entry.
pub(crate) fn propose_grant(state: &SharedState, stored: &StoredScopeGrant) -> crate::Result<()> {
    let entry = CatalogEntry::PutScopeGrant(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)?;
    if log_index == 0 {
        state.credentials.catalog().put_scope_grant(stored)?;
        state.scope_grants.install_replicated_grant(stored);
    }
    Ok(())
}

/// Replicate a scope-grant removal. Same dual path as [`propose_grant`].
pub(crate) fn propose_revoke(
    state: &SharedState,
    scope_name: &str,
    grantee_type: &str,
    grantee_id: &str,
) -> crate::Result<()> {
    let entry = CatalogEntry::DeleteScopeGrant {
        scope_name: scope_name.to_string(),
        grantee_type: grantee_type.to_string(),
        grantee_id: grantee_id.to_string(),
    };
    let log_index = propose_catalog_entry(state, &entry)?;
    if log_index == 0 {
        state
            .credentials
            .catalog()
            .delete_scope_grant(scope_name, grantee_type, grantee_id)?;
        state
            .scope_grants
            .install_replicated_revoke(scope_name, grantee_type, grantee_id);
    }
    Ok(())
}

/// What a `RENEW SCOPE` resolves to against the current in-memory state.
pub enum RenewOutcome {
    /// No grant exists for this `(scope, grantee_type, grantee_id)`.
    NotFound,
    /// The grant never expires, so there is nothing to extend and nothing
    /// to replicate.
    AlreadyPermanent,
    /// Propose this record to move the expiry out; a renew is an ordinary
    /// upsert carrying a later `expires_at`, so it reuses `PutScopeGrant`.
    Extend(Box<StoredScopeGrant>),
}

impl ScopeGrantStore {
    /// Build a `StoredScopeGrant` ready for replication without mutating state.
    pub fn prepare_grant(&self, params: ScopeGrantParams<'_>) -> crate::Result<StoredScopeGrant> {
        let ScopeGrantParams {
            scope_name,
            grantee_type,
            grantee_id,
            granted_by,
            expires_at,
            grace_period_secs,
            on_expire_action,
            conditions,
        } = params;

        ScopeGrant {
            scope_name: scope_name.into(),
            grantee_type: grantee_type.into(),
            grantee_id: grantee_id.into(),
            granted_by: granted_by.into(),
            granted_at: now_secs(),
            expires_at,
            grace_period_secs,
            on_expire_action: on_expire_action.into(),
            conditions,
        }
        .to_stored()
    }

    /// Build the record that extends an existing grant's expiry by
    /// `extend_secs`, without mutating state.
    ///
    /// The extension is measured from whichever of the current expiry and now
    /// is later, so renewing a grant that already lapsed does not silently
    /// back-date the new deadline into the past.
    pub fn prepare_renew(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
        extend_secs: u64,
    ) -> crate::Result<RenewOutcome> {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        let Some(existing) = grants.get(&key) else {
            return Ok(RenewOutcome::NotFound);
        };
        if existing.expires_at == 0 {
            return Ok(RenewOutcome::AlreadyPermanent);
        }
        let mut renewed = existing.clone();
        renewed.expires_at = existing.expires_at.max(now_secs()) + extend_secs;
        Ok(RenewOutcome::Extend(Box::new(renewed.to_stored()?)))
    }

    /// Install a replicated scope grant into the in-memory map.
    ///
    /// A record whose conditions do not decode is skipped rather than
    /// installed unconditionally: an unreadable restriction has to deny,
    /// never widen. The catalog row the applier already wrote keeps the
    /// payload, so a build that understands it picks the grant up at restart.
    pub fn install_replicated_grant(&self, stored: &StoredScopeGrant) {
        let grant = match ScopeGrant::from_stored(stored) {
            Ok(grant) => grant,
            Err(e) => {
                tracing::warn!(
                    scope = %stored.scope_name,
                    grantee_type = %stored.grantee_type,
                    grantee_id = %stored.grantee_id,
                    error = %e,
                    "install_replicated_grant: undecodable conditions — skipping"
                );
                return;
            }
        };
        let key = grant_key(&stored.scope_name, &stored.grantee_type, &stored.grantee_id);
        self.grants
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, grant);
    }

    /// Remove a replicated scope grant from the in-memory map.
    pub fn install_replicated_revoke(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
    ) -> bool {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        self.grants
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key)
            .is_some()
    }

    /// Build and install a grant in one step, exactly as the replicated apply
    /// path does. Test-only: production must propose `PutScopeGrant` so the
    /// grant reaches every node, and this shorthand deliberately skips that.
    #[cfg(test)]
    pub(crate) fn grant(&self, params: ScopeGrantParams<'_>) -> crate::Result<()> {
        let stored = self.prepare_grant(params)?;
        self.install_replicated_grant(&stored);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params<'a>(
        scope: &'a str,
        grantee_type: &'a str,
        grantee_id: &'a str,
    ) -> ScopeGrantParams<'a> {
        ScopeGrantParams {
            scope_name: scope,
            grantee_type,
            grantee_id,
            granted_by: "admin",
            expires_at: 0,
            grace_period_secs: 0,
            on_expire_action: "",
            conditions: Vec::new(),
        }
    }

    fn install(store: &ScopeGrantStore, p: ScopeGrantParams<'_>) {
        let stored = store.prepare_grant(p).expect("prepare");
        store.install_replicated_grant(&stored);
    }

    #[test]
    fn grant_and_check() {
        let store = ScopeGrantStore::new();
        install(&store, params("profile:read", "user", "u1"));

        assert!(store.has_scope("u1", &[], "profile:read"));
        assert!(!store.has_scope("u1", &[], "orders:write"));
        assert!(!store.has_scope("u2", &[], "profile:read"));
    }

    #[test]
    fn org_scope_inheritance() {
        let store = ScopeGrantStore::new();
        install(&store, params("pro:all", "org", "acme"));

        // User u1 is member of acme → inherits pro:all.
        assert!(store.has_scope("u1", &["acme".into()], "pro:all"));
        // User u2 is NOT member → doesn't inherit.
        assert!(!store.has_scope("u2", &[], "pro:all"));
    }

    #[test]
    fn effective_scopes_union() {
        let store = ScopeGrantStore::new();
        install(&store, params("scope_a", "user", "u1"));
        install(&store, params("scope_b", "org", "acme"));
        install(&store, params("scope_c", "org", "beta"));

        let effective = store.effective_scopes("u1", &["acme".into()]);
        assert!(effective.contains("scope_a")); // Direct user grant.
        assert!(effective.contains("scope_b")); // Via acme org.
        assert!(!effective.contains("scope_c")); // Not member of beta.
    }

    #[test]
    fn replicated_revoke_removes_grant() {
        let store = ScopeGrantStore::new();
        install(&store, params("s1", "user", "u1"));
        assert!(store.has_scope("u1", &[], "s1"));

        assert!(store.install_replicated_revoke("s1", "user", "u1"));
        assert!(!store.has_scope("u1", &[], "s1"));
        // Revoking again reports that nothing was there to remove.
        assert!(!store.install_replicated_revoke("s1", "user", "u1"));
    }

    /// `prepare_grant` is the proposal builder: it must not make the grant
    /// visible, or a failed propose would leave the proposing node
    /// authorizing on a grant no other node has.
    #[test]
    fn prepare_grant_does_not_install() {
        let store = ScopeGrantStore::new();
        let stored = store
            .prepare_grant(params("s1", "user", "u1"))
            .expect("prepare");
        assert_eq!(stored.scope_name, "s1");
        assert_eq!(store.count(), 0);
        assert!(!store.has_scope("u1", &[], "s1"));
    }

    #[test]
    fn renew_extends_from_the_later_of_now_and_current_expiry() {
        let store = ScopeGrantStore::new();
        let future = now_secs() + 1_000;
        install(
            &store,
            ScopeGrantParams {
                expires_at: future,
                ..params("s1", "user", "u1")
            },
        );

        let outcome = store.prepare_renew("s1", "user", "u1", 500).expect("renew");
        match outcome {
            RenewOutcome::Extend(stored) => assert_eq!(stored.expires_at, future + 500),
            _ => panic!("expected an extension"),
        }
        // Still pure: the in-memory grant keeps its original expiry until the
        // replicated put lands.
        assert_eq!(store.scope_expires_at("s1", "user", "u1"), future);
    }

    #[test]
    fn renew_reports_missing_and_permanent_grants() {
        let store = ScopeGrantStore::new();
        assert!(matches!(
            store.prepare_renew("s1", "user", "u1", 500).expect("renew"),
            RenewOutcome::NotFound
        ));

        install(&store, params("s1", "user", "u1"));
        assert!(matches!(
            store.prepare_renew("s1", "user", "u1", 500).expect("renew"),
            RenewOutcome::AlreadyPermanent
        ));
    }
}
