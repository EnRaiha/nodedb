// SPDX-License-Identifier: BUSL-1.1

//! [`ClientRequestScope`] — a resolved request scope bound to the client
//! address it was resolved against.
//!
//! # Why this type exists
//!
//! The request-admission gate needs two things about a request: the resolved
//! [`RequestAuthScope`] (for the identity, account status, `$auth.risk_score`
//! and the rate-limit key) and the client's peer address (for the IP half of
//! the blacklist). Those used to be two independent arguments, which meant a
//! transport could hand the gate a real peer address while having built its
//! scope without one. That is not a hypothetical: the ingest and sync
//! admission doors did exactly that, so `$auth.risk_score` was never stamped,
//! the gate refused every request as unassessed the moment `[auth.risk]` was
//! enabled, and every `REQUIRE IP` scope grant was silently withheld — while
//! the call site looked correct, because the address *was* right there in the
//! argument list.
//!
//! Pairing them in one value removes the failure mode by construction: the
//! address enters exactly once, through
//! [`RequestAuthScopeBuilder::build_for_client`](super::RequestAuthScopeBuilder::build_for_client),
//! and both the scope's risk scoring / `REQUIRE IP` evaluation and the gate's
//! IP blacklist read that same string. There is no second argument to disagree
//! with.
//!
//! The admission gate takes only this type, never a bare [`RequestAuthScope`],
//! so a future transport that resolves an address-less scope through
//! [`RequestAuthScope::for_database`] and tries to admit it does not compile.
//!
//! # Requests with no client socket
//!
//! There is no address-less variant here, and that is deliberate. Every door
//! this type reaches is a client-facing one, and server-originated work
//! (Raft apply, trigger dispatch, the scheduler, CRDT replay, WAL replay) is
//! already handled by a named case one layer up: those requests carry an
//! [`AuthenticatedIdentity::is_internal_service`] identity, which both
//! admission doors short-circuit on before any address is consulted. Adding a
//! "no address" variant here would give a client-facing transport a legal way
//! to say nothing at all — which is the omission this type exists to prevent.
//!
//! Scopes that never reach an admission door (row-level-security and redaction
//! resolution inside the SQL execution tree, metering readbacks) keep using
//! [`RequestAuthScope::for_database`] and are unaffected.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;

use super::resolved::RequestAuthScope;
use super::stores::AuthStores;

/// A [`RequestAuthScope`] together with the client address it was resolved
/// against — the only shape the request-admission gate accepts.
///
/// `'a` is the scope's own lifetime (borrowed identity); `'p` is the peer
/// address's, which is typically shorter (a `SocketAddr` rendered into a
/// local `String` for the duration of one request).
#[derive(Debug)]
pub struct ClientRequestScope<'a, 'p> {
    scope: RequestAuthScope<'a>,
    peer_addr: &'p str,
}

impl<'a, 'p> ClientRequestScope<'a, 'p> {
    /// Bind an already-built scope to the address it was built from.
    ///
    /// Only [`RequestAuthScopeBuilder::build_for_client`](super::RequestAuthScopeBuilder::build_for_client)
    /// may call this: it is the single point where the address stamped into
    /// the scope and the address carried alongside it are guaranteed to be
    /// the same string.
    pub(super) fn new(scope: RequestAuthScope<'a>, peer_addr: &'p str) -> Self {
        Self { scope, peer_addr }
    }

    /// Resolve a scope pinned to `database_id` against the transport's real
    /// peer address — the constructor every admission door uses.
    ///
    /// This is [`RequestAuthScope::for_database`] with the address the gate
    /// requires, and it exists so a transport that needs no other builder
    /// option writes one call rather than a builder chain it could get
    /// half-right.
    ///
    /// `peer_addr` must be the genuine remote address in whatever shape the
    /// socket layer produced (`10.1.2.3:5432`, `[::1]:5432`, or a bare
    /// address) — never a placeholder or a transport label. Anything that
    /// does not parse as an address leaves the scope unassessed, which the
    /// admission gate refuses whenever risk scoring is enabled, and cannot
    /// satisfy a `REQUIRE IP` grant condition.
    pub fn for_database(
        identity: &'a AuthenticatedIdentity,
        stores: AuthStores<'a>,
        database_id: DatabaseId,
        peer_addr: &'p str,
    ) -> Self {
        RequestAuthScope::builder(identity, stores)
            .with_session_database(Some(database_id))
            .build_for_client(peer_addr)
    }

    /// The resolved scope, for everything downstream of admission (planning,
    /// row-level security, redaction, metering).
    pub fn scope(&self) -> &RequestAuthScope<'a> {
        &self.scope
    }

    /// The client address this scope was resolved against.
    pub fn peer_addr(&self) -> &'p str {
        self.peer_addr
    }

    /// Consume the binding once admission has run, keeping the scope.
    pub fn into_scope(self) -> RequestAuthScope<'a> {
        self.scope
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::security::metering::quota::QuotaManager;
    use crate::control::security::risk::{RiskConfig, RiskScorer};
    use crate::control::security::scope::grant::ScopeGrantStore;
    use crate::types::TenantId;

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// The whole point of the type: the address the gate reads and the
    /// address the scope was scored against are the same string.
    #[test]
    fn the_bound_address_is_the_one_the_scope_was_scored_against() {
        let identity = identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::new(RiskConfig {
            enabled: true,
            allow_threshold: 1.0,
            deny_threshold: 2.0,
            ..Default::default()
        });
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let request = ClientRequestScope::for_database(
            &identity,
            stores,
            DatabaseId::new(4),
            "10.0.0.1:5432",
        );

        assert_eq!(request.peer_addr(), "10.0.0.1:5432");
        assert!(
            request.scope().auth().risk_score.is_some(),
            "the bound address must have reached the risk scorer"
        );
        assert_eq!(request.scope().database_id(), DatabaseId::new(4));
        assert_eq!(
            request.scope().auth().database_id,
            Some(DatabaseId::new(4)),
            "for_database must still stamp both database fields in lockstep"
        );
    }

    #[test]
    fn into_scope_keeps_the_resolved_scope() {
        let identity = identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = ClientRequestScope::for_database(
            &identity,
            stores,
            DatabaseId::new(9),
            "10.0.0.1:5432",
        )
        .into_scope();

        assert_eq!(scope.database_id(), DatabaseId::new(9));
    }
}
