// SPDX-License-Identifier: BUSL-1.1

//! [`RequestAuthScope`] — the resolved, transport-neutral, request-scoped
//! auth contract.
//!
//! Every transport (pgwire, HTTP, native) authenticates a connection once
//! into an [`AuthenticatedIdentity`], but each *request* on that connection
//! still needs its own resolved
//! database and a fully enriched [`AuthContext`]. Prior to this type, that
//! resolution was duplicated per transport in `session_auth::context`
//! (`build_auth_context`, `build_auth_context_with_session`), and each
//! duplicate was one more place that could forget to stamp
//! `auth.database_id` in lockstep with the scalar `database_id` used for
//! `PhysicalTask::database_id`, or forget to call
//! `enrich_auth_context_with_scopes` at all. `RequestAuthScope` makes that
//! resolution a single, infallible, transport-neutral value that a caller
//! either has in fully-resolved form or does not have at all.

use nodedb_types::DatabaseId;

use crate::control::security::auth_context::AuthContext;
use crate::control::security::deny::DenyMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;

use super::builder::RequestAuthScopeBuilder;
use super::stores::AuthStores;

/// A fully resolved, request-scoped authorization contract.
///
/// # Why the fields are private
///
/// `database_id` (the scalar consumed by `PhysicalTask::database_id`) and
/// `auth.database_id` (the `Option<DatabaseId>` consumed by `$auth.*` RLS
/// substitution) must always agree — they describe the same resolved
/// database from two call sites that read at different layers. If either
/// field were a public field or reachable through `&mut`, a caller could
/// update one and forget the other, silently reintroducing the drift this
/// type exists to prevent. The only ways to construct or mutate a
/// `RequestAuthScope` are [`RequestAuthScope::builder`] (which stamps both
/// atomically) and [`RequestAuthScope::rebind_database`] (which re-stamps
/// both atomically). There is no other path in or out.
#[derive(Debug, Clone)]
pub struct RequestAuthScope<'a> {
    /// The session-lifetime identity this scope was resolved from.
    identity: &'a AuthenticatedIdentity,
    /// The rich, RLS-substitutable session context for this request, with
    /// `database_id` already stamped to match [`Self::database_id`].
    auth: AuthContext,
    /// The resolved database for this request, as a non-`Option` scalar —
    /// the exact shape `PhysicalTask::database_id` needs. Always the same
    /// value as `auth.database_id`, which holds it wrapped in `Some`.
    database_id: DatabaseId,
    /// The tenant this scope belongs to, taken from `identity.tenant_id`.
    /// Never sourced from a claim or a builder argument — tenant identity
    /// is server-issued only.
    tenant_id: TenantId,
}

impl<'a> RequestAuthScope<'a> {
    /// Start resolving a `RequestAuthScope` for `identity`.
    ///
    /// `stores` is a required argument (not an optional builder method) so
    /// that scope/quota enrichment cannot be silently skipped by a transport
    /// that forgets to opt in — see [`RequestAuthScopeBuilder`] for why that
    /// matters.
    pub fn builder(
        identity: &'a AuthenticatedIdentity,
        stores: AuthStores<'a>,
    ) -> RequestAuthScopeBuilder<'a> {
        RequestAuthScopeBuilder::new(identity, stores)
    }

    /// Resolve a scope pinned to a specific `database_id`, bypassing
    /// `identity.default_database` / session-database precedence.
    ///
    /// This is the common case at dispatch call sites that already know
    /// exactly which database the physical task must target (RESP, which
    /// always pins `DatabaseId::DEFAULT`; user-issued DDL/DML dispatch, which
    /// already resolved `database_id` upstream) — each such call site was
    /// independently writing `builder(..).with_session_database(Some(db)).build()`,
    /// which is exactly the kind of duplication that let the task/`$auth.*`
    /// database drift this type exists to prevent creep back in a second time.
    ///
    /// This constructor resolves **no client address**, so the scope it
    /// returns carries no `$auth.risk_score` and cannot satisfy a `REQUIRE IP`
    /// grant condition. That is correct for its callers — scopes resolved
    /// downstream of a transport that already admitted the request, for
    /// row-level security, redaction and metering — and it is why the
    /// request-admission gate does not accept this type at all. A scope that
    /// will be presented to an admission door comes from
    /// [`ClientRequestScope::for_database`](super::ClientRequestScope::for_database)
    /// instead, which takes the peer address as a required argument.
    pub fn for_database(
        identity: &'a AuthenticatedIdentity,
        stores: AuthStores<'a>,
        database_id: DatabaseId,
    ) -> Self {
        Self::builder(identity, stores)
            .with_session_database(Some(database_id))
            .build()
    }

    /// Assemble a `RequestAuthScope` from its already-resolved parts.
    ///
    /// Only [`RequestAuthScopeBuilder::build`] may call this — it is the
    /// single point where `database_id` and `auth.database_id` are
    /// guaranteed to have been stamped together.
    pub(super) fn new(
        identity: &'a AuthenticatedIdentity,
        auth: AuthContext,
        database_id: DatabaseId,
    ) -> Self {
        let tenant_id = identity.tenant_id;
        Self {
            identity,
            auth,
            database_id,
            tenant_id,
        }
    }

    /// The session-lifetime identity this scope was resolved from.
    pub fn identity(&self) -> &AuthenticatedIdentity {
        self.identity
    }

    /// The resolved `AuthContext` for `$auth.*` RLS substitution.
    ///
    /// There is deliberately no `auth_mut()` — mutating `auth.database_id`
    /// directly would desynchronize it from [`Self::database_id`]. Callers
    /// that need a per-query `ON DENY` override apply it through
    /// [`Self::with_on_deny_override`] instead.
    pub fn auth(&self) -> &AuthContext {
        &self.auth
    }

    /// The resolved database for this request, as the scalar
    /// `PhysicalTask::database_id` needs.
    pub fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// The tenant this scope belongs to.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Apply a per-query `ON DENY` override (e.g. from `SELECT ... ON DENY
    /// ERROR ...` or `SET LOCAL nodedb.on_deny`).
    ///
    /// Consuming (`self -> Self`) rather than `&mut self` so this stays a
    /// deliberate, explicit rebuild step rather than an ambient mutation
    /// point on the resolved scope.
    pub fn with_on_deny_override(mut self, mode: Option<DenyMode>) -> Self {
        self.auth.on_deny_override = mode;
        self
    }

    /// Re-stamp the scope's database after construction.
    ///
    /// This is the *only* sanctioned way to change a `RequestAuthScope`'s
    /// database once built: it updates [`Self::database_id`] and
    /// `auth.database_id` together, in one place. Any other route (a
    /// public setter on either field individually) would let the two
    /// values drift apart — exactly the defect this type exists to
    /// prevent, since one is read by the physical-plan layer and the other
    /// by RLS predicate substitution.
    pub fn rebind_database(mut self, db: DatabaseId) -> Self {
        self.database_id = db;
        self.auth.database_id = Some(db);
        self
    }
}
