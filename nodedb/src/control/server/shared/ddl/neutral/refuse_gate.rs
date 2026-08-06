// SPDX-License-Identifier: BUSL-1.1

//! The authorization step for hand-built reads that carry no row-filter slot.
//!
//! [`CollectionReadGate`](super::read_gate::CollectionReadGate) injects the
//! caller's RLS read predicate into a plan that has somewhere to put it. A
//! graph traversal, a pattern match, an algorithm run, an edge counter, a CRDT
//! version read, and a checkpoint listing have nowhere: each returns topology,
//! per-node scalars, a counter, merged document state, or catalog metadata,
//! and none of those can carry a row filter.
//!
//! The planner's own injection pass reaches exactly that verdict for these
//! shapes (see `planner::rls_injection::graph` and `::crdt`), but every handler
//! that uses this gate reaches the Data Plane through a broadcast, a scatter,
//! or a hand-built task, so the pass never runs on their plans and the verdict
//! has to be reached at the handler instead — identically, or the read returns
//! rows the policy says are not the caller's to see.
//!
//! [`RefusingReadGate`] is that step in one place: the RBAC check, then the
//! refusal while a read policy applies. A read that names its collection asks
//! the narrow per-collection question; one that cannot name it — an unscoped
//! `MATCH`, tenant-wide `SHOW GRAPH STATS` — asks the tenant-wide one, because
//! a read that names no collection cannot be shown to avoid a protected one.

use nodedb_types::DatabaseId;

use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::rls::PolicyType;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::result::DdlError;
use super::read_gate::CollectionReadGate;

/// SQLSTATE for a policy this delivery shape cannot express.
const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// SQLSTATE for a catalog lookup the gate needs and could not complete.
const SYSTEM_ERROR: &str = "58000";

/// A read authorized like any other, whose result cannot carry a row filter.
///
/// Wraps a [`CollectionReadGate`] so the RBAC half and the redaction inputs
/// stay the ones every other hand-built read resolves, and adds the two
/// questions only a filter-less result has to ask: refuse on the named
/// collection's read policy, and refuse on the tenant-wide one when no
/// collection can be named.
pub struct RefusingReadGate<'a> {
    state: &'a SharedState,
    database_id: DatabaseId,
    inner: CollectionReadGate<'a>,
}

impl<'a> RefusingReadGate<'a> {
    /// Open a gate that has authorized nothing yet.
    ///
    /// For a read whose collection scope is optional (`SHOW GRAPH STATS`) or
    /// spans the tenant (an unscoped `MATCH`), where the caller decides which
    /// question to ask after inspecting its own request.
    pub fn for_request(
        state: &'a SharedState,
        identity: &'a AuthenticatedIdentity,
        database_id: DatabaseId,
    ) -> Self {
        Self {
            state,
            database_id,
            inner: CollectionReadGate::for_request(state, identity, database_id),
        }
    }

    /// Authorize `collection` for reading and refuse while a policy applies —
    /// the shape every single-collection filter-less read needs.
    pub fn open(
        state: &'a SharedState,
        identity: &'a AuthenticatedIdentity,
        database_id: DatabaseId,
        collection: &str,
        what: &str,
    ) -> Result<Self, DdlError> {
        let gate = Self::for_request(state, identity, database_id);
        gate.gate_collection(collection, what)?;
        Ok(gate)
    }

    /// Fail closed unless the caller holds `Read` on `collection` and no read
    /// policy restricts it there.
    ///
    /// `what` completes the sentence "RLS policies on '<collection>' are not
    /// supported with {what}", so it must name the operation and say what its
    /// result carries instead of rows.
    pub fn gate_collection(&self, collection: &str, what: &str) -> Result<(), DdlError> {
        self.inner.authorize(collection)?;
        self.inner.refuse_if_read_policy(collection, what)
    }

    /// Fail closed while any read policy applies to this identity anywhere in
    /// the tenant.
    ///
    /// For a read that names no collection, so the narrow question cannot be
    /// asked and the read cannot be shown to avoid a protected collection.
    /// Mirrors `rls_injection`'s tenant-wide fallback for the same shapes.
    pub fn refuse_if_any_read_policy(&self, what: &str) -> Result<(), DdlError> {
        if self.auth().is_superuser() || !self.tenant_has_read_policy() {
            return Ok(());
        }
        Err(DdlError {
            sqlstate: FEATURE_NOT_SUPPORTED.to_string(),
            message: format!(
                "RLS is not supported with {what} while a read policy applies to this identity \
                 and the read names no collection"
            ),
        })
    }

    /// Fail closed unless the caller holds `Read` on every active collection of
    /// the gate's database.
    ///
    /// For a read whose scope is the whole tenant rather than one collection:
    /// the set it may touch is the set it must be granted, so the first denial
    /// refuses the read rather than letting it walk on and disclose the rest.
    pub fn authorize_every_collection(&self) -> Result<(), DdlError> {
        for collection in self.active_collections()? {
            self.inner.authorize(&collection)?;
        }
        Ok(())
    }

    /// Whether the caller holds `Read` on `collection`.
    ///
    /// Answers the same question [`Self::gate_collection`] fails closed on, but
    /// as a value and without emitting a denial: a tenant-wide read narrows its
    /// result to the collections this returns `true` for, and a row dropped
    /// that way is not a denied request.
    pub fn may_read(&self, collection: &str) -> bool {
        crate::control::server::shared::authorization::authorize_collection(
            self.inner.identity(),
            self.database_id,
            collection,
            Permission::Read,
            &self.state.permissions,
            &self.state.roles,
            &NoopAuditEmitter,
        )
        .is_ok()
    }

    /// The tenant this gate is scoped to.
    pub fn tenant_id(&self) -> TenantId {
        self.inner.tenant_id()
    }

    /// The resolved authorization context for `$auth.*` substitution and for
    /// the redaction refusals a graph read applies beside this gate.
    pub fn auth(&self) -> &AuthContext {
        self.inner.auth()
    }

    /// Names of the database's currently active collections.
    ///
    /// A catalog failure is an error rather than an empty list: an empty list
    /// would make [`Self::authorize_every_collection`] pass vacuously.
    fn active_collections(&self) -> Result<Vec<String>, DdlError> {
        let catalog = self.state.credentials.catalog();
        let records = catalog
            .load_collections_for_tenant(self.database_id, self.tenant_id().as_u64())
            .map_err(|error| DdlError {
                sqlstate: SYSTEM_ERROR.to_string(),
                message: format!(
                    "unable to resolve the tenant's collections to authorize: {error}"
                ),
            })?;
        Ok(records
            .into_iter()
            .filter(|record| record.is_active)
            .map(|record| record.name)
            .collect())
    }

    /// Whether any enabled, non-vacuous read policy exists in this tenant.
    ///
    /// A policy with no compiled predicate filters nothing, so it is ignored
    /// here exactly as `combined_read_predicate_with_auth` ignores it.
    fn tenant_has_read_policy(&self) -> bool {
        self.state
            .rls
            .all_policies_for_tenant(self.tenant_id().as_u64())
            .iter()
            .any(|policy| {
                policy.enabled
                    && policy.compiled_predicate.is_some()
                    && matches!(policy.policy_type, PolicyType::Read | PolicyType::All)
            })
    }
}
