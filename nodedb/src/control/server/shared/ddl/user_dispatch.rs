// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane dispatch for DDL and DSL statements a user issued.
//!
//! These statements have a principal behind them, so they take the authorized
//! door: the plan is authorized into a capability, row-level security is
//! applied, and the capability is what reaches storage. Statement handlers use
//! this instead of the system door, which exists only for work no user asked
//! for.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::authorization::{AuthorizedTask, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::sync_dispatch::dispatch_authorized;

/// Whether the caller already ran
/// [`check_request_admission`](crate::control::server::session_auth::check_request_admission)
/// for this request at its own transport entry point.
///
/// Every native/pgwire/HTTP caller of this door reaches it only through
/// `shared::ddl::dispatch`, which every one of those transports calls AFTER
/// its own single per-request admission gate — so those callers must pass
/// [`RequestAdmission::AlreadyAdmitted`] or the request is charged against
/// its rate-limit budget twice. The one caller that reaches this door
/// directly, bypassing `shared::ddl::dispatch` entirely — the CDC-sync
/// shape-subscription snapshot (`sync::async_dispatch::shape::snapshot`) —
/// has no earlier admission call on its path (shape subscribe deliberately
/// runs only blacklist + quota, not the full gate) and must pass
/// [`RequestAdmission::NotYetAdmitted`] so this remains the one place that
/// request is ever admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestAdmission {
    /// The caller's own transport entry already ran the full admission gate
    /// for this request; running it again here would double-charge it.
    AlreadyAdmitted,
    /// Nothing upstream of this call has admitted the request yet — this is
    /// the one gate it passes through.
    NotYetAdmitted,
}

/// Authorize `plan` for `identity`, apply row-level security, and dispatch it.
///
/// Returns the Data-Plane payload. Authorization failures and policy refusals
/// surface as typed errors before anything reaches storage.
pub(crate) async fn dispatch_for_identity(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
    admission: RequestAdmission,
) -> crate::Result<Vec<u8>> {
    let authorized =
        authorize_for_identity(state, identity, database_id, collection, plan, admission)?;
    dispatch_authorized(state, authorized, collection, timeout).await
}

/// Resolve the request-scoped auth contract for `identity` against
/// `database_id`, apply row-level security, and authorize the resulting
/// `PhysicalTask`.
///
/// Split out from [`dispatch_for_identity`] so this synchronous
/// authorization step — the part that must never let the task's database and
/// `$auth.database_id` diverge — is directly unit-testable without spinning
/// up the Data Plane dispatch machinery.
///
/// `database_id` flows through [`RequestAuthScope::builder`] as the session
/// database rather than being used directly for `PhysicalTask::database_id`
/// while `$auth.database_id` is resolved separately from `identity` — that
/// split was the defect this function exists to close. `scope.database_id()`
/// is what actually lands on the task, so the two provably cannot disagree.
fn authorize_for_identity(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    admission: RequestAdmission,
) -> crate::Result<AuthorizedTask> {
    let mut plan = plan;
    let scope = resolve_dispatch_scope(state, identity, database_id);

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, then rate limit — before RLS injection and task authorization,
    // so load is shed before it is spent. This door has no network peer
    // address, so the peer address used for blacklist/audit purposes is the
    // empty string; the IP blacklist check is a no-op in that case while the
    // user/org/rate-limit checks still apply in full. Skipped when the
    // caller's own transport entry already ran this gate for the request —
    // see [`RequestAdmission`] for why both cases exist.
    if admission == RequestAdmission::NotYetAdmitted {
        crate::control::server::session_auth::check_request_admission(
            state,
            &scope,
            "",
            operation_for_plan(&plan),
        )?;
    }

    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        identity.tenant_id.as_u64(),
        &mut plan,
        &state.rls,
        scope.auth(),
    )?;

    let task = PhysicalTask {
        tenant_id: identity.tenant_id,
        vshard_id: VShardId::from_collection_in_database(scope.database_id(), collection),
        database_id: scope.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned an empty capability set".into(),
    })
}

/// Resolve the request-scoped auth contract for a user-issued dispatch.
///
/// The single source both [`PhysicalTask::database_id`] (via
/// [`RequestAuthScope::database_id`]) and `$auth.database_id` (via
/// [`RequestAuthScope::auth`]) are read from — split out from
/// [`authorize_for_identity`] so that guarantee is directly unit-testable.
/// Thin wrapper over [`RequestAuthScope::for_database`] that reads
/// `scope_grants` off `state`.
/// Map a physical plan to the rate-limiter `operation` string (see
/// `control::security::ratelimit::config::default_endpoint_costs`).
///
/// This door carries only a handful of engine-specific DSL/TVF operations
/// (CRDT read/merge, timeseries last-value, GraphRAG fusion, snapshot scan),
/// so a coarse top-level match is enough to apply the right cost tier; an
/// engine with no natural cost-table counterpart falls back to the default
/// cost of 1.
fn operation_for_plan(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::Vector(_) => "vector_search",
        PhysicalPlan::Graph(_) => "graph_hop",
        PhysicalPlan::Document(_) => "document_scan",
        PhysicalPlan::Kv(_) => "kv_scan",
        PhysicalPlan::Text(_) => "text_search",
        PhysicalPlan::Columnar(_) | PhysicalPlan::Timeseries(_) | PhysicalPlan::Spatial(_) => {
            "document_scan"
        }
        PhysicalPlan::Crdt(_) => "point_get",
        PhysicalPlan::Query(_) => "aggregate",
        PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => "sql",
    }
}

fn resolve_dispatch_scope<'a>(
    state: &'a SharedState,
    identity: &'a AuthenticatedIdentity,
    database_id: DatabaseId,
) -> RequestAuthScope<'a> {
    RequestAuthScope::for_database(identity, &state.scope_grants, database_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::state::SharedState;
    use crate::types::TenantId;
    use crate::wal::WalManager;
    use nodedb_physical::physical_plan::KvOp;

    use super::*;

    fn trivial_kv_get_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: "widgets".into(),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    /// The exact regression this module exists to prevent: an identity whose
    /// session default database differs from the database the caller passed
    /// in for this dispatch. Before the `RequestAuthScope` fix, the
    /// `PhysicalTask` was built from the passed-in `database_id` while
    /// `$auth.database_id` came from `build_auth_context(identity)`, which
    /// stamps `identity.default_database` — so an RLS policy comparing
    /// `database_id = $auth.database_id` would evaluate against the wrong
    /// database. This test fails if `resolve_dispatch_scope` regresses to
    /// resolving `$auth.database_id` from `identity.default_database`
    /// instead of the passed-in `database_id`: it asserts both
    /// `scope.database_id()` (what lands on the task) and
    /// `scope.auth().database_id` (what RLS substitutes for `$auth.*`)
    /// equal the passed-in database, not the identity's default.
    #[test]
    fn scope_database_and_auth_database_track_passed_in_database_not_identity_default() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let identity_default = DatabaseId::new(7);
        let dispatch_target = DatabaseId::new(99);
        assert_ne!(identity_default, dispatch_target);

        let mut identity = AuthenticatedIdentity::new_regular(
            1,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        identity.default_database = Some(identity_default);

        let scope = resolve_dispatch_scope(&state, &identity, dispatch_target);

        assert_eq!(scope.database_id(), dispatch_target);
        assert_eq!(scope.auth().database_id, Some(dispatch_target));
    }

    /// End-to-end sanity check that the resolved scope's database is what
    /// actually lands on the authorized `PhysicalTask`, using the same
    /// mismatched-identity setup as the test above.
    #[test]
    fn authorized_task_database_matches_passed_in_database_not_identity_default() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let identity_default = DatabaseId::new(7);
        let dispatch_target = DatabaseId::new(99);

        let mut identity = AuthenticatedIdentity::new_regular(
            1,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        identity.default_database = Some(identity_default);

        let plan = trivial_kv_get_plan();

        let authorized = authorize_for_identity(
            &state,
            &identity,
            dispatch_target,
            "widgets",
            plan,
            RequestAdmission::NotYetAdmitted,
        )
        .expect("authorize task for identity");

        assert_eq!(authorized.database_id(), dispatch_target);
    }

    /// The regression this module exists to prevent going forward: a caller
    /// that has already run the transport's own admission gate must not be
    /// charged against the rate-limit budget a second time here. Two calls
    /// with `AlreadyAdmitted` must both succeed with no consumed budget,
    /// which `NotYetAdmitted` would eventually reject once the budget is
    /// exhausted — this test only needs to prove `AlreadyAdmitted` never
    /// touches the limiter at all, so a large repeat count would still pass
    /// even if a future regression re-added the check, making a direct
    /// "did it run" assertion the only way to catch a re-added call. Since
    /// `check_request_admission` has no test-visible counter, this instead
    /// pins the observable contract: `AlreadyAdmitted` runs no blacklist
    /// check, so a blacklisted identity is still authorized when the caller
    /// asserts it already admitted the request — the exact bypass a re-added
    /// call would break.
    #[test]
    fn already_admitted_skips_the_gate_even_for_a_blacklisted_identity() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let identity = AuthenticatedIdentity::new_regular(
            2,
            "blocked-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        // `NotYetAdmitted` still enforces the gate: the blacklist rejects it.
        let denied = authorize_for_identity(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "widgets",
            trivial_kv_get_plan(),
            RequestAdmission::NotYetAdmitted,
        );
        assert!(
            denied.is_err(),
            "NotYetAdmitted must still run the full gate and reject a blacklisted identity"
        );

        // `AlreadyAdmitted` skips it: the same blacklisted identity is
        // authorized, because the caller's own transport entry already
        // admitted (or would have rejected) this request.
        let allowed = authorize_for_identity(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "widgets",
            trivial_kv_get_plan(),
            RequestAdmission::AlreadyAdmitted,
        );
        assert!(
            allowed.is_ok(),
            "AlreadyAdmitted must skip the gate so an already-admitted request is not \
             double-charged or re-evaluated"
        );
    }
}
