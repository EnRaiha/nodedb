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
) -> crate::Result<Vec<u8>> {
    let authorized = authorize_for_identity(state, identity, database_id, collection, plan)?;
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
) -> crate::Result<AuthorizedTask> {
    let mut plan = plan;
    let scope = resolve_dispatch_scope(state, identity, database_id);

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

        let authorized =
            authorize_for_identity(&state, &identity, dispatch_target, "widgets", plan)
                .expect("authorize task for identity");

        assert_eq!(authorized.database_id(), dispatch_target);
    }
}
