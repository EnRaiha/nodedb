// SPDX-License-Identifier: BUSL-1.1

//! The authorized Data-Plane door for version-history reads.
//!
//! `SELECT … AT VERSION` and `SELECT DIFF(…)` are user SQL that returns stored
//! document content — a historical merged state, and the oplog deltas a state
//! was built from. Both once reached storage through `dispatch_system`, the
//! door reserved for work the server starts on its own schedule, which performs
//! no authorization because there is no user behind it. There is a user behind
//! these, so they mint a capability instead: the plan that reaches storage is
//! the plan authorization approved.

use std::time::Duration;

use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::server::shared::ddl::sync_dispatch::dispatch_authorized;
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};

use super::super::super::result::DdlError;

/// Authorize `plan` for `identity` and dispatch it to the Data Plane.
///
/// The capability is minted by `intercept_and_authorize`, which clone-checks
/// before it authorizes (`authorize_task_set` resolves the plan's own
/// collection requirements, exactly as the planner-driven read path does) and
/// is consumed by the dispatch, so no plan other than the authorized one can
/// reach storage. This is a READ door, so the clone-read hook it inherits is
/// what keeps a history read against a `Shadowed` clone from answering out of
/// the target alone, which holds post-clone writes only.
pub(super) async fn dispatch_authorized_read(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
) -> Result<Vec<u8>, DdlError> {
    let task = PhysicalTask {
        tenant_id: identity.tenant_id,
        database_id,
        vshard_id: VShardId::from_collection_in_database(database_id, collection),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let audit =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    let outcome = intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task,
        identity,
        tenant_id: identity.tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &audit,
    })
    .await
    .map_err(gate_error)?;

    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    match outcome {
        // The clone-read hook merged the target with its source and produced the
        // answer itself; the payload is flattened the same way the Data Plane's
        // own response is, so the two are indistinguishable to the caller.
        CloneCheckedOutcome::Handled(response) => {
            payload_or_typed_error(response).map_err(|e| DdlError::new("XX000", format!("{e}")))
        }
        CloneCheckedOutcome::Proceed(checked) => {
            dispatch_authorized(state, checked, collection, timeout)
                .await
                .map_err(|e| DdlError::new("XX000", format!("dispatch: {e}")))
        }
    }
}

/// Render a gate failure as a `DdlError`.
///
/// An authorization denial keeps the `42501` it had when this door called
/// `authorize_task_set` directly — the gate wraps the same `AuthorizationError`
/// into [`crate::Error::RejectedAuthz`], so the client-visible SQLSTATE stays
/// the same with clone interception running ahead of it. Everything else the gate
/// can raise (a clone read shape with no sound rewrite, a catalog read failure)
/// is an internal-error class the client cannot act on by SQLSTATE alone, so it
/// carries its own message under `XX000`. Both SQLSTATEs have exactly one
/// `ErrorCode` meaning, so `DdlError::new` derives the right code for each.
fn gate_error(error: crate::Error) -> DdlError {
    match error {
        crate::Error::RejectedAuthz { resource, .. } => {
            DdlError::new("42501", format!("permission denied: {resource}"))
        }
        other => DdlError::new("XX000", format!("version-history read: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::CrdtOp;
    use nodedb_types::{CloneOrigin, CloneStatus, Lsn, TenantId};

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    use super::*;

    const CLONE: &str = "widgets";
    const SOURCE: &str = "widgets_source";

    /// State whose catalog holds `CLONE` as a `Shadowed` clone of `SOURCE`.
    ///
    /// `clone_created_at` is `ZERO` so any query LSN the resolver derives is at
    /// or after it — otherwise the read would resolve as pre-dating the clone
    /// and never reach the rewrite this test is about.
    fn shadowed_clone_fixture(tenant_id: TenantId) -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let mut clone = StoredCollection::new(tenant_id.as_u64(), CLONE, "owner");
        clone.cloned_from = Some(CloneOrigin {
            source_database: DatabaseId::DEFAULT,
            source_collection: SOURCE.to_string(),
            as_of_lsn: Lsn::new(u64::MAX),
            clone_created_at: Lsn::ZERO,
            kv_surrogate_ceiling: None,
        });
        clone.clone_status = CloneStatus::Shadowed;
        state
            .credentials
            .catalog()
            .put_collection(DatabaseId::DEFAULT, &clone)
            .expect("store the shadowed clone descriptor");

        (state, dir)
    }

    /// The door reaches the clone-read hook.
    ///
    /// A CRDT document read over a `Shadowed` clone has no sound source-side
    /// rewrite, so the hook refuses it by name rather than answering out of the
    /// target — which holds post-clone writes alone. Before this door consumed
    /// a clone-checked capability it ran no clone interception at all, so the
    /// same call dispatched straight to the Data Plane and this refusal never
    /// appeared. No fake responder is registered: reaching a dispatch at all
    /// would fail the assertion below.
    #[tokio::test]
    async fn a_shadowed_clone_read_is_refused_by_the_clone_read_hook() {
        let tenant_id = TenantId::new(1);
        let (state, _dir) = shadowed_clone_fixture(tenant_id);
        let identity = AuthenticatedIdentity::new_regular(
            1,
            "alice",
            tenant_id,
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );

        let plan = PhysicalPlan::Crdt(CrdtOp::Read {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, CLONE),
            document_id: "doc-1".to_string(),
        });
        let error = dispatch_authorized_read(&state, &identity, DatabaseId::DEFAULT, CLONE, plan)
            .await
            .expect_err("a shadowed-clone read with no source rewrite must be refused");

        assert!(
            error
                .message
                .contains("cannot be read through an unmaterialized clone"),
            "the refusal must come from the clone-read hook, got: {}",
            error.message
        );
    }
}
