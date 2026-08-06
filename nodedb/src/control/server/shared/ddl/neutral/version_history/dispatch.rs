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
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::server::shared::ddl::sync_dispatch::dispatch_authorized;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};

use super::super::super::result::DdlError;

/// Authorize `plan` for `identity` and dispatch it to the Data Plane.
///
/// The task set is authorized as a whole (`authorize_task_set` resolves the
/// plan's own collection requirements, exactly as the planner-driven read path
/// does) and the resulting capability is consumed by the dispatch, so no plan
/// other than the authorized one can reach storage.
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
    let authorized = authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|error| DdlError {
        sqlstate: "42501".to_string(),
        message: format!("permission denied: {}", error.resource()),
    })?;

    let Some(authorized_task) = authorized.into_tasks().pop() else {
        return Err(DdlError {
            sqlstate: "XX000".to_string(),
            message: "version-history read lost its authorized task".to_string(),
        });
    };

    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    dispatch_authorized(state, authorized_task, collection, timeout)
        .await
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("dispatch: {e}"),
        })
}
